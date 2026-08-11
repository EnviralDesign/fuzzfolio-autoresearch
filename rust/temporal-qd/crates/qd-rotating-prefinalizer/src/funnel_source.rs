//! Native v5 proposal-funnel source assembly.
//!
//! The construction transaction owns proposal attempts and their construction
//! stage transcript.  The directional v4 tail seal owns post-evaluation
//! window facts.  This module is deliberately the only join between them: it
//! never opens a worker result, a v3 index, or a Python-selected candidate
//! list.  Attempt input is JSONL-scanned once; the only retained state is one
//! candidate row plus its bounded current-panel windows.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, anyhow, ensure};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use temporal_qd_contract::{
    CONTRACT_VERSION, canonical_json_bytes, canonical_json_line, canonical_sha256,
    canonical_sha256_without_object_field,
};

use super::core_receipt::{load_proposal_attempt_authority, validate_g0_selected_attempt};
use super::{array, exact_keys, file_sha, member, object, sha, stream_jsonl, text, unsigned};

pub const INPUT_SCHEMA: &str = "temporal_qd_v5_native_funnel_reduction_input_v2";
pub const SOURCE_SCHEMA: &str = "temporal_qd_native_funnel_reduction_source_v1";
pub const V4_INDEX_SCHEMA: &str = "temporal_qd_tail_result_index_v4";
pub const ASSEMBLY_RECEIPT_SCHEMA: &str = "temporal_qd_v5_native_funnel_assembly_receipt_v2";
pub const ASSEMBLY_EXECUTION_SCHEMA: &str = "temporal_qd_v5_native_funnel_assembly_execution_v2";
pub const ASSEMBLY_RECEIPT_PATH: &str = "funnel-assembly-receipt.json";
pub const ASSEMBLY_RECEIPT_BINDING_SCHEMA: &str =
    "temporal_qd_v5_native_funnel_assembly_receipt_binding_v1";
const ASSEMBLY_INPUT_DESCRIPTOR_SCHEMA: &str =
    "temporal_qd_v5_native_funnel_assembly_input_descriptor_v1";
const ASSEMBLY_SOURCE_DESCRIPTOR_SCHEMA: &str = "temporal_qd_v5_native_funnel_source_descriptor_v1";
const V4_ENTRY_SCHEMA: &str = "temporal_qd_tail_result_index_entry_v4";
const TAIL_AUTHORITY_SCHEMA: &str = "temporal_qd_v5_directional_tail_authority_v1";
const CORE_ATTEMPT_ROW_SCHEMA: &str = "temporal_qd_v5_proposal_funnel_entry_v1";
const FUNNEL_STAGE_SCHEMA: &str = "temporal_qd_proposal_funnel_stage_v1";

/// Assemble the source consumed by `qd-generation-finalizer`.
///
/// `input` is self-authenticated by `inputSha256`.  The input's proposal
/// attempt stream is the full construction ordinal stream, not the
/// accepted-only publication fragment.  The v4 tail index is opened only as
/// an authenticated compact receipt and only its `funnelProjection` is used.
pub fn assemble(input: &Value) -> Result<Value> {
    validate_input(input)?;
    let generation = unsigned(input, "generationIndex")?;
    let (attempt_stream, proposal_accounting, g0_selected, _) =
        load_proposal_attempt_authority(member(input, "proposalAttemptAuthority")?)?;
    ensure!(
        unsigned(member(input, "tailAuthority")?, "generationIndex")? == generation,
        "v5 tail authority generation binding drifted"
    );
    let panel = member(input, "evaluationPanel")?;
    let expected_windows = panel_windows(panel)?;
    let tail = load_v4_funnel_rows(
        member(input, "tailAuthority")?,
        member(input, "campaignSeal")?,
        member(input, "tailResultIndex")?,
        &expected_windows,
    )?;
    let min_total = unsigned(input, "minimumTotalTrades")?;
    let min_per_window = unsigned(input, "minimumTradesPerWindow")?;

    let mut attempts = Vec::new();
    let mut attempt_ids = BTreeSet::new();
    let mut ordinals = BTreeSet::new();
    let mut candidate_rows = BTreeMap::new();
    let mut disposition_counts = BTreeMap::<String, u64>::new();
    let mut origin_counts = BTreeMap::<String, u64>::new();

    stream_jsonl(&attempt_stream, "v5 proposal attempt stream", |row| {
        let prepared = prepare_attempt(
            &row,
            generation,
            g0_selected,
            &tail,
            &expected_windows,
            min_total,
            min_per_window,
        )?;
        ensure!(
            ordinals.insert(prepared.ordinal),
            "v5 proposal attempt stream repeats proposal ordinal"
        );
        ensure!(
            attempt_ids.insert(prepared.identity.clone()),
            "v5 proposal attempt stream repeats attempt SHA"
        );
        *disposition_counts
            .entry(prepared.disposition.clone())
            .or_default() += 1;
        *origin_counts.entry(prepared.origin.clone()).or_default() += 1;
        if let Some(candidate) = prepared.candidate {
            let candidate_id = text(&candidate, "candidateId")?.to_owned();
            ensure!(
                candidate_rows.insert(candidate_id, candidate).is_none(),
                "v5 proposal attempt stream materializes candidate more than once"
            );
        }
        let mut attempt = json!({
            "proposalOrdinal": prepared.ordinal,
            "attemptIdentitySha256": prepared.identity,
            "originKind": prepared.origin,
            "disposition": prepared.disposition,
        });
        attempt
            .as_object_mut()
            .expect("attempt object")
            .extend(prepared.attempt_candidate);
        attempts.push(attempt);
        Ok(())
    })?;
    ensure!(!attempts.is_empty(), "v5 proposal attempt stream is empty");
    ensure!(
        ordinals == (0..attempts.len() as u64).collect(),
        "v5 proposal attempt stream has an ordinal gap"
    );
    validate_accounting(&proposal_accounting, &disposition_counts, &origin_counts)?;
    attempts.sort_by_key(|row| unsigned(row, "proposalOrdinal").unwrap_or(u64::MAX));

    let mut source = json!({
        "schemaVersion": SOURCE_SCHEMA,
        "preArchiveProjection": true,
        "completenessPolicy": completeness_policy(),
        "proposalAccounting": proposal_accounting,
        "proposalAttempts": attempts,
        "candidateStageRows": candidate_rows.into_values().collect::<Vec<_>>(),
        // A narrow provenance fence makes the source independently auditable
        // without changing the finalizer's legacy-compatible reduction keys.
        "v5NativeInputs": {
            "generationIndex": generation,
            "proposalAttemptStreamSha256": sha(&attempt_stream, "rawSha256")?,
            "tailResultIndexSha256": sha(member(input, "tailResultIndex")?, "rawSha256")?,
            "tailAuthoritySha256": sha(member(input, "tailAuthority")?, "tailAuthoritySha256")?,
        },
    });
    // `qd-generation-finalizer` currently requires an exact legacy source
    // shape.  Keep the provenance in the sealed input/receipt instead of
    // adding an unknown source key that it would (correctly) reject.
    source
        .as_object_mut()
        .expect("object")
        .remove("v5NativeInputs");
    add_hash(&mut source, "funnelSourceSha256")?;
    Ok(source)
}

/// Immutable publish/restart seam for the reducer.  A completed source is
/// verified from its own bytes and returned without reopening the large
/// attempt stream or v4 index.
pub fn assemble_to_path(input_path: &Path, output_path: &Path) -> Result<Value> {
    let raw = fs::read(input_path).context("read v5 funnel input")?;
    let input: Value = serde_json::from_slice(&raw).context("parse v5 funnel input")?;
    ensure!(
        canonical_json_line(&input)? == raw,
        "v5 funnel input must be canonical JSON plus LF"
    );
    validate_input(&input)?;
    if output_path.is_file() {
        let existing = fs::read(output_path).context("read published v5 funnel source")?;
        let value: Value =
            serde_json::from_slice(&existing).context("parse published v5 funnel source")?;
        ensure!(
            canonical_json_line(&value)? == existing,
            "published v5 funnel source must be canonical JSON plus LF"
        );
        ensure!(
            text(&value, "schemaVersion")? == SOURCE_SCHEMA,
            "published v5 funnel source schema drifted"
        );
        ensure!(
            canonical_sha256_without_object_field(&value, "funnelSourceSha256")?
                == sha(&value, "funnelSourceSha256")?,
            "published v5 funnel source self hash drifted"
        );
        // Historical/noncompact callers have no receipt binding the original
        // input bytes, so they must rederive on every restart. Only the
        // versioned compact seam below supports input-free restart.
        let rebuilt = assemble(&input)?;
        ensure!(
            rebuilt == value,
            "published v5 funnel source belongs to a different v2 input"
        );
        return Ok(value);
    }
    let value = assemble(&input)?;
    let bytes = canonical_json_line(&value)?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    let mut output = options
        .open(output_path)
        .context("create v5 funnel source")?;
    output.write_all(&bytes)?;
    output.sync_all()?;
    Ok(value)
}

/// Current supervisor ABI: publish the full source internally, then publish a
/// fixed, self-hashed receipt as the final durable handoff.  Stdout contains
/// only this bounded execution object.
pub fn assemble_to_path_compact(input_path: &Path, output_path: &Path) -> Result<Value> {
    let input_path = exact_existing_file(input_path, "v5 funnel input")?;
    let output_path = exact_output_path(output_path, "v5 funnel source")?;
    let receipt_path = output_path
        .parent()
        .context("v5 funnel source has no parent")?
        .join(ASSEMBLY_RECEIPT_PATH);
    if receipt_path.exists() {
        let fixed_receipt = exact_existing_file(&receipt_path, "v5 funnel assembly receipt")?;
        ensure!(
            fixed_receipt == receipt_path,
            "v5 funnel assembly receipt path is not fixed/canonical"
        );
        let receipt = read_canonical_file(&receipt_path, "v5 funnel assembly receipt")?;
        validate_assembly_receipt(&input_path, &output_path, &receipt)?;
        return Ok(json!({
            "schemaVersion":ASSEMBLY_EXECUTION_SCHEMA,
            "restart":true,
            "receipt":receipt,
        }));
    }

    let input = read_canonical_file(&input_path, "v5 funnel input")?;
    validate_input(&input)?;
    let source = assemble(&input)?;
    publish_json_once(&output_path, &source, "v5 funnel source")?;
    let receipt = expected_assembly_receipt(&input_path, &output_path, &input, &source)?;
    // Receipt-last: a source is not a supervisor-visible commit until this
    // fixed receipt exists.
    publish_json_once(&receipt_path, &receipt, "v5 funnel assembly receipt")?;
    Ok(json!({
        "schemaVersion":ASSEMBLY_EXECUTION_SCHEMA,
        "restart":false,
        "receipt":receipt,
    }))
}

/// Reopen a receipt binding carried by the prefinalizer base manifest,
/// authenticate its source bytes, then reassemble from the small inline input
/// and require byte-for-byte equality.  Python never parses either the v4
/// index or the candidate-scale source.
pub fn reopen_assembly_receipt(
    inline_input: &Value,
    binding: &Value,
    binding_root: &Path,
) -> Result<Value> {
    exact_keys(
        object(binding, "funnel assembly receipt binding")?,
        &[
            "schemaVersion",
            "path",
            "rawSha256",
            "sizeBytes",
            "receiptSha256",
        ],
        "funnel assembly receipt binding",
    )?;
    ensure!(
        text(binding, "schemaVersion")? == ASSEMBLY_RECEIPT_BINDING_SCHEMA,
        "funnel assembly receipt binding schema drifted"
    );
    let path = bound_path(
        binding_root,
        text(binding, "path")?,
        "funnel assembly receipt",
    )?;
    let raw = fs::read(&path)?;
    ensure!(
        raw.len() as u64 == unsigned(binding, "sizeBytes")?
            && raw_sha(&raw) == sha(binding, "rawSha256")?,
        "funnel assembly receipt binding bytes drifted"
    );
    let receipt = read_canonical_file(&path, "v5 funnel assembly receipt")?;
    ensure!(
        sha(&receipt, "receiptSha256")? == sha(binding, "receiptSha256")?,
        "funnel assembly receipt semantic binding drifted"
    );
    ensure!(
        sha(inline_input, "inputSha256")? == sha(member(&receipt, "input")?, "inputSha256")?,
        "inline funnel input/assembly receipt drifted"
    );
    let source_path = PathBuf::from(text(member(&receipt, "source")?, "path")?);
    let input_path = PathBuf::from(text(member(&receipt, "input")?, "path")?);
    let source = validate_assembly_receipt(&input_path, &source_path, &receipt)?;
    let rebuilt = assemble(inline_input)?;
    ensure!(
        canonical_json_line(&rebuilt)? == fs::read(&source_path)?,
        "funnel assembly source differs from inline input reassembly"
    );
    ensure!(rebuilt == source, "funnel assembly semantic source drifted");
    Ok(source)
}

fn validate_assembly_receipt(
    input_path: &Path,
    output_path: &Path,
    receipt: &Value,
) -> Result<Value> {
    exact_keys(
        object(receipt, "v5 funnel assembly receipt")?,
        &[
            "schemaVersion",
            "contractVersion",
            "generationIndex",
            "input",
            "source",
            "proposalAttemptReceiptSha256",
            "campaignSealSha256",
            "tailResultIndexSha256",
            "tailAuthoritySha256",
            "receiptSha256",
        ],
        "v5 funnel assembly receipt",
    )?;
    ensure!(
        text(receipt, "schemaVersion")? == ASSEMBLY_RECEIPT_SCHEMA
            && text(receipt, "contractVersion")? == CONTRACT_VERSION
            && canonical_sha256_without_object_field(receipt, "receiptSha256")?
                == sha(receipt, "receiptSha256")?,
        "v5 funnel assembly receipt schema/self hash drifted"
    );
    let input = read_descriptor(
        member(receipt, "input")?,
        ASSEMBLY_INPUT_DESCRIPTOR_SCHEMA,
        input_path,
        "inputSha256",
        "v5 funnel input",
    )?;
    validate_input(&input)?;
    let source = read_descriptor(
        member(receipt, "source")?,
        ASSEMBLY_SOURCE_DESCRIPTOR_SCHEMA,
        output_path,
        "funnelSourceSha256",
        "v5 funnel source",
    )?;
    ensure!(
        text(&source, "schemaVersion")? == SOURCE_SCHEMA,
        "v5 funnel source schema drifted"
    );
    let expected = expected_assembly_receipt(input_path, output_path, &input, &source)?;
    ensure!(
        receipt == &expected,
        "v5 funnel assembly receipt closure drifted"
    );
    Ok(source)
}

fn expected_assembly_receipt(
    input_path: &Path,
    output_path: &Path,
    input: &Value,
    source: &Value,
) -> Result<Value> {
    let proposal = member(input, "proposalAttemptAuthority")?;
    let seal = member(input, "campaignSeal")?;
    let mut receipt = json!({
        "schemaVersion":ASSEMBLY_RECEIPT_SCHEMA,
        "contractVersion":CONTRACT_VERSION,
        "generationIndex":unsigned(input,"generationIndex")?,
        "input":json_descriptor(
            ASSEMBLY_INPUT_DESCRIPTOR_SCHEMA,input_path,"inputSha256",sha(input,"inputSha256")?
        )?,
        "source":json_descriptor(
            ASSEMBLY_SOURCE_DESCRIPTOR_SCHEMA,output_path,"funnelSourceSha256",sha(source,"funnelSourceSha256")?
        )?,
        "proposalAttemptReceiptSha256":sha(proposal,"receiptSha256")?,
        "campaignSealSha256":sha(seal,"campaignSealSha256")?,
        "tailResultIndexSha256":sha(member(seal,"tailResultIndex")?,"sha256")?,
        "tailAuthoritySha256":sha(member(input,"tailAuthority")?,"tailAuthoritySha256")?,
    });
    add_hash(&mut receipt, "receiptSha256")?;
    Ok(receipt)
}

fn json_descriptor(
    schema: &str,
    path: &Path,
    semantic_field: &str,
    semantic_sha: &str,
) -> Result<Value> {
    let path = exact_existing_file(path, "funnel descriptor file")?;
    let raw = fs::read(&path)?;
    let mut descriptor = json!({
        "schemaVersion":schema,
        "path":path,
        "rawSha256":raw_sha(&raw),
        "sizeBytes":raw.len(),
    });
    descriptor
        .as_object_mut()
        .expect("descriptor object")
        .insert(semantic_field.to_owned(), json!(semantic_sha));
    Ok(descriptor)
}

fn read_descriptor(
    descriptor: &Value,
    schema: &str,
    expected_path: &Path,
    semantic_field: &str,
    name: &str,
) -> Result<Value> {
    exact_keys(
        object(descriptor, &format!("{name} descriptor"))?,
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
        "{name} descriptor schema drifted"
    );
    let path = exact_existing_file(
        Path::new(text(descriptor, "path")?),
        &format!("{name} descriptor file"),
    )?;
    ensure!(path == expected_path, "{name} descriptor path drifted");
    let raw = fs::read(&path)?;
    ensure!(
        raw.len() as u64 == unsigned(descriptor, "sizeBytes")?
            && raw_sha(&raw) == sha(descriptor, "rawSha256")?,
        "{name} descriptor bytes drifted"
    );
    let value = read_canonical_file(&path, name)?;
    ensure!(
        canonical_sha256_without_object_field(&value, semantic_field)?
            == sha(&value, semantic_field)?
            && sha(&value, semantic_field)? == sha(descriptor, semantic_field)?,
        "{name} descriptor semantic hash drifted"
    );
    Ok(value)
}

fn exact_existing_file(path: &Path, name: &str) -> Result<PathBuf> {
    ensure!(path.is_file(), "{name} is not a file");
    ensure_no_symlink_components(path, name)?;
    ensure!(
        !fs::symlink_metadata(path)?.file_type().is_symlink(),
        "{name} symlink is forbidden"
    );
    Ok(fs::canonicalize(path)?)
}

fn exact_output_path(path: &Path, name: &str) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    ensure!(
        !absolute
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::CurDir)),
        "{name} path contains an alias"
    );
    let parent = absolute.parent().context("output path has no parent")?;
    ensure!(parent.is_dir(), "{name} parent is not a directory");
    ensure_no_symlink_components(parent, name)?;
    let file_name = absolute
        .file_name()
        .context("output path has no file name")?;
    let absolute = fs::canonicalize(parent)?.join(file_name);
    if absolute.exists() {
        ensure!(
            absolute.is_file() && !fs::symlink_metadata(&absolute)?.file_type().is_symlink(),
            "{name} symlink or non-file is forbidden"
        );
    }
    Ok(absolute)
}

fn bound_path(root: &Path, value: &str, name: &str) -> Result<PathBuf> {
    let path = Path::new(value);
    ensure!(
        !path
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::CurDir)),
        "{name} traversal/alias is forbidden"
    );
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    exact_existing_file(&joined, name)
}

fn publish_json_once(path: &Path, value: &Value, name: &str) -> Result<()> {
    let bytes = canonical_json_line(value)?;
    if path.exists() {
        ensure!(fs::read(path)? == bytes, "published {name} differs");
        return Ok(());
    }
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}

fn read_canonical_file(path: &Path, name: &str) -> Result<Value> {
    let raw = fs::read(path)?;
    let value: Value = serde_json::from_slice(&raw).with_context(|| format!("parse {name}"))?;
    ensure!(
        canonical_json_line(&value)? == raw,
        "{name} must be canonical JSON plus LF"
    );
    Ok(value)
}

fn raw_sha(raw: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(raw))
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

struct PreparedAttempt {
    ordinal: u64,
    identity: String,
    origin: String,
    disposition: String,
    attempt_candidate: Map<String, Value>,
    candidate: Option<Value>,
}

fn prepare_attempt(
    row: &Value,
    generation: u64,
    g0_selected: bool,
    tail: &BTreeMap<String, BTreeMap<String, TailWindow>>,
    expected_windows: &BTreeMap<String, String>,
    min_total: u64,
    min_per_window: u64,
) -> Result<PreparedAttempt> {
    if g0_selected {
        return prepare_g0_selected_attempt(
            row,
            generation,
            tail,
            expected_windows,
            min_total,
            min_per_window,
        );
    }
    let map = object(row, "v5 proposal funnel attempt")?;
    let has_candidate = map.contains_key("candidate");
    let expected =
        if has_candidate && row.get("disposition").and_then(Value::as_str) == Some("accepted") {
            &[
                "schemaVersion",
                "entrySha256",
                "proposalOrdinal",
                "originKind",
                "disposition",
                "candidate",
                "proposal",
                "funnelCandidate",
                "acceptedCompactRecordSha256",
            ] as &[_]
        } else if has_candidate {
            &[
                "schemaVersion",
                "entrySha256",
                "proposalOrdinal",
                "originKind",
                "disposition",
                "candidate",
                "proposal",
                "funnelCandidate",
            ] as &[_]
        } else {
            &[
                "schemaVersion",
                "entrySha256",
                "proposalOrdinal",
                "originKind",
                "disposition",
            ] as &[_]
        };
    exact_keys(map, expected, "v5 proposal funnel attempt")?;
    ensure!(
        text(row, "schemaVersion")? == CORE_ATTEMPT_ROW_SCHEMA,
        "unsupported/non-native v5 proposal funnel attempt schema"
    );
    let ordinal = unsigned(row, "proposalOrdinal")?;
    let identity = sha(row, "entrySha256")?.to_owned();
    let origin = text(row, "originKind")?.to_owned();
    ensure!(
        matches!(origin.as_str(), "random_immigrant" | "structural_offspring"),
        "v5 funnel origin is invalid"
    );
    let disposition = text(row, "disposition")?.to_owned();
    ensure!(
        matches!(disposition.as_str(), "accepted" | "no_op" | "rejected"),
        "v5 funnel disposition is invalid"
    );
    if !has_candidate {
        ensure!(
            disposition != "accepted",
            "candidate-free v5 attempt cannot be accepted"
        );
        return Ok(PreparedAttempt {
            ordinal,
            identity,
            origin,
            disposition,
            attempt_candidate: Map::new(),
            candidate: None,
        });
    }

    let candidate = member(row, "candidate")?;
    let proposal = member(row, "proposal")?;
    let stage = member(row, "funnelCandidate")?;
    let candidate_id = text(candidate, "candidateId")?.to_owned();
    let raw = sha(candidate, "sourceProfileSha256")?.to_owned();
    ensure!(
        text(proposal, "candidateId")? == candidate_id
            && sha(proposal, "rawSourceProfileSha256")? == raw,
        "v5 attempt candidate/proposal binding drifted"
    );
    validate_stage(stage, &candidate_id, &raw, false)?;
    let admission = member(stage, "admission")?;
    let admission_outcome = text(admission, "outcome")?;
    if disposition == "accepted" {
        ensure!(
            admission_outcome == "admitted",
            "accepted v5 attempt lacks admitted stage"
        );
        sha(row, "acceptedCompactRecordSha256")?;
    } else {
        ensure!(
            admission_outcome == "rejected_duplicate",
            "candidate-bearing rejected v5 attempt must be duplicate"
        );
    }
    let attempt_candidate = Map::from_iter([
        ("candidateId".into(), json!(candidate_id)),
        ("rawSourceProfileSha256".into(), json!(raw)),
    ]);
    let candidate = stage_row(
        stage,
        generation,
        tail.get(text(candidate, "candidateId")?),
        expected_windows,
        min_total,
        min_per_window,
    )?;
    Ok(PreparedAttempt {
        ordinal,
        identity,
        origin,
        disposition,
        attempt_candidate,
        candidate: Some(candidate),
    })
}

fn prepare_g0_selected_attempt(
    row: &Value,
    generation: u64,
    tail: &BTreeMap<String, BTreeMap<String, TailWindow>>,
    expected_windows: &BTreeMap<String, String>,
    min_total: u64,
    min_per_window: u64,
) -> Result<PreparedAttempt> {
    let ordinal = unsigned(row, "proposalOrdinal")?;
    validate_g0_selected_attempt(row, ordinal)?;
    let construction = member(row, "constructionAttempt")?;
    let candidate = member(construction, "candidate")?;
    let proposal = member(construction, "proposal")?;
    let stage = member(construction, "funnelCandidate")?;
    let candidate_id = text(candidate, "candidateId")?.to_owned();
    let raw = sha(candidate, "sourceProfileSha256")?.to_owned();
    ensure!(
        text(proposal, "candidateId")? == candidate_id
            && sha(proposal, "rawSourceProfileSha256")? == raw
            && text(construction, "disposition")? == "accepted",
        "G0 selected proposal construction binding drifted"
    );
    // G0 is allowed to carry null canonical evidence only because the
    // validated wrapper contains the bootstrap v2 proof.  This is explicitly
    // authority-based rather than a generation-number shortcut.
    validate_stage(stage, &candidate_id, &raw, true)?;
    let attempt_candidate = Map::from_iter([
        ("candidateId".into(), json!(candidate_id)),
        ("rawSourceProfileSha256".into(), json!(raw)),
    ]);
    let candidate = stage_row(
        stage,
        generation,
        tail.get(text(candidate, "candidateId")?),
        expected_windows,
        min_total,
        min_per_window,
    )?;
    Ok(PreparedAttempt {
        ordinal,
        identity: sha(construction, "entrySha256")?.to_owned(),
        origin: text(construction, "originKind")?.to_owned(),
        disposition: "accepted".to_owned(),
        attempt_candidate,
        candidate: Some(candidate),
    })
}

fn stage_row(
    stage: &Value,
    _generation: u64,
    tail: Option<&BTreeMap<String, TailWindow>>,
    expected: &BTreeMap<String, String>,
    min_total: u64,
    min_per_window: u64,
) -> Result<Value> {
    let candidate_id = text(stage, "candidateId")?;
    let raw = sha(stage, "rawSourceProfileSha256")?;
    let static_row = member(stage, "staticReachability")?;
    let native_row = member(stage, "nativeValidation")?;
    let admission = member(stage, "admission")?;
    let mut identity = json!({
        "candidateId":candidate_id,
        "rawSourceProfileSha256":raw,
        // The Python generic reducer normalizes unavailable identities to
        // explicit nulls. Preserve those exact bytes; the finalizer will bind
        // archiveMemberIdentitySha256 after its archive projection.
        "validationIdentitySha256": Value::Null,
        "archiveMemberIdentitySha256": Value::Null,
    });
    if text(native_row, "outcome")? == "valid" {
        for field in [
            "resolvedProfileSha256",
            "programSha256",
            "validationReportSha256",
        ] {
            identity
                .as_object_mut()
                .unwrap()
                .insert(field.into(), json!(sha(native_row, field)?));
        }
    }
    if !member(admission, "canonicalEvidenceIdentitySha256")?.is_null() {
        identity.as_object_mut().unwrap().insert(
            "canonicalEvidenceIdentitySha256".into(),
            json!(sha(admission, "canonicalEvidenceIdentitySha256")?),
        );
    }
    let mut stages = Map::new();
    stages.insert("proposed".into(), stage_outcome("proposed", &[]));
    stages.insert(
        "staticallyReachable".into(),
        stage_outcome(text(static_row, "outcome")?, &reasons(static_row)?),
    );
    let mut terminal = None;
    if text(static_row, "outcome")? == "rejected" {
        terminal = Some("static_reachability_rejected".to_owned());
    }
    if terminal.is_none() {
        stages.insert(
            "nativeValid".into(),
            stage_outcome(text(native_row, "outcome")?, &reasons(native_row)?),
        );
        if text(native_row, "outcome")? == "rejected" {
            terminal = Some("native_validation_rejected".to_owned());
        }
    }
    if terminal.is_none() {
        stages.insert(
            "uniqueAdmitted".into(),
            stage_outcome(text(admission, "outcome")?, &reasons(admission)?),
        );
        if text(admission, "outcome")? == "rejected_duplicate" {
            terminal = Some("duplicate_rejected".to_owned());
        }
    }
    if terminal.is_none() {
        stages.insert("syntheticEvidence".into(), stage_outcome("unmeasured", &[]));
        let tail = tail.context("admitted v5 candidate lacks v4 tail receipt coverage")?;
        let observed = tail.keys().cloned().collect::<Vec<_>>();
        ensure!(
            observed.iter().all(|id| expected.contains_key(id)),
            "v4 tail contains a window outside evaluation panel"
        );
        let outcome = if observed.len() == expected.len() {
            "evaluated"
        } else if observed.is_empty() {
            "rejected"
        } else {
            "partial"
        };
        let mut evaluated = stage_outcome(outcome, &[]);
        evaluated.as_object_mut().unwrap().insert(
            "expectedWindowIds".into(),
            json!(expected.keys().collect::<Vec<_>>()),
        );
        evaluated
            .as_object_mut()
            .unwrap()
            .insert("observedWindowIds".into(), json!(observed));
        stages.insert("evaluated".into(), evaluated);
        if outcome != "evaluated" {
            terminal = Some(format!("evaluation_{outcome}"));
        } else {
            let (quality, quality_reasons) = quality(tail, min_total, min_per_window)?;
            let activation_outcome = if quality == "eligible" {
                "recorded"
            } else {
                "quality_rejected"
            };
            let mut activation = stage_outcome(activation_outcome, &quality_reasons);
            activation
                .as_object_mut()
                .unwrap()
                .insert("qualityDisposition".into(), json!(quality));
            stages.insert("activationQuality".into(), activation);
            if activation_outcome != "recorded" {
                terminal = Some(format!("activation_{activation_outcome}"));
            }
            if activation_outcome == "recorded" {
                // This is the exact empty-archive projection produced by the
                // Python oracle. The finalizer removes/replaces it after its
                // immutable archive selection.
                stages.insert(
                    "archiveRetention".into(),
                    stage_outcome("not_retained", &[]),
                );
            }
        }
    }
    let terminal = terminal.unwrap_or_else(|| "pre_archive_pending_retention".to_owned());
    // The finalizer's pre-archive pass expects selected candidates to have a
    // terminal which it can replace.  `not_retained` is the truthful empty
    // archive projection and is then deterministically rewritten if retained.
    let terminal = if terminal == "pre_archive_pending_retention" {
        "not_retained".to_owned()
    } else {
        terminal
    };
    Ok(json!({
        "candidateId": candidate_id,
        "identity": identity,
        "operatorIds": ["unclassified"],
        "motifIds": ["unclassified"],
        "direction": "unclassified",
        "stages": stages,
        "terminalDisposition": terminal,
    }))
}

fn stage_outcome(outcome: &str, reasons: &[String]) -> Value {
    json!({"outcome":outcome,"reasons":reasons})
}

fn reasons(value: &Value) -> Result<Vec<String>> {
    let mut output = array(value, "reasons")?
        .iter()
        .map(|reason| {
            reason
                .as_str()
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| anyhow!("v5 funnel stage reason must be a nonempty string"))
        })
        .collect::<Result<Vec<_>>>()?;
    output.sort();
    output.dedup();
    Ok(output)
}

#[derive(Clone)]
struct TailWindow {
    behavior: Value,
    terminal_net: Option<f64>,
    terminal_drawdown: Option<f64>,
}

fn load_v4_funnel_rows(
    authority: &Value,
    campaign_seal: &Value,
    descriptor: &Value,
    expected_windows: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, BTreeMap<String, TailWindow>>> {
    validate_authority(authority)?;
    validate_campaign_seal(campaign_seal)?;
    let map = object(descriptor, "v4 tail descriptor")?;
    exact_keys(
        map,
        &["path", "rawSha256", "sizeBytes"],
        "v4 tail descriptor",
    )?;
    let path = Path::new(text(descriptor, "path")?);
    ensure!(
        path.is_absolute()
            && !path
                .components()
                .any(|part| matches!(part, Component::ParentDir | Component::CurDir)),
        "v4 tail result index path is not canonical absolute"
    );
    let path = exact_existing_file(path, "v4 tail result index")?;
    ensure!(
        fs::metadata(&path)?.len() == unsigned(descriptor, "sizeBytes")?,
        "v4 tail index size drifted"
    );
    ensure!(
        file_sha(&path)? == sha(descriptor, "rawSha256")?,
        "v4 tail index raw identity drifted"
    );
    let raw = fs::read(path)?;
    let index: Value = serde_json::from_slice(&raw).context("parse v4 tail index")?;
    // v4 deliberately uses canonical JSON *without* a terminal LF.  Treating
    // it as a JSONL artifact would silently route the live branch back through
    // the older Python/v3 shape.
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
        member(&index, "funnelProjectionIncluded")? == &Value::Bool(true),
        "v4 tail index lacks funnel projection"
    );
    ensure!(
        sha(&index, "tailResultIndexSha256")?
            == sha(member(campaign_seal, "tailResultIndex")?, "sha256")?,
        "v5 campaign seal/index semantic binding drifted"
    );
    ensure!(
        unsigned(&index, "taskCount")? == array(&index, "entries")?.len() as u64,
        "v4 tail index accounting drifted"
    );
    let mut output = BTreeMap::<String, BTreeMap<String, TailWindow>>::new();
    for entry in array(&index, "entries")? {
        let entry_map = object(entry, "v4 tail entry")?;
        let rejected = entry_map.contains_key("rejection");
        let fields = if rejected {
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
        exact_keys(entry_map, fields, "v4 tail entry")?;
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
        let candidate_id = text(task, "candidateId")?.to_owned();
        let window_id = expected_windows
            .iter()
            .find_map(|(id, bounds)| {
                (bounds
                    == &format!(
                        "{}/{}",
                        text(task, "analysisWindowStart").unwrap_or(""),
                        text(task, "analysisWindowEnd").unwrap_or("")
                    ))
                    .then(|| id.clone())
            })
            .context("v4 tail task is outside evaluation panel")?;
        if rejected {
            output.entry(candidate_id).or_default();
            continue;
        }
        let funnel = member(entry, "funnelProjection")?;
        let mut behavior = member(funnel, "resultBehavior")?.clone();
        behavior
            .as_object_mut()
            .context("v4 funnel result behavior must be object")?
            .insert("windowId".into(), json!(window_id.clone()));
        ensure!(
            sha(&behavior, "resultSha256")? == sha(member(entry, "rawResultRef")?, "resultSha256")?,
            "v4 funnel result binding drifted"
        );
        // The Python oracle reports missing/non-finite economics as a
        // candidate-level quality rejection, rather than declaring the raw
        // v4 receipt malformed. Preserve that distinction exactly.
        let terminal_net = optional_finite_number(funnel, "terminalAdjustedConservativeNetR")?;
        let terminal_drawdown = optional_finite_number(funnel, "terminalAdjustedMaxDrawdownR")?;
        ensure!(
            output
                .entry(candidate_id)
                .or_default()
                .insert(
                    window_id,
                    TailWindow {
                        behavior,
                        terminal_net,
                        terminal_drawdown,
                    }
                )
                .is_none(),
            "v4 tail repeats candidate/window"
        );
    }
    Ok(output)
}

fn validate_input(value: &Value) -> Result<()> {
    let map = object(value, "v5 native funnel input")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "contractVersion",
            "generationIndex",
            "proposalAttemptAuthority",
            "evaluationPanel",
            "tailAuthority",
            "campaignSeal",
            "tailResultIndex",
            "minimumTotalTrades",
            "minimumTradesPerWindow",
            "inputSha256",
        ],
        "v5 native funnel input",
    )?;
    ensure!(
        text(value, "schemaVersion")? == INPUT_SCHEMA
            && text(value, "contractVersion")? == CONTRACT_VERSION,
        "v5 native funnel input schema/version is invalid"
    );
    ensure!(
        unsigned(value, "generationIndex")? > 0,
        "v5 funnel generation must be positive"
    );
    ensure!(
        canonical_sha256_without_object_field(value, "inputSha256")? == sha(value, "inputSha256")?,
        "v5 native funnel input self hash drifted"
    );
    Ok(())
}

fn validate_authority(value: &Value) -> Result<()> {
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
        text(value, "schemaVersion")? == TAIL_AUTHORITY_SCHEMA
            && text(value, "tailResultIndexSchema")? == V4_INDEX_SCHEMA
            && text(value, "tailResultEntrySchema")? == V4_ENTRY_SCHEMA,
        "v5 tail authority is invalid"
    );
    ensure!(
        canonical_sha256_without_object_field(value, "tailAuthoritySha256")?
            == sha(value, "tailAuthoritySha256")?,
        "v5 tail authority self hash drifted"
    );
    Ok(())
}

fn validate_campaign_seal(value: &Value) -> Result<()> {
    let map = object(value, "v5 campaign seal")?;
    ensure!(
        text(value, "schemaVersion")? == "temporal_qd_campaign_seal_v1",
        "v5 funnel requires a native campaign seal"
    );
    ensure!(
        canonical_sha256_without_object_field(value, "campaignSealSha256")?
            == sha(value, "campaignSealSha256")?,
        "v5 campaign seal self hash drifted"
    );
    let index = member(value, "tailResultIndex")?;
    // The pathname on a campaign seal is deliberately relative to its own
    // root. The prefinalizer's reopenable descriptor supplies the absolute
    // safe path and byte identity, so never compare/resolve the two paths.
    ensure!(
        text(index, "path")? == "tail-result-index-v4.json",
        "v5 campaign seal index path is not v4"
    );
    let _ = map;
    Ok(())
}

fn panel_windows(panel: &Value) -> Result<BTreeMap<String, String>> {
    let mut output = BTreeMap::new();
    for window in array(panel, "windows")? {
        let id = text(window, "windowId")?.to_owned();
        let bounds = format!(
            "{}/{}",
            text(window, "analysisWindowStart")?,
            text(window, "analysisWindowEnd")?
        );
        ensure!(
            output.insert(id, bounds).is_none(),
            "evaluation panel repeats window id"
        );
    }
    ensure!(!output.is_empty(), "evaluation panel has no windows");
    Ok(output)
}

fn validate_stage(
    value: &Value,
    candidate_id: &str,
    raw: &str,
    allow_g0_bootstrap_null_evidence: bool,
) -> Result<()> {
    let map = object(value, "v5 candidate funnel stage")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "candidateId",
            "rawSourceProfileSha256",
            "staticReachability",
            "nativeValidation",
            "admission",
        ],
        "v5 candidate funnel stage",
    )?;
    ensure!(
        text(value, "schemaVersion")? == FUNNEL_STAGE_SCHEMA
            && text(value, "candidateId")? == candidate_id
            && sha(value, "rawSourceProfileSha256")? == raw,
        "v5 candidate funnel stage identity drifted"
    );
    let static_row = member(value, "staticReachability")?;
    ensure!(
        matches!(text(static_row, "outcome")?, "reachable" | "rejected"),
        "v5 static reachability outcome is invalid"
    );
    let native = member(value, "nativeValidation")?;
    ensure!(
        matches!(text(native, "outcome")?, "valid" | "rejected"),
        "v5 native validation outcome is invalid"
    );
    if text(native, "outcome")? == "valid" {
        for key in [
            "resolvedProfileSha256",
            "programSha256",
            "validationReportSha256",
        ] {
            sha(native, key)?;
        }
    }
    let admission = member(value, "admission")?;
    ensure!(
        matches!(
            text(admission, "outcome")?,
            "admitted" | "rejected_duplicate"
        ),
        "v5 admission outcome is invalid"
    );
    if text(admission, "outcome")? == "admitted" {
        let evidence = member(admission, "canonicalEvidenceIdentitySha256")?;
        if evidence.is_null() {
            ensure!(
                allow_g0_bootstrap_null_evidence,
                "admitted v5 candidate lacks canonical evidence authority"
            );
        } else {
            sha(admission, "canonicalEvidenceIdentitySha256")?;
        }
    }
    Ok(())
}

fn quality(
    tail: &BTreeMap<String, TailWindow>,
    min_total: u64,
    min_per_window: u64,
) -> Result<(String, Vec<String>)> {
    let mut trades = 0u64;
    let mut reasons = Vec::new();
    let mut worst = f64::INFINITY;
    let mut finite_economics = true;
    for window in tail.values() {
        let closed = unsigned(&window.behavior, "tradeCloseCount")?;
        trades = trades
            .checked_add(closed)
            .context("v5 funnel trade count overflow")?;
        if closed < min_per_window && !reasons.contains(&"minimum_trades_per_window".to_owned()) {
            reasons.push("minimum_trades_per_window".to_owned());
        }
        match (window.terminal_net, window.terminal_drawdown) {
            (Some(net), Some(_)) => worst = worst.min(net),
            _ => finite_economics = false,
        }
    }
    if trades < min_total {
        reasons.push("minimum_total_trades".to_owned());
    }
    if !finite_economics {
        reasons.push("finite_economics".to_owned());
    }
    if worst.is_finite() && worst < 0.0 {
        reasons.push("nonnegative_worst_window_conservative_net_r".to_owned());
    }
    Ok((
        if reasons.is_empty() {
            "eligible"
        } else {
            "not_eligible"
        }
        .to_owned(),
        reasons,
    ))
}

fn optional_finite_number(value: &Value, field: &str) -> Result<Option<f64>> {
    let value = member(value, field)?;
    if value.is_null() {
        return Ok(None);
    }
    let number = value
        .as_f64()
        .ok_or_else(|| anyhow!("{field} must be numeric or null"))?;
    Ok(number.is_finite().then_some(number))
}

fn validate_accounting(
    value: &Value,
    dispositions: &BTreeMap<String, u64>,
    origins: &BTreeMap<String, u64>,
) -> Result<()> {
    ensure!(
        count_map(member(value, "dispositionCounts")?)? == *dispositions,
        "v5 funnel proposal disposition accounting mismatch"
    );
    ensure!(
        count_map(member(value, "originProposalCounts")?)? == *origins,
        "v5 funnel proposal origin accounting mismatch"
    );
    Ok(())
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

fn completeness_policy() -> Value {
    json!({
        "schemaVersion":"temporal_generation_funnel_completeness_v1",
        "policyName":"strict_identity_bound_generation_funnel",
        "proposal":"every source row must have a unique candidateId and raw source SHA",
        "staticReachability":"every proposal must have one explicit disposition",
        "nativeValidation":"statically reachable candidates require one explicit disposition",
        "uniqueAdmission":"native-valid candidates require one explicit disposition",
        "syntheticEvidence":{"required":false,"missingDisposition":"unmeasured","rule":"absence is reported as unmeasured and never inferred from evaluation"},
        "evaluation":"uniquely admitted candidates require an explicit plan/disposition",
        "activationQuality":"evaluated candidates require one explicit activation/quality record",
        "archiveRetention":"quality-recorded candidates require one explicit retained/not_retained decision; quality-rejected candidates may record only non-promotable scheduled negative-novelty exploration retention",
        "identity":"records may omit unavailable fields but may never contradict an available binding",
        "attemptLedger":"every generation attempt is recorded independently; attempts without a materialized candidate remain attempt-only"
    })
}

fn add_hash(value: &mut Value, field: &str) -> Result<()> {
    let digest = canonical_sha256(value)?;
    value
        .as_object_mut()
        .ok_or_else(|| anyhow!("self-hashed value must be object"))?
        .insert(field.to_owned(), json!(digest));
    Ok(())
}
