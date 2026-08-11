//! Adapter from Core's v2 public evaluation artifact to the reducer JSONL.
//!
//! Core owns the complete ordinal transcript.  This module does not infer
//! missing attempts from the accepted population: it authenticates the v2
//! fragment receipt against the public `evaluation-population.json` bytes and
//! copies its `funnelEntries` in their stored order into immutable JSONL.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use temporal_qd_contract::{
    CONTRACT_VERSION, canonical_json_bytes, canonical_json_line, canonical_sha256,
    canonical_sha256_without_object_field,
};
use temporal_qd_kernel::{
    v5::V5SelectedProjection,
    v5_g0_funnel::{
        V5G0FunnelFragmentReceiptObjectBinding, V5G0FunnelFragments,
        V5G0FunnelProjectionStreamReceiptObjectBinding, verify_v5_g0_funnel_projection_stream,
    },
    v5_transaction::V5SelectedProjectionIndex,
};

use super::{array, exact_keys, file_sha, member, object, sha, text, unsigned};

pub const INPUT_SCHEMA: &str = "temporal_qd_v5_core_funnel_receipt_adapter_input_v1";
pub const ATTEMPT_RECEIPT_SCHEMA: &str = "temporal_qd_v5_proposal_attempt_adapter_receipt_v1";
pub const EVOLVED_CHAIN_INPUT_SCHEMA: &str =
    "temporal_qd_v5_evolved_attempt_adapter_chain_input_v1";
const FRAGMENTS_SCHEMA: &str = "temporal_qd_v5_evolved_publication_fragments_v2";
const EVOLVED_ADAPTER_SCHEMA: &str =
    "temporal_qd_native_v5_evolved_generation_construction_adapter_v3";
const EVOLVED_INVOCATION_SCHEMA: &str = "temporal_qd_native_v5_evolved_invocation_descriptor_v1";
const INVOCATION_DOCUMENT_SCHEMA: &str = "temporal_qd_native_v5_invocation_document_descriptor_v1";
const OUTER_MANIFEST_SCHEMA: &str = "temporal_qd_native_v5_proposal_construction_manifest_v1";
const EVOLVED_RESULT_SCHEMA: &str = "temporal_qd_native_v5_evolved_construction_result_v3";
const EVOLVED_RECEIPT_SCHEMA: &str = "temporal_qd_native_v5_evolved_construction_receipt_v3";
const LEGACY_EVOLVED_RESULT_SCHEMA: &str = "temporal_qd_native_v5_evolved_construction_result_v2";
const LEGACY_EVOLVED_RECEIPT_SCHEMA: &str = "temporal_qd_native_v5_evolved_construction_receipt_v2";
const FUNNEL_FRAGMENT_KIND: &str = "evaluationFunnelEntries";
const ATTEMPT_ROW_SCHEMA: &str = "temporal_qd_v5_proposal_funnel_entry_v1";
const G0_CHAIN_INPUT_SCHEMA: &str = "temporal_qd_v5_g0_funnel_source_chain_input_v1";
const G0_ADAPTER_SCHEMA: &str = "temporal_qd_native_v5_generation_construction_adapter_v3";
const G0_INVOCATION_SCHEMA: &str = "temporal_qd_native_v5_g0_invocation_descriptor_v1";
const G0_RESULT_SCHEMA: &str = "temporal_qd_native_v5_proposal_construction_result_v5";
const G0_RECEIPT_SCHEMA: &str = "temporal_qd_native_v5_proposal_construction_receipt_v5";
const LEGACY_G0_RESULT_SCHEMA: &str = "temporal_qd_native_v5_proposal_construction_result_v4";
const LEGACY_G0_RECEIPT_SCHEMA: &str = "temporal_qd_native_v5_proposal_construction_receipt_v4";
const OUTPUT_INVENTORY_SCHEMA: &str = "temporal_qd_native_v5_proposal_output_inventory_v2";
const LEGACY_OUTPUT_INVENTORY_SCHEMA: &str = "temporal_qd_native_v5_proposal_output_inventory_v1";
const OBJECT_STORE_CLOSURE_SCHEMA: &str = "temporal_qd_native_v5_proposal_object_store_closure_v2";
const OBJECT_INVENTORY_DESCRIPTOR_SCHEMA: &str =
    "temporal_qd_native_v5_proposal_object_inventory_descriptor_v1";
const OBJECT_INVENTORY_ROW_SCHEMA: &str = "temporal_qd_native_v5_proposal_object_inventory_row_v1";
const OBJECT_INVENTORY_PATH: &str = "v5-native/object-inventory.jsonl";
pub const G0_SELECTED_ATTEMPT_RECEIPT_SCHEMA: &str =
    "temporal_qd_v5_g0_selected_attempt_stream_receipt_v1";
pub const G0_SELECTED_ATTEMPT_ROW_SCHEMA: &str = "temporal_qd_v5_g0_selected_proposal_attempt_v1";
const EVOLVED_STREAM_RECEIPT_SCHEMA: &str = "temporal_qd_v5_evolved_attempt_stream_receipt_v1";

/// Extract a content-addressed full attempt stream. Existing output is a
/// restart checkpoint and is verified without reopening the evaluation file.
pub fn extract_to_path(input_path: &Path, output_path: &Path) -> Result<Value> {
    let raw = fs::read(input_path).context("read core funnel adapter input")?;
    let input: Value = serde_json::from_slice(&raw).context("parse core funnel adapter input")?;
    ensure!(
        canonical_json_line(&input)? == raw,
        "core funnel adapter input must be canonical JSON plus LF"
    );
    validate_input(&input)?;
    if output_path.is_file() {
        return verify_descriptor(output_path);
    }
    let output = extract(&input)?;
    let bytes = output.bytes;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    let mut file = options
        .open(output_path)
        .context("create core funnel attempt stream")?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(
        json!({"path":output_path,"rawSha256":output.sha256,"sizeBytes":bytes.len(),"recordCount":output.count}),
    )
}

/// Commits the immutable adapter receipt required by the v5 prefinalizer.
/// The receipt binds the input, Core's fragment bundle, the public evaluation
/// population and the complete ordered attempt stream.  It is intentionally a
/// small descriptor, never a Python-provided list of attempts.
pub fn extract_receipt_to_path(
    input_path: &Path,
    attempts_path: &Path,
    receipt_path: &Path,
) -> Result<Value> {
    let raw = fs::read(input_path).context("read core funnel adapter input")?;
    let input: Value = serde_json::from_slice(&raw).context("parse core funnel adapter input")?;
    ensure!(
        canonical_json_line(&input)? == raw,
        "core funnel adapter input must be canonical JSON plus LF"
    );
    validate_input(&input)?;
    let stream = extract_to_path(input_path, attempts_path)?;
    let fragments = member(&input, "coreFragments")?;
    let receipt = {
        let mut value = json!({
            "schemaVersion": ATTEMPT_RECEIPT_SCHEMA,
            "contractVersion": CONTRACT_VERSION,
            "inputSha256": sha(&input, "inputSha256")?,
            "fragmentBundleSha256": sha(fragments, "fragmentBundleSha256")?,
            "evaluationPopulationSha256": sha(member(&input,"evaluationPopulation")?, "rawSha256")?,
            "attemptStream": stream,
        });
        let digest = temporal_qd_contract::canonical_sha256(&value)?;
        value
            .as_object_mut()
            .expect("object")
            .insert("receiptSha256".into(), Value::String(digest));
        value
    };
    if receipt_path.exists() {
        let existing: Value = serde_json::from_slice(&fs::read(receipt_path)?)?;
        ensure!(
            existing == receipt,
            "immutable attempt adapter receipt differs"
        );
    } else {
        let mut out = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(receipt_path)?;
        out.write_all(&canonical_json_line(&receipt)?)?;
        out.sync_all()?;
    }
    Ok(receipt)
}

pub fn validate_attempt_receipt(value: &Value) -> Result<()> {
    exact_keys(
        object(value, "attempt adapter receipt")?,
        &[
            "schemaVersion",
            "contractVersion",
            "inputSha256",
            "fragmentBundleSha256",
            "evaluationPopulationSha256",
            "attemptStream",
            "receiptSha256",
        ],
        "attempt adapter receipt",
    )?;
    ensure!(
        text(value, "schemaVersion")? == ATTEMPT_RECEIPT_SCHEMA
            && text(value, "contractVersion")? == CONTRACT_VERSION,
        "attempt adapter receipt schema/version is invalid"
    );
    ensure!(
        canonical_sha256_without_object_field(value, "receiptSha256")?
            == sha(value, "receiptSha256")?,
        "attempt adapter receipt self hash drifted"
    );
    let stream = member(value, "attemptStream")?;
    exact_keys(
        object(stream, "attempt stream descriptor")?,
        &["path", "rawSha256", "sizeBytes", "recordCount"],
        "attempt stream descriptor",
    )?;
    Ok(())
}

/// The only native-v5 evolved accessor accepted for full-attempt extraction.
/// G0 adapters and lookalike fragment descriptors are deliberately rejected.
pub fn extract_evolved_adapter_to_path(adapter_path: &Path, output_path: &Path) -> Result<Value> {
    let raw = fs::read(adapter_path).context("read evolved construction adapter")?;
    let adapter: Value =
        serde_json::from_slice(&raw).context("parse evolved construction adapter")?;
    ensure!(
        canonical_json_line(&adapter)? == raw,
        "evolved construction adapter must be canonical JSON plus LF"
    );
    let map = object(&adapter, "evolved construction adapter")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "operation",
            "completed",
            "generationKind",
            "generationIndex",
            "generationConfigSha256",
            "authoritySha256",
            "attemptCount",
            "acceptedCandidateCount",
            "selectedEvaluationCandidateCount",
            "publicationPlanSha256",
            "publicationRequestSha256",
            "proposalResultSha256",
            "proposalReceiptSha256",
            "outputInventorySha256",
            "population",
            "evaluationPopulation",
            "generationJournal",
            "identityLedger",
            "evolvedPublicationFragments",
            "nativeV5Invocation",
            "adapterSha256",
        ],
        "evolved construction adapter",
    )?;
    ensure!(
        text(&adapter, "schemaVersion")? == EVOLVED_ADAPTER_SCHEMA
            && text(&adapter, "operation")? == "native_v5_proposal_construction"
            && adapter["completed"] == Value::Bool(true)
            && text(&adapter, "generationKind")? == "evolved",
        "evolved construction adapter is incompatible"
    );
    ensure!(
        canonical_sha256_without_object_field(&adapter, "adapterSha256")?
            == sha(&adapter, "adapterSha256")?,
        "evolved construction adapter self hash drifted"
    );
    let invocation = validate_evolved_invocation(member(&adapter, "nativeV5Invocation")?)?;
    ensure!(
        sha(&adapter, "proposalResultSha256")? == invocation.result_sha256
            && sha(&adapter, "proposalReceiptSha256")? == invocation.receipt_sha256
            && sha(&adapter, "outputInventorySha256")? == invocation.inventory_sha256,
        "evolved adapter invocation root binding drifted"
    );
    let fragment = member(&adapter, "evolvedPublicationFragments")?;
    exact_keys(
        object(fragment, "evolved publication fragments descriptor")?,
        &[
            "schemaVersion",
            "coreSchemaVersion",
            "relativePath",
            "absolutePath",
            "semanticSha256",
            "fileSha256",
            "byteLength",
        ],
        "evolved publication fragments descriptor",
    )?;
    ensure!(
        text(fragment, "schemaVersion")?
            == "temporal_qd_native_v5_evolved_publication_fragments_descriptor_v1"
            && text(fragment, "coreSchemaVersion")? == FRAGMENTS_SCHEMA,
        "evolved publication fragments descriptor kind is invalid"
    );
    let path = Path::new(text(fragment, "absolutePath")?);
    ensure!(
        path.is_file() && !fs::symlink_metadata(path)?.file_type().is_symlink(),
        "evolved publication fragments path is invalid"
    );
    ensure!(
        fs::metadata(path)?.len() == unsigned(fragment, "byteLength")?
            && file_sha(path)? == sha(fragment, "fileSha256")?,
        "evolved publication fragments file binding drifted"
    );
    let fragments: Value = serde_json::from_slice(&fs::read(path)?)?;
    ensure!(
        canonical_sha256_without_object_field(&fragments, "fragmentBundleSha256")?
            == sha(&fragments, "fragmentBundleSha256")?
            && sha(&fragments, "fragmentBundleSha256")? == sha(fragment, "semanticSha256")?,
        "evolved publication fragments semantic binding drifted"
    );
    let population = member(&adapter, "evaluationPopulation")?;
    exact_keys(
        object(population, "evolved evaluation population descriptor")?,
        &[
            "relativePath",
            "absolutePath",
            "semanticSha256",
            "fileSha256",
            "byteLength",
        ],
        "evolved evaluation population descriptor",
    )?;
    let mut input = json!({"schemaVersion":INPUT_SCHEMA,"contractVersion":CONTRACT_VERSION,"coreFragments":fragments,"evaluationPopulation":{"path":text(population,"absolutePath")?,"rawSha256":sha(population,"fileSha256")?,"sizeBytes":unsigned(population,"byteLength")?}});
    let input_hash = temporal_qd_contract::canonical_sha256(&input)?;
    input
        .as_object_mut()
        .expect("object")
        .insert("inputSha256".into(), Value::String(input_hash));
    let temp = output_path.with_extension("adapter-input.json");
    if temp.exists() {
        ensure!(
            fs::read(&temp)? == canonical_json_line(&input)?,
            "evolved adapter normalized input differs"
        )
    } else {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        f.write_all(&canonical_json_line(&input)?)?;
        f.sync_all()?;
    }
    extract_to_path(&temp, output_path)
}

/// Opens an evolved adapter only through its sealed proposal manifest/result/
/// receipt chain.  A self-rehashed adapter is not an authority by itself.
pub fn extract_evolved_chain_to_path(input_path: &Path, output_path: &Path) -> Result<Value> {
    let input = read_canonical_value(input_path, "evolved adapter chain input")?;
    exact_keys(
        object(&input, "evolved adapter chain input")?,
        &[
            "schemaVersion",
            "contractVersion",
            "manifest",
            "result",
            "adapter",
            "inputSha256",
        ],
        "evolved adapter chain input",
    )?;
    ensure!(
        text(&input, "schemaVersion")? == EVOLVED_CHAIN_INPUT_SCHEMA
            && text(&input, "contractVersion")? == CONTRACT_VERSION
            && canonical_sha256_without_object_field(&input, "inputSha256")?
                == sha(&input, "inputSha256")?,
        "evolved adapter chain input is invalid"
    );
    let manifest = read_bound_json(member(&input, "manifest")?, "outer proposal manifest")?;
    let result = read_bound_json(member(&input, "result")?, "outer evolved result")?;
    let adapter_path = bound_path(member(&input, "adapter")?, "evolved adapter")?;
    let adapter = read_canonical_value(&adapter_path, "evolved adapter")?;
    ensure!(
        text(&manifest, "schemaVersion")? == OUTER_MANIFEST_SCHEMA
            && canonical_sha256_without_object_field(&manifest, "manifestSha256")?
                == sha(&manifest, "manifestSha256")?,
        "outer proposal manifest is invalid"
    );
    let compact_closure = match text(&result, "schemaVersion")? {
        EVOLVED_RESULT_SCHEMA => true,
        LEGACY_EVOLVED_RESULT_SCHEMA => false,
        _ => false,
    };
    ensure!(
        matches!(
            text(&result, "schemaVersion")?,
            EVOLVED_RESULT_SCHEMA | LEGACY_EVOLVED_RESULT_SCHEMA
        ) && canonical_sha256_without_object_field(&result, "resultSha256")?
            == sha(&result, "resultSha256")?
            && sha(&result, "manifestSha256")? == sha(&manifest, "manifestSha256")?,
        "outer evolved result is invalid"
    );
    let receipt = member(&result, "receipt")?;
    ensure!(
        text(receipt, "schemaVersion")?
            == if compact_closure {
                EVOLVED_RECEIPT_SCHEMA
            } else {
                LEGACY_EVOLVED_RECEIPT_SCHEMA
            }
            && canonical_sha256_without_object_field(receipt, "receiptSha256")?
                == sha(receipt, "receiptSha256")?
            && sha(&result, "receiptSha256")? == sha(receipt, "receiptSha256")?,
        "outer evolved receipt is invalid"
    );
    let inventory = member(receipt, "outputInventory")?;
    ensure!(
        canonical_sha256_without_object_field(inventory, "outputInventorySha256")?
            == sha(inventory, "outputInventorySha256")?
            && sha(receipt, "outputInventorySha256")? == sha(inventory, "outputInventorySha256")?
            && sha(&result, "outputInventorySha256")? == sha(inventory, "outputInventorySha256")?,
        "outer inventory is invalid"
    );
    if compact_closure {
        validate_current_evolved_outer_shape(&manifest, &result, receipt, inventory)?;
    }
    ensure!(
        sha(&adapter, "proposalResultSha256")? == sha(&result, "resultSha256")?
            && sha(&adapter, "proposalReceiptSha256")? == sha(receipt, "receiptSha256")?
            && sha(&adapter, "outputInventorySha256")? == sha(inventory, "outputInventorySha256")?,
        "adapter is not bound to the sealed outer chain"
    );
    let invocation = validate_evolved_invocation(member(&adapter, "nativeV5Invocation")?)?;
    ensure!(
        invocation.manifest == manifest
            && invocation.result == result
            && invocation.receipt_sha256 == sha(receipt, "receiptSha256")?
            && invocation.inventory_sha256 == sha(inventory, "outputInventorySha256")?,
        "evolved adapter invocation differs from the sealed outer chain"
    );
    let fragment = member(&adapter, "evolvedPublicationFragments")?;
    let root = sha(&result, "publicationFragmentsSha256")?;
    ensure!(
        sha(receipt, "publicationFragmentsSha256")? == root
            && sha(fragment, "semanticSha256")? == root,
        "adapter fragment root differs from sealed receipt"
    );
    if compact_closure {
        validate_compact_inventory_shape(
            inventory,
            &[
                "publicationFragments",
                "publicationPlan",
                "publicationReceipt",
                "transaction",
            ],
        )?;
        validate_compact_inventory_object(inventory, "publicationFragments", root, fragment)?;
    } else {
        let objects = array(member(inventory, "objectStore")?, "objects")?;
        let matches = objects
            .iter()
            .filter(|x| sha(x, "objectSha256").ok() == Some(root))
            .collect::<Vec<_>>();
        ensure!(
            matches.len() == 1
                && sha(matches[0], "fileSha256")? == sha(fragment, "fileSha256")?
                && unsigned(matches[0], "byteLength")? == unsigned(fragment, "byteLength")?,
            "adapter fragment descriptor is not the exact historical inventory object"
        );
    }
    extract_evolved_adapter_to_path(&adapter_path, output_path)
}

fn validate_current_evolved_outer_shape(
    manifest: &Value,
    result: &Value,
    receipt: &Value,
    inventory: &Value,
) -> Result<()> {
    exact_keys(
        object(result, "current evolved result")?,
        &[
            "schemaVersion",
            "contractVersion",
            "operation",
            "status",
            "authoritySha256",
            "manifestSha256",
            "expectedAuthoritySha256",
            "generationConfigSha256",
            "generationIndex",
            "requestedCount",
            "acceptedRecordCount",
            "attemptCount",
            "transactionSha256",
            "parentArchiveInputBindingSha256",
            "identityLedgerInputBindingSha256",
            "publicationRequestSha256",
            "publicationPlanSha256",
            "publicationReceiptSha256",
            "publicationFragmentsSha256",
            "evaluationPopulationSize",
            "identityLedgerSha256",
            "outputInventorySha256",
            "receipt",
            "receiptSha256",
            "resultSha256",
        ],
        "current evolved result",
    )?;
    exact_keys(
        object(receipt, "current evolved receipt")?,
        &[
            "schemaVersion",
            "authoritySha256",
            "manifestSha256",
            "expectedAuthoritySha256",
            "generationConfigSha256",
            "generationIndex",
            "requestedCount",
            "acceptedRecordCount",
            "attemptCount",
            "transactionSha256",
            "parentArchiveInputBindingSha256",
            "identityLedgerInputBindingSha256",
            "publicationRequestSha256",
            "publicationPlanSha256",
            "publicationReceiptSha256",
            "publicationFragmentsSha256",
            "evaluationPopulationSize",
            "identityLedgerSha256",
            "outputInventory",
            "outputInventorySha256",
            "nativeBatchAuthoritySha256",
            "threadCap",
            "constructionSummary",
            "receiptSha256",
        ],
        "current evolved receipt",
    )?;
    ensure!(
        text(result, "contractVersion")? == CONTRACT_VERSION
            && text(result, "operation")? == "native_v5_proposal_construction"
            && text(result, "status")? == "completed"
            && text(manifest, "generationKind")? == "evolved"
            && text(inventory, "outputRoot")? == text(manifest, "outputRoot")?,
        "current evolved outer identity is invalid"
    );
    for key in [
        "authoritySha256",
        "manifestSha256",
        "expectedAuthoritySha256",
        "generationConfigSha256",
        "transactionSha256",
        "parentArchiveInputBindingSha256",
        "identityLedgerInputBindingSha256",
        "publicationRequestSha256",
        "publicationPlanSha256",
        "publicationReceiptSha256",
        "publicationFragmentsSha256",
        "identityLedgerSha256",
        "outputInventorySha256",
    ] {
        ensure!(
            sha(result, key)? == sha(receipt, key)?,
            "current evolved receipt/result {key} binding drifted"
        );
    }
    for key in [
        "generationIndex",
        "requestedCount",
        "acceptedRecordCount",
        "attemptCount",
        "evaluationPopulationSize",
    ] {
        ensure!(
            unsigned(result, key)? == unsigned(receipt, key)?,
            "current evolved receipt/result {key} count drifted"
        );
    }
    validate_compact_inventory_shape(
        inventory,
        &[
            "publicationFragments",
            "publicationPlan",
            "publicationReceipt",
            "transaction",
        ],
    )?;
    validate_current_artifacts(
        inventory,
        &[
            ("evaluationPopulation", "evaluation-population.json"),
            ("generationJournal", "generation-journal.json"),
            ("identityLedger", "v5-native/identity-ledger.json"),
            ("pairConfig", "pair-config.json"),
            ("population", "population.json"),
        ],
    )?;
    for (role, field) in [
        ("publicationFragments", "publicationFragmentsSha256"),
        ("publicationPlan", "publicationPlanSha256"),
        ("publicationReceipt", "publicationReceiptSha256"),
        ("transaction", "transactionSha256"),
    ] {
        validate_compact_root(inventory, role, sha(result, field)?)?;
    }
    Ok(())
}

/// Production evolved handoff: seals the authenticated all-attempt stream and
/// its compact accounting in a receipt-last, write-once publication.
pub fn extract_evolved_chain_to_receipt_path(
    input_path: &Path,
    attempts_path: &Path,
    receipt_path: &Path,
) -> Result<Value> {
    let input = read_canonical_value(input_path, "evolved attempt chain input")?;
    ensure!(
        text(&input, "schemaVersion")? == EVOLVED_CHAIN_INPUT_SCHEMA
            && canonical_sha256_without_object_field(&input, "inputSha256")?
                == sha(&input, "inputSha256")?,
        "evolved attempt chain input is invalid"
    );
    if receipt_path.is_file() {
        let receipt = read_canonical_value(receipt_path, "evolved attempt stream receipt")?;
        validate_evolved_stream_receipt(&receipt, attempts_path)?;
        ensure!(
            sha(&receipt, "inputSha256")? == sha(&input, "inputSha256")?,
            "existing evolved attempt receipt belongs to different input"
        );
        return Ok(receipt);
    }
    ensure!(
        !attempts_path.exists(),
        "evolved attempt stream exists without receipt"
    );
    // This authenticates the complete v3 adapter/manifest/result/receipt/
    // inventory closure before any stream bytes are made durable.
    let descriptor = extract_evolved_chain_to_path(input_path, attempts_path)?;
    let manifest = read_bound_json(member(&input, "manifest")?, "outer proposal manifest")?;
    let result = read_bound_json(member(&input, "result")?, "outer evolved result")?;
    let adapter = read_canonical_value(
        &bound_path(member(&input, "adapter")?, "evolved adapter")?,
        "evolved adapter",
    )?;
    let fragment = member(&adapter, "evolvedPublicationFragments")?;
    let fragment_value = read_descriptor_json(fragment, "evolved publication fragments")?;
    validate_fragments(&fragment_value)?;
    let rows = read_jsonl(
        attempts_path
            .to_str()
            .context("attempt stream path is not UTF-8")?,
    )?;
    for (ordinal, row) in rows.iter().enumerate() {
        validate_attempt(row, ordinal as u64)?;
    }
    let accounting = evolved_accounting(&rows)?;
    ensure!(
        rows.len() as u64 == unsigned(&fragment_value, "proposalAttemptCount")?
            && accounting["dispositionCounts"]["accepted"]
                .as_u64()
                .unwrap_or(0)
                == unsigned(&fragment_value, "acceptedCandidateCount")?,
        "evolved attempt accounting differs from fragment receipt"
    );
    let outer_receipt = member(&result, "receipt")?;
    let inventory = member(outer_receipt, "outputInventory")?;
    let mut receipt = json!({
        "schemaVersion": EVOLVED_STREAM_RECEIPT_SCHEMA,
        "inputSha256": sha(&input, "inputSha256")?,
        "proposalResultSha256": sha(&result, "resultSha256")?,
        "proposalReceiptSha256": sha(outer_receipt, "receiptSha256")?,
        "outputInventorySha256": sha(inventory, "outputInventorySha256")?,
        "fragmentBundleSha256": sha(&fragment_value, "fragmentBundleSha256")?,
        "evaluationPopulationSha256": sha(member(&adapter, "evaluationPopulation")?, "semanticSha256")?,
        "attemptStream": {"path":attempts_path,"rawSha256":sha(&descriptor,"rawSha256")?,"sizeBytes":unsigned(&descriptor,"sizeBytes")?,"recordCount":unsigned(&descriptor,"recordCount")?,"rowSchema":ATTEMPT_ROW_SCHEMA},
        "proposalAccounting": accounting,
    });
    let receipt_sha = canonical_sha256(&receipt)?;
    receipt
        .as_object_mut()
        .expect("receipt")
        .insert("receiptSha256".into(), Value::String(receipt_sha));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(receipt_path)?;
    file.write_all(&canonical_json_line(&receipt)?)?;
    file.sync_all()?;
    // Manifest stays only as an explicit parse/cross-binding fence. Keep it
    // live to make accidental removal of the outer input validation visible.
    let _ = sha(&manifest, "manifestSha256")?;
    Ok(receipt)
}

fn evolved_accounting(rows: &[Value]) -> Result<Value> {
    let mut dispositions = BTreeMap::<String, u64>::new();
    let mut origins = BTreeMap::<String, u64>::new();
    for row in rows {
        *dispositions
            .entry(text(row, "disposition")?.to_owned())
            .or_default() += 1;
        *origins
            .entry(text(row, "originKind")?.to_owned())
            .or_default() += 1;
    }
    Ok(
        json!({"proposalAttemptCount":rows.len(),"originProposalCounts":origins,"dispositionCounts":dispositions}),
    )
}

fn validate_evolved_stream_receipt(value: &Value, attempts_path: &Path) -> Result<()> {
    exact_keys(
        object(value, "evolved attempt stream receipt")?,
        &[
            "schemaVersion",
            "inputSha256",
            "proposalResultSha256",
            "proposalReceiptSha256",
            "outputInventorySha256",
            "fragmentBundleSha256",
            "evaluationPopulationSha256",
            "attemptStream",
            "proposalAccounting",
            "receiptSha256",
        ],
        "evolved attempt stream receipt",
    )?;
    ensure!(
        text(value, "schemaVersion")? == EVOLVED_STREAM_RECEIPT_SCHEMA
            && canonical_sha256_without_object_field(value, "receiptSha256")?
                == sha(value, "receiptSha256")?,
        "evolved attempt stream receipt is invalid"
    );
    let stream = member(value, "attemptStream")?;
    exact_keys(
        object(stream, "evolved attempt stream descriptor")?,
        &["path", "rawSha256", "sizeBytes", "recordCount", "rowSchema"],
        "evolved attempt stream descriptor",
    )?;
    ensure!(
        text(stream, "path")? == attempts_path.to_string_lossy()
            && text(stream, "rowSchema")? == ATTEMPT_ROW_SCHEMA
            && file_sha(attempts_path)? == sha(stream, "rawSha256")?
            && fs::metadata(attempts_path)?.len() == unsigned(stream, "sizeBytes")?,
        "evolved attempt stream receipt file binding drifted"
    );
    let rows = read_jsonl(
        attempts_path
            .to_str()
            .context("attempt stream path is not UTF-8")?,
    )?;
    for (ordinal, row) in rows.iter().enumerate() {
        validate_attempt(row, ordinal as u64)?;
    }
    ensure!(
        rows.len() as u64 == unsigned(stream, "recordCount")?
            && evolved_accounting(&rows)? == *member(value, "proposalAccounting")?,
        "evolved attempt stream receipt accounting drifted"
    );
    Ok(())
}

/// The v2 funnel input owns one receipt descriptor, never a separately
/// supplied stream or accounting authority.  Normalize that receipt into the
/// operational stream descriptor needed by the assembler.
pub fn load_proposal_attempt_authority(authority: &Value) -> Result<(Value, Value, bool, Value)> {
    exact_keys(
        object(authority, "proposal attempt authority")?,
        &[
            "kind",
            "receiptPath",
            "receiptFileSha256",
            "receiptSizeBytes",
            "receiptSha256",
        ],
        "proposal attempt authority",
    )?;
    let receipt_path = Path::new(text(authority, "receiptPath")?);
    ensure!(
        receipt_path.is_file()
            && !fs::symlink_metadata(receipt_path)?.file_type().is_symlink()
            && fs::metadata(receipt_path)?.len() == unsigned(authority, "receiptSizeBytes")?
            && file_sha(receipt_path)? == sha(authority, "receiptFileSha256")?,
        "proposal attempt receipt descriptor drifted"
    );
    let receipt = read_canonical_value(receipt_path, "proposal attempt receipt")?;
    ensure!(
        sha(&receipt, "receiptSha256")? == sha(authority, "receiptSha256")?,
        "proposal attempt receipt semantic identity drifted"
    );
    match text(authority, "kind")? {
        "g0_selected" => {
            ensure!(
                text(&receipt, "schemaVersion")? == G0_SELECTED_ATTEMPT_RECEIPT_SCHEMA,
                "G0 proposal attempt authority has wrong receipt schema"
            );
            let relative = text(member(&receipt, "attemptStream")?, "relativePath")?;
            let stream_path = receipt_path
                .parent()
                .context("G0 selected attempt receipt has no parent")?
                .join(relative);
            validate_g0_selected_attempt_receipt(&receipt, &stream_path)?;
            let descriptor = member(&receipt, "attemptStream")?;
            Ok((
                json!({
                    "path": stream_path,
                    "rawSha256": sha(descriptor, "rawSha256")?,
                    "sizeBytes": unsigned(descriptor, "sizeBytes")?,
                    "recordCount": unsigned(descriptor, "recordCount")?,
                }),
                member(&receipt, "proposalAccounting")?.clone(),
                true,
                receipt,
            ))
        }
        "evolved" => {
            ensure!(
                text(&receipt, "schemaVersion")? == EVOLVED_STREAM_RECEIPT_SCHEMA,
                "evolved proposal attempt authority has wrong receipt schema"
            );
            let path = PathBuf::from(text(member(&receipt, "attemptStream")?, "path")?);
            validate_evolved_stream_receipt(&receipt, &path)?;
            let descriptor = member(&receipt, "attemptStream")?;
            Ok((
                json!({
                    "path": path,
                    "rawSha256": sha(descriptor, "rawSha256")?,
                    "sizeBytes": unsigned(descriptor, "sizeBytes")?,
                    "recordCount": unsigned(descriptor, "recordCount")?,
                }),
                member(&receipt, "proposalAccounting")?.clone(),
                false,
                receipt,
            ))
        }
        _ => anyhow::bail!("proposal attempt authority kind is invalid"),
    }
}

/// Extract the selected-only G0 construction authority.  This deliberately
/// stops before the directional tail: `funnel_source::assemble` is the only
/// author of evaluation, quality, retention, and terminal facts.
pub fn extract_g0_selected_attempts_to_path(
    input_path: &Path,
    attempts_path: &Path,
    receipt_path: &Path,
) -> Result<Value> {
    let input = read_canonical_value(input_path, "G0 selected attempt chain input")?;
    exact_keys(
        object(&input, "G0 funnel source chain input")?,
        &[
            "schemaVersion",
            "contractVersion",
            "manifest",
            "result",
            "adapter",
            "inputSha256",
        ],
        "G0 selected attempt chain input",
    )?;
    ensure!(
        text(&input, "schemaVersion")? == G0_CHAIN_INPUT_SCHEMA
            && text(&input, "contractVersion")? == CONTRACT_VERSION
            && canonical_sha256_without_object_field(&input, "inputSha256")?
                == sha(&input, "inputSha256")?,
        "G0 selected attempt chain input is invalid"
    );
    let manifest = read_bound_json(member(&input, "manifest")?, "G0 proposal manifest")?;
    let result = read_bound_json(member(&input, "result")?, "G0 proposal result")?;
    let adapter_path = bound_path(member(&input, "adapter")?, "G0 construction adapter")?;
    let adapter = read_canonical_value(&adapter_path, "G0 construction adapter")?;
    validate_g0_outer_chain(&manifest, &result, &adapter)
        .context("validate G0 outer manifest/result/adapter chain")?;

    // Receipt-last restart: a completed receipt authenticates the immutable
    // selected stream without reopening the construction closure.
    if receipt_path.is_file() {
        let existing = read_canonical_value(receipt_path, "G0 selected attempt receipt")?;
        validate_g0_selected_attempt_receipt(&existing, attempts_path)?;
        ensure!(
            sha(&existing, "inputSha256")? == sha(&input, "inputSha256")?,
            "existing G0 selected attempt receipt belongs to different input"
        );
        return Ok(existing);
    }
    let extracted = extract_g0_selected_attempts(&manifest, &result, &adapter)?;
    let bytes = extracted.bytes;
    if attempts_path.exists() {
        // A crash after stream durability but before receipt publication may
        // adopt only the exact reauthenticated bytes.
        ensure!(
            fs::read(attempts_path)? == bytes,
            "orphan G0 selected attempt stream differs from authenticated reconstruction"
        );
    } else {
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(attempts_path)?;
        output.write_all(&bytes)?;
        output.sync_all()?;
    }
    let mut receipt = json!({
        "schemaVersion": G0_SELECTED_ATTEMPT_RECEIPT_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "generationIndex": 1,
        "inputSha256": sha(&input, "inputSha256")?,
        "proposalManifestSha256": sha(&manifest, "manifestSha256")?,
        "proposalResultSha256": sha(&result, "resultSha256")?,
        "proposalReceiptSha256": extracted.receipt_sha256,
        "outputInventorySha256": extracted.inventory_sha256,
        "g0FunnelFragmentsSha256": extracted.fragments_sha256,
        "g0FunnelProjectionStreamReceiptSha256": extracted.stream_sha256,
        "selectedProjectionIndexSha256": extracted.selected_index_sha256,
        "ordering":"candidate_id_ascending_v1",
        "attemptStream": {"relativePath":"g0-selected-proposal-attempts.jsonl","rowSchema":G0_SELECTED_ATTEMPT_ROW_SCHEMA,"rawSha256":format!("sha256:{:x}", Sha256::digest(&bytes)),"sizeBytes":bytes.len(),"recordCount":extracted.record_count},
        "proposalAccounting": extracted.accounting,
    });
    let receipt_sha = canonical_sha256(&receipt)?;
    receipt
        .as_object_mut()
        .expect("object")
        .insert("receiptSha256".into(), Value::String(receipt_sha));
    let mut receipt_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(receipt_path)?;
    receipt_file.write_all(&canonical_json_line(&receipt)?)?;
    receipt_file.sync_all()?;
    Ok(receipt)
}

struct G0Extracted {
    bytes: Vec<u8>,
    record_count: u64,
    accounting: Value,
    receipt_sha256: String,
    inventory_sha256: String,
    fragments_sha256: String,
    stream_sha256: String,
    selected_index_sha256: String,
}

fn validate_g0_outer_chain(manifest: &Value, result: &Value, adapter: &Value) -> Result<()> {
    exact_keys(
        object(manifest, "G0 proposal manifest")?,
        &[
            "schemaVersion",
            "contractVersion",
            "operation",
            "authoritySha256",
            "executionAuthority",
            "frozenAuthority",
            "expectedAuthoritySha256",
            "outputRoot",
            "finalNewline",
            "generationConfig",
            "generationConfigSha256",
            "generationIndex",
            "generationKind",
            "requestedCount",
            "evaluationPopulationSize",
            "maxProposalAttempts",
            "threadCap",
            "inputs",
            "resultPath",
            "manifestSha256",
        ],
        "G0 proposal manifest",
    )?;
    exact_keys(
        object(result, "G0 proposal result")?,
        &[
            "schemaVersion",
            "contractVersion",
            "operation",
            "status",
            "authoritySha256",
            "manifestSha256",
            "expectedAuthoritySha256",
            "generationConfigSha256",
            "generationIndex",
            "requestedCount",
            "acceptedRecordCount",
            "attemptCount",
            "attemptJournalSha256",
            "publicationRequestSha256",
            "publicationPlanSha256",
            "g0FunnelFragmentsSha256",
            "g0FunnelProjectionStreamReceiptSha256",
            "evaluationPopulationSize",
            "compactJournalSha256",
            "identityLedgerSha256",
            "selectedProjectionIndexSha256",
            "outputInventorySha256",
            "receipt",
            "receiptSha256",
            "resultSha256",
        ],
        "G0 proposal result",
    )?;
    let valid_manifest = (|| -> Result<bool> {
        Ok(text(manifest, "schemaVersion")? == OUTER_MANIFEST_SCHEMA
            && text(manifest, "generationKind")? == "g0"
            && unsigned(manifest, "generationIndex")? == 1
            && canonical_sha256_without_object_field(manifest, "manifestSha256")?
                == sha(manifest, "manifestSha256")?)
    })()
    .context("read G0 proposal manifest identity")?;
    ensure!(valid_manifest, "G0 proposal manifest is invalid");
    let result_schema = text(result, "schemaVersion")?;
    let compact_closure = match result_schema {
        G0_RESULT_SCHEMA => true,
        LEGACY_G0_RESULT_SCHEMA => false,
        _ => false,
    };
    let valid_result = (|| -> Result<bool> {
        Ok(
            matches!(result_schema, G0_RESULT_SCHEMA | LEGACY_G0_RESULT_SCHEMA)
                && canonical_sha256_without_object_field(result, "resultSha256")?
                    == sha(result, "resultSha256")?
                && sha(result, "manifestSha256")? == sha(manifest, "manifestSha256")?,
        )
    })()
    .context("read G0 proposal result identity")?;
    ensure!(valid_result, "G0 proposal result is invalid");
    let receipt = member(result, "receipt")?;
    exact_keys(
        object(receipt, "G0 proposal receipt")?,
        &[
            "schemaVersion",
            "authoritySha256",
            "manifestSha256",
            "expectedAuthoritySha256",
            "generationConfigSha256",
            "generationIndex",
            "requestedCount",
            "acceptedRecordCount",
            "attemptCount",
            "attemptJournalSha256",
            "publicationRequestSha256",
            "publicationPlanSha256",
            "g0FunnelFragmentsSha256",
            "g0FunnelProjectionStreamReceiptSha256",
            "evaluationPopulationSize",
            "compactJournalSha256",
            "identityLedgerSha256",
            "selectedProjectionIndexSha256",
            "outputInventory",
            "outputInventorySha256",
            "nativeBatchAuthoritySha256",
            "threadCap",
            "constructionSummary",
            "receiptSha256",
        ],
        "G0 proposal receipt",
    )?;
    let valid_receipt = (|| -> Result<bool> {
        Ok(text(receipt, "schemaVersion")?
            == if compact_closure {
                G0_RECEIPT_SCHEMA
            } else {
                LEGACY_G0_RECEIPT_SCHEMA
            }
            && canonical_sha256_without_object_field(receipt, "receiptSha256")?
                == sha(receipt, "receiptSha256")?
            && sha(result, "receiptSha256")? == sha(receipt, "receiptSha256")?)
    })()
    .context("read G0 proposal receipt identity")?;
    ensure!(valid_receipt, "G0 proposal receipt is invalid");
    let inventory = member(receipt, "outputInventory")?;
    exact_keys(
        object(inventory, "G0 output inventory")?,
        &[
            "schemaVersion",
            "outputRoot",
            "outputRootSha256",
            "artifacts",
            "objectStore",
            "outputInventorySha256",
        ],
        "G0 output inventory",
    )?;
    let valid_inventory = (|| -> Result<bool> {
        Ok(text(inventory, "schemaVersion")?
            == if compact_closure {
                OUTPUT_INVENTORY_SCHEMA
            } else {
                LEGACY_OUTPUT_INVENTORY_SCHEMA
            }
            && canonical_sha256_without_object_field(inventory, "outputInventorySha256")?
                == sha(inventory, "outputInventorySha256")?
            && sha(result, "outputInventorySha256")? == sha(inventory, "outputInventorySha256")?)
    })()
    .context("read G0 output inventory identity")?;
    ensure!(valid_inventory, "G0 output inventory is invalid");
    exact_keys(
        object(adapter, "G0 construction adapter")?,
        &[
            "schemaVersion",
            "operation",
            "completed",
            "generationKind",
            "generationIndex",
            "generationConfigSha256",
            "authoritySha256",
            "attemptCount",
            "acceptedCandidateCount",
            "selectedEvaluationCandidateCount",
            "publicationPlanSha256",
            "publicationRequestSha256",
            "proposalResultSha256",
            "proposalReceiptSha256",
            "outputInventorySha256",
            "population",
            "evaluationPopulation",
            "generationJournal",
            "identityLedger",
            "g0FunnelFragments",
            "g0FunnelProjectionStream",
            "nativeV5Invocation",
            "adapterSha256",
        ],
        "G0 construction adapter",
    )?;
    let valid_adapter = (|| -> Result<bool> {
        Ok(text(adapter, "schemaVersion")? == G0_ADAPTER_SCHEMA
            && text(adapter, "generationKind")? == "g0"
            && canonical_sha256_without_object_field(adapter, "adapterSha256")?
                == sha(adapter, "adapterSha256")?
            && adapter.get("evolvedPublicationFragments").is_none()
            && sha(adapter, "proposalResultSha256")? == sha(result, "resultSha256")?
            && sha(adapter, "proposalReceiptSha256")? == sha(receipt, "receiptSha256")?
            && sha(adapter, "outputInventorySha256")? == sha(inventory, "outputInventorySha256")?)
    })()
    .context("read G0 construction adapter identity")?;
    ensure!(
        valid_adapter,
        "G0 construction adapter is not bound to outer chain"
    );
    validate_g0_invocation(
        member(adapter, "nativeV5Invocation")?,
        manifest,
        result,
        result_schema,
    )
    .context("validate G0 native invocation")?;
    validate_g0_inventory_shape(inventory, compact_closure)
        .context("validate G0 output inventory shape")?;
    if compact_closure {
        ensure!(
            text(inventory, "outputRoot")? == text(manifest, "outputRoot")?,
            "G0 compact output inventory root differs from manifest"
        );
        validate_compact_root(
            inventory,
            "g0FunnelFragments",
            sha(result, "g0FunnelFragmentsSha256")?,
        )?;
        validate_compact_root(
            inventory,
            "g0FunnelProjectionStreamReceipt",
            sha(result, "g0FunnelProjectionStreamReceiptSha256")?,
        )?;
        validate_compact_root(
            inventory,
            "publicationPlan",
            sha(result, "publicationPlanSha256")?,
        )?;
    }
    Ok(())
}

fn validate_g0_inventory_shape(inventory: &Value, compact_closure: bool) -> Result<()> {
    let expected = [
        "attemptJournal",
        "attemptRows",
        "compactJournal",
        "identityLedger",
        "selectedProjectionIndex",
        "sharedAuthority",
        "g0FunnelProjectionStream",
        "evaluationPopulation",
        "generationJournal",
        "pairConfig",
        "population",
        "g0AcceptedPool",
        "g0CampaignConstructionLedger",
        "g0Selection",
    ];
    let artifacts = array(inventory, "artifacts")?;
    ensure!(
        artifacts.len() == expected.len(),
        "G0 output inventory artifact count drifted"
    );
    let mut seen = std::collections::BTreeSet::new();
    for artifact in artifacts {
        exact_keys(
            object(artifact, "G0 output inventory artifact")?,
            &[
                "kind",
                "relativePath",
                "fileSha256",
                "byteLength",
                "semanticSha256",
            ],
            "G0 output inventory artifact",
        )?;
        ensure!(
            seen.insert(text(artifact, "kind")?.to_owned()),
            "G0 output inventory repeats artifact kind"
        );
    }
    ensure!(
        seen == expected.into_iter().map(str::to_owned).collect(),
        "G0 output inventory artifact family drifted"
    );
    if compact_closure {
        validate_compact_inventory_shape(
            inventory,
            &[
                "g0FunnelFragments",
                "g0FunnelProjectionStreamReceipt",
                "publicationPlan",
            ],
        )?;
        return validate_current_artifacts(
            inventory,
            &[
                ("attemptJournal", "v5-native/attempt-journal-root.json"),
                ("attemptRows", "v5-native/attempts.jsonl"),
                ("compactJournal", "v5-native/accepted-records.jsonl"),
                ("evaluationPopulation", "evaluation-population.json"),
                ("g0AcceptedPool", "g0-bootstrap/accepted-pool.json"),
                (
                    "g0CampaignConstructionLedger",
                    "g0-bootstrap/campaign-construction-ledger.json",
                ),
                (
                    "g0FunnelProjectionStream",
                    "v5-native/g0-funnel-projections.jsonl",
                ),
                ("g0Selection", "g0-bootstrap/selection.json"),
                ("generationJournal", "generation-journal.json"),
                ("identityLedger", "v5-native/identity-ledger.json"),
                ("pairConfig", "pair-config.json"),
                ("population", "population.json"),
                (
                    "selectedProjectionIndex",
                    "v5-native/selected-projections.jsonl",
                ),
                (
                    "sharedAuthority",
                    "v5-native/authority/shared-authority.json",
                ),
            ],
        );
    }
    let store = member(inventory, "objectStore")?;
    exact_keys(
        object(store, "G0 object store inventory")?,
        &[
            "schemaVersion",
            "relativeRoot",
            "objectCount",
            "byteCount",
            "objects",
            "objectStoreInventorySha256",
        ],
        "G0 object store inventory",
    )?;
    let objects = array(store, "objects")?;
    ensure!(
        objects.len() as u64 == unsigned(store, "objectCount")?,
        "G0 object store count drifted"
    );
    let mut object_ids = std::collections::BTreeSet::new();
    for item in objects {
        exact_keys(
            object(item, "G0 object store entry")?,
            &["relativePath", "objectSha256", "fileSha256", "byteLength"],
            "G0 object store entry",
        )?;
        ensure!(
            object_ids.insert(sha(item, "objectSha256")?.to_owned()),
            "G0 object store repeats identity"
        );
    }
    Ok(())
}

fn validate_g0_invocation(
    value: &Value,
    manifest: &Value,
    result: &Value,
    result_schema: &str,
) -> Result<()> {
    exact_keys(
        object(value, "G0 native invocation")?,
        &[
            "schemaVersion",
            "proposalManifest",
            "proposalResult",
            "proposalReceiptSha256",
            "outputInventorySha256",
        ],
        "G0 native invocation",
    )?;
    ensure!(
        text(value, "schemaVersion")? == G0_INVOCATION_SCHEMA,
        "G0 native invocation schema is invalid"
    );
    let m = read_invocation_document(
        member(value, "proposalManifest")?,
        OUTER_MANIFEST_SCHEMA,
        "manifest.json",
    )?;
    let r = read_invocation_document(
        member(value, "proposalResult")?,
        result_schema,
        "v5-proposal-result.json",
    )?;
    let root = format!(
        "native-batch/v5-proposal/{}/",
        sha(&m, "manifestSha256")?.trim_start_matches("sha256:")
    );
    ensure!(
        m == *manifest
            && r == *result
            && text(member(value, "proposalManifest")?, "relativePath")?
                == format!("{root}manifest.json")
            && text(member(value, "proposalResult")?, "relativePath")?
                == format!("{root}v5-proposal-result.json")
            && sha(value, "proposalReceiptSha256")? == sha(result, "receiptSha256")?
            && sha(value, "outputInventorySha256")? == sha(result, "outputInventorySha256")?,
        "G0 native invocation differs from outer chain"
    );
    Ok(())
}

fn read_descriptor_json(descriptor: &Value, label: &str) -> Result<Value> {
    let path = Path::new(text(descriptor, "absolutePath")?);
    ensure!(
        path.is_file()
            && !fs::symlink_metadata(path)?.file_type().is_symlink()
            && fs::metadata(path)?.len() == unsigned(descriptor, "byteLength")?
            && file_sha(path)? == sha(descriptor, "fileSha256")?,
        "{label} file binding drifted"
    );
    read_canonical_value(path, label)
}

fn inventory_artifact<'a>(inventory: &'a Value, kind: &str) -> Result<&'a Value> {
    let matches = array(inventory, "artifacts")?
        .iter()
        .filter(|v| v.get("kind").and_then(Value::as_str) == Some(kind))
        .collect::<Vec<_>>();
    ensure!(
        matches.len() == 1,
        "G0 output inventory must contain exactly one {kind} artifact"
    );
    Ok(matches[0])
}

fn validate_compact_inventory_shape(inventory: &Value, expected_roles: &[&str]) -> Result<()> {
    ensure!(
        text(inventory, "schemaVersion")? == OUTPUT_INVENTORY_SCHEMA,
        "compact output inventory schema is invalid"
    );
    exact_keys(
        object(inventory, "compact output inventory")?,
        &[
            "schemaVersion",
            "outputRoot",
            "outputRootSha256",
            "artifacts",
            "objectStore",
            "outputInventorySha256",
        ],
        "compact output inventory",
    )?;
    sha(inventory, "outputRootSha256")?;
    ensure!(
        canonical_sha256_without_object_field(inventory, "outputInventorySha256")?
            == sha(inventory, "outputInventorySha256")?,
        "compact output inventory self hash drifted"
    );

    let store = member(inventory, "objectStore")?;
    exact_keys(
        object(store, "compact object-store closure")?,
        &[
            "schemaVersion",
            "relativeRoot",
            "inventory",
            "roots",
            "objectStoreSha256",
        ],
        "compact object-store closure",
    )?;
    ensure!(
        text(store, "schemaVersion")? == OBJECT_STORE_CLOSURE_SCHEMA
            && text(store, "relativeRoot")? == "v5-native/objects"
            && canonical_sha256_without_object_field(store, "objectStoreSha256")?
                == sha(store, "objectStoreSha256")?,
        "compact object-store closure is invalid"
    );
    let descriptor = member(store, "inventory")?;
    exact_keys(
        object(descriptor, "compact object inventory descriptor")?,
        &[
            "schemaVersion",
            "rowSchemaVersion",
            "relativePath",
            "fileSha256",
            "byteLength",
            "objectCount",
            "objectByteCount",
            "descriptorSha256",
        ],
        "compact object inventory descriptor",
    )?;
    ensure!(
        text(descriptor, "schemaVersion")? == OBJECT_INVENTORY_DESCRIPTOR_SCHEMA
            && text(descriptor, "rowSchemaVersion")? == OBJECT_INVENTORY_ROW_SCHEMA
            && text(descriptor, "relativePath")? == OBJECT_INVENTORY_PATH
            && canonical_sha256_without_object_field(descriptor, "descriptorSha256")?
                == sha(descriptor, "descriptorSha256")?,
        "compact object inventory descriptor is invalid"
    );
    sha(descriptor, "fileSha256")?;
    for field in ["byteLength", "objectCount", "objectByteCount"] {
        unsigned(descriptor, field)?;
    }

    let roots = array(store, "roots")?;
    ensure!(
        roots.len() == expected_roles.len() && roots.len() <= 4,
        "compact object-store root projection is not exact and bounded"
    );
    let mut object_ids = BTreeSet::new();
    for (entry, expected_role) in roots.iter().zip(expected_roles) {
        exact_keys(
            object(entry, "compact object-store root")?,
            &[
                "role",
                "relativePath",
                "objectSha256",
                "fileSha256",
                "byteLength",
            ],
            "compact object-store root",
        )?;
        let object_sha = sha(entry, "objectSha256")?;
        ensure!(
            text(entry, "role")? == *expected_role
                && text(entry, "relativePath")?
                    == format!("sha256/{}.json", object_sha.trim_start_matches("sha256:"))
                && object_ids.insert(object_sha.to_owned()),
            "compact object-store root role/path/order is invalid"
        );
        sha(entry, "fileSha256")?;
        unsigned(entry, "byteLength")?;
    }
    Ok(())
}

fn validate_current_artifacts(inventory: &Value, expected: &[(&str, &str)]) -> Result<()> {
    let artifacts = array(inventory, "artifacts")?;
    ensure!(
        artifacts.len() == expected.len(),
        "current output artifact projection is not exact"
    );
    for (artifact, (kind, path)) in artifacts.iter().zip(expected) {
        exact_keys(
            object(artifact, "current output artifact")?,
            &[
                "kind",
                "relativePath",
                "fileSha256",
                "byteLength",
                "semanticSha256",
            ],
            "current output artifact",
        )?;
        ensure!(
            text(artifact, "kind")? == *kind && text(artifact, "relativePath")? == *path,
            "current output artifact order/path drifted"
        );
        sha(artifact, "fileSha256")?;
        sha(artifact, "semanticSha256")?;
        unsigned(artifact, "byteLength")?;
    }
    Ok(())
}

fn compact_inventory_root<'a>(inventory: &'a Value, role: &str) -> Result<&'a Value> {
    let roots = array(member(inventory, "objectStore")?, "roots")?;
    let matches = roots
        .iter()
        .filter(|entry| entry.get("role").and_then(Value::as_str) == Some(role))
        .collect::<Vec<_>>();
    ensure!(
        matches.len() == 1,
        "compact object-store closure lacks exact {role} root"
    );
    Ok(matches[0])
}

fn validate_compact_root(inventory: &Value, role: &str, root: &str) -> Result<()> {
    let entry = compact_inventory_root(inventory, role)?;
    ensure!(
        sha(entry, "objectSha256")? == root,
        "compact object-store {role} semantic root drifted"
    );
    Ok(())
}

fn legacy_inventory_object<'a>(inventory: &'a Value, root: &str) -> Result<&'a Value> {
    let objects = array(member(inventory, "objectStore")?, "objects")?;
    let path = format!("sha256/{}.json", root.trim_start_matches("sha256:"));
    let matches = objects
        .iter()
        .filter(|v| {
            v.get("objectSha256").and_then(Value::as_str) == Some(root)
                && v.get("relativePath").and_then(Value::as_str) == Some(path.as_str())
        })
        .collect::<Vec<_>>();
    ensure!(
        matches.len() == 1,
        "G0 root does not resolve to one immutable object"
    );
    Ok(matches[0])
}

fn validate_inventory_object(inventory: &Value, root: &str, descriptor: &Value) -> Result<()> {
    let item = legacy_inventory_object(inventory, root)?;
    ensure!(
        sha(item, "fileSha256")? == sha(descriptor, "fileSha256")?
            && unsigned(item, "byteLength")? == unsigned(descriptor, "byteLength")?
            && text(descriptor, "relativePath")?
                == format!("v5-native/objects/{}", text(item, "relativePath")?),
        "G0 object descriptor is not the exact inventory object"
    );
    Ok(())
}

fn validate_compact_inventory_object(
    inventory: &Value,
    role: &str,
    root: &str,
    descriptor: &Value,
) -> Result<()> {
    let item = compact_inventory_root(inventory, role)?;
    ensure!(
        sha(item, "objectSha256")? == root
            && sha(item, "fileSha256")? == sha(descriptor, "fileSha256")?
            && unsigned(item, "byteLength")? == unsigned(descriptor, "byteLength")?
            && text(descriptor, "relativePath")?
                == format!("v5-native/objects/{}", text(item, "relativePath")?),
        "compact object descriptor is not the exact bounded inventory root"
    );
    Ok(())
}

fn validate_inventory_artifact(
    inventory: &Value,
    kind: &str,
    descriptor: &Value,
    root: &str,
) -> Result<()> {
    let item = inventory_artifact(inventory, kind)?;
    ensure!(
        text(item, "relativePath")? == text(descriptor, "relativePath")?
            && sha(item, "fileSha256")? == sha(descriptor, "fileSha256")?
            && unsigned(item, "byteLength")? == unsigned(descriptor, "byteLength")?
            && sha(item, "semanticSha256")? == root,
        "G0 artifact descriptor is not the exact inventory artifact"
    );
    Ok(())
}

fn safe_output_path(root: &Path, relative: &str) -> Result<PathBuf> {
    ensure!(
        !relative.is_empty()
            && !relative
                .split('/')
                .any(|p| p.is_empty() || p == "." || p == "..")
            && !Path::new(relative).is_absolute(),
        "G0 output path is unsafe"
    );
    let path = root.join(relative);
    ensure!(
        path.is_file() && !fs::symlink_metadata(&path)?.file_type().is_symlink(),
        "G0 output path is invalid"
    );
    Ok(path)
}

fn read_jsonl(path: &str) -> Result<Vec<Value>> {
    let file = fs::File::open(path)?;
    let mut rows = Vec::new();
    for line in BufReader::new(file).split(b'\n') {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_slice(&line)?;
        ensure!(
            canonical_json_bytes(&value)? == line,
            "G0 JSONL row is not canonical"
        );
        rows.push(value);
    }
    Ok(rows)
}

fn read_selected_projection_rows(path: &Path) -> Result<Vec<Value>> {
    let rows = read_jsonl(
        path.to_str()
            .context("selected projection path is not UTF-8")?,
    )?;
    for row in &rows {
        V5SelectedProjection::from_value(row).context("parse G0 selected projection row")?;
    }
    Ok(rows)
}

fn validate_g0_selected_attempt_receipt(value: &Value, attempts_path: &Path) -> Result<()> {
    exact_keys(
        object(value, "G0 selected attempt receipt")?,
        &[
            "schemaVersion",
            "contractVersion",
            "generationIndex",
            "inputSha256",
            "proposalManifestSha256",
            "proposalResultSha256",
            "proposalReceiptSha256",
            "outputInventorySha256",
            "g0FunnelFragmentsSha256",
            "g0FunnelProjectionStreamReceiptSha256",
            "selectedProjectionIndexSha256",
            "ordering",
            "attemptStream",
            "proposalAccounting",
            "receiptSha256",
        ],
        "G0 selected attempt receipt",
    )?;
    ensure!(
        text(value, "schemaVersion")? == G0_SELECTED_ATTEMPT_RECEIPT_SCHEMA
            && text(value, "contractVersion")? == CONTRACT_VERSION
            && unsigned(value, "generationIndex")? == 1
            && text(value, "ordering")? == "candidate_id_ascending_v1"
            && canonical_sha256_without_object_field(value, "receiptSha256")?
                == sha(value, "receiptSha256")?,
        "G0 selected attempt receipt is invalid"
    );
    let stream = member(value, "attemptStream")?;
    exact_keys(
        object(stream, "G0 selected attempt stream descriptor")?,
        &[
            "relativePath",
            "rowSchema",
            "rawSha256",
            "sizeBytes",
            "recordCount",
        ],
        "G0 selected attempt stream descriptor",
    )?;
    ensure!(
        text(stream, "relativePath")? == "g0-selected-proposal-attempts.jsonl"
            && text(stream, "rowSchema")? == G0_SELECTED_ATTEMPT_ROW_SCHEMA
            && file_sha(attempts_path)? == sha(stream, "rawSha256")?
            && fs::metadata(attempts_path)?.len() == unsigned(stream, "sizeBytes")?,
        "G0 selected attempt stream binding drifted"
    );
    let rows = read_jsonl(
        attempts_path
            .to_str()
            .context("G0 selected path is not UTF-8")?,
    )?;
    let accounting = g0_selected_accounting(&rows)?;
    let received_accounting = member(value, "proposalAccounting")?;
    exact_keys(
        object(received_accounting, "G0 selected proposal accounting")?,
        &[
            "proposalAttemptCount",
            "dispositionCounts",
            "originProposalCounts",
            "g0ConstructionProposalAccounting",
        ],
        "G0 selected proposal accounting",
    )?;
    for key in [
        "proposalAttemptCount",
        "dispositionCounts",
        "originProposalCounts",
    ] {
        ensure!(
            member(&accounting, key)? == member(received_accounting, key)?,
            "G0 selected attempt receipt compact accounting drifted"
        );
    }
    validate_g0_construction_accounting(
        member(received_accounting, "g0ConstructionProposalAccounting")?,
        rows.len() as u64,
    )?;
    ensure!(
        rows.len() as u64 == unsigned(stream, "recordCount")?,
        "G0 selected attempt receipt accounting drifted"
    );
    Ok(())
}

fn validate_g0_construction_accounting(value: &Value, selected: u64) -> Result<()> {
    exact_keys(
        object(value, "G0 construction proposal accounting")?,
        &[
            "proposalAttemptCount",
            "acceptedCount",
            "selectedCount",
            "attemptJournalSha256",
            "acceptedPoolSha256",
            "selectionSha256",
            "campaignLedgerSha256",
            "compactIdentityLedgerSha256",
        ],
        "G0 construction proposal accounting",
    )?;
    ensure!(
        unsigned(value, "proposalAttemptCount")? >= unsigned(value, "acceptedCount")?
            && unsigned(value, "acceptedCount")? >= selected
            && unsigned(value, "selectedCount")? == selected,
        "G0 construction proposal accounting count drifted"
    );
    for key in [
        "attemptJournalSha256",
        "acceptedPoolSha256",
        "selectionSha256",
        "campaignLedgerSha256",
        "compactIdentityLedgerSha256",
    ] {
        sha(value, key)?;
    }
    Ok(())
}

fn evaluation_row_from_construction(construction: &Value) -> Result<Value> {
    validate_attempt(construction, unsigned(construction, "proposalOrdinal")?)?;
    ensure!(
        text(construction, "disposition")? == "accepted",
        "G0 selected construction row is not accepted"
    );
    let mut evaluation = construction.clone();
    evaluation
        .as_object_mut()
        .expect("validated construction object")
        .insert(
            "entrySha256".to_owned(),
            json!(sha(construction, "acceptedCompactRecordSha256")?),
        );
    Ok(evaluation)
}

fn verify_g0_evaluation_fragment(fragments: &Value, rows: &[Value]) -> Result<()> {
    let descriptor = member(fragments, "evaluationPopulationFunnelEntries")?;
    let bytes = rows
        .iter()
        .enumerate()
        .try_fold(Vec::new(), |mut bytes, (i, row)| {
            if i != 0 {
                bytes.push(b',');
            }
            bytes.extend(canonical_json_bytes(row)?);
            Ok::<_, anyhow::Error>(bytes)
        })?;
    ensure!(
        rows.len() as u64 == unsigned(descriptor, "rowCount")?
            && bytes.len() as u64 == unsigned(descriptor, "encodedBytes")?
            && format!("sha256:{:x}", Sha256::digest(&bytes)) == sha(descriptor, "fragmentSha256")?,
        "G0 selected candidate-ID fragment differs from Core evaluation population funnel entries"
    );
    Ok(())
}

fn g0_selected_attempt_row(
    ordinal: u64,
    construction: &Value,
    projection: &V5SelectedProjection,
) -> Result<Value> {
    let stage = member(construction, "funnelCandidate")?;
    ensure!(
        text(member(stage, "admission")?, "outcome")? == "admitted",
        "G0 selected construction row lacks admitted funnel stage"
    );
    let static_native = json!({
        "static": member(stage, "staticReachability")?,
        "native": member(stage, "nativeValidation")?,
    });
    let projection_value = projection
        .to_value()
        .context("encode G0 selected projection")?;
    let mut proof = json!({
        "schemaVersion":"temporal_qd_v5_g0_funnel_proof_v2",
        "selectedProjectionSha256":sha(&projection_value, "projectionSha256")?,
        "nativeStaticProofSha256":canonical_sha256(&static_native)?,
    });
    let proof_sha = canonical_sha256(&proof)?;
    proof
        .as_object_mut()
        .expect("proof object")
        .insert("proofSha256".into(), json!(proof_sha));
    let mut row = json!({
        "schemaVersion":G0_SELECTED_ATTEMPT_ROW_SCHEMA,
        "proposalOrdinal":ordinal,
        "constructionAttempt":construction,
        "g0BootstrapProof":proof,
    });
    let row_sha = canonical_sha256(&row)?;
    row.as_object_mut()
        .expect("G0 selected attempt")
        .insert("selectedAttemptSha256".into(), json!(row_sha));
    validate_g0_selected_attempt(&row, ordinal)?;
    Ok(row)
}

pub fn validate_g0_selected_attempt(value: &Value, selected_ordinal: u64) -> Result<()> {
    exact_keys(
        object(value, "G0 selected proposal attempt")?,
        &[
            "schemaVersion",
            "proposalOrdinal",
            "constructionAttempt",
            "g0BootstrapProof",
            "selectedAttemptSha256",
        ],
        "G0 selected proposal attempt",
    )?;
    ensure!(
        text(value, "schemaVersion")? == G0_SELECTED_ATTEMPT_ROW_SCHEMA
            && unsigned(value, "proposalOrdinal")? == selected_ordinal
            && canonical_sha256_without_object_field(value, "selectedAttemptSha256")?
                == sha(value, "selectedAttemptSha256")?,
        "G0 selected proposal attempt identity drifted"
    );
    let construction = member(value, "constructionAttempt")?;
    validate_attempt(construction, unsigned(construction, "proposalOrdinal")?)?;
    ensure!(
        text(construction, "disposition")? == "accepted"
            && text(
                member(member(construction, "funnelCandidate")?, "admission")?,
                "outcome"
            )? == "admitted",
        "G0 selected proposal attempt construction binding is invalid"
    );
    let proof = member(value, "g0BootstrapProof")?;
    exact_keys(
        object(proof, "G0 bootstrap proof")?,
        &[
            "schemaVersion",
            "selectedProjectionSha256",
            "nativeStaticProofSha256",
            "proofSha256",
        ],
        "G0 bootstrap proof",
    )?;
    let stage = member(construction, "funnelCandidate")?;
    let static_native = json!({
        "static": member(stage, "staticReachability")?,
        "native": member(stage, "nativeValidation")?,
    });
    ensure!(
        text(proof, "schemaVersion")? == "temporal_qd_v5_g0_funnel_proof_v2"
            && canonical_sha256(&static_native)? == sha(proof, "nativeStaticProofSha256")?
            && canonical_sha256_without_object_field(proof, "proofSha256")?
                == sha(proof, "proofSha256")?,
        "G0 bootstrap proof binding drifted"
    );
    Ok(())
}

fn g0_selected_accounting(rows: &[Value]) -> Result<Value> {
    ensure!(
        !rows.is_empty(),
        "G0 selected proposal attempt stream is empty"
    );
    let mut dispositions = BTreeMap::<String, u64>::new();
    let mut origins = BTreeMap::<String, u64>::new();
    let mut candidates = BTreeSet::new();
    for (ordinal, row) in rows.iter().enumerate() {
        validate_g0_selected_attempt(row, ordinal as u64)?;
        let construction = member(row, "constructionAttempt")?;
        ensure!(
            candidates.insert(text(member(construction, "candidate")?, "candidateId")?.to_owned()),
            "G0 selected proposal attempt stream repeats candidate"
        );
        *dispositions
            .entry(text(construction, "disposition")?.to_owned())
            .or_default() += 1;
        *origins
            .entry(text(construction, "originKind")?.to_owned())
            .or_default() += 1;
    }
    Ok(json!({
        "proposalAttemptCount":rows.len(),
        "dispositionCounts":dispositions,
        "originProposalCounts":origins,
    }))
}

fn extract_g0_selected_attempts(
    manifest: &Value,
    result: &Value,
    adapter: &Value,
) -> Result<G0Extracted> {
    let compact_closure = text(result, "schemaVersion")? == G0_RESULT_SCHEMA;
    let receipt = member(result, "receipt")?;
    let inventory = member(receipt, "outputInventory")?;
    let fragments_root = sha(result, "g0FunnelFragmentsSha256")?.to_owned();
    let stream_root = sha(result, "g0FunnelProjectionStreamReceiptSha256")?.to_owned();
    let index_root = sha(result, "selectedProjectionIndexSha256")?.to_owned();
    for key in [
        "g0FunnelFragmentsSha256",
        "g0FunnelProjectionStreamReceiptSha256",
        "selectedProjectionIndexSha256",
    ] {
        ensure!(
            sha(receipt, key)? == sha(result, key)?,
            "G0 receipt/result root binding drifted"
        );
    }
    let fragments = member(adapter, "g0FunnelFragments")?;
    exact_keys(
        object(fragments, "G0 funnel fragments descriptor")?,
        &[
            "schemaVersion",
            "coreSchemaVersion",
            "relativePath",
            "absolutePath",
            "semanticSha256",
            "fileSha256",
            "byteLength",
        ],
        "G0 funnel fragments descriptor",
    )?;
    ensure!(
        text(fragments, "schemaVersion")?
            == "temporal_qd_native_v5_g0_funnel_fragments_descriptor_v1"
            && text(fragments, "coreSchemaVersion")? == "temporal_qd_v5_g0_funnel_fragments_v1"
            && sha(fragments, "semanticSha256")? == fragments_root,
        "G0 funnel fragments descriptor is invalid"
    );
    let fragment_value = read_descriptor_json(fragments, "G0 funnel fragments object")?;
    let fragment_binding = V5G0FunnelFragmentReceiptObjectBinding::from_value(&fragment_value)
        .context("parse G0 funnel fragments object")?;
    let funnel_receipt = V5G0FunnelFragments::from_value(&fragment_binding.value)
        .context("parse G0 funnel fragments receipt")?;
    ensure!(
        fragment_binding.g0_funnel_fragments_sha256 == fragments_root
            && fragment_binding.relative_path == text(fragments, "relativePath")?,
        "G0 funnel fragments object root/path drifted"
    );
    if compact_closure {
        validate_compact_inventory_object(
            inventory,
            "g0FunnelFragments",
            &fragments_root,
            fragments,
        )?;
    } else {
        validate_inventory_object(inventory, &fragments_root, fragments)?;
    }

    let stream = member(adapter, "g0FunnelProjectionStream")?;
    exact_keys(
        object(stream, "G0 projection stream descriptor")?,
        &[
            "schemaVersion",
            "coreReceiptSchemaVersion",
            "rowSchemaVersion",
            "stream",
            "receiptObject",
        ],
        "G0 projection stream descriptor",
    )?;
    ensure!(
        text(stream, "schemaVersion")?
            == "temporal_qd_native_v5_g0_funnel_projection_stream_descriptor_v1"
            && text(stream, "coreReceiptSchemaVersion")?
                == "temporal_qd_v5_g0_funnel_projection_stream_receipt_v1"
            && text(stream, "rowSchemaVersion")? == ATTEMPT_ROW_SCHEMA,
        "G0 projection stream descriptor is invalid"
    );
    let stream_file = member(stream, "stream")?;
    let stream_object = member(stream, "receiptObject")?;
    for descriptor in [stream_file, stream_object] {
        exact_keys(
            object(descriptor, "G0 stream file descriptor")?,
            &[
                "relativePath",
                "absolutePath",
                "semanticSha256",
                "fileSha256",
                "byteLength",
            ],
            "G0 stream file descriptor",
        )?;
        ensure!(
            sha(descriptor, "semanticSha256")? == stream_root,
            "G0 projection stream root drifted"
        );
    }
    ensure!(
        text(stream_file, "relativePath")? == "v5-native/g0-funnel-projections.jsonl",
        "G0 projection stream path drifted"
    );
    let stream_binding_value =
        read_descriptor_json(stream_object, "G0 projection stream receipt object")?;
    let stream_binding =
        V5G0FunnelProjectionStreamReceiptObjectBinding::from_value(&stream_binding_value)
            .context("parse G0 projection stream receipt object")?;
    ensure!(
        stream_binding.g0_funnel_projection_stream_receipt_sha256 == stream_root
            && stream_binding.relative_path == text(stream_object, "relativePath")?,
        "G0 projection stream object root/path drifted"
    );
    if compact_closure {
        validate_compact_inventory_object(
            inventory,
            "g0FunnelProjectionStreamReceipt",
            &stream_root,
            stream_object,
        )?;
    } else {
        validate_inventory_object(inventory, &stream_root, stream_object)?;
    }
    validate_inventory_artifact(
        inventory,
        "g0FunnelProjectionStream",
        stream_file,
        &stream_root,
    )?;
    let mut stream_handle = fs::File::open(text(stream_file, "absolutePath")?)?;
    verify_v5_g0_funnel_projection_stream(
        &funnel_receipt,
        &stream_binding.value,
        &mut stream_handle,
    )
    .context("verify G0 public projection stream")?;

    let index_artifact = inventory_artifact(inventory, "selectedProjectionIndex")?;
    ensure!(
        text(index_artifact, "relativePath")? == "v5-native/selected-projections.jsonl"
            && sha(index_artifact, "semanticSha256")? == index_root,
        "G0 selected projection artifact drifted"
    );
    let output_root = Path::new(text(manifest, "outputRoot")?);
    let index_path = safe_output_path(output_root, text(index_artifact, "relativePath")?)?;
    ensure!(
        file_sha(&index_path)? == sha(index_artifact, "fileSha256")?
            && fs::metadata(&index_path)?.len() == unsigned(index_artifact, "byteLength")?,
        "G0 selected projection artifact file binding drifted"
    );
    let selected_rows = read_selected_projection_rows(&index_path)?;
    let index = if compact_closure {
        V5SelectedProjectionIndex {
            generation_index: 1,
            shared_authority_sha256: sha(manifest, "expectedAuthoritySha256")?.to_owned(),
            accepted_pool_sha256:
                fragment_binding.value["acceptedPoolMembership"]["semanticSha256"]
                    .as_str()
                    .context("G0 accepted pool semantic root is invalid")?
                    .to_owned(),
            selection_sha256: fragment_binding.value["g0Selection"]["semanticSha256"]
                .as_str()
                .context("G0 selection semantic root is invalid")?
                .to_owned(),
            projections: selected_rows
                .iter()
                .map(V5SelectedProjection::from_value)
                .collect::<std::result::Result<Vec<_>, _>>()
                .context("parse compact selected projection rows")?,
        }
    } else {
        let index_object = legacy_inventory_object(inventory, &index_root)?;
        let index_object_path = safe_output_path(
            output_root,
            &format!("v5-native/objects/{}", text(index_object, "relativePath")?),
        )?;
        ensure!(
            file_sha(&index_object_path)? == sha(index_object, "fileSha256")?
                && fs::metadata(&index_object_path)?.len() == unsigned(index_object, "byteLength")?,
            "G0 selected index object binding drifted"
        );
        let index_value =
            read_canonical_value(&index_object_path, "G0 selected projection index object")?;
        V5SelectedProjectionIndex::from_value(&index_value)
            .context("parse historical G0 selected projection index")?
    };
    ensure!(
        index.selected_projection_index_sha256()? == index_root
            && index.generation_index == 1
            && index.shared_authority_sha256 == sha(manifest, "expectedAuthoritySha256")?
            && index.accepted_pool_sha256
                == fragment_binding.value["acceptedPoolMembership"]["semanticSha256"]
            && index.selection_sha256 == fragment_binding.value["g0Selection"]["semanticSha256"],
        "G0 selected projection index root/authority drifted"
    );
    let expected = index
        .projections
        .iter()
        .map(|p| p.to_value())
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("encode selected projections")?;
    ensure!(
        selected_rows == expected,
        "G0 selected projection JSONL differs from immutable index"
    );

    let attempt_rows = read_jsonl(text(stream_file, "absolutePath")?)?;
    let mut by_record = BTreeMap::new();
    for (ordinal, row) in attempt_rows.into_iter().enumerate() {
        validate_attempt(&row, ordinal as u64)?;
        if let Some(record) = row
            .get("acceptedCompactRecordSha256")
            .and_then(Value::as_str)
        {
            ensure!(
                by_record.insert(record.to_owned(), row).is_none(),
                "G0 proposal stream repeats accepted compact record"
            );
        }
    }
    let mut selected = Vec::with_capacity(index.projections.len());
    for projection in &index.projections {
        let row = by_record
            .remove(&projection.record_sha256)
            .context("G0 selected projection does not resolve one construction row")?;
        let candidate = member(&row, "candidate")?;
        ensure!(
            projection.generation_index == 1
                && projection.proposal_ordinal == unsigned(&row, "proposalOrdinal")?
                && projection.origin_kind == text(&row, "originKind")?
                && projection.candidate_id == text(candidate, "candidateId")?
                && projection.compiled.raw_pair_sha256 == sha(candidate, "sourceProfileSha256")?,
            "G0 selected projection/construction row identity drifted"
        );
        selected.push((projection, row));
    }
    selected.sort_by(|(_, a), (_, b)| {
        text(member(a, "candidate").unwrap(), "candidateId")
            .unwrap()
            .cmp(text(member(b, "candidate").unwrap(), "candidateId").unwrap())
    });
    ensure!(
        selected.windows(2).all(|pair| text(
            member(&pair[0].1, "candidate").unwrap(),
            "candidateId"
        )
        .unwrap()
            < text(member(&pair[1].1, "candidate").unwrap(), "candidateId").unwrap()),
        "G0 selected projection repeats candidate ID"
    );
    let evaluation_rows = selected
        .iter()
        .map(|(_, row)| evaluation_row_from_construction(row))
        .collect::<Result<Vec<_>>>()?;
    verify_g0_evaluation_fragment(&fragment_binding.value, &evaluation_rows)?;
    let rows = selected
        .iter()
        .enumerate()
        .map(|(ordinal, (projection, construction))| {
            g0_selected_attempt_row(ordinal as u64, construction, projection)
        })
        .collect::<Result<Vec<_>>>()?;
    let bytes = rows.iter().try_fold(Vec::new(), |mut bytes, row| {
        bytes.extend(canonical_json_line(row)?);
        Ok::<_, anyhow::Error>(bytes)
    })?;
    let accounting = g0_selected_accounting(&rows)?;
    let construction = json!({
        "proposalAttemptCount": funnel_receipt.proposal_attempt_count,
        "acceptedCount": funnel_receipt.accepted_count,
        "selectedCount": funnel_receipt.selected_count,
        "attemptJournalSha256": funnel_receipt.attempt_journal_sha256,
        "acceptedPoolSha256": funnel_receipt.accepted_pool_membership.semantic_sha256,
        "selectionSha256": funnel_receipt.g0_selection.semantic_sha256,
        "campaignLedgerSha256": funnel_receipt.campaign_ledger.semantic_sha256,
        "compactIdentityLedgerSha256": funnel_receipt.compact_identity_ledger.semantic_sha256,
    });
    ensure!(
        unsigned(&construction, "selectedCount")? == rows.len() as u64,
        "G0 selected stream count differs from Core construction receipt"
    );
    let mut accounting_map = accounting
        .as_object()
        .expect("G0 accounting object")
        .clone();
    accounting_map.insert("g0ConstructionProposalAccounting".to_owned(), construction);
    Ok(G0Extracted {
        bytes,
        record_count: rows.len() as u64,
        accounting: Value::Object(accounting_map),
        receipt_sha256: sha(receipt, "receiptSha256")?.to_owned(),
        inventory_sha256: sha(inventory, "outputInventorySha256")?.to_owned(),
        fragments_sha256: fragments_root,
        stream_sha256: stream_root,
        selected_index_sha256: index_root,
    })
}

struct EvolvedInvocation {
    manifest: Value,
    result: Value,
    result_sha256: String,
    receipt_sha256: String,
    inventory_sha256: String,
}

fn validate_evolved_invocation(value: &Value) -> Result<EvolvedInvocation> {
    exact_keys(
        object(value, "evolved native invocation")?,
        &[
            "schemaVersion",
            "proposalManifest",
            "proposalResult",
            "proposalReceiptSha256",
            "outputInventorySha256",
        ],
        "evolved native invocation",
    )?;
    ensure!(
        text(value, "schemaVersion")? == EVOLVED_INVOCATION_SCHEMA,
        "evolved native invocation schema is invalid"
    );
    let manifest = read_invocation_document(
        member(value, "proposalManifest")?,
        OUTER_MANIFEST_SCHEMA,
        "manifest.json",
    )?;
    let manifest_sha = sha(&manifest, "manifestSha256")?.to_owned();
    let result_descriptor = member(value, "proposalResult")?;
    let result_schema = text(result_descriptor, "documentSchemaVersion")?;
    ensure!(
        matches!(
            result_schema,
            EVOLVED_RESULT_SCHEMA | LEGACY_EVOLVED_RESULT_SCHEMA
        ),
        "evolved invocation result schema is not an explicit supported version"
    );
    let result =
        read_invocation_document(result_descriptor, result_schema, "v5-proposal-result.json")?;
    if result_schema == EVOLVED_RESULT_SCHEMA {
        ensure!(
            canonical_sha256_without_object_field(&result, "resultSha256")?
                == sha(&result, "resultSha256")?
                && sha(&result, "manifestSha256")? == sha(&manifest, "manifestSha256")?,
            "current evolved invocation result identity drifted"
        );
        let receipt = member(&result, "receipt")?;
        ensure!(
            text(receipt, "schemaVersion")? == EVOLVED_RECEIPT_SCHEMA
                && canonical_sha256_without_object_field(receipt, "receiptSha256")?
                    == sha(receipt, "receiptSha256")?
                && sha(&result, "receiptSha256")? == sha(receipt, "receiptSha256")?,
            "current evolved invocation receipt identity drifted"
        );
        let inventory = member(receipt, "outputInventory")?;
        ensure!(
            canonical_sha256_without_object_field(inventory, "outputInventorySha256")?
                == sha(inventory, "outputInventorySha256")?
                && sha(receipt, "outputInventorySha256")?
                    == sha(inventory, "outputInventorySha256")?,
            "current evolved invocation inventory identity drifted"
        );
        validate_current_evolved_outer_shape(&manifest, &result, receipt, inventory)?;
    }
    let result_sha = sha(&result, "resultSha256")?.to_owned();
    let expected_root = format!(
        "native-batch/v5-proposal/{}/",
        manifest_sha.strip_prefix("sha256:").unwrap_or("")
    );
    ensure!(
        text(member(value, "proposalManifest")?, "relativePath")?
            == format!("{expected_root}manifest.json")
            && text(member(value, "proposalResult")?, "relativePath")?
                == format!("{expected_root}v5-proposal-result.json")
            && sha(&result, "manifestSha256")? == manifest_sha
            && sha(value, "proposalReceiptSha256")? == sha(&result, "receiptSha256")?
            && sha(value, "outputInventorySha256")? == sha(&result, "outputInventorySha256")?,
        "evolved native invocation binding drifted"
    );
    Ok(EvolvedInvocation {
        manifest,
        result,
        result_sha256: result_sha,
        receipt_sha256: sha(value, "proposalReceiptSha256")?.to_owned(),
        inventory_sha256: sha(value, "outputInventorySha256")?.to_owned(),
    })
}

fn read_invocation_document(value: &Value, expected_schema: &str, filename: &str) -> Result<Value> {
    exact_keys(
        object(value, "evolved invocation document descriptor")?,
        &[
            "schemaVersion",
            "documentSchemaVersion",
            "relativePath",
            "absolutePath",
            "semanticSha256",
            "fileSha256",
            "byteLength",
        ],
        "evolved invocation document descriptor",
    )?;
    ensure!(
        text(value, "schemaVersion")? == INVOCATION_DOCUMENT_SCHEMA
            && text(value, "documentSchemaVersion")? == expected_schema
            && text(value, "relativePath")?.ends_with(filename)
            && !text(value, "relativePath")?
                .split('/')
                .any(|part| part == ".."),
        "evolved invocation document descriptor is invalid"
    );
    let path = Path::new(text(value, "absolutePath")?);
    ensure!(
        path.is_file()
            && !fs::symlink_metadata(path)?.file_type().is_symlink()
            && fs::metadata(path)?.len() == unsigned(value, "byteLength")?
            && file_sha(path)? == sha(value, "fileSha256")?,
        "evolved invocation document file binding drifted"
    );
    let document = read_canonical_value(path, "evolved invocation document")?;
    ensure!(
        text(&document, "schemaVersion")? == expected_schema
            && (if filename == "manifest.json" {
                sha(&document, "manifestSha256")?
            } else {
                sha(&document, "resultSha256")?
            }) == sha(value, "semanticSha256")?,
        "evolved invocation document semantic binding drifted"
    );
    Ok(document)
}

fn read_canonical_value(path: &Path, label: &str) -> Result<Value> {
    let raw = fs::read(path)?;
    let v: Value = serde_json::from_slice(&raw)?;
    ensure!(
        canonical_json_line(&v)? == raw,
        "{label} must be canonical JSON plus LF"
    );
    Ok(v)
}
fn bound_path(value: &Value, label: &str) -> Result<std::path::PathBuf> {
    exact_keys(
        object(value, label)?,
        &["path", "rawSha256", "sizeBytes"],
        label,
    )?;
    let p = Path::new(text(value, "path")?);
    ensure!(
        p.is_file()
            && !fs::symlink_metadata(p)?.file_type().is_symlink()
            && fs::metadata(p)?.len() == unsigned(value, "sizeBytes")?
            && file_sha(p)? == sha(value, "rawSha256")?,
        "{label} descriptor drifted"
    );
    Ok(p.to_path_buf())
}
fn read_bound_json(value: &Value, label: &str) -> Result<Value> {
    let p = bound_path(value, label)?;
    read_canonical_value(&p, label)
}

struct Extracted {
    bytes: Vec<u8>,
    sha256: String,
    count: usize,
}

fn extract(input: &Value) -> Result<Extracted> {
    let fragments = member(input, "coreFragments")?;
    validate_fragments(fragments)?;
    let funnel = member(fragments, "evaluationFunnelEntries")?;
    let evaluation = member(input, "evaluationPopulation")?;
    let path = Path::new(text(evaluation, "path")?);
    ensure!(path.is_file(), "public evaluation population is not a file");
    ensure!(
        fs::metadata(path)?.len() == unsigned(evaluation, "sizeBytes")?,
        "public evaluation population size drifted"
    );
    ensure!(
        file_sha(path)? == sha(evaluation, "rawSha256")?,
        "public evaluation population raw identity drifted"
    );
    let raw = fs::read(path)?;
    let value: Value =
        serde_json::from_slice(&raw).context("parse public evaluation population")?;
    ensure!(
        canonical_json_line(&value)? == raw,
        "public evaluation population must be canonical JSON plus LF"
    );
    ensure!(
        unsigned(&value, "candidateCount")? == unsigned(fragments, "acceptedCandidateCount")?,
        "public evaluation accepted count drifted"
    );
    ensure!(
        unsigned(&value, "proposalAttempts")? == unsigned(fragments, "proposalAttemptCount")?,
        "public evaluation attempt count drifted"
    );
    let rows = array(&value, "funnelEntries")?;
    ensure!(
        rows.len() as u64 == unsigned(funnel, "rowCount")?,
        "public evaluation funnel row count drifted"
    );
    let mut bytes = Vec::new();
    for (index, row) in rows.iter().enumerate() {
        validate_attempt(row, index as u64)?;
        let encoded = canonical_json_bytes(row)?;
        if index != 0 {
            bytes.push(b',');
        }
        bytes.extend(encoded);
    }
    ensure!(
        format!("sha256:{:x}", Sha256::digest(&bytes)) == sha(funnel, "fragmentSha256")?,
        "core v2 funnel fragment bytes drifted"
    );
    ensure!(
        bytes.len() as u64 == unsigned(funnel, "encodedBytes")?,
        "core v2 funnel fragment size drifted"
    );
    let mut jsonl = Vec::new();
    for row in rows {
        jsonl.extend(canonical_json_line(row)?);
    }
    Ok(Extracted {
        sha256: format!("sha256:{:x}", Sha256::digest(&jsonl)),
        bytes: jsonl,
        count: rows.len(),
    })
}

fn validate_input(value: &Value) -> Result<()> {
    let map = object(value, "core funnel adapter input")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "contractVersion",
            "coreFragments",
            "evaluationPopulation",
            "inputSha256",
        ],
        "core funnel adapter input",
    )?;
    ensure!(
        text(value, "schemaVersion")? == INPUT_SCHEMA
            && text(value, "contractVersion")? == CONTRACT_VERSION,
        "core funnel adapter input schema/version is invalid"
    );
    ensure!(
        canonical_sha256_without_object_field(value, "inputSha256")? == sha(value, "inputSha256")?,
        "core funnel adapter input self hash drifted"
    );
    let descriptor = object(
        member(value, "evaluationPopulation")?,
        "public evaluation descriptor",
    )?;
    exact_keys(
        descriptor,
        &["path", "rawSha256", "sizeBytes"],
        "public evaluation descriptor",
    )?;
    Ok(())
}

fn validate_fragments(value: &Value) -> Result<()> {
    let map = object(value, "core v2 fragments")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "acceptedCandidateCount",
            "proposalAttemptCount",
            "populationCandidates",
            "evaluationCandidates",
            "evaluationFunnelEntries",
            "generationJournalBindings",
            "fragmentBundleSha256",
        ],
        "core v2 fragments",
    )?;
    ensure!(
        text(value, "schemaVersion")? == FRAGMENTS_SCHEMA,
        "unsupported core funnel receipt (v2 required)"
    );
    ensure!(
        canonical_sha256_without_object_field(value, "fragmentBundleSha256")?
            == sha(value, "fragmentBundleSha256")?,
        "core v2 fragment receipt self hash drifted"
    );
    let accepted = unsigned(value, "acceptedCandidateCount")?;
    let attempts = unsigned(value, "proposalAttemptCount")?;
    ensure!(
        attempts >= accepted && accepted > 0,
        "core v2 fragment counts are invalid"
    );
    for field in [
        "populationCandidates",
        "evaluationCandidates",
        "generationJournalBindings",
    ] {
        let fragment = member(value, field)?;
        ensure!(
            unsigned(fragment, "rowCount")? == accepted,
            "core v2 accepted fragment count drifted"
        );
    }
    let funnel = member(value, "evaluationFunnelEntries")?;
    ensure!(
        text(funnel, "kind")? == FUNNEL_FRAGMENT_KIND && unsigned(funnel, "rowCount")? == attempts,
        "core v2 funnel fragment count drifted"
    );
    for field in [
        "populationCandidates",
        "evaluationCandidates",
        "evaluationFunnelEntries",
        "generationJournalBindings",
    ] {
        let fragment = member(value, field)?;
        let map = object(fragment, "core v2 fragment")?;
        exact_keys(
            map,
            &["kind", "fragmentSha256", "encodedBytes", "rowCount"],
            "core v2 fragment",
        )?;
        sha(fragment, "fragmentSha256")?;
    }
    Ok(())
}

fn validate_attempt(value: &Value, expected_ordinal: u64) -> Result<()> {
    let map = object(value, "core public funnel attempt")?;
    let candidate_bearing = map.contains_key("candidate");
    let accepted = map.get("disposition").and_then(Value::as_str) == Some("accepted");
    let keys: &[&str] = if !candidate_bearing {
        &[
            "schemaVersion",
            "entrySha256",
            "proposalOrdinal",
            "originKind",
            "disposition",
        ]
    } else if accepted {
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
        ]
    } else {
        &[
            "schemaVersion",
            "entrySha256",
            "proposalOrdinal",
            "originKind",
            "disposition",
            "candidate",
            "proposal",
            "funnelCandidate",
        ]
    };
    exact_keys(map, keys, "core public funnel attempt")?;
    ensure!(
        text(value, "schemaVersion")? == ATTEMPT_ROW_SCHEMA
            && unsigned(value, "proposalOrdinal")? == expected_ordinal,
        "core public funnel attempt schema/ordinal drifted"
    );
    sha(value, "entrySha256")?;
    if candidate_bearing {
        let candidate = member(value, "candidate")?;
        let proposal = member(value, "proposal")?;
        ensure!(
            text(candidate, "candidateId")? == text(proposal, "candidateId")?
                && sha(candidate, "sourceProfileSha256")?
                    == sha(proposal, "rawSourceProfileSha256")?,
            "core public funnel candidate/proposal drifted"
        );
        let stage = member(value, "funnelCandidate")?;
        ensure!(
            text(stage, "schemaVersion")? == "temporal_qd_proposal_funnel_stage_v1"
                && text(stage, "candidateId")? == text(candidate, "candidateId")?,
            "core public funnel stage drifted"
        );
    } else {
        ensure!(
            !accepted,
            "candidate-free core funnel attempt cannot be accepted"
        );
    }
    Ok(())
}

fn verify_descriptor(path: &Path) -> Result<Value> {
    ensure!(path.is_file(), "core funnel attempt stream is not a file");
    let raw = fs::read(path)?;
    let mut count = 0usize;
    for line in raw.split_inclusive(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        ensure!(
            line.ends_with(b"\n"),
            "core funnel attempt stream lacks terminal LF"
        );
        let row: Value = serde_json::from_slice(&line[..line.len() - 1])?;
        validate_attempt(&row, count as u64)?;
        count += 1;
    }
    Ok(
        json!({"path":path,"rawSha256":format!("sha256:{:x}",Sha256::digest(&raw)),"sizeBytes":raw.len(),"recordCount":count}),
    )
}

#[cfg(test)]
mod compact_closure_tests {
    use super::*;

    fn digest(tag: &str) -> String {
        canonical_sha256(&json!({"tag":tag})).unwrap()
    }

    fn seal(mut value: Value, field: &str) -> Value {
        let hash = canonical_sha256(&value).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert(field.to_owned(), Value::String(hash));
        value
    }

    fn reseal(value: &mut Value, field: &str) {
        value.as_object_mut().unwrap().remove(field);
        let hash = canonical_sha256(value).unwrap();
        value[field] = Value::String(hash);
    }

    fn compact_inventory(roles: &[&str], artifacts: &[(&str, &str)]) -> Value {
        let descriptor = seal(
            json!({
                "schemaVersion":OBJECT_INVENTORY_DESCRIPTOR_SCHEMA,
                "rowSchemaVersion":OBJECT_INVENTORY_ROW_SCHEMA,
                "relativePath":OBJECT_INVENTORY_PATH,
                "fileSha256":digest("sidecar-file"),
                "byteLength":4096,
                "objectCount":4000,
                "objectByteCount":9000000,
            }),
            "descriptorSha256",
        );
        let roots = roles
            .iter()
            .map(|role| {
                let object = digest(role);
                json!({
                    "role":role,
                    "relativePath":format!("sha256/{}.json", object.trim_start_matches("sha256:")),
                    "objectSha256":object,
                    "fileSha256":digest(&format!("{role}-file")),
                    "byteLength":100,
                })
            })
            .collect::<Vec<_>>();
        let store = seal(
            json!({
                "schemaVersion":OBJECT_STORE_CLOSURE_SCHEMA,
                "relativeRoot":"v5-native/objects",
                "inventory":descriptor,
                "roots":roots,
            }),
            "objectStoreSha256",
        );
        let artifact_values = artifacts
            .iter()
            .map(|(kind, path)| {
                json!({
                    "kind":kind,
                    "relativePath":path,
                    "fileSha256":digest(&format!("{kind}-file")),
                    "byteLength":10,
                    "semanticSha256":digest(&format!("{kind}-semantic")),
                })
            })
            .collect::<Vec<_>>();
        seal(
            json!({
                "schemaVersion":OUTPUT_INVENTORY_SCHEMA,
                "outputRoot":"C:/sealed/output",
                "outputRootSha256":digest("output-root"),
                "artifacts":artifact_values,
                "objectStore":store,
            }),
            "outputInventorySha256",
        )
    }

    #[test]
    fn compact_closure_is_bounded_and_never_requires_the_sidecar_file() {
        let roles = [
            "publicationFragments",
            "publicationPlan",
            "publicationReceipt",
            "transaction",
        ];
        let inventory = compact_inventory(&roles, &[]);
        validate_compact_inventory_shape(&inventory, &roles).unwrap();
        assert_eq!(inventory["objectStore"]["inventory"]["objectCount"], 4000);
    }

    #[test]
    fn compact_closure_rejects_missing_reordered_duplicate_foreign_and_tampered_roots() {
        let roles = [
            "publicationFragments",
            "publicationPlan",
            "publicationReceipt",
            "transaction",
        ];
        for mutation in ["missing", "reordered", "duplicate", "foreign", "traversal"] {
            let mut inventory = compact_inventory(&roles, &[]);
            let roots = inventory["objectStore"]["roots"].as_array_mut().unwrap();
            match mutation {
                "missing" => {
                    roots.pop();
                }
                "reordered" => roots.swap(0, 1),
                "duplicate" => roots[1] = roots[0].clone(),
                "foreign" => roots[0]["role"] = json!("foreignRoot"),
                "traversal" => roots[0]["relativePath"] = json!("../foreign.json"),
                _ => unreachable!(),
            }
            reseal(&mut inventory["objectStore"], "objectStoreSha256");
            reseal(&mut inventory, "outputInventorySha256");
            assert!(
                validate_compact_inventory_shape(&inventory, &roles).is_err(),
                "{mutation} self-consistent compact root substitution must reject"
            );
        }
        let mut descriptor_tamper = compact_inventory(&roles, &[]);
        descriptor_tamper["objectStore"]["inventory"]["relativePath"] =
            json!("../object-inventory.jsonl");
        reseal(
            &mut descriptor_tamper["objectStore"]["inventory"],
            "descriptorSha256",
        );
        reseal(&mut descriptor_tamper["objectStore"], "objectStoreSha256");
        reseal(&mut descriptor_tamper, "outputInventorySha256");
        assert!(validate_compact_inventory_shape(&descriptor_tamper, &roles).is_err());
    }

    #[test]
    fn current_evolved_v3_outer_shape_accepts_only_the_exact_compact_closure() {
        let roles = [
            "publicationFragments",
            "publicationPlan",
            "publicationReceipt",
            "transaction",
        ];
        let artifacts = [
            ("evaluationPopulation", "evaluation-population.json"),
            ("generationJournal", "generation-journal.json"),
            ("identityLedger", "v5-native/identity-ledger.json"),
            ("pairConfig", "pair-config.json"),
            ("population", "population.json"),
        ];
        let inventory = compact_inventory(&roles, &artifacts);
        let manifest_sha = digest("manifest");
        let authority = digest("authority");
        let expected_authority = digest("expected-authority");
        let config = digest("config");
        let parent = digest("parent-binding");
        let ledger_input = digest("ledger-input-binding");
        let request = digest("publication-request");
        let ledger = digest("ledger");
        let manifest = json!({
            "schemaVersion":OUTER_MANIFEST_SCHEMA,
            "generationKind":"evolved",
            "outputRoot":"C:/sealed/output",
            "manifestSha256":manifest_sha,
        });
        let receipt = seal(
            json!({
                "schemaVersion":EVOLVED_RECEIPT_SCHEMA,
                "authoritySha256":authority,
                "manifestSha256":manifest_sha,
                "expectedAuthoritySha256":expected_authority,
                "generationConfigSha256":config,
                "generationIndex":2,
                "requestedCount":1,
                "acceptedRecordCount":1,
                "attemptCount":4,
                "transactionSha256":digest("transaction"),
                "parentArchiveInputBindingSha256":parent,
                "identityLedgerInputBindingSha256":ledger_input,
                "publicationRequestSha256":request,
                "publicationPlanSha256":digest("publicationPlan"),
                "publicationReceiptSha256":digest("publicationReceipt"),
                "publicationFragmentsSha256":digest("publicationFragments"),
                "evaluationPopulationSize":1,
                "identityLedgerSha256":ledger,
                "outputInventory":inventory,
                "outputInventorySha256":inventory["outputInventorySha256"],
                "nativeBatchAuthoritySha256":digest("batch"),
                "threadCap":1,
                "constructionSummary":{},
            }),
            "receiptSha256",
        );
        let result = seal(
            json!({
                "schemaVersion":EVOLVED_RESULT_SCHEMA,
                "contractVersion":CONTRACT_VERSION,
                "operation":"native_v5_proposal_construction",
                "status":"completed",
                "authoritySha256":authority,
                "manifestSha256":manifest_sha,
                "expectedAuthoritySha256":expected_authority,
                "generationConfigSha256":config,
                "generationIndex":2,
                "requestedCount":1,
                "acceptedRecordCount":1,
                "attemptCount":4,
                "transactionSha256":digest("transaction"),
                "parentArchiveInputBindingSha256":parent,
                "identityLedgerInputBindingSha256":ledger_input,
                "publicationRequestSha256":request,
                "publicationPlanSha256":digest("publicationPlan"),
                "publicationReceiptSha256":digest("publicationReceipt"),
                "publicationFragmentsSha256":digest("publicationFragments"),
                "evaluationPopulationSize":1,
                "identityLedgerSha256":ledger,
                "outputInventorySha256":inventory["outputInventorySha256"],
                "receipt":receipt,
                "receiptSha256":receipt["receiptSha256"],
            }),
            "resultSha256",
        );
        validate_current_evolved_outer_shape(&manifest, &result, &receipt, &inventory).unwrap();

        let mut same_schema_array_substitution = inventory;
        same_schema_array_substitution["objectStore"] = json!({"objects":[]});
        assert!(
            validate_current_evolved_outer_shape(
                &manifest,
                &result,
                &receipt,
                &same_schema_array_substitution,
            )
            .is_err()
        );
    }
}
