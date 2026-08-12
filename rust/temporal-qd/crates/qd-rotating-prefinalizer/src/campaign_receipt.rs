//! Frozen v2 native campaign receipt. Execution paths never enter its semantic hash.
use super::{exact_keys, file_sha, member, object, sha, text, unsigned};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::{fs, path::Path};
use temporal_qd_contract::{
    CONTRACT_VERSION, canonical_sha256, canonical_sha256_without_object_field,
};
pub const INPUT_SCHEMA: &str = "temporal_qd_v5_rotating_campaign_receipt_input_v2";
pub const RECEIPT_SCHEMA: &str = "temporal_qd_v5_rotating_campaign_receipt_v2";
pub const OUTPUT_CHECKPOINT_SCHEMA: &str = "temporal_qd_v5_campaign_output_checkpoint_v1";

pub fn build_to_path(input_path: &Path, output_path: &Path) -> Result<Value> {
    let raw = fs::read(input_path).context("read campaign receipt input")?;
    let input: Value = serde_json::from_slice(&raw).context("parse campaign receipt input")?;
    ensure!(
        temporal_qd_contract::canonical_json_line(&input)? == raw,
        "campaign receipt input must be canonical JSON plus LF"
    );
    let receipt = build(&input)?;
    if output_path.exists() {
        let existing: Value = serde_json::from_slice(&fs::read(output_path)?)?;
        validate_receipt(&existing)?;
        ensure!(existing == receipt, "immutable campaign receipt differs");
    } else {
        let mut out = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output_path)?;
        use std::io::Write;
        out.write_all(&temporal_qd_contract::canonical_json_line(&receipt)?)?;
        out.sync_all()?;
    }
    Ok(receipt)
}

/// Re-validates a committed receipt without consulting worker raw results.
/// This is the admission boundary used by the chained prefinalizer manifests.
pub fn validate_receipt(value: &Value) -> Result<()> {
    match text(value, "schemaVersion")? {
        RECEIPT_SCHEMA => validate_v2_receipt(value),
        OUTPUT_CHECKPOINT_SCHEMA => validate_output_checkpoint(value),
        _ => anyhow::bail!("campaign receipt schema is incompatible"),
    }
}

fn validate_v2_receipt(value: &Value) -> Result<()> {
    let m = object(value, "campaign receipt")?;
    exact_keys(
        m,
        &[
            "schemaVersion",
            "contractVersion",
            "generationIndex",
            "campaignRole",
            "panelId",
            "rotatingEvidenceSha256",
            "cohortSource",
            "campaignFreeze",
            "campaignSeal",
            "evaluatedMembers",
            "candidatePanelBundles",
            "semanticReceiptSha256",
            "runtimeAuthoritySha256",
            "executionBindings",
            "receiptSha256",
        ],
        "campaign receipt",
    )?;
    ensure!(
        text(value, "schemaVersion")? == RECEIPT_SCHEMA
            && text(value, "contractVersion")? == CONTRACT_VERSION
            && unsigned(value, "generationIndex")? > 0
            && matches!(
                text(value, "campaignRole")?,
                "proposal_current_panel" | "retained_parent_current_panel" | "prior_panel_backfill"
            ),
        "campaign receipt schema/version/role is invalid"
    );
    let mut semantic = json!({
        "schemaVersion": RECEIPT_SCHEMA, "contractVersion": CONTRACT_VERSION,
        "generationIndex": unsigned(value,"generationIndex")?, "campaignRole": text(value,"campaignRole")?,
        "panelId": text(value,"panelId")?, "rotatingEvidenceSha256": sha(value,"rotatingEvidenceSha256")?,
        "cohortSource": member(value,"cohortSource")?, "campaignFreeze": member(value,"campaignFreeze")?,
        "campaignSeal": member(value,"campaignSeal")?, "evaluatedMembers": member(value,"evaluatedMembers")?,
        "candidatePanelBundles": member(value,"candidatePanelBundles")?,
    });
    ensure!(
        canonical_sha256(&semantic)? == sha(value, "semanticReceiptSha256")?,
        "campaign semantic receipt hash drifted"
    );
    let mut full = semantic.as_object_mut().expect("object").clone();
    full.insert(
        "semanticReceiptSha256".into(),
        member(value, "semanticReceiptSha256")?.clone(),
    );
    full.insert(
        "runtimeAuthoritySha256".into(),
        member(value, "runtimeAuthoritySha256")?.clone(),
    );
    full.insert(
        "executionBindings".into(),
        member(value, "executionBindings")?.clone(),
    );
    ensure!(
        canonical_sha256(&Value::Object(full))? == sha(value, "receiptSha256")?,
        "campaign receipt hash drifted"
    );
    // Check all descriptors' shapes now.  The prefinalizer deliberately reads
    // just the sealed JSONL files later, and therefore a restart reads no raw
    // gateway result.
    let execution = member(value, "executionBindings")?;
    let em = object(execution, "campaign execution bindings")?;
    for key in [
        "freezeManifest",
        "freezeTransaction",
        "campaign",
        "taskManifest",
        "evaluationIdentity",
        "gatewayCompletion",
        "checkpoint",
        "campaignSeal",
        "tailResultIndex",
        "tailTransaction",
        "evaluatedMembersJsonl",
        "candidatePanelBundlesJsonl",
    ] {
        let d = em
            .get(key)
            .ok_or_else(|| anyhow::anyhow!("campaign execution binding missing {key}"))?;
        exact_keys(
            object(d, "campaign execution descriptor")?,
            &["path", "rawSha256", "sizeBytes"],
            "campaign execution descriptor",
        )?;
        sha(d, "rawSha256")?;
        unsigned(d, "sizeBytes")?;
    }
    Ok(())
}

fn validate_output_checkpoint(value: &Value) -> Result<()> {
    let m = object(value, "campaign-output checkpoint")?;
    exact_keys(
        m,
        &[
            "schemaVersion",
            "contractVersion",
            "generationIndex",
            "campaignRole",
            "panelId",
            "rotatingEvidenceSha256",
            "cohortSource",
            "campaignInput",
            "campaignSeal",
            "campaignSealDocument",
            "evaluatedMembers",
            "candidatePanelBundles",
            "semanticReceiptSha256",
            "manifestSha256",
            "runtimeAuthoritySha256",
            "gatewayReceiptSha256",
            "gatewaySemanticReceiptSha256",
            "executionBindings",
            "receiptSha256",
        ],
        "campaign-output checkpoint",
    )?;
    ensure!(
        text(value, "schemaVersion")? == OUTPUT_CHECKPOINT_SCHEMA
            && text(value, "contractVersion")? == CONTRACT_VERSION
            && unsigned(value, "generationIndex")? > 0
            && matches!(
                text(value, "campaignRole")?,
                "proposal_current_panel" | "retained_parent_current_panel" | "prior_panel_backfill"
            ),
        "campaign-output checkpoint schema/version/role is invalid"
    );
    for field in [
        "rotatingEvidenceSha256",
        "semanticReceiptSha256",
        "manifestSha256",
        "runtimeAuthoritySha256",
        "gatewayReceiptSha256",
        "gatewaySemanticReceiptSha256",
        "receiptSha256",
    ] {
        sha(value, field)?;
    }
    let cohort = member(value, "cohortSource")?;
    let cohort_map = object(cohort, "campaign-output cohort source")?;
    exact_keys(
        cohort_map,
        &[
            "kind",
            "sourceSemanticSha256",
            "candidateCount",
            "selectionSha256",
        ],
        "campaign-output cohort source",
    )?;
    let proposal = text(cohort, "kind")? == "proposal_evaluation_population";
    ensure!(
        proposal || text(cohort, "kind")? == "sealed_cohort_selection",
        "campaign-output cohort source kind is invalid"
    );
    sha(cohort, "sourceSemanticSha256")?;
    unsigned(cohort, "candidateCount")?;
    if proposal {
        ensure!(
            member(cohort, "selectionSha256")?.is_null(),
            "proposal cohort source cannot name a selection"
        );
    } else {
        sha(cohort, "selectionSha256")?;
    }

    let campaign_input = member(value, "campaignInput")?;
    exact_keys(
        object(campaign_input, "campaign-input checkpoint summary")?,
        &[
            "checkpointSha256",
            "cohortPopulationSha256",
            "authorityId",
            "evaluationIdentitySha256",
            "campaignSha256",
            "taskMatrixSha256",
            "candidateCount",
            "windowCount",
            "taskCount",
        ],
        "campaign-input checkpoint summary",
    )?;
    for field in [
        "checkpointSha256",
        "cohortPopulationSha256",
        "authorityId",
        "evaluationIdentitySha256",
        "campaignSha256",
        "taskMatrixSha256",
    ] {
        sha(campaign_input, field)?;
    }
    for field in ["candidateCount", "windowCount", "taskCount"] {
        unsigned(campaign_input, field)?;
    }

    let campaign_seal = member(value, "campaignSeal")?;
    exact_keys(
        object(campaign_seal, "campaign-output seal summary")?,
        &[
            "directionalTailAuthoritySha256",
            "campaignSealSha256",
            "tailResultIndexSha256",
            "tailTransactionSha256",
        ],
        "campaign-output seal summary",
    )?;
    for field in [
        "directionalTailAuthoritySha256",
        "campaignSealSha256",
        "tailResultIndexSha256",
        "tailTransactionSha256",
    ] {
        sha(campaign_seal, field)?;
    }
    let campaign_seal_document = member(value, "campaignSealDocument")?;
    ensure!(
        text(campaign_seal_document, "schemaVersion")? == "temporal_qd_campaign_seal_v1"
            && canonical_sha256_without_object_field(campaign_seal_document, "campaignSealSha256",)?
                == sha(campaign_seal_document, "campaignSealSha256")?
            && sha(campaign_seal_document, "campaignSealSha256")?
                == sha(campaign_seal, "campaignSealSha256")?,
        "campaign-output campaign seal document drifted"
    );

    for key in ["evaluatedMembers", "candidatePanelBundles"] {
        let descriptor = member(value, key)?;
        exact_keys(
            object(descriptor, key)?,
            &["rawSha256", "sizeBytes", "recordCount", "rowSchema"],
            key,
        )?;
        sha(descriptor, "rawSha256")?;
        unsigned(descriptor, "sizeBytes")?;
        unsigned(descriptor, "recordCount")?;
    }
    ensure!(
        text(member(value, "evaluatedMembers")?, "rowSchema")? == "temporal_qd_evaluated_member_v1"
            && text(member(value, "candidatePanelBundles")?, "rowSchema")?
                == "temporal_qd_candidate_panel_evidence_bundle_v1",
        "campaign-output row schema drifted"
    );

    let mut semantic = json!({
        "schemaVersion": OUTPUT_CHECKPOINT_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "generationIndex": unsigned(value, "generationIndex")?,
        "campaignRole": text(value, "campaignRole")?,
        "panelId": text(value, "panelId")?,
        "rotatingEvidenceSha256": sha(value, "rotatingEvidenceSha256")?,
        "cohortSource": cohort,
        "campaignInput": campaign_input,
        "campaignSeal": campaign_seal,
        "campaignSealDocument": campaign_seal_document,
        "evaluatedMembers": member(value, "evaluatedMembers")?,
        "candidatePanelBundles": member(value, "candidatePanelBundles")?,
    });
    ensure!(
        canonical_sha256(&semantic)? == sha(value, "semanticReceiptSha256")?,
        "campaign-output semantic receipt hash drifted"
    );

    let execution = member(value, "executionBindings")?;
    let execution_map = object(execution, "campaign-output execution bindings")?;
    exact_keys(
        execution_map,
        &[
            "campaignInputCheckpoint",
            "gatewayExecutionReceipt",
            "tailResultIndex",
            "evaluatedMembersJsonl",
            "candidatePanelBundlesJsonl",
        ],
        "campaign-output execution bindings",
    )?;
    for key in execution_map.keys() {
        let descriptor = member(execution, key)?;
        exact_keys(
            object(descriptor, "campaign-output execution descriptor")?,
            &["path", "rawSha256", "sizeBytes"],
            "campaign-output execution descriptor",
        )?;
        ensure!(
            Path::new(text(descriptor, "path")?).is_absolute(),
            "campaign-output execution descriptor path must be absolute"
        );
        sha(descriptor, "rawSha256")?;
        unsigned(descriptor, "sizeBytes")?;
    }

    let full = semantic.as_object_mut().expect("semantic receipt object");
    for field in [
        "semanticReceiptSha256",
        "manifestSha256",
        "runtimeAuthoritySha256",
        "gatewayReceiptSha256",
        "gatewaySemanticReceiptSha256",
        "executionBindings",
    ] {
        full.insert(field.into(), member(value, field)?.clone());
    }
    ensure!(
        canonical_sha256(&semantic)? == sha(value, "receiptSha256")?,
        "campaign-output checkpoint hash drifted"
    );
    Ok(())
}

pub fn build(input: &Value) -> Result<Value> {
    let m = object(input, "campaign receipt input")?;
    exact_keys(
        m,
        &[
            "schemaVersion",
            "contractVersion",
            "generationIndex",
            "campaignRole",
            "panelId",
            "rotatingEvidenceSha256",
            "cohortSource",
            "campaignFreeze",
            "campaignSeal",
            "evaluatedMembers",
            "candidatePanelBundles",
            "runtimeAuthoritySha256",
            "executionBindings",
            "inputSha256",
        ],
        "campaign receipt input",
    )?;
    ensure!(
        text(input, "schemaVersion")? == INPUT_SCHEMA
            && text(input, "contractVersion")? == CONTRACT_VERSION
            && matches!(
                text(input, "campaignRole")?,
                "proposal_current_panel" | "retained_parent_current_panel" | "prior_panel_backfill"
            )
            && unsigned(input, "generationIndex")? > 0,
        "campaign receipt input invalid"
    );
    ensure!(
        canonical_sha256_without_object_field(input, "inputSha256")? == sha(input, "inputSha256")?,
        "campaign receipt input hash drifted"
    );
    let cohort = member(input, "cohortSource")?;
    let cm = object(cohort, "cohort source")?;
    exact_keys(
        cm,
        &[
            "kind",
            "sourceSemanticSha256",
            "candidateCount",
            "selectionSha256",
        ],
        "cohort source",
    )?;
    let proposal = text(cohort, "kind")? == "proposal_evaluation_population";
    ensure!(
        proposal || text(cohort, "kind")? == "sealed_cohort_selection",
        "cohort source kind invalid"
    );
    if proposal {
        ensure!(
            cohort["selectionSha256"].is_null(),
            "proposal cohort must not carry selection"
        )
    } else {
        sha(cohort, "selectionSha256")?;
    }
    let freeze = member(input, "campaignFreeze")?;
    let f = object(freeze, "campaign freeze")?;
    exact_keys(
        f,
        &[
            "transactionSha256",
            "cohortPopulationSha256",
            "preparationSha256",
            "authorityId",
            "evaluationIdentitySha256",
            "campaignSha256",
            "taskMatrixSha256",
            "candidateCount",
            "windowCount",
            "taskCount",
        ],
        "campaign freeze",
    )?;
    for k in [
        "transactionSha256",
        "cohortPopulationSha256",
        "preparationSha256",
        "authorityId",
        "evaluationIdentitySha256",
        "campaignSha256",
        "taskMatrixSha256",
    ] {
        sha(freeze, k)?;
    }
    let seal = member(input, "campaignSeal")?;
    let s = object(seal, "campaign seal")?;
    exact_keys(
        s,
        &[
            "directionalTailAuthoritySha256",
            "campaignSealSha256",
            "tailResultIndexSha256",
            "tailTransactionSha256",
        ],
        "campaign seal",
    )?;
    for k in s.keys() {
        sha(seal, k)?;
    }
    for key in ["evaluatedMembers", "candidatePanelBundles"] {
        let d = member(input, key)?;
        let dm = object(d, key)?;
        exact_keys(
            dm,
            &["rawSha256", "sizeBytes", "recordCount", "rowSchema"],
            key,
        )?;
        sha(d, "rawSha256")?;
        unsigned(d, "sizeBytes")?;
        unsigned(d, "recordCount")?;
    }
    ensure!(
        text(member(input, "evaluatedMembers")?, "rowSchema")? == "temporal_qd_evaluated_member_v1"
            && text(member(input, "candidatePanelBundles")?, "rowSchema")?
                == "temporal_qd_candidate_panel_evidence_bundle_v1",
        "campaign receipt row schema drifted"
    );
    validate_execution(
        member(input, "executionBindings")?,
        freeze,
        seal,
        member(input, "evaluatedMembers")?,
        member(input, "candidatePanelBundles")?,
    )?;
    let mut semantic = json!({"schemaVersion":RECEIPT_SCHEMA,"contractVersion":CONTRACT_VERSION,"generationIndex":unsigned(input,"generationIndex")?,"campaignRole":text(input,"campaignRole")?,"panelId":text(input,"panelId")?,"rotatingEvidenceSha256":sha(input,"rotatingEvidenceSha256")?,"cohortSource":cohort,"campaignFreeze":freeze,"campaignSeal":seal,"evaluatedMembers":member(input,"evaluatedMembers")?,"candidatePanelBundles":member(input,"candidatePanelBundles")?});
    let semantic_hash = canonical_sha256(&semantic)?;
    let mut out = semantic.as_object_mut().unwrap().clone();
    out.insert("semanticReceiptSha256".into(), Value::String(semantic_hash));
    out.insert(
        "runtimeAuthoritySha256".into(),
        member(input, "runtimeAuthoritySha256")?.clone(),
    );
    out.insert(
        "executionBindings".into(),
        member(input, "executionBindings")?.clone(),
    );
    let mut value = Value::Object(out);
    let h = canonical_sha256(&value)?;
    value
        .as_object_mut()
        .unwrap()
        .insert("receiptSha256".into(), Value::String(h));
    Ok(value)
}
fn validate_execution(
    v: &Value,
    freeze: &Value,
    seal: &Value,
    members: &Value,
    bundles: &Value,
) -> Result<()> {
    let m = object(v, "execution bindings")?;
    for key in [
        "freezeManifest",
        "freezeTransaction",
        "campaign",
        "taskManifest",
        "evaluationIdentity",
        "gatewayCompletion",
        "checkpoint",
        "campaignSeal",
        "tailResultIndex",
        "tailTransaction",
        "evaluatedMembersJsonl",
        "candidatePanelBundlesJsonl",
    ] {
        let d = member(v, key)?;
        let dm = object(d, key)?;
        exact_keys(
            dm,
            &["path", "rawSha256", "sizeBytes"],
            "execution descriptor",
        )?;
        let p = Path::new(text(d, "path")?);
        ensure!(
            p.is_file()
                && fs::metadata(p)?.len() == unsigned(d, "sizeBytes")?
                && file_sha(p)? == sha(d, "rawSha256")?,
            "execution binding {key} drifted"
        );
    }
    let _ = (m, freeze, seal, members, bundles);
    Ok(())
}
