//! Experiment-only topology co-adaptation overlay v5.
//!
//! Pair-leg receipts, graph chaining, and interaction vs useful innovation.
//! Parsing this contract never schedules market work.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::json;
use temporal_qd_contract::{Map, Value, canonical_sha256};

pub const COADAPTATION_SCHEMA: &str = "temporal_qd_topology_coadaptation_matrix_v5";
pub const COADAPTATION_MODE: &str =
    "frozen_complete_2x2_insert_setup_then_topology_local_event_pair_receipts_v5";
const ARMS: [&str; 4] = [
    "exact_parent_clone",
    "topology_only_child",
    "event_only_control",
    "topology_then_topology_local_event",
];
const ROOT_KEYS: [&str; 24] = [
    "schemaVersion",
    "mode",
    "includeCrossover",
    "cloneControl",
    "productionArchiveWrite",
    "mutationDepth",
    "morphologyNurseryDeferred",
    "lexicographicFirstSettlingPlanForbidden",
    "parents",
    "panelIdentities",
    "firstExperimentOperation",
    "forbiddenFirstExperimentOperations",
    "topologyLocalEventRequired",
    "arms",
    "topologyPlans",
    "eventPrimitives",
    "slots",
    "blocks",
    "materializationReceipts",
    "designScope",
    "settling",
    "successCalculation",
    "notAdmittedOnFrontGenerationPath",
    "contractSha256",
];
const RECEIPT_KEYS: [&str; 35] = [
    "receiptId",
    "blockId",
    "arm",
    "parentCandidateId",
    "side",
    "eligibility",
    "changedSideGenomeSha256",
    "changedSideProgramSha256",
    "changedSideProfileSha256",
    "topologySignature",
    "resourceFingerprint",
    "longProgramSha256",
    "shortProgramSha256",
    "unchangedOppositeProgramSha256",
    "unchangedOppositeProgramPreserved",
    "reconstructedPairProgramIdentitySha256",
    "frozenPairIdentitySha256",
    "pairCandidateIdentitySha256",
    "pairProfileSha256",
    "normalizedProfileSnapshotSha256",
    "moduleCompilerPolicySha256",
    "moduleCompileArtifactSha256",
    "canonicalPairCompilerAuthoritySha256",
    "canonicalPairCompileReportSha256",
    "nativeValidationRan",
    "nativeValidationAuthoritySha256",
    "nativeValidationReportSha256",
    "pairCompileStatus",
    "applicationParentGenomeSha256",
    "applicationChildGenomeSha256",
    "topologySemanticDelta",
    "operatorApplicationAudit",
    "eventAttachesToAddedSetupNode",
    "productionArchiveWrite",
    "failureReason",
];
const FORBIDDEN_RECEIPT_LABELS: [&str; 2] = ["pairIdentitySha256", "nativeCompileValidationIdentity"];

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum CoadaptationV5Error {
    #[error("{0}")]
    Contract(String),
}

pub type Result<T> = std::result::Result<T, CoadaptationV5Error>;

fn contract(message: impl Into<String>) -> CoadaptationV5Error {
    CoadaptationV5Error::Contract(message.into())
}

fn unexpected(label: &str) -> CoadaptationV5Error {
    contract(format!("{label} has an unexpected schema"))
}

fn exact_keys(object: &Map<String, Value>, required: &[&str], label: &str) -> Result<()> {
    let known: BTreeSet<&str> = required.iter().copied().collect();
    if !required.iter().all(|key| object.contains_key(*key))
        || object.keys().any(|key| !known.contains(key.as_str()))
    {
        return Err(unexpected(label));
    }
    Ok(())
}

fn object<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>> {
    value.as_object().ok_or_else(|| unexpected(label))
}

fn text<'a>(fields: &'a Map<String, Value>, key: &str, label: &str) -> Result<&'a str> {
    fields
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| contract(format!("{label} drifted")))
}

fn require_text(fields: &Map<String, Value>, key: &str, expected: &str, label: &str) -> Result<()> {
    if text(fields, key, label)? != expected {
        return Err(contract(format!("{label} drifted")));
    }
    Ok(())
}

fn require_bool(fields: &Map<String, Value>, key: &str, expected: bool, label: &str) -> Result<()> {
    match fields.get(key).and_then(Value::as_bool) {
        Some(value) if value == expected => Ok(()),
        _ => Err(contract(format!("{label} drifted"))),
    }
}

fn require_sha(value: &Value, label: &str) -> Result<String> {
    let text = value
        .as_str()
        .ok_or_else(|| contract(format!("{label} drifted")))?;
    if text.len() != 71
        || !text.starts_with("sha256:")
        || !text.as_bytes()[7..]
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(contract(format!("{label} drifted")));
    }
    Ok(text.to_owned())
}

pub fn from_generation_config(config: &Value) -> Result<Option<Value>> {
    let Some(fields) = config.as_object() else {
        return Ok(None);
    };
    match fields.get("topologyCoadaptationMatrix") {
        None | Some(Value::Null) => Ok(None),
        Some(value) => {
            validate(value)?;
            Ok(Some(value.clone()))
        }
    }
}

pub fn validate(payload: &Value) -> Result<Value> {
    let fields = object(payload, "topology coadaptation v5")?;
    exact_keys(fields, &ROOT_KEYS, "topology coadaptation v5")?;
    require_text(fields, "schemaVersion", COADAPTATION_SCHEMA, "schema")?;
    require_text(fields, "mode", COADAPTATION_MODE, "mode")?;
    require_bool(fields, "includeCrossover", false, "includeCrossover")?;
    require_bool(fields, "productionArchiveWrite", false, "productionArchiveWrite")?;
    require_bool(
        fields,
        "notAdmittedOnFrontGenerationPath",
        true,
        "notAdmittedOnFrontGenerationPath",
    )?;
    let success = object(
        fields
            .get("successCalculation")
            .ok_or_else(|| unexpected("successCalculation"))?,
        "successCalculation",
    )?;
    require_bool(
        success,
        "usefulProgressiveInnovationRequiresTeGreaterThanP",
        true,
        "usefulProgressiveInnovationRequiresTeGreaterThanP",
    )?;
    require_bool(
        success,
        "promisingMeansUsefulProgressiveInnovationNotMereInteraction",
        true,
        "promisingMeansUsefulProgressiveInnovationNotMereInteraction",
    )?;
    let design = object(
        fields
            .get("designScope")
            .ok_or_else(|| unexpected("designScope"))?,
        "designScope",
    )?;
    require_bool(design, "doNotLaunch", true, "doNotLaunch")?;
    require_bool(design, "insertSetupIsTimingMutation", true, "insertSetupIsTimingMutation")?;

    let mut parent_programs: BTreeMap<String, (String, String)> = BTreeMap::new();
    for item in fields
        .get("parents")
        .and_then(Value::as_array)
        .ok_or_else(|| unexpected("parents"))?
    {
        let parent = object(item, "parent")?;
        let candidate = text(parent, "candidateId", "parent")?.to_owned();
        parent_programs.insert(
            candidate,
            (
                require_sha(
                    parent
                        .get("longProgramSha256")
                        .ok_or_else(|| unexpected("longProgramSha256"))?,
                    "longProgramSha256",
                )?,
                require_sha(
                    parent
                        .get("shortProgramSha256")
                        .ok_or_else(|| unexpected("shortProgramSha256"))?,
                    "shortProgramSha256",
                )?,
            ),
        );
    }

    let mut complete_blocks: BTreeSet<String> = BTreeSet::new();
    for item in fields
        .get("blocks")
        .and_then(Value::as_array)
        .ok_or_else(|| unexpected("blocks"))?
    {
        let block = object(item, "block")?;
        if text(block, "classification", "classification")? == "complete_2x2_block" {
            complete_blocks.insert(text(block, "blockId", "blockId")?.to_owned());
        }
    }

    let mut complete_arms: BTreeMap<String, BTreeMap<String, Map<String, Value>>> = complete_blocks
        .iter()
        .cloned()
        .map(|block_id| (block_id, BTreeMap::new()))
        .collect();
    let receipts = fields
        .get("materializationReceipts")
        .and_then(Value::as_array)
        .ok_or_else(|| unexpected("materializationReceipts"))?;
    for item in receipts {
        let row = object(item, "materialization receipt")?;
        for forbidden in FORBIDDEN_RECEIPT_LABELS {
            if row.contains_key(forbidden) {
                return Err(contract(
                    "module hashes cannot be labeled as pair or native validation identities",
                ));
            }
        }
        exact_keys(row, &RECEIPT_KEYS, "materialization receipt")?;
        require_bool(row, "productionArchiveWrite", false, "productionArchiveWrite")?;
        require_bool(row, "nativeValidationRan", false, "nativeValidationRan")?;
        let block_id = text(row, "blockId", "receipt block")?.to_owned();
        let arm = text(row, "arm", "receipt arm")?.to_owned();
        if complete_blocks.contains(&block_id) {
            require_text(row, "eligibility", "eligible", "complete-block receipts must materialize")?;
            let parent_id = text(row, "parentCandidateId", "parentCandidateId")?;
            let side = text(row, "side", "side")?;
            let changed = require_sha(
                row.get("changedSideProgramSha256")
                    .ok_or_else(|| unexpected("changedSideProgramSha256"))?,
                "changedSideProgramSha256",
            )?;
            let programs = parent_programs
                .get(parent_id)
                .ok_or_else(|| contract("receipt parent is not in the frozen parent set"))?;
            let (long, short) = if side == "long" {
                (changed.clone(), programs.1.clone())
            } else {
                (programs.0.clone(), changed)
            };
            let expected_pair = canonical_sha256(&json!({
                "parentCandidateId": parent_id,
                "longProgramSha256": long,
                "shortProgramSha256": short,
            }))
            .map_err(|err| contract(err.to_string()))?;
            if text(row, "reconstructedPairProgramIdentitySha256", "pair identity")? != expected_pair {
                return Err(contract("reconstructed pair program identity drifted"));
            }
            if arm == "topology_then_topology_local_event" {
                require_bool(
                    row,
                    "eventAttachesToAddedSetupNode",
                    true,
                    "TE receipt must prove the event attaches to the added setup node",
                )?;
            }
            complete_arms
                .entry(block_id)
                .or_default()
                .insert(arm, row.clone());
        }
    }
    for (block_id, arms) in &complete_arms {
        if ARMS.iter().any(|arm| !arms.contains_key(*arm)) {
            return Err(contract("all P/T/E/TE children must materialize, compile, and audit"));
        }
        let parent = arms.get("exact_parent_clone").expect("P");
        let topology = arms.get("topology_only_child").expect("T");
        let event = arms.get("event_only_control").expect("E");
        let combined = arms.get("topology_then_topology_local_event").expect("TE");
        let parent_genome = text(parent, "changedSideGenomeSha256", "P genome")?;
        let topology_genome = text(topology, "changedSideGenomeSha256", "T genome")?;
        if text(event, "applicationParentGenomeSha256", "E parent")? != parent_genome {
            return Err(contract("E application parent must be the P genome"));
        }
        if text(combined, "applicationParentGenomeSha256", "TE parent")? != topology_genome {
            return Err(contract("TE application parent must be the T genome"));
        }
        let _ = block_id;
    }
    Ok(payload.clone())
}
