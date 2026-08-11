//! Bounded, restartable pre-finalization for the rotating-evidence campaign.
//!
//! This is deliberately the seam *before* `qd-generation-finalizer`: campaign
//! workers still own raw replay execution, while this crate owns compact
//! cohorting, diverse provisional selection, panel-coverage obligations and
//! assembly of the finalizer's sealed source.  It never opens raw replay
//! results and it never falls back to Python over a population or panel matrix.

#![recursion_limit = "256"]

pub mod campaign_receipt;
pub mod core_receipt;
pub mod funnel_source;
pub mod panel_receipt;
pub mod v5;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, ensure};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use temporal_qd_contract::{
    CONTRACT_VERSION, canonical_json_line, canonical_sha256, canonical_sha256_without_object_field,
};

pub const MANIFEST_SCHEMA: &str = "temporal_qd_rotating_prefinalizer_manifest_v1";
pub const INPUT_SCHEMA: &str = "temporal_qd_rotating_prefinalizer_input_v1";
pub const TRANSACTION_SCHEMA: &str = "temporal_qd_rotating_prefinalizer_transaction_v1";
pub const TASK_PLAN_SCHEMA: &str = "temporal_qd_rotating_prefinalizer_task_plan_v1";
pub const FINALIZER_SOURCE_SCHEMA: &str = "temporal_qd_generation_finalization_source_v1";
pub const FINALIZER_MANIFEST_SCHEMA: &str = "temporal_qd_generation_finalization_manifest_v1";
pub const OPERATION: &str = "prepare_rotating_generation_finalization";
pub const TRANSACTION_PATH: &str = "prefinalization-transaction.json";
pub const TASK_PLAN_PATH: &str = "prefinalization-task-plan.json";
pub const FINALIZER_SOURCE_PATH: &str = "source.json";
pub const FINALIZER_MANIFEST_PATH: &str = "manifest.json";
pub const SELECTED_CANDIDATES_PATH: &str = "selected-backfill-candidates.jsonl";
pub const COHORT_SELECTION_SCHEMA: &str = "temporal_qd_rotating_cohort_selection_v1";

const ROTATING_SCHEMA: &str = "temporal_qd_rotating_evidence_v1";
const COHORT_SCHEMA: &str = "temporal_qd_current_panel_evaluation_cohort_v1";
const PROVISIONAL_SCHEMA: &str = "temporal_qd_provisional_survivors_v1";
const BUNDLE_SCHEMA: &str = "temporal_qd_candidate_panel_evidence_bundle_v1";

#[derive(Clone, Debug)]
struct Manifest {
    input_path: PathBuf,
    input_sha256: String,
    manifest_sha256: String,
}

#[derive(Clone, Debug)]
struct CompactMember {
    candidate_id: String,
    identity: String,
    program_sha256: String,
    profile_snapshot_sha256: String,
    cell_id: String,
    rank: f64,
}

/// Execute one compact transaction.  The input references JSONL artifacts;
/// population rows are scanned twice at most (compact selection then selected
/// rich-member hydration), so retained live state is O(population + selected
/// bundles), never O(population × panels).
/// Dispatches the frozen v5 transaction by its exact manifest schema while
/// retaining the old prototype reader solely for the historical parity tests.
pub fn execute_manifest(manifest_path: &Path) -> Result<Value> {
    let raw = fs::read(manifest_path).context("read prefinalizer manifest")?;
    let value: Value = serde_json::from_slice(&raw).context("parse prefinalizer manifest")?;
    if value.get("schemaVersion").and_then(Value::as_str) == Some(v5::BASE_MANIFEST_SCHEMA)
        || value.get("schemaVersion").and_then(Value::as_str) == Some(v5::BASE_MANIFEST_SCHEMA_V2)
        || value.get("schemaVersion").and_then(Value::as_str) == Some(v5::RESUME_MANIFEST_SCHEMA)
        || value.get("schemaVersion").and_then(Value::as_str) == Some(v5::RESUME_MANIFEST_SCHEMA_V2)
    {
        return v5::execute_manifest(manifest_path);
    }
    execute_legacy_manifest(manifest_path)
}

/// Execute the current process ABI. Native-v5 manifests return only the
/// bounded, receipt-last supervisor handoff; the legacy prototype remains an
/// explicit historical execution shape.
pub fn execute_manifest_compact(manifest_path: &Path) -> Result<Value> {
    let raw = fs::read(manifest_path).context("read prefinalizer manifest")?;
    let value: Value = serde_json::from_slice(&raw).context("parse prefinalizer manifest")?;
    if value.get("schemaVersion").and_then(Value::as_str) == Some(v5::BASE_MANIFEST_SCHEMA)
        || value.get("schemaVersion").and_then(Value::as_str) == Some(v5::BASE_MANIFEST_SCHEMA_V2)
        || value.get("schemaVersion").and_then(Value::as_str) == Some(v5::RESUME_MANIFEST_SCHEMA)
        || value.get("schemaVersion").and_then(Value::as_str) == Some(v5::RESUME_MANIFEST_SCHEMA_V2)
    {
        return v5::execute_manifest_compact(manifest_path);
    }
    execute_legacy_manifest(manifest_path)
}

fn execute_legacy_manifest(manifest_path: &Path) -> Result<Value> {
    let manifest_path = existing_file(manifest_path, "rotating prefinalizer manifest")?;
    let root = manifest_path
        .parent()
        .context("prefinalizer manifest has no parent")?;
    let manifest = parse_manifest(&fs::read(&manifest_path)?)?;
    let input = read_self_hashed(&manifest.input_path, "inputSha256", INPUT_SCHEMA)?;
    ensure!(
        sha(&input, "inputSha256")? == manifest.input_sha256,
        "manifest/input identity drifted"
    );
    if root.join(TRANSACTION_PATH).is_file() {
        let transaction = read_self_hashed(
            &root.join(TRANSACTION_PATH),
            "transactionSha256",
            TRANSACTION_SCHEMA,
        )?;
        ensure!(
            sha(&transaction, "manifestSha256")? == manifest.manifest_sha256,
            "committed transaction manifest drifted"
        );
        validate_restart(root, &transaction)?;
        return Ok(json!({
            "schemaVersion": "temporal_qd_rotating_prefinalizer_execution_v1",
            "restart": true,
            "restartValidation": "compact_transaction_and_output_hashes",
            "transaction": transaction,
        }));
    }

    let result = prepare(&input, root)?;
    publish_once(root.join(TASK_PLAN_PATH), &result.plan)?;
    let mut transaction = json!({
        "schemaVersion": TRANSACTION_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "manifestSha256": manifest.manifest_sha256,
        "inputSha256": manifest.input_sha256,
        "generationIndex": result.generation_index,
        "cohort": result.cohort,
        "provisional": result.provisional,
        "taskPlan": result.plan,
        "status": result.status,
    });
    if let Some((source, finalizer_manifest)) = result.finalization {
        publish_once(root.join(FINALIZER_SOURCE_PATH), &source)?;
        publish_once(root.join(FINALIZER_MANIFEST_PATH), &finalizer_manifest)?;
        transaction.as_object_mut().expect("object").insert(
            "finalizerSource".into(),
            descriptor(FINALIZER_SOURCE_PATH, &source, "sourceSha256")?,
        );
        transaction.as_object_mut().expect("object").insert(
            "finalizerManifest".into(),
            descriptor(
                FINALIZER_MANIFEST_PATH,
                &finalizer_manifest,
                "manifestSha256",
            )?,
        );
    }
    add_hash(&mut transaction, "transactionSha256")?;
    publish_once(root.join(TRANSACTION_PATH), &transaction)?;
    Ok(json!({
        "schemaVersion": "temporal_qd_rotating_prefinalizer_execution_v1",
        "restart": false,
        "transaction": transaction,
    }))
}

struct Prepared {
    generation_index: u64,
    cohort: Value,
    provisional: Value,
    plan: Value,
    status: &'static str,
    finalization: Option<(Value, Value)>,
}

fn prepare(input: &Value, root: &Path) -> Result<Prepared> {
    let map = object(input, "prefinalizer input")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "contractVersion",
            "generationIndex",
            "rotatingEvidence",
            "proposalCampaignSeal",
            "proposalMembers",
            "proposalMembersCampaignSealSha256",
            "proposalPopulationBinding",
            "retainedParents",
            "retainedParentArchiveBinding",
            "currentPanelReceipts",
            "panelBundleReceipts",
            "previousCandidatePanelBundles",
            "finalizerContext",
            "inputSha256",
        ],
        "prefinalizer input",
    )?;
    ensure!(
        text(input, "contractVersion")? == CONTRACT_VERSION,
        "input contract version mismatch"
    );
    validate_source_binding(
        member(input, "proposalPopulationBinding")?,
        "proposal_evaluation_population",
    )?;
    validate_source_binding(
        member(input, "retainedParentArchiveBinding")?,
        "retained_parent_archive",
    )?;
    let generation_index = unsigned(input, "generationIndex")?;
    ensure!(generation_index > 0, "generation index must be positive");
    let rotating = member(input, "rotatingEvidence")?;
    validate_rotating(rotating)?;
    let rotating_sha = sha(rotating, "rotatingEvidenceSha256")?;
    let panels = array(rotating, "panels")?;
    let cycle = unsigned(
        member(rotating, "absoluteGenerationMapping")?,
        "cycleLength",
    )? as usize;
    ensure!(
        cycle == panels.len() && cycle > 0,
        "rotating panel cycle drifted"
    );
    let current_panel =
        text(&panels[(generation_index as usize - 1) % cycle], "panelId")?.to_owned();
    let required_panels = required_panels(panels, generation_index)?;
    let proposal_seal = member(input, "proposalCampaignSeal")?;
    validate_campaign_seal(proposal_seal)?;
    ensure!(
        sha(input, "proposalMembersCampaignSealSha256")?
            == sha(proposal_seal, "campaignSealSha256")?,
        "proposal member rows are not bound to their native campaign seal"
    );

    let proposal = scan_members(
        member(input, "proposalMembers")?,
        "proposal campaign seal members",
    )?;
    let parents = scan_parent_candidates(member(input, "retainedParents")?)?;
    let cohort = cohort(
        &proposal,
        &parents,
        &rotating_sha,
        generation_index,
        &current_panel,
    )?;

    // Proposal members are already native campaign-seal reductions.  Parent
    // rows are admitted only from an explicitly bound retained-parent seal.
    let mut current = proposal;
    let mut received_parent_ids = BTreeSet::new();
    for receipt in array(input, "currentPanelReceipts")? {
        validate_member_receipt(receipt, &current_panel, "retained_parent_current_panel")?;
        for (_, row) in scan_members(
            member(receipt, "members")?,
            "retained parent current panel members",
        )? {
            ensure!(
                parents.contains_key(&row.candidate_id),
                "parent receipt contains a non-retained candidate"
            );
            ensure!(
                received_parent_ids.insert(row.candidate_id.clone()),
                "parent receipt repeats a candidate"
            );
            ensure!(
                current.insert(row.candidate_id.clone(), row).is_none(),
                "proposal/parent current-panel union duplicates candidate"
            );
        }
    }
    let expected_parents = parents.keys().cloned().collect::<BTreeSet<_>>();
    if received_parent_ids != expected_parents {
        let pending_parent_ids = expected_parents
            .difference(&received_parent_ids)
            .cloned()
            .collect::<Vec<_>>();
        let parent_projection = materialize_candidate_projection_from_jsonl(
            root.join("retained-parent-current-candidates.jsonl"),
            member(input, "retainedParents")?,
            &pending_parent_ids,
            false,
        )?;
        let selection = publish_cohort_selection(
            root,
            generation_index,
            "retained_parent_current_panel",
            &current_panel,
            pending_parent_ids.clone(),
            parent_projection,
            source_bindings(input)?,
            &rotating_sha,
        )?;
        let mut plan = task_plan(
            generation_index,
            &rotating_sha,
            &current_panel,
            "retained_parent_current_panel",
            pending_parent_ids,
            &[],
            json!({"retainedParents": member(input, "retainedParents")?}),
        )?;
        attach_selections(&mut plan, &[selection])?;
        let provisional = empty_provisional(generation_index, &current_panel, &cohort)?;
        return Ok(Prepared {
            generation_index,
            cohort,
            provisional,
            plan,
            status: "awaiting_current_parent_results",
            finalization: None,
        });
    }

    let provisional = provisional(
        &current,
        generation_index,
        &current_panel,
        &cohort,
        rotating,
    )?;
    let selected = array(&provisional, "candidates")?
        .iter()
        .map(|v| text(v, "candidateId").map(str::to_owned))
        .collect::<Result<BTreeSet<_>>>()?;
    let available = collect_bundles(input, &selected, &required_panels, &rotating_sha)?;
    let current_missing = selected
        .iter()
        .filter(|candidate| {
            !available
                .get(*candidate)
                .is_some_and(|panels| panels.contains(&current_panel))
        })
        .cloned()
        .collect::<Vec<_>>();
    if !current_missing.is_empty() {
        // The current panel is already an authority-bound proposal/retained
        // campaign.  A missing native panel-bundle receipt is an admission
        // boundary, never an excuse to relabel it as a prior-panel backfill.
        let plan = task_plan(
            generation_index,
            &rotating_sha,
            &current_panel,
            "await_current_panel_bundle_receipt",
            Vec::new(),
            &[],
            json!({"missingCurrentPanelBundleCandidateIds": current_missing}),
        )?;
        return Ok(Prepared {
            generation_index,
            cohort,
            provisional,
            plan,
            status: "awaiting_current_panel_bundle_receipts",
            finalization: None,
        });
    }
    let mut missing = BTreeMap::<String, Vec<String>>::new();
    for candidate in &selected {
        for panel in &required_panels {
            if panel == &current_panel {
                continue;
            }
            if !available
                .get(candidate)
                .is_some_and(|ids| ids.contains(panel))
            {
                missing
                    .entry(panel.clone())
                    .or_default()
                    .push(candidate.clone());
            }
        }
    }
    if !missing.is_empty() {
        let grouped = missing.into_iter().collect::<Vec<_>>();
        let selected_members = hydrate_selected_members(
            member(input, "proposalMembers")?,
            array(input, "currentPanelReceipts")?,
            &selected,
        )?;
        let selected_descriptor = materialize_candidate_projection_from_values(
            root.join(SELECTED_CANDIDATES_PATH),
            &selected_members,
            true,
        )?;
        let mut selections = Vec::new();
        for (panel, candidate_ids) in &grouped {
            selections.push(publish_cohort_selection(
                root,
                generation_index,
                "prior_panel_backfill",
                panel,
                candidate_ids.clone(),
                selected_descriptor.clone(),
                source_bindings(input)?,
                &rotating_sha,
            )?);
        }
        let mut plan = task_plan(
            generation_index,
            &rotating_sha,
            &current_panel,
            "prior_panel_backfill",
            Vec::new(),
            &grouped,
            json!({"selectedCandidates": selected_descriptor}),
        )?;
        attach_selections(&mut plan, &selections)?;
        return Ok(Prepared {
            generation_index,
            cohort,
            provisional,
            plan,
            status: "awaiting_panel_bundle_receipts",
            finalization: None,
        });
    }

    let plan = task_plan(
        generation_index,
        &rotating_sha,
        &current_panel,
        "complete",
        Vec::new(),
        &[],
        json!({}),
    )?;
    let rich_members = hydrate_selected_members(
        member(input, "proposalMembers")?,
        array(input, "currentPanelReceipts")?,
        &selected,
    )?;
    let source = finalizer_source(
        input,
        generation_index,
        rotating,
        cohort.clone(),
        provisional.clone(),
        rich_members,
        current.len(),
        &selected,
        &required_panels,
    )?;
    let source_sha = sha(&source, "sourceSha256")?;
    let mut finalizer_manifest = json!({
        "schemaVersion": FINALIZER_MANIFEST_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "operation": "finalize_rotating_generation",
        "sourcePath": root.join(FINALIZER_SOURCE_PATH).to_string_lossy(),
        "sourceSha256": source_sha,
        "resultPath": "generation-commit.json",
    });
    if let Some(value) = member(input, "finalizerContext")?.get("runtimeAuthoritySha256") {
        finalizer_manifest
            .as_object_mut()
            .expect("object")
            .insert("runtimeAuthoritySha256".into(), value.clone());
    }
    add_hash(&mut finalizer_manifest, "manifestSha256")?;
    Ok(Prepared {
        generation_index,
        cohort,
        provisional,
        plan,
        status: "ready_for_finalizer",
        finalization: Some((source, finalizer_manifest)),
    })
}

fn validate_rotating(value: &Value) -> Result<()> {
    ensure!(
        text(value, "schemaVersion")? == ROTATING_SCHEMA,
        "unsupported rotating evidence contract"
    );
    verify_hash(value, "rotatingEvidenceSha256", "rotating evidence")?;
    Ok(())
}

fn validate_campaign_seal(value: &Value) -> Result<()> {
    ensure!(
        text(value, "schemaVersion")? == "temporal_qd_campaign_seal_v1",
        "proposal input must be a native campaign seal"
    );
    verify_hash(value, "campaignSealSha256", "proposal campaign seal")
}

fn validate_source_binding(value: &Value, role: &str) -> Result<()> {
    ensure!(
        text(value, "schemaVersion")? == "temporal_qd_rotating_candidate_source_binding_v1"
            && text(value, "sourceRole")? == role,
        "candidate source binding role/schema drifted"
    );
    sha(value, "sourceSemanticSha256")?;
    verify_hash(value, "bindingSha256", "candidate source binding")
}

fn source_bindings(input: &Value) -> Result<Value> {
    Ok(json!({
        "proposalEvaluationPopulation": member(input, "proposalPopulationBinding")?,
        "retainedParentArchive": member(input, "retainedParentArchiveBinding")?,
    }))
}

fn selection_filename(role: &str, panel: &str) -> String {
    format!("cohort-selection-{role}-{panel}.json")
}

fn publish_cohort_selection(
    root: &Path,
    generation: u64,
    role: &str,
    panel: &str,
    mut candidate_ids: Vec<String>,
    candidate_projection: Value,
    source_bindings: Value,
    rotating_sha: &str,
) -> Result<Value> {
    candidate_ids.sort();
    candidate_ids.dedup();
    ensure!(
        !candidate_ids.is_empty(),
        "cohort selection must not be empty"
    );
    let filename = selection_filename(role, panel);
    let projection = projection_descriptor(&candidate_projection, root)?;
    let mut selection = json!({
        "schemaVersion": COHORT_SELECTION_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "generationIndex": generation,
        "campaignRole": role,
        "panelId": panel,
        "rotatingEvidenceSha256": rotating_sha,
        "candidateIds": candidate_ids,
        "candidateProjection": projection,
        "sourceBindings": source_bindings,
    });
    add_hash(&mut selection, "selectionSha256")?;
    publish_once(root.join(&filename), &selection)?;
    Ok(descriptor(&filename, &selection, "selectionSha256")?)
}

fn projection_descriptor(descriptor: &Value, root: &Path) -> Result<Value> {
    let absolute = PathBuf::from(text(descriptor, "path")?);
    let relative = absolute
        .strip_prefix(root)
        .ok()
        .and_then(|path| path.to_str())
        .map(str::to_owned)
        .ok_or_else(|| {
            anyhow!("candidate projection must be materialized under the prefinalizer root")
        })?;
    Ok(json!({
        "relativePath": relative.replace('\\', "/"),
        "rawSha256": sha(descriptor, "rawSha256")?,
        "sizeBytes": unsigned(descriptor, "sizeBytes")?,
        "recordCount": unsigned(descriptor, "recordCount")?,
        "rowSchema": "temporal_qd_rotating_candidate_projection_row_v1",
    }))
}

fn attach_selections(plan: &mut Value, selections: &[Value]) -> Result<()> {
    let tasks = array(plan, "tasks")?.to_vec();
    let mut by_key = BTreeMap::new();
    for selection in selections {
        by_key.insert(
            (
                text(selection, "path")?.to_owned(),
                sha(selection, "sha256")?.to_owned(),
            ),
            selection.clone(),
        );
    }
    let mut updated = Vec::new();
    for mut task in tasks {
        let role = text(&task, "role")?;
        let panel = text(&task, "panelId")?;
        let expected = selection_filename(role, panel);
        let selection = by_key
            .iter()
            .find(|((path, _), _)| path == &expected)
            .map(|(_, selection)| selection.clone())
            .ok_or_else(|| anyhow!("task lacks sealed cohort selection"))?;
        task.as_object_mut()
            .expect("task object")
            .insert("cohortSelection".into(), selection);
        updated.push(task);
    }
    let map = plan
        .as_object_mut()
        .ok_or_else(|| anyhow!("task plan must be object"))?;
    map.insert("tasks".into(), Value::Array(updated));
    map.remove("taskPlanSha256");
    add_hash(plan, "taskPlanSha256")
}

fn cohort(
    proposal: &BTreeMap<String, CompactMember>,
    parents: &BTreeMap<String, String>,
    rotating_sha: &str,
    generation: u64,
    panel: &str,
) -> Result<Value> {
    let mut rows = BTreeMap::<String, (String, &'static str)>::new();
    for row in proposal.values() {
        rows.insert(
            row.candidate_id.clone(),
            (row.identity.clone(), "new_proposal"),
        );
    }
    for (candidate, identity) in parents {
        match rows.get_mut(candidate) {
            Some((old, role)) => {
                ensure!(old == identity, "cohort candidate identity conflict");
                *role = "new_proposal";
            }
            None => {
                rows.insert(
                    candidate.clone(),
                    (identity.clone(), "retained_parent_evaluation"),
                );
            }
        }
    }
    let candidates = rows.iter().map(|(id, (identity, role))| json!({"candidateId": id, "candidateIdentitySha256": identity, "cohortRole": role})).collect::<Vec<_>>();
    let new_ids = rows
        .iter()
        .filter(|(_, (_, role))| *role == "new_proposal")
        .map(|(id, _)| json!(id))
        .collect::<Vec<_>>();
    let parent_ids = rows
        .iter()
        .filter(|(_, (_, role))| *role == "retained_parent_evaluation")
        .map(|(id, _)| json!(id))
        .collect::<Vec<_>>();
    let mut value = json!({"schemaVersion": COHORT_SCHEMA, "rotatingEvidenceSha256": rotating_sha, "generationIndex": generation, "panelId": panel, "candidates": candidates, "newProposalCandidateIds": new_ids, "retainedParentEvaluationCandidateIds": parent_ids, "parentReevaluationIsProposal": false});
    add_hash(&mut value, "cohortSha256")?;
    Ok(value)
}

fn provisional(
    current: &BTreeMap<String, CompactMember>,
    generation: u64,
    panel: &str,
    cohort: &Value,
    rotating: &Value,
) -> Result<Value> {
    let mut counts = BTreeMap::<String, usize>::new();
    for row in current.values() {
        *counts.entry(row.cell_id.clone()).or_default() += 1;
    }
    let mut groups = BTreeMap::<String, Vec<&CompactMember>>::new();
    for row in current.values() {
        groups.entry(row.cell_id.clone()).or_default().push(row);
    }
    for values in groups.values_mut() {
        values.sort_by(|a, b| {
            b.rank
                .total_cmp(&a.rank)
                .then_with(|| a.candidate_id.cmp(&b.candidate_id))
        });
    }
    let limit = unsigned(member(rotating, "provisionalReduction")?, "maxCandidates")? as usize;
    let mut selection = Vec::new();
    loop {
        let mut added = false;
        for rows in groups.values_mut() {
            if selection.len() == limit {
                break;
            }
            if !rows.is_empty() {
                selection.push(rows.remove(0));
                added = true;
            }
        }
        if !added || selection.len() == limit {
            break;
        }
    }
    let candidates = selection.into_iter().map(|row| json!({
        "candidateId": row.candidate_id, "candidateIdentitySha256": row.identity,
        "programSha256": row.program_sha256,
        "profileSnapshotSha256": row.profile_snapshot_sha256,
        "cellId": row.cell_id, "costView": "research_conservative", "currentPanelRank": row.rank,
        "novelty": 1.0 / *counts.get(&row.cell_id).expect("count") as f64,
    })).collect::<Vec<_>>();
    let mut value = json!({"schemaVersion": PROVISIONAL_SCHEMA, "generationIndex": generation, "panelId": panel, "cohortSha256": sha(cohort, "cohortSha256")?, "candidateCount": candidates.len(), "candidates": candidates});
    add_hash(&mut value, "provisionalSha256")?;
    Ok(value)
}

fn empty_provisional(generation: u64, panel: &str, cohort: &Value) -> Result<Value> {
    let mut value = json!({"schemaVersion": PROVISIONAL_SCHEMA, "generationIndex": generation, "panelId": panel, "cohortSha256": sha(cohort, "cohortSha256")?, "candidateCount": 0, "candidates": []});
    add_hash(&mut value, "provisionalSha256")?;
    Ok(value)
}

fn required_panels(panels: &[Value], generation: u64) -> Result<Vec<String>> {
    let mut ids = Vec::new();
    for index in 0..generation as usize {
        let id = text(&panels[index % panels.len()], "panelId")?.to_owned();
        if !ids.contains(&id) {
            ids.push(id);
        }
    }
    Ok(ids)
}

fn task_plan(
    generation: u64,
    rotating_sha: &str,
    current_panel: &str,
    phase: &str,
    parents: Vec<String>,
    backfills: &[(String, Vec<String>)],
    candidate_sources: Value,
) -> Result<Value> {
    let mut tasks = Vec::new();
    if !parents.is_empty() {
        tasks.push(json!({"role":"retained_parent_current_panel","panelId":current_panel,"candidateIds":parents,"candidateCount":parents.len()}));
    }
    for (panel, ids) in backfills {
        tasks.push(json!({"role":"prior_panel_backfill","panelId":panel,"candidateIds":ids,"candidateCount":ids.len()}));
    }
    let mut value = json!({"schemaVersion": TASK_PLAN_SCHEMA,"contractVersion":CONTRACT_VERSION,"generationIndex":generation,"rotatingEvidenceSha256":rotating_sha,"currentPanelId":current_panel,"phase":phase,"tasks":tasks,"taskCount":tasks.len(),"candidateSources":candidate_sources});
    add_hash(&mut value, "taskPlanSha256")?;
    Ok(value)
}

fn scan_members(descriptor: &Value, name: &str) -> Result<BTreeMap<String, CompactMember>> {
    let mut result = BTreeMap::new();
    stream_jsonl(descriptor, name, |value| {
        let candidate_id = text(&value, "candidateId")?.to_owned();
        let candidate = member(&value, "candidate")?;
        let identity = sha(candidate, "candidateIdentitySha256")?.to_owned();
        let program_sha256 = sha(candidate, "programSha256")?.to_owned();
        let profile_snapshot_sha256 = sha(candidate, "profileSnapshotSha256")?.to_owned();
        let cell_id = text(member(&value, "descriptor")?, "cellId")?.to_owned();
        let rank = number(member(&value, "aggregate")?, "totalConservativeNetR")?;
        ensure!(rank.is_finite(), "member rank must be finite");
        ensure!(
            result
                .insert(
                    candidate_id.clone(),
                    CompactMember {
                        candidate_id,
                        identity,
                        program_sha256,
                        profile_snapshot_sha256,
                        cell_id,
                        rank,
                    }
                )
                .is_none(),
            "sealed member artifact repeats candidate"
        );
        Ok(())
    })?;
    Ok(result)
}

fn scan_parent_candidates(descriptor: &Value) -> Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    stream_jsonl(descriptor, "retained parent candidates", |candidate| {
        let id = text(&candidate, "candidateId")?.to_owned();
        let identity = sha(&candidate, "candidateIdentitySha256")?.to_owned();
        ensure!(
            result.insert(id, identity).is_none(),
            "retained parent artifact repeats candidate"
        );
        Ok(())
    })?;
    Ok(result)
}

fn hydrate_selected_members(
    proposal: &Value,
    receipts: &[Value],
    selected: &BTreeSet<String>,
) -> Result<Vec<Value>> {
    let mut hydrated = BTreeMap::new();
    let mut descriptors = vec![proposal];
    for receipt in receipts {
        descriptors.push(member(receipt, "members")?);
    }
    for descriptor in descriptors {
        stream_jsonl(descriptor, "sealed member hydration", |row| {
            let candidate_id = text(&row, "candidateId")?.to_owned();
            if selected.contains(&candidate_id) {
                ensure!(
                    hydrated.insert(candidate_id, row).is_none(),
                    "selected member appears in multiple sealed artifacts"
                );
            }
            Ok(())
        })?;
    }
    ensure!(
        hydrated.len() == selected.len(),
        "selected candidate could not be hydrated from sealed rows"
    );
    Ok(hydrated.into_values().collect())
}

fn validate_member_receipt(value: &Value, panel: &str, role: &str) -> Result<()> {
    ensure!(
        text(value, "schemaVersion")? == "temporal_qd_rotating_member_receipt_v1",
        "invalid current-panel receipt schema"
    );
    ensure!(
        text(value, "role")? == role && text(value, "panelId")? == panel,
        "current-panel receipt role/panel drifted"
    );
    verify_hash(value, "receiptSha256", "current-panel receipt")?;
    validate_campaign_seal(member(value, "campaignSeal")?)?;
    Ok(())
}

fn collect_bundles(
    input: &Value,
    selected: &BTreeSet<String>,
    required: &[String],
    rotating_sha: &str,
) -> Result<BTreeMap<String, BTreeSet<String>>> {
    let mut output = BTreeMap::<String, BTreeSet<String>>::new();
    for bundle in array(input, "previousCandidatePanelBundles")? {
        insert_bundle(bundle, selected, required, rotating_sha, &mut output)?;
    }
    for receipt in array(input, "panelBundleReceipts")? {
        validate_v5_panel_bundle_receipt(receipt)?;
        let role = text(receipt, "role")?;
        ensure!(
            matches!(
                role,
                "proposal_current_panel" | "retained_parent_current_panel" | "prior_panel_backfill"
            ),
            "unsupported panel bundle receipt role"
        );
        for bundle in array(receipt, "candidatePanelBundles")? {
            insert_bundle(bundle, selected, required, rotating_sha, &mut output)?;
        }
    }
    Ok(output)
}

fn validate_v5_panel_bundle_receipt(receipt: &Value) -> Result<()> {
    let map = object(receipt, "v5 panel bundle receipt")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "role",
            "campaignSeal",
            "compactEvidenceSource",
            "candidatePanelBundles",
            "receiptSha256",
        ],
        "v5 panel bundle receipt",
    )?;
    ensure!(
        text(receipt, "schemaVersion")? == "temporal_qd_v5_rotating_panel_bundle_receipt_v1",
        "v3/generic panel bundle receipts are forbidden on v5"
    );
    verify_hash(receipt, "receiptSha256", "v5 panel bundle receipt")?;
    validate_campaign_seal(member(receipt, "campaignSeal")?)?;
    let source = member(receipt, "compactEvidenceSource")?;
    let source_map = object(source, "v5 compact evidence source")?;
    exact_keys(
        source_map,
        &[
            "schemaVersion",
            "tailAuthority",
            "tailResultIndex",
            "compactEvidenceSourceSha256",
        ],
        "v5 compact evidence source",
    )?;
    ensure!(
        text(source, "schemaVersion")? == "temporal_qd_v5_rotating_compact_evidence_source_v1",
        "v5 receipt must use compact v4 evidence source"
    );
    verify_hash(
        source,
        "compactEvidenceSourceSha256",
        "v5 compact evidence source",
    )?;
    let authority = member(source, "tailAuthority")?;
    ensure!(
        text(authority, "schemaVersion")? == "temporal_qd_v5_directional_tail_authority_v1",
        "v5 receipt tail authority is invalid"
    );
    verify_hash(
        authority,
        "tailAuthoritySha256",
        "v5 directional tail authority",
    )?;
    let index = member(source, "tailResultIndex")?;
    let index_map = object(index, "v5 tail-result index descriptor")?;
    exact_keys(
        index_map,
        &["schemaVersion", "relativePath", "tailResultIndexSha256"],
        "v5 tail-result index descriptor",
    )?;
    ensure!(
        text(index, "schemaVersion")? == "temporal_qd_v5_tail_result_index_v4_descriptor_v1",
        "v3/raw tail index descriptor is forbidden on v5"
    );
    sha(index, "tailResultIndexSha256")?;
    ensure!(
        !text(index, "relativePath")?.is_empty(),
        "v5 tail index descriptor path is empty"
    );
    Ok(())
}

fn insert_bundle(
    bundle: &Value,
    selected: &BTreeSet<String>,
    required: &[String],
    rotating_sha: &str,
    output: &mut BTreeMap<String, BTreeSet<String>>,
) -> Result<()> {
    ensure!(
        text(bundle, "schemaVersion")? == BUNDLE_SCHEMA,
        "unsupported candidate panel bundle schema"
    );
    verify_hash(bundle, "bundleSha256", "candidate panel bundle")?;
    ensure!(
        sha(bundle, "rotatingEvidenceSha256")? == rotating_sha,
        "candidate panel bundle rotating authority drifted"
    );
    let candidate = text(bundle, "candidateId")?.to_owned();
    let panel = text(bundle, "panelId")?.to_owned();
    ensure!(
        selected.contains(&candidate),
        "bundle does not belong to provisional candidate"
    );
    ensure!(
        required.contains(&panel),
        "bundle panel is not required for this generation"
    );
    ensure!(
        output.entry(candidate).or_default().insert(panel),
        "duplicate candidate/panel bundle"
    );
    Ok(())
}

fn finalizer_source(
    input: &Value,
    generation: u64,
    rotating: &Value,
    cohort: Value,
    provisional: Value,
    rich_members: Vec<Value>,
    current_member_count: usize,
    selected: &BTreeSet<String>,
    required: &[String],
) -> Result<Value> {
    let context = object(member(input, "finalizerContext")?, "finalizer context")?;
    for field in [
        "previousCumulativeArchive",
        "previousParentArchiveSummary",
        "archivePolicy",
        "cellCapacity",
        "campaigns",
        "artifactLedgerBase",
        "publicationPaths",
        "funnelReductionSource",
        "generationRecordBase",
        "stateTransitionBase",
    ] {
        ensure!(
            context.contains_key(field),
            "finalizer context lacks {field}"
        );
    }
    let mut bundles = Vec::new();
    for receipt in array(input, "panelBundleReceipts")? {
        for bundle in array(receipt, "candidatePanelBundles")? {
            if selected.contains(text(bundle, "candidateId")?)
                && required.contains(&text(bundle, "panelId")?.to_owned())
            {
                bundles.push(bundle.clone());
            }
        }
    }
    for bundle in array(input, "previousCandidatePanelBundles")? {
        if selected.contains(text(bundle, "candidateId")?)
            && required.contains(&text(bundle, "panelId")?.to_owned())
        {
            bundles.push(bundle.clone());
        }
    }
    bundles.sort_by(|a, b| {
        (
            text(a, "candidateId").unwrap_or(""),
            text(a, "panelId").unwrap_or(""),
        )
            .cmp(&(
                text(b, "candidateId").unwrap_or(""),
                text(b, "panelId").unwrap_or(""),
            ))
    });
    let mut seen = BTreeSet::new();
    for bundle in &bundles {
        ensure!(
            seen.insert((
                text(bundle, "candidateId")?.to_owned(),
                text(bundle, "panelId")?.to_owned()
            )),
            "finalizer snapshot repeats a bundle"
        );
    }
    ensure!(
        bundles.len()
            == selected
                .len()
                .checked_mul(required.len())
                .context("bundle coverage overflow")?,
        "finalizer snapshot has incomplete coverage"
    );
    let final_context = member(input, "finalizerContext")?;
    let mut source = json!({
        "schemaVersion": FINALIZER_SOURCE_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "generationIndex": generation,
        "rotatingEvidence": rotating,
        "cohort": cohort,
        "provisional": provisional,
        "baselineCandidatePanelBundles": bundles,
        "completeBundleSnapshot": true,
        "auxiliaryPlan": null,
        "auxiliaryCampaignReceipts": [],
        "previousCumulativeArchive": member(final_context, "previousCumulativeArchive")?,
        "previousParentArchiveSummary": member(final_context, "previousParentArchiveSummary")?,
        "archivePolicy": member(final_context, "archivePolicy")?,
        "richMembers": rich_members,
        "currentMemberCount": current_member_count,
        "cellCapacity": member(final_context, "cellCapacity")?,
        "campaigns": member(final_context, "campaigns")?,
        "artifactLedgerBase": member(final_context, "artifactLedgerBase")?,
        "publicationPaths": member(final_context, "publicationPaths")?,
        "funnelReductionSource": member(final_context, "funnelReductionSource")?,
        "generationRecordBase": member(final_context, "generationRecordBase")?,
        "stateTransitionBase": member(final_context, "stateTransitionBase")?,
    });
    add_hash(&mut source, "sourceSha256")?;
    Ok(source)
}

fn stream_jsonl(
    descriptor: &Value,
    name: &str,
    mut visit: impl FnMut(Value) -> Result<()>,
) -> Result<()> {
    let map = object(descriptor, name)?;
    exact_keys(
        map,
        &["path", "rawSha256", "sizeBytes", "recordCount"],
        name,
    )?;
    let path = existing_file(Path::new(text(descriptor, "path")?), name)?;
    let expected_bytes = unsigned(descriptor, "sizeBytes")?;
    ensure!(
        fs::metadata(&path)?.len() == expected_bytes,
        "{name} size drifted"
    );
    let expected_sha = sha(descriptor, "rawSha256")?;
    ensure!(
        file_sha(&path)? == expected_sha,
        "{name} raw identity drifted"
    );
    let reader = BufReader::new(File::open(&path)?);
    let mut count = 0u64;
    for line in reader.lines() {
        let line = line?;
        ensure!(!line.is_empty(), "{name} contains blank JSONL row");
        visit(serde_json::from_str(&line).with_context(|| format!("parse {name} JSONL row"))?)?;
        count += 1;
    }
    ensure!(
        count == unsigned(descriptor, "recordCount")?,
        "{name} record count drifted"
    );
    Ok(())
}

fn parse_manifest(raw: &[u8]) -> Result<Manifest> {
    let value: Value = serde_json::from_slice(raw).context("parse prefinalizer manifest")?;
    ensure!(
        canonical_json_line(&value)? == raw,
        "prefinalizer manifest must be canonical JSON plus LF"
    );
    let map = object(&value, "prefinalizer manifest")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "contractVersion",
            "operation",
            "inputPath",
            "inputSha256",
            "resultPath",
            "manifestSha256",
        ],
        "prefinalizer manifest",
    )?;
    ensure!(
        text(&value, "schemaVersion")? == MANIFEST_SCHEMA
            && text(&value, "contractVersion")? == CONTRACT_VERSION
            && text(&value, "operation")? == OPERATION
            && text(&value, "resultPath")? == TRANSACTION_PATH,
        "prefinalizer manifest binding is invalid"
    );
    verify_hash(&value, "manifestSha256", "prefinalizer manifest")?;
    Ok(Manifest {
        input_path: PathBuf::from(text(&value, "inputPath")?),
        input_sha256: sha(&value, "inputSha256")?.to_owned(),
        manifest_sha256: sha(&value, "manifestSha256")?.to_owned(),
    })
}

fn validate_restart(root: &Path, transaction: &Value) -> Result<()> {
    let plan = read_self_hashed(
        &root.join(TASK_PLAN_PATH),
        "taskPlanSha256",
        TASK_PLAN_SCHEMA,
    )?;
    ensure!(
        member(transaction, "taskPlan")? == &plan,
        "restart task plan drifted"
    );
    if text(transaction, "status")? == "ready_for_finalizer" {
        let source = read_self_hashed(
            &root.join(FINALIZER_SOURCE_PATH),
            "sourceSha256",
            FINALIZER_SOURCE_SCHEMA,
        )?;
        let manifest = read_self_hashed(
            &root.join(FINALIZER_MANIFEST_PATH),
            "manifestSha256",
            FINALIZER_MANIFEST_SCHEMA,
        )?;
        ensure!(
            member(transaction, "finalizerSource")?.get("sha256")
                == Some(&Value::String(sha(&source, "sourceSha256")?.to_owned())),
            "restart finalizer source drifted"
        );
        ensure!(
            member(transaction, "finalizerManifest")?.get("sha256")
                == Some(&Value::String(sha(&manifest, "manifestSha256")?.to_owned())),
            "restart finalizer manifest drifted"
        );
    }
    Ok(())
}

fn descriptor(path: &str, value: &Value, field: &str) -> Result<Value> {
    Ok(json!({"path":path,"sha256":sha(value,field)?}))
}
fn read_self_hashed(path: &Path, field: &str, schema: &str) -> Result<Value> {
    let raw = fs::read(existing_file(path, schema)?)?;
    let value: Value = serde_json::from_slice(&raw)?;
    ensure!(
        canonical_json_line(&value)? == raw,
        "{schema} is not canonical JSON plus LF"
    );
    ensure!(
        text(&value, "schemaVersion")? == schema,
        "{schema} schema mismatch"
    );
    verify_hash(&value, field, schema)?;
    Ok(value)
}
fn publish_once(path: PathBuf, value: &Value) -> Result<()> {
    let bytes = canonical_json_line(value)?;
    if path.exists() {
        ensure!(
            fs::read(&path)? == bytes,
            "immutable output exists with different bytes: {}",
            path.display()
        );
        return Ok(());
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    let mut file = options.open(&path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}

fn materialize_candidate_projection_from_jsonl(
    path: PathBuf,
    source: &Value,
    selected: &[String],
    source_rows_are_members: bool,
) -> Result<Value> {
    let selected = selected.iter().cloned().collect::<BTreeSet<_>>();
    let mut rows = BTreeMap::new();
    stream_jsonl(source, "candidate projection source", |row| {
        let candidate = if source_rows_are_members {
            member(&row, "candidate")?.clone()
        } else {
            row
        };
        let candidate_id = text(&candidate, "candidateId")?.to_owned();
        if selected.contains(&candidate_id) {
            ensure!(
                rows.insert(candidate_id, candidate).is_none(),
                "candidate projection source repeats selected candidate"
            );
        }
        Ok(())
    })?;
    ensure!(
        rows.len() == selected.len(),
        "candidate projection source is incomplete"
    );
    publish_candidate_projection(path, rows.into_values().collect())
}

fn materialize_candidate_projection_from_values(
    path: PathBuf,
    source_rows: &[Value],
    source_rows_are_members: bool,
) -> Result<Value> {
    let candidates = source_rows
        .iter()
        .map(|row| {
            if source_rows_are_members {
                member(row, "candidate").cloned()
            } else {
                Ok(row.clone())
            }
        })
        .collect::<Result<Vec<_>>>()?;
    publish_candidate_projection(path, candidates)
}

fn publish_candidate_projection(path: PathBuf, candidates: Vec<Value>) -> Result<Value> {
    let mut ordered = BTreeMap::new();
    for candidate in candidates {
        let candidate_id = text(&candidate, "candidateId")?.to_owned();
        let identity = sha(&candidate, "candidateIdentitySha256")?.to_owned();
        let mut row = json!({
            "schemaVersion": "temporal_qd_rotating_candidate_projection_row_v1",
            "candidateId": candidate_id,
            "candidateIdentitySha256": identity,
            "candidate": candidate,
        });
        add_hash(&mut row, "projectionRowSha256")?;
        ensure!(
            ordered
                .insert(text(&row, "candidateId")?.to_owned(), row)
                .is_none(),
            "candidate projection repeats candidate"
        );
    }
    let rows = ordered.into_values().collect::<Vec<_>>();
    let mut bytes = Vec::new();
    for row in rows {
        bytes.extend(canonical_json_line(&row)?);
    }
    if path.exists() {
        ensure!(
            fs::read(&path)? == bytes,
            "immutable JSONL output exists with different bytes: {}",
            path.display()
        );
    } else {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        let mut file = options.open(&path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    Ok(json!({
        "path": path,
        "rawSha256": format!("sha256:{:x}", Sha256::digest(&bytes)),
        "sizeBytes": bytes.len(),
        "recordCount": bytes.iter().filter(|byte| **byte == b'\n').count(),
    }))
}
fn existing_file(path: &Path, name: &str) -> Result<PathBuf> {
    ensure!(path.is_file(), "{name} is not a file: {}", path.display());
    Ok(path.to_path_buf())
}
fn file_sha(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buf = [0u8; 65536];
    loop {
        let count = file.read(&mut buf)?;
        if count == 0 {
            break;
        }
        hash.update(&buf[..count]);
    }
    Ok(format!("sha256:{:x}", hash.finalize()))
}
fn add_hash(value: &mut Value, field: &str) -> Result<()> {
    let digest = canonical_sha256(value)?;
    value
        .as_object_mut()
        .ok_or_else(|| anyhow!("self-hashed value must be object"))?
        .insert(field.into(), json!(digest));
    Ok(())
}
fn verify_hash(value: &Value, field: &str, name: &str) -> Result<()> {
    ensure!(
        canonical_sha256_without_object_field(value, field)? == sha(value, field)?,
        "{name} identity mismatch"
    );
    Ok(())
}
fn sha<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    let value = member(value, field)?;
    let text = value
        .as_str()
        .ok_or_else(|| anyhow!("{field} must be digest"))?;
    ensure!(
        text.starts_with("sha256:") && text.len() == 71,
        "{field} must be sha256 digest"
    );
    Ok(text)
}
fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    member(value, field)?
        .as_str()
        .ok_or_else(|| anyhow!("{field} must be string"))
}
fn number(value: &Value, field: &str) -> Result<f64> {
    member(value, field)?
        .as_f64()
        .ok_or_else(|| anyhow!("{field} must be numeric"))
}
fn unsigned(value: &Value, field: &str) -> Result<u64> {
    member(value, field)?
        .as_u64()
        .ok_or_else(|| anyhow!("{field} must be unsigned integer"))
}
fn member<'a>(value: &'a Value, field: &str) -> Result<&'a Value> {
    object(value, "object")?
        .get(field)
        .ok_or_else(|| anyhow!("object lacks {field}"))
}
fn object<'a>(value: &'a Value, name: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| anyhow!("{name} must be object"))
}
fn array<'a>(value: &'a Value, field: &str) -> Result<&'a [Value]> {
    member(value, field)?
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| anyhow!("{field} must be array"))
}
fn exact_keys(map: &Map<String, Value>, expected: &[&str], name: &str) -> Result<()> {
    let actual = map.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let wanted = expected.iter().copied().collect::<BTreeSet<_>>();
    ensure!(actual == wanted, "{name} keys are not exact");
    Ok(())
}
