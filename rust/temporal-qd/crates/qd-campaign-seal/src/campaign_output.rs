use super::*;
use temporal_qd_campaign_freeze::{open_v5_campaign_input_checkpoint, V5CampaignInputCheckpoint};
use temporal_qd_rotating_prefinalizer::panel_receipt;

pub const MANIFEST_SCHEMA: &str = "temporal_qd_v5_campaign_output_manifest_v1";
pub const CHECKPOINT_SCHEMA: &str = "temporal_qd_v5_campaign_output_checkpoint_v1";
pub const RESULT_SCHEMA: &str = "temporal_qd_v5_campaign_output_result_v1";
pub const OPERATION: &str = "commit_campaign_output_checkpoint";
pub const MANIFEST_PATH: &str = "campaign-output-manifest.json";
pub const CHECKPOINT_PATH: &str = "campaign-output-checkpoint.json";
pub const PANEL_BUNDLES_PATH: &str = "candidate-panel-bundles.jsonl";
pub const AUTHENTICATED_GRAPH_SCHEMA: &str =
    "temporal_qd_v5_authenticated_campaign_output_graph_v1";
const GATEWAY_RECEIPT_SCHEMA: &str = "temporal_qd_native_gateway_execution_receipt_v3";
const SOURCE_ROOT_SCHEMA: &str = "temporal_qd_v5_campaign_output_source_roots_v1";
const ARTIFACT_DESCRIPTOR_SCHEMA: &str = "temporal_qd_v5_campaign_output_artifact_v1";
const EVALUATED_MEMBER_SCHEMA: &str = "temporal_qd_evaluated_member_v1";
const PANEL_BUNDLE_SCHEMA: &str = "temporal_qd_candidate_panel_evidence_bundle_v1";

#[derive(Clone, Debug)]
struct CampaignOutputManifest {
    manifest_sha256: String,
    runtime_authority_sha256: String,
    campaign_input_checkpoint_path: PathBuf,
    campaign_input_checkpoint_sha256: String,
    gateway_execution_receipt_path: PathBuf,
    gateway_execution_receipt_sha256: String,
    generation_index: u64,
    campaign_role: String,
    panel_id: String,
    rotating_evidence_sha256: String,
    panel: Value,
    cohort_source: Value,
    minimum_total_trades: i64,
    minimum_trades_per_window: i64,
    cap_trades: i64,
    provisional_limit: usize,
}

#[derive(Clone, Debug)]
pub struct CampaignOutputCheckpoint {
    pub value: Value,
    pub root: PathBuf,
    pub checkpoint_sha256: String,
    pub generation_index: u64,
    pub campaign_role: String,
    pub panel_id: String,
    pub rotating_evidence_sha256: String,
    pub task_matrix_sha256: String,
    pub task_count: u64,
    pub evaluated_members_path: PathBuf,
    pub evaluated_member_count: u64,
    pub panel_bundles_path: PathBuf,
    pub panel_bundle_count: u64,
}

pub fn execute_manifest(manifest_path: &Path) -> Result<Value> {
    let started = Instant::now();
    let manifest_path = existing_file(manifest_path, "campaign-output manifest")?;
    ensure!(
        manifest_path.file_name().and_then(|name| name.to_str()) == Some(MANIFEST_PATH),
        "campaign-output manifest path is not fixed"
    );
    let output_root = manifest_path
        .parent()
        .ok_or_else(|| anyhow!("campaign-output manifest has no parent"))?;
    ensure_real_directory(output_root, "campaign-output root")?;
    let manifest = parse_manifest(&manifest_path)?;
    let checkpoint_path = output_root.join(CHECKPOINT_PATH);
    if checkpoint_path.is_file() {
        let checkpoint = open_checkpoint(&checkpoint_path)?;
        validate_manifest_checkpoint_binding(&manifest, &checkpoint)?;
        return result_value(&checkpoint, true, started);
    }

    let input = open_v5_campaign_input_checkpoint(&manifest.campaign_input_checkpoint_path)?;
    validate_input_binding(&manifest, &input)?;
    let (source, gateway_receipt) = build_source(&manifest, &input)?;
    let directional_authority = directional_tail_authority(
        &manifest.runtime_authority_sha256,
        manifest.generation_index,
    )?;
    let internal_manifest = Manifest {
        runtime_authority_sha256: manifest.runtime_authority_sha256.clone(),
        evaluation_path: input.cohort_population_path.clone(),
        evaluation_sha256: input.cohort_population_sha256.clone(),
        generation_index: manifest.generation_index,
        minimum_total_trades: manifest.minimum_total_trades,
        minimum_trades_per_window: manifest.minimum_trades_per_window,
        cap_trades: manifest.cap_trades,
        provisional_limit: manifest.provisional_limit,
        manifest_sha256: manifest.manifest_sha256.clone(),
    };

    let staging = output_root.join(".campaign-output.staging");
    reset_staging_directory(&staging)?;
    let (index, raw_inventory, metrics) = build_index_and_inventory(&source)?;
    let index_bytes = canonical_json_bytes(&index)?;
    let index_path = staging.join(DIRECTIONAL_INDEX_PATH);
    write_new_synced_file(
        &index_path,
        &index_bytes,
        "campaign-output tail-result index",
    )?;
    let seal = build_campaign_seal(
        &internal_manifest,
        &source,
        &index,
        sha_bytes(&raw_inventory),
        raw_inventory.len() as u64,
        &metrics,
    )?;
    let transaction = run_tail_transaction(&staging, &internal_manifest, &source, &seal)?;

    let tail_authority_path = staging.join(temporal_qd_tail_reducer::TAIL_AUTHORITY_PATH);
    let tail_authority = read_canonical_value(&tail_authority_path, "tail authority")?;
    let tail_result_index_descriptor = panel_tail_index_descriptor(&staging, &index)?;
    let mut panel_input = json!({
        "schemaVersion": panel_receipt::INPUT_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "generationIndex": manifest.generation_index,
        "campaignRole": manifest.campaign_role,
        "campaignSeal": seal,
        "tailAuthority": {
            "receiptPath": absolute_string(&tail_authority_path)?,
            "receiptSha256": sha_field(object(&tail_authority, "tail authority")?, "tailAuthoritySha256")?,
        },
        "tailResultIndex": tail_result_index_descriptor,
        "directionalTailAuthority": directional_authority,
        "rotatingEvidence": {
            "rotatingEvidenceSha256": manifest.rotating_evidence_sha256,
        },
        "panel": manifest.panel,
    });
    add_identity(&mut panel_input, "inputSha256")?;
    let panel_receipt = panel_receipt::build(&panel_input)?;
    let bundles = field(
        object(&panel_receipt, "panel receipt")?,
        "candidatePanelBundles",
    )?
    .as_array()
    .ok_or_else(|| anyhow!("panel receipt bundles must be an array"))?;
    write_jsonl(
        &staging.join(PANEL_BUNDLES_PATH),
        bundles,
        PANEL_BUNDLE_SCHEMA,
        "bundleSha256",
    )?;

    let members_descriptor = checkpoint_jsonl_descriptor(
        &staging,
        temporal_qd_tail_reducer::MEMBERS_PATH,
        EVALUATED_MEMBER_SCHEMA,
        "candidate",
    )?;
    let bundles_descriptor = checkpoint_jsonl_descriptor(
        &staging,
        PANEL_BUNDLES_PATH,
        PANEL_BUNDLE_SCHEMA,
        "bundleSha256",
    )?;
    let gateway_map = object(&gateway_receipt, "gateway execution receipt")?;
    let seal_map = object(&seal, "campaign seal")?;
    let transaction_map = object(&transaction, "tail transaction")?;
    let directional_authority = directional_tail_authority(
        &manifest.runtime_authority_sha256,
        manifest.generation_index,
    )?;
    let campaign_input = json!({
        "checkpointSha256": input.checkpoint_sha256,
        "cohortPopulationSha256": input.cohort_population_sha256,
        "authorityId": input.authority_id,
        "evaluationIdentitySha256": input.evaluation_identity_sha256,
        "campaignSha256": input.campaign_sha256,
        "taskMatrixSha256": input.task_matrix_sha256,
        "candidateCount": input.candidate_count,
        "windowCount": input.window_count,
        "taskCount": input.task_count,
    });
    let campaign_seal = json!({
        "directionalTailAuthoritySha256": sha_field(
            object(&directional_authority, "directional tail authority")?,
            "tailAuthoritySha256",
        )?,
        "campaignSealSha256": sha_field(seal_map, "campaignSealSha256")?,
        "tailResultIndexSha256": sha_field(
            object(&index, "tail result index")?,
            "tailResultIndexSha256",
        )?,
        "tailTransactionSha256": sha_field(transaction_map, "transactionSha256")?,
    });
    let semantic_members = semantic_jsonl_descriptor(&members_descriptor)?;
    let semantic_bundles = semantic_jsonl_descriptor(&bundles_descriptor)?;
    let mut checkpoint = json!({
        "schemaVersion": CHECKPOINT_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "generationIndex": manifest.generation_index,
        "campaignRole": manifest.campaign_role,
        "panelId": manifest.panel_id,
        "rotatingEvidenceSha256": manifest.rotating_evidence_sha256,
        "cohortSource": manifest.cohort_source,
        "campaignInput": campaign_input,
        "campaignSeal": campaign_seal,
        "campaignSealDocument": seal,
        "evaluatedMembers": semantic_members,
        "candidatePanelBundles": semantic_bundles,
    });
    let semantic_receipt_sha256 = canonical_sha256(&checkpoint)?;

    promote_staged_file(&staging, output_root, DIRECTIONAL_INDEX_PATH)?;
    promote_staged_file(
        &staging,
        output_root,
        temporal_qd_tail_reducer::MEMBERS_PATH,
    )?;
    promote_staged_file(&staging, output_root, PANEL_BUNDLES_PATH)?;

    let execution_bindings = json!({
        "campaignInputCheckpoint": execution_descriptor(&manifest.campaign_input_checkpoint_path)?,
        "gatewayExecutionReceipt": execution_descriptor(&manifest.gateway_execution_receipt_path)?,
        "tailResultIndex": execution_descriptor(&output_root.join(DIRECTIONAL_INDEX_PATH))?,
        "evaluatedMembersJsonl": execution_descriptor(
            &output_root.join(temporal_qd_tail_reducer::MEMBERS_PATH),
        )?,
        "candidatePanelBundlesJsonl": execution_descriptor(
            &output_root.join(PANEL_BUNDLES_PATH),
        )?,
    });
    {
        let full = checkpoint
            .as_object_mut()
            .ok_or_else(|| anyhow!("campaign-output semantic receipt is not an object"))?;
        full.insert(
            "semanticReceiptSha256".into(),
            Value::String(semantic_receipt_sha256),
        );
        full.insert(
            "manifestSha256".into(),
            Value::String(manifest.manifest_sha256.clone()),
        );
        full.insert(
            "runtimeAuthoritySha256".into(),
            Value::String(manifest.runtime_authority_sha256.clone()),
        );
        full.insert(
            "gatewayReceiptSha256".into(),
            Value::String(sha_field(gateway_map, "receiptSha256")?),
        );
        full.insert(
            "gatewaySemanticReceiptSha256".into(),
            Value::String(sha_field(gateway_map, "semanticReceiptSha256")?),
        );
        full.insert("executionBindings".into(), execution_bindings);
    }
    let receipt_sha256 = canonical_sha256(&checkpoint)?;
    checkpoint
        .as_object_mut()
        .expect("campaign-output checkpoint object")
        .insert("receiptSha256".into(), Value::String(receipt_sha256));
    temporal_qd_rotating_prefinalizer::campaign_receipt::validate_receipt(&checkpoint)?;
    publish_bytes_once(
        output_root,
        CHECKPOINT_PATH,
        &canonical_json_line(&checkpoint)?,
    )?;
    let _ = fs::remove_dir_all(&staging);
    let checkpoint = open_checkpoint(&checkpoint_path)?;
    validate_manifest_checkpoint_binding(&manifest, &checkpoint)?;
    result_value_with_metrics(&checkpoint, false, started, Some(&metrics))
}

pub fn open_checkpoint(path: &Path) -> Result<CampaignOutputCheckpoint> {
    let path = existing_file(path, "campaign-output checkpoint")?.canonicalize()?;
    ensure!(
        path.file_name().and_then(|name| name.to_str()) == Some(CHECKPOINT_PATH),
        "campaign-output checkpoint path is not fixed"
    );
    let root = path
        .parent()
        .ok_or_else(|| anyhow!("campaign-output checkpoint has no parent"))?
        .to_path_buf();
    ensure_real_directory(&root, "campaign-output checkpoint root")?;
    let value = read_canonical_value(&path, "campaign-output checkpoint")?;
    temporal_qd_rotating_prefinalizer::campaign_receipt::validate_receipt(&value)?;
    let map = object(&value, "campaign-output checkpoint")?;
    let checkpoint_sha256 = sha_field(map, "receiptSha256")?;
    let generation_index = unsigned(map, "generationIndex")?;
    let campaign_role = text(map, "campaignRole")?;
    let panel_id = text(map, "panelId")?;
    let rotating_evidence_sha256 = sha_field(map, "rotatingEvidenceSha256")?;
    let campaign_input = object(
        field(map, "campaignInput")?,
        "campaign-output input checkpoint summary",
    )?;
    let task_matrix_sha256 = sha_field(campaign_input, "taskMatrixSha256")?;
    let task_count = unsigned(campaign_input, "taskCount")?;
    let campaign_seal = object(field(map, "campaignSeal")?, "campaign-output seal summary")?;
    let execution = object(
        field(map, "executionBindings")?,
        "campaign-output execution bindings",
    )?;

    let index_path = validate_bound_output(
        &root,
        field(execution, "tailResultIndex")?,
        DIRECTIONAL_INDEX_PATH,
    )?;
    let index_raw = fs::read(&index_path)?;
    let index: Value = serde_json::from_slice(&index_raw).context("parse checkpoint tail index")?;
    ensure!(
        canonical_json_bytes(&index)? == index_raw
            && text(object(&index, "checkpoint tail index")?, "schemaVersion")?
                == DIRECTIONAL_INDEX_SCHEMA
            && canonical_sha256_without_object_field(&index, "tailResultIndexSha256")?
                == sha_field(
                    object(&index, "checkpoint tail index")?,
                    "tailResultIndexSha256"
                )?
            && sha_field(
                object(&index, "checkpoint tail index")?,
                "tailResultIndexSha256"
            )? == sha_field(campaign_seal, "tailResultIndexSha256")?
            && unsigned(object(&index, "checkpoint tail index")?, "taskCount")? == task_count,
        "campaign-output tail index binding drifted"
    );

    let evaluated_members_path = validate_receipt_jsonl(
        &root,
        field(map, "evaluatedMembers")?,
        field(execution, "evaluatedMembersJsonl")?,
        temporal_qd_tail_reducer::MEMBERS_PATH,
        EVALUATED_MEMBER_SCHEMA,
        None,
    )?;
    let evaluated_member_count = unsigned(
        object(
            field(map, "evaluatedMembers")?,
            "evaluated members descriptor",
        )?,
        "recordCount",
    )?;
    let panel_bundles_path = validate_receipt_jsonl(
        &root,
        field(map, "candidatePanelBundles")?,
        field(execution, "candidatePanelBundlesJsonl")?,
        PANEL_BUNDLES_PATH,
        PANEL_BUNDLE_SCHEMA,
        Some("bundleSha256"),
    )?;
    let panel_bundle_count = unsigned(
        object(
            field(map, "candidatePanelBundles")?,
            "panel bundle descriptor",
        )?,
        "recordCount",
    )?;
    ensure!(
        panel_bundle_count == evaluated_member_count
            && evaluated_member_count == unsigned(campaign_input, "candidateCount")?,
        "campaign-output panel bundle/member/input count drifted"
    );
    Ok(CampaignOutputCheckpoint {
        value,
        root,
        checkpoint_sha256,
        generation_index,
        campaign_role,
        panel_id,
        rotating_evidence_sha256,
        task_matrix_sha256,
        task_count,
        evaluated_members_path,
        evaluated_member_count,
        panel_bundles_path,
        panel_bundle_count,
    })
}

/// Open the complete durable campaign-output graph for scientific reduction.
///
/// `open_checkpoint` deliberately remains a compact restart boundary.  This
/// stricter opener is for post-run analysis: it reopens the manifest, campaign
/// input, cohort, task pack, gateway receipt/journal/result pack, tail index,
/// evaluated members, and panel bundles before returning any scientific data.
/// Callers therefore cannot substitute precomputed summaries for production
/// evidence while claiming that the underlying identities were checked.
pub fn authenticate_output_graph(path: &Path) -> Result<Value> {
    let checkpoint = open_checkpoint(path)?;
    let manifest_path = checkpoint.root.join(MANIFEST_PATH);
    let manifest = parse_manifest(&manifest_path)?;
    validate_manifest_checkpoint_binding(&manifest, &checkpoint)?;

    let input = open_v5_campaign_input_checkpoint(&manifest.campaign_input_checkpoint_path)?;
    validate_input_binding(&manifest, &input)?;
    let (_source, gateway_receipt) = build_source(&manifest, &input)?;

    let checkpoint_map = object(&checkpoint.value, "campaign-output checkpoint")?;
    let execution = object(
        field(checkpoint_map, "executionBindings")?,
        "campaign-output execution bindings",
    )?;
    validate_external_execution_binding(
        field(execution, "campaignInputCheckpoint")?,
        &manifest.campaign_input_checkpoint_path,
        "campaign-input checkpoint",
    )?;
    validate_external_execution_binding(
        field(execution, "gatewayExecutionReceipt")?,
        &manifest.gateway_execution_receipt_path,
        "gateway execution receipt",
    )?;

    let cohort_population = read_authenticated_json_value(
        &input.cohort_population_path,
        "authenticated cohort population",
    )?;
    let task_rows = read_canonical_jsonl_values(
        &input.task_pack_path,
        input.task_count,
        "authenticated campaign task pack",
    )?;
    let evaluated_members = read_canonical_jsonl_values(
        &checkpoint.evaluated_members_path,
        checkpoint.evaluated_member_count,
        "authenticated evaluated members",
    )?;
    let candidate_panel_bundles = read_canonical_jsonl_values(
        &checkpoint.panel_bundles_path,
        checkpoint.panel_bundle_count,
        "authenticated candidate panel bundles",
    )?;
    let tail_result_index = read_authenticated_json_value(
        &checkpoint.root.join(DIRECTIONAL_INDEX_PATH),
        "authenticated tail result index",
    )?;

    let mut graph = json!({
        "schemaVersion": AUTHENTICATED_GRAPH_SCHEMA,
        "checkpointPath": absolute_string(&checkpoint.root.join(CHECKPOINT_PATH))?,
        "checkpointSha256": checkpoint.checkpoint_sha256,
        "manifestSha256": manifest.manifest_sha256,
        "runtimeAuthoritySha256": manifest.runtime_authority_sha256,
        "generationIndex": checkpoint.generation_index,
        "campaignRole": checkpoint.campaign_role,
        "panelId": checkpoint.panel_id,
        "rotatingEvidenceSha256": checkpoint.rotating_evidence_sha256,
        "taskMatrixSha256": checkpoint.task_matrix_sha256,
        "taskCount": checkpoint.task_count,
        "campaignOutputCheckpoint": checkpoint.value,
        "campaignInputCheckpoint": input.value,
        "gatewayExecutionReceipt": gateway_receipt,
        "cohortPopulation": cohort_population,
        "campaignTasks": task_rows,
        "tailResultIndex": tail_result_index,
        "evaluatedMembers": evaluated_members,
        "candidatePanelBundles": candidate_panel_bundles,
    });
    add_identity(&mut graph, "authenticatedGraphSha256")?;
    Ok(graph)
}

fn validate_external_execution_binding(
    descriptor: &Value,
    expected_path: &Path,
    label: &str,
) -> Result<()> {
    let map = object(descriptor, label)?;
    let expected = existing_file(expected_path, label)?.canonicalize()?;
    let bound = PathBuf::from(text(map, "path")?)
        .canonicalize()
        .with_context(|| format!("open bound {label}"))?;
    ensure!(
        bound == expected
            && fs::metadata(&expected)?.len() == unsigned(map, "sizeBytes")?
            && sha_file(&expected)? == sha_field(map, "rawSha256")?,
        "campaign-output external execution binding drifted for {label}"
    );
    Ok(())
}

fn read_canonical_jsonl_values(path: &Path, expected: u64, label: &str) -> Result<Vec<Value>> {
    let path = existing_file(path, label)?;
    let mut rows = Vec::new();
    for line in BufReader::new(File::open(path)?).lines() {
        let line = line?;
        ensure!(!line.is_empty(), "{label} contains a blank row");
        let row: Value = serde_json::from_str(&line).with_context(|| format!("parse {label}"))?;
        ensure!(
            canonical_json_bytes(&row)? == line.as_bytes(),
            "{label} contains a noncanonical row"
        );
        rows.push(row);
    }
    ensure!(rows.len() as u64 == expected, "{label} count drifted");
    Ok(rows)
}

fn read_authenticated_json_value(path: &Path, label: &str) -> Result<Value> {
    let path = existing_file(path, label)?;
    serde_json::from_reader(File::open(path)?).with_context(|| format!("parse {label}"))
}

fn validate_bound_output(
    root: &Path,
    descriptor: &Value,
    expected_relative: &str,
) -> Result<PathBuf> {
    let map = object(descriptor, "campaign-output execution descriptor")?;
    let expected = root
        .join(expected_relative)
        .canonicalize()
        .with_context(|| format!("open campaign-output artifact: {expected_relative}"))?;
    let path = PathBuf::from(text(map, "path")?);
    ensure!(
        path == expected
            && path.parent() == Some(root)
            && fs::metadata(&path)?.len() == unsigned(map, "sizeBytes")?
            && sha_file(&path)? == sha_field(map, "rawSha256")?,
        "campaign-output execution artifact binding drifted"
    );
    Ok(path)
}

fn validate_receipt_jsonl(
    root: &Path,
    semantic: &Value,
    execution: &Value,
    expected_relative: &str,
    row_schema: &str,
    hash_field: Option<&str>,
) -> Result<PathBuf> {
    let path = validate_bound_output(root, execution, expected_relative)?;
    let semantic_map = object(semantic, "campaign-output JSONL semantic descriptor")?;
    let execution_map = object(execution, "campaign-output JSONL execution descriptor")?;
    ensure!(
        text(semantic_map, "rowSchema")? == row_schema
            && sha_field(semantic_map, "rawSha256")? == sha_field(execution_map, "rawSha256")?
            && unsigned(semantic_map, "sizeBytes")? == unsigned(execution_map, "sizeBytes")?,
        "campaign-output JSONL semantic/execution descriptor drifted"
    );
    let mut count = 0_u64;
    for line in BufReader::new(File::open(&path)?).lines() {
        let line = line?;
        ensure!(
            !line.is_empty(),
            "campaign-output JSONL contains a blank row"
        );
        let row: Value = serde_json::from_str(&line)?;
        ensure!(
            canonical_json_bytes(&row)? == line.as_bytes()
                && text(object(&row, "campaign-output JSONL row")?, "schemaVersion")? == row_schema,
            "campaign-output JSONL row is noncanonical or has wrong schema"
        );
        if let Some(field_name) = hash_field {
            ensure!(
                canonical_sha256_without_object_field(&row, field_name)?
                    == sha_field(object(&row, "campaign-output JSONL row")?, field_name)?,
                "campaign-output JSONL row identity drifted"
            );
        }
        count = count
            .checked_add(1)
            .ok_or_else(|| anyhow!("campaign-output JSONL count overflow"))?;
    }
    ensure!(
        count == unsigned(semantic_map, "recordCount")?,
        "campaign-output JSONL record count drifted"
    );
    Ok(path)
}

fn parse_manifest(path: &Path) -> Result<CampaignOutputManifest> {
    let value = read_canonical_value(path, "campaign-output manifest")?;
    let map = object(&value, "campaign-output manifest")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "contractVersion",
            "operation",
            "runtimeAuthoritySha256",
            "campaignInputCheckpointPath",
            "campaignInputCheckpointSha256",
            "gatewayExecutionReceiptPath",
            "gatewayExecutionReceiptSha256",
            "generationIndex",
            "campaignRole",
            "panelId",
            "rotatingEvidenceSha256",
            "panel",
            "cohortSource",
            "minimumTotalTrades",
            "minimumTradesPerWindow",
            "capTrades",
            "provisionalLimit",
            "resultPath",
            "manifestSha256",
        ],
        "campaign-output manifest",
    )?;
    ensure!(
        text(map, "schemaVersion")? == MANIFEST_SCHEMA
            && text(map, "contractVersion")? == CONTRACT_VERSION
            && text(map, "operation")? == OPERATION
            && text(map, "resultPath")? == CHECKPOINT_PATH,
        "campaign-output manifest schema/version/operation is incompatible"
    );
    let manifest_sha256 = sha_field(map, "manifestSha256")?;
    ensure!(
        canonical_sha256_without_object_field(&value, "manifestSha256")? == manifest_sha256,
        "campaign-output manifest identity drifted"
    );
    let campaign_role = text(map, "campaignRole")?;
    ensure!(
        matches!(
            campaign_role.as_str(),
            "proposal_current_panel" | "retained_parent_current_panel" | "prior_panel_backfill"
        ),
        "campaign-output manifest role is invalid"
    );
    let generation_index = unsigned(map, "generationIndex")?;
    ensure!(
        generation_index > 0,
        "campaign-output generation must be positive"
    );
    let panel = field(map, "panel")?.clone();
    ensure!(
        text(object(&panel, "campaign-output panel")?, "panelId")? == text(map, "panelId")?,
        "campaign-output panel identity drifted"
    );
    validate_cohort_source(field(map, "cohortSource")?)?;
    let provisional_limit = usize::try_from(unsigned(map, "provisionalLimit")?)?;
    ensure!(
        provisional_limit > 0,
        "campaign-output provisional limit must be positive"
    );
    Ok(CampaignOutputManifest {
        manifest_sha256,
        runtime_authority_sha256: sha_field(map, "runtimeAuthoritySha256")?,
        campaign_input_checkpoint_path: absolute_file_path(
            &text(map, "campaignInputCheckpointPath")?,
            "campaign-input checkpoint",
        )?,
        campaign_input_checkpoint_sha256: sha_field(map, "campaignInputCheckpointSha256")?,
        gateway_execution_receipt_path: absolute_file_path(
            &text(map, "gatewayExecutionReceiptPath")?,
            "gateway execution receipt",
        )?,
        gateway_execution_receipt_sha256: sha_field(map, "gatewayExecutionReceiptSha256")?,
        generation_index,
        campaign_role,
        panel_id: text(map, "panelId")?,
        rotating_evidence_sha256: sha_field(map, "rotatingEvidenceSha256")?,
        panel,
        cohort_source: field(map, "cohortSource")?.clone(),
        minimum_total_trades: nonnegative(map, "minimumTotalTrades")?,
        minimum_trades_per_window: nonnegative(map, "minimumTradesPerWindow")?,
        cap_trades: nonnegative(map, "capTrades")?,
        provisional_limit,
    })
}

fn validate_input_binding(
    manifest: &CampaignOutputManifest,
    input: &V5CampaignInputCheckpoint,
) -> Result<()> {
    ensure!(
        input.checkpoint_sha256 == manifest.campaign_input_checkpoint_sha256
            && input.generation_index == manifest.generation_index
            && input.campaign_role == manifest.campaign_role
            && input.panel_id == manifest.panel_id,
        "campaign-output manifest/input checkpoint binding drifted"
    );
    // The input checkpoint is authored by qd-campaign-freeze, while the output
    // manifest binds the independently pinned qd-campaign-seal execution.  The
    // checkpoint hash transitively preserves the freezer runtime authority;
    // requiring it to equal the sealer authority makes every real multi-binary
    // handoff impossible.
    Ok(())
}

fn build_source(
    manifest: &CampaignOutputManifest,
    input: &V5CampaignInputCheckpoint,
) -> Result<(Source, Value)> {
    let receipt_path = existing_file(
        &manifest.gateway_execution_receipt_path,
        "gateway execution receipt",
    )?;
    ensure!(
        receipt_path.file_name().and_then(|name| name.to_str()) == Some("execution-receipt.json")
            && receipt_path
                .parent()
                .and_then(Path::file_name)
                .and_then(|name| name.to_str())
                == Some(".native-gateway-dispatch"),
        "gateway execution receipt path is not fixed"
    );
    let sidecar = receipt_path
        .parent()
        .ok_or_else(|| anyhow!("gateway execution receipt has no sidecar root"))?;
    ensure_real_directory(sidecar, "gateway dispatcher sidecar")?;
    let receipt = read_canonical_value(&receipt_path, "gateway execution receipt")?;
    let map = object(&receipt, "gateway execution receipt")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "runtimeRoleSha256",
            "authorityId",
            "taskMatrixSha256",
            "campaignInputCheckpointSha256",
            "campaignTaskPackRawSha256",
            "campaignTaskPackSizeBytes",
            "taskIndexRootSha256",
            "taskCount",
            "completedTaskCount",
            "resultSetSemanticSha256",
            "completionJournalSha256",
            "resultPackSha256",
            "resultPackSizeBytes",
            "resultCount",
            "semanticReceiptSha256",
            "receiptSha256",
        ],
        "gateway execution receipt",
    )?;
    let receipt_sha256 = sha_field(map, "receiptSha256")?;
    ensure!(
        text(map, "schemaVersion")? == GATEWAY_RECEIPT_SCHEMA
            && canonical_sha256_without_object_field(&receipt, "receiptSha256")? == receipt_sha256
            && receipt_sha256 == manifest.gateway_execution_receipt_sha256
            && sha_field(map, "campaignInputCheckpointSha256")? == input.checkpoint_sha256
            && sha_field(map, "campaignTaskPackRawSha256")? == input.task_pack_raw_sha256
            && unsigned(map, "campaignTaskPackSizeBytes")? == input.task_pack_size_bytes
            && sha_field(map, "authorityId")? == input.authority_id
            && sha_field(map, "taskMatrixSha256")? == input.task_matrix_sha256
            && unsigned(map, "taskCount")? == input.task_count
            && unsigned(map, "completedTaskCount")? == input.task_count
            && unsigned(map, "resultCount")? == input.task_count,
        "gateway execution receipt/input checkpoint binding drifted"
    );
    let semantic = json!({
        "schemaVersion": GATEWAY_RECEIPT_SCHEMA,
        "runtimeRoleSha256": sha_field(map, "runtimeRoleSha256")?,
        "authorityId": input.authority_id,
        "taskMatrixSha256": input.task_matrix_sha256,
        "campaignInputCheckpointSha256": input.checkpoint_sha256,
        "campaignTaskPackRawSha256": input.task_pack_raw_sha256,
        "campaignTaskPackSizeBytes": input.task_pack_size_bytes,
        "taskIndexRootSha256": sha_field(map, "taskIndexRootSha256")?,
        "taskCount": input.task_count,
        "completedTaskCount": input.task_count,
        "resultSetSemanticSha256": sha_field(map, "resultSetSemanticSha256")?,
    });
    ensure!(
        canonical_sha256(&semantic)? == sha_field(map, "semanticReceiptSha256")?,
        "gateway execution semantic receipt drifted"
    );

    let journal_path = existing_file(
        &sidecar.join("completion-journal.jsonl"),
        "gateway completion journal",
    )?;
    let result_pack_path = existing_file(&sidecar.join("results.pack"), "gateway result pack")?;
    ensure!(
        sha_file(&journal_path)? == sha_field(map, "completionJournalSha256")?
            && sha_file(&result_pack_path)? == sha_field(map, "resultPackSha256")?
            && fs::metadata(&result_pack_path)?.len() == unsigned(map, "resultPackSizeBytes")?,
        "gateway journal/result-pack byte binding drifted"
    );
    let completions = load_packed_gateway_completions(&journal_path, map)?;

    let mut tasks = Vec::with_capacity(input.task_count as usize);
    let mut seen = BTreeSet::new();
    let reader = BufReader::new(File::open(&input.task_pack_path)?);
    for line in reader.lines() {
        let line = line?;
        ensure!(
            !line.is_empty(),
            "campaign-input task pack contains an empty row"
        );
        let row: Value = serde_json::from_str(&line).context("parse campaign-input task row")?;
        ensure!(
            canonical_json_bytes(&row)? == line.as_bytes(),
            "campaign-input task row is not canonical"
        );
        let mut task = source_task_from_manifest_row(&row)?;
        let task_id = text(object(&task.task, "source task")?, "taskId")?;
        ensure!(
            seen.insert(task_id.clone()),
            "campaign-input task id repeats"
        );
        let completion = completions
            .get(&task_id)
            .ok_or_else(|| anyhow!("campaign-input task lacks a terminal gateway record"))?;
        ensure!(
            completion.task_sha256 == canonical_sha256(&row)?,
            "gateway completion task identity drifted"
        );
        let record_map = object(&completion.record, "gateway completion record")?;
        task.raw_result_path = result_pack_path.clone();
        task.raw_result_offset_bytes = Some(unsigned(record_map, "resultPackOffsetBytes")?);
        task.raw_result_length_bytes = Some(unsigned(record_map, "resultPackLengthBytes")?);
        task.raw_ref = json!({
            "schemaVersion": "temporal_qd_tail_raw_result_ref_v1",
            "relativePath": "results.pack",
            "resultSha256": field(record_map, "resultSha256")?,
            "codec": "gzip-json-v1",
            "semanticSizeBytes": field(record_map, "resultSemanticSizeBytes")?,
            "uncompressedSha256": field(record_map, "resultUncompressedSha256")?,
            "uncompressedSizeBytes": field(record_map, "resultUncompressedSizeBytes")?,
            "blobSha256": field(record_map, "resultBlobSha256")?,
            "blobSizeBytes": field(record_map, "resultBlobSizeBytes")?,
        });
        tasks.push(task);
    }
    ensure!(
        tasks.len() as u64 == input.task_count && completions.len() == tasks.len(),
        "campaign-output task/result cardinality drifted"
    );
    tasks.sort_by(|left, right| {
        text(object(&left.task, "left task").unwrap(), "taskId")
            .unwrap()
            .cmp(&text(object(&right.task, "right task").unwrap(), "taskId").unwrap())
    });
    let source_root = json!({
        "schemaVersion": SOURCE_ROOT_SCHEMA,
        "campaignInputCheckpointSha256": input.checkpoint_sha256,
        "gatewayExecutionReceiptSha256": receipt_sha256,
        "gatewayResultSetSemanticSha256": sha_field(map, "resultSetSemanticSha256")?,
        "gatewayResultPackSha256": sha_field(map, "resultPackSha256")?,
        "taskMatrixSha256": input.task_matrix_sha256,
        "taskCount": input.task_count,
    });
    Ok((
        Source {
            authority_id: input.authority_id.clone(),
            authority_sha256: input.authority_id.clone(),
            task_matrix_sha256: input.task_matrix_sha256.clone(),
            task_manifest_sha256: input.task_pack_raw_sha256.clone(),
            checkpoint_sha256: sha_field(map, "semanticReceiptSha256")?,
            include_funnel: true,
            tasks,
            source_sha256: canonical_sha256(&source_root)?,
        },
        receipt,
    ))
}

#[derive(Clone, Debug)]
struct PackedGatewayCompletion {
    task_sha256: String,
    record: Value,
}

fn load_packed_gateway_completions(
    journal_path: &Path,
    receipt: &Map<String, Value>,
) -> Result<BTreeMap<String, PackedGatewayCompletion>> {
    let mut completions = BTreeMap::new();
    let mut semantic_rows = BTreeMap::new();
    let mut ordinal = 0_u64;
    let mut expected_pack_offset = 0_u64;
    for line in BufReader::new(File::open(journal_path)?).lines() {
        let line = line?;
        ensure!(
            !line.is_empty(),
            "gateway completion journal has an empty row"
        );
        let value: Value = serde_json::from_str(&line)?;
        ensure!(
            canonical_json_bytes(&value)? == line.as_bytes(),
            "gateway completion journal row is not canonical"
        );
        let row = object(&value, "gateway completion journal row")?;
        exact_keys(
            row,
            &[
                "schemaVersion",
                "ordinal",
                "taskId",
                "taskSha256",
                "completionSha256",
                "record",
                "entrySha256",
            ],
            "gateway completion journal row",
        )?;
        ensure!(
            text(row, "schemaVersion")? == "temporal_qd_native_gateway_completion_journal_entry_v1"
                && unsigned(row, "ordinal")? == ordinal
                && canonical_sha256_without_object_field(&value, "entrySha256")?
                    == sha_field(row, "entrySha256")?,
            "gateway completion journal row identity drifted"
        );
        let task_id = safe_task_id(&text(row, "taskId")?)?;
        let task_sha256 = sha_field(row, "taskSha256")?;
        let completion_sha256 = sha_field(row, "completionSha256")?;
        let record = field(row, "record")?.clone();
        let record_map = object(&record, "gateway completion record")?;
        let mut expected: BTreeSet<&str> = [
            "resultSha256",
            "candidateId",
            "resultCodec",
            "resultSemanticSha256",
            "resultSemanticSizeBytes",
            "resultUncompressedSha256",
            "resultUncompressedSizeBytes",
            "resultBlobSha256",
            "resultBlobSizeBytes",
            "resultPackOffsetBytes",
            "resultPackLengthBytes",
        ]
        .into_iter()
        .collect();
        let rejected = record_map.get("outcome") == Some(&Value::String("rejected".into()));
        if rejected {
            expected.insert("outcome");
            expected.insert("rejectionCode");
        }
        ensure!(
            record_map.len() == expected.len()
                && expected
                    .iter()
                    .all(|field_name| record_map.contains_key(*field_name)),
            "gateway completion record fields are not exact"
        );
        for field_name in [
            "resultSha256",
            "resultSemanticSha256",
            "resultUncompressedSha256",
            "resultBlobSha256",
        ] {
            sha_field(record_map, field_name)?;
        }
        for field_name in [
            "resultSemanticSizeBytes",
            "resultUncompressedSizeBytes",
            "resultBlobSizeBytes",
            "resultPackOffsetBytes",
            "resultPackLengthBytes",
        ] {
            unsigned(record_map, field_name)?;
        }
        ensure!(
            text(record_map, "resultCodec")? == "gzip-json-v1"
                && sha_field(record_map, "resultSha256")?
                    == sha_field(record_map, "resultSemanticSha256")?
                && unsigned(record_map, "resultPackOffsetBytes")? == expected_pack_offset
                && unsigned(record_map, "resultPackLengthBytes")?
                    == unsigned(record_map, "resultBlobSizeBytes")?,
            "gateway completion result-pack binding drifted"
        );
        if rejected {
            ensure!(
                matches!(
                    text(record_map, "rejectionCode")?.as_str(),
                    "aligned_scoring_warmup_insufficient"
                        | "duplicate_break_even_execution_invariant"
                ),
                "gateway completion rejection code is incompatible"
            );
        }
        expected_pack_offset = expected_pack_offset
            .checked_add(unsigned(record_map, "resultPackLengthBytes")?)
            .ok_or_else(|| anyhow!("gateway result-pack size overflow"))?;
        ensure!(
            completions
                .insert(
                    task_id.clone(),
                    PackedGatewayCompletion {
                        task_sha256: task_sha256.clone(),
                        record: record.clone(),
                    },
                )
                .is_none(),
            "gateway completion journal repeats a task"
        );
        semantic_rows.insert(
            task_id.clone(),
            json!({
                "taskId": task_id,
                "taskSha256": task_sha256,
                "completionSha256": completion_sha256,
                "resultSemanticSha256": sha_field(record_map, "resultSemanticSha256")?,
                "outcome": record_map
                    .get("outcome")
                    .cloned()
                    .unwrap_or(Value::String("admitted".into())),
            }),
        );
        ordinal = ordinal
            .checked_add(1)
            .ok_or_else(|| anyhow!("gateway completion ordinal overflow"))?;
    }
    ensure!(
        ordinal == unsigned(receipt, "resultCount")?
            && ordinal == unsigned(receipt, "taskCount")?
            && expected_pack_offset == unsigned(receipt, "resultPackSizeBytes")?
            && canonical_sha256(&Value::Array(semantic_rows.into_values().collect()))?
                == sha_field(receipt, "resultSetSemanticSha256")?,
        "gateway completion journal semantic/size closure drifted"
    );
    Ok(completions)
}

fn directional_tail_authority(runtime: &str, generation: u64) -> Result<Value> {
    let mut value = json!({
        "schemaVersion": DIRECTIONAL_TAIL_AUTHORITY_SCHEMA,
        "generationIndex": generation,
        "runtimeAuthoritySha256": runtime,
        "tailResultIndexSchema": DIRECTIONAL_INDEX_SCHEMA,
        "tailResultEntrySchema": DIRECTIONAL_ENTRY_SCHEMA,
        "rawRotatingProvenanceSchema": RAW_ROTATING_PROVENANCE_SCHEMA,
    });
    add_identity(&mut value, "tailAuthoritySha256")?;
    Ok(value)
}

fn panel_tail_index_descriptor(root: &Path, index: &Value) -> Result<Value> {
    let path = root.join(DIRECTIONAL_INDEX_PATH);
    Ok(json!({
        "path": absolute_string(&path)?,
        "relativePath": DIRECTIONAL_INDEX_PATH,
        "rawSha256": sha_file(&path)?,
        "sizeBytes": fs::metadata(&path)?.len(),
        "tailResultIndexSha256": sha_field(object(index, "tail index")?, "tailResultIndexSha256")?,
    }))
}

fn checkpoint_artifact_descriptor(
    root: &Path,
    relative: &str,
    semantic: Option<(&str, String)>,
    count: Option<(&str, u64)>,
) -> Result<Value> {
    let path = existing_file(&root.join(relative), "campaign-output staged artifact")?;
    let mut descriptor = Map::from_iter([
        (
            "schemaVersion".to_owned(),
            Value::String(ARTIFACT_DESCRIPTOR_SCHEMA.to_owned()),
        ),
        (
            "relativePath".to_owned(),
            Value::String(relative.to_owned()),
        ),
        ("rawSha256".to_owned(), Value::String(sha_file(&path)?)),
        (
            "sizeBytes".to_owned(),
            Value::from(fs::metadata(path)?.len()),
        ),
    ]);
    if let Some((field_name, value)) = semantic {
        descriptor.insert(field_name.to_owned(), Value::String(value));
    }
    if let Some((field_name, value)) = count {
        descriptor.insert(field_name.to_owned(), Value::from(value));
    }
    Ok(Value::Object(descriptor))
}

fn checkpoint_jsonl_descriptor(
    root: &Path,
    relative: &str,
    row_schema: &str,
    hash_field: &str,
) -> Result<Value> {
    let path = existing_file(&root.join(relative), "campaign-output staged JSONL")?;
    let mut count = 0_u64;
    for line in BufReader::new(File::open(path)?).lines() {
        let line = line?;
        ensure!(
            !line.is_empty(),
            "campaign-output JSONL contains a blank row"
        );
        let row: Value = serde_json::from_str(&line)?;
        ensure!(
            canonical_json_bytes(&row)? == line.as_bytes()
                && text(object(&row, "campaign-output JSONL row")?, "schemaVersion")? == row_schema,
            "campaign-output JSONL row schema/canonical form drifted"
        );
        if row_schema == PANEL_BUNDLE_SCHEMA {
            ensure!(
                canonical_sha256_without_object_field(&row, hash_field)?
                    == sha_field(object(&row, "campaign-output bundle row")?, hash_field)?,
                "campaign-output panel bundle identity drifted"
            );
        } else {
            field(object(&row, "campaign-output member row")?, hash_field)?;
        }
        count = count
            .checked_add(1)
            .ok_or_else(|| anyhow!("campaign-output row count overflow"))?;
    }
    let mut descriptor =
        checkpoint_artifact_descriptor(root, relative, None, Some(("recordCount", count)))?;
    descriptor
        .as_object_mut()
        .expect("artifact descriptor")
        .insert("rowSchema".into(), Value::String(row_schema.to_owned()));
    Ok(descriptor)
}

fn semantic_jsonl_descriptor(descriptor: &Value) -> Result<Value> {
    let map = object(descriptor, "campaign-output JSONL descriptor")?;
    Ok(json!({
        "rawSha256": sha_field(map, "rawSha256")?,
        "sizeBytes": unsigned(map, "sizeBytes")?,
        "recordCount": unsigned(map, "recordCount")?,
        "rowSchema": text(map, "rowSchema")?,
    }))
}

fn execution_descriptor(path: &Path) -> Result<Value> {
    let path = existing_file(path, "campaign-output execution artifact")?.canonicalize()?;
    Ok(json!({
        "path": absolute_string(&path)?,
        "rawSha256": sha_file(&path)?,
        "sizeBytes": fs::metadata(path)?.len(),
    }))
}

fn validate_cohort_source(value: &Value) -> Result<()> {
    let map = object(value, "campaign-output cohort source")?;
    exact_keys(
        map,
        &[
            "kind",
            "sourceSemanticSha256",
            "candidateCount",
            "selectionSha256",
        ],
        "campaign-output cohort source",
    )?;
    ensure!(
        matches!(
            text(map, "kind")?.as_str(),
            "proposal_evaluation_population" | "sealed_cohort_selection"
        ),
        "campaign-output cohort source kind is invalid"
    );
    sha_field(map, "sourceSemanticSha256")?;
    unsigned(map, "candidateCount")?;
    if text(map, "kind")? == "proposal_evaluation_population" {
        ensure!(
            field(map, "selectionSha256")?.is_null(),
            "proposal cohort source cannot name a selection"
        );
    } else {
        sha_field(map, "selectionSha256")?;
    }
    Ok(())
}

fn validate_manifest_checkpoint_binding(
    manifest: &CampaignOutputManifest,
    checkpoint: &CampaignOutputCheckpoint,
) -> Result<()> {
    let map = object(&checkpoint.value, "campaign-output checkpoint")?;
    let campaign_input = object(
        field(map, "campaignInput")?,
        "campaign-output input summary",
    )?;
    ensure!(
        sha_field(map, "manifestSha256")? == manifest.manifest_sha256
            && sha_field(map, "runtimeAuthoritySha256")? == manifest.runtime_authority_sha256
            && sha_field(campaign_input, "checkpointSha256")?
                == manifest.campaign_input_checkpoint_sha256
            && sha_field(map, "gatewayReceiptSha256")? == manifest.gateway_execution_receipt_sha256
            && checkpoint.generation_index == manifest.generation_index
            && checkpoint.campaign_role == manifest.campaign_role
            && checkpoint.panel_id == manifest.panel_id
            && checkpoint.rotating_evidence_sha256 == manifest.rotating_evidence_sha256
            && field(map, "cohortSource")? == &manifest.cohort_source,
        "campaign-output checkpoint/manifest binding drifted"
    );
    Ok(())
}

fn result_value(
    checkpoint: &CampaignOutputCheckpoint,
    restart: bool,
    started: Instant,
) -> Result<Value> {
    result_value_with_metrics(checkpoint, restart, started, None)
}

fn result_value_with_metrics(
    checkpoint: &CampaignOutputCheckpoint,
    restart: bool,
    started: Instant,
    metrics: Option<&ReadMetrics>,
) -> Result<Value> {
    let output_bytes = [
        CHECKPOINT_PATH,
        DIRECTIONAL_INDEX_PATH,
        temporal_qd_tail_reducer::MEMBERS_PATH,
        PANEL_BUNDLES_PATH,
    ]
    .into_iter()
    .try_fold(0_u64, |total, relative| {
        total
            .checked_add(fs::metadata(checkpoint.root.join(relative))?.len())
            .ok_or_else(|| anyhow!("campaign-output payload byte count overflow"))
    })?;
    let control_bytes = fs::metadata(checkpoint.root.join(MANIFEST_PATH))?.len();
    let durable_bytes = output_bytes
        .checked_add(control_bytes)
        .ok_or_else(|| anyhow!("campaign-output durable byte count overflow"))?;
    Ok(json!({
        "schemaVersion": RESULT_SCHEMA,
        "restart": restart,
        "checkpointPath": checkpoint.root.join(CHECKPOINT_PATH),
        "checkpointSha256": checkpoint.checkpoint_sha256,
        "generationIndex": checkpoint.generation_index,
        "campaignRole": checkpoint.campaign_role,
        "panelId": checkpoint.panel_id,
        "taskCount": checkpoint.task_count,
        "evaluatedMemberCount": checkpoint.evaluated_member_count,
        "panelBundleCount": checkpoint.panel_bundle_count,
        "semanticReceiptSha256": checkpoint.value["semanticReceiptSha256"],
        "receiptSha256": checkpoint.value["receiptSha256"],
        "campaignSeal": checkpoint.value["campaignSealDocument"],
        "directionalTailAuthority": directional_tail_authority(
            checkpoint.value["runtimeAuthoritySha256"].as_str().ok_or_else(|| anyhow!("campaign-output runtime authority is invalid"))?,
            checkpoint.generation_index,
        )?,
        "tailResultIndex": {
            "path": checkpoint.root.join(DIRECTIONAL_INDEX_PATH),
            "relativePath": DIRECTIONAL_INDEX_PATH,
            "rawSha256": sha_file(&checkpoint.root.join(DIRECTIONAL_INDEX_PATH))?,
            "sizeBytes": fs::metadata(checkpoint.root.join(DIRECTIONAL_INDEX_PATH))?.len(),
            "tailResultIndexSha256": checkpoint.value["campaignSeal"]["tailResultIndexSha256"],
        },
        "artifactMetrics": {
            "durableFileCount": 5,
            "durableBytes": durable_bytes,
            "checkpointFileCount": 4,
            "checkpointBytes": output_bytes,
            "controlFileCount": 1,
            "controlBytes": control_bytes,
            "rawResultReadCount": metrics.map_or(0, |value| value.raw_reads),
            "rawResultBlobBytes": metrics.map_or(0, |value| value.blob_bytes),
            "rawResultUncompressedBytes": metrics.map_or(0, |value| value.uncompressed_bytes),
            "rawResultSemanticBytes": metrics.map_or(0, |value| value.semantic_bytes),
            "elapsedMilliseconds": started.elapsed().as_millis() as u64,
        },
    }))
}

fn reset_staging_directory(path: &Path) -> Result<()> {
    if path.exists() {
        ensure_real_directory(path, "campaign-output staging directory")?;
        fs::remove_dir_all(path).context("remove stale campaign-output staging directory")?;
    }
    fs::create_dir(path).context("create campaign-output staging directory")?;
    ensure_real_directory(path, "campaign-output staging directory")
}

fn write_new_synced_file(path: &Path, bytes: &[u8], label: &str) -> Result<()> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .with_context(|| format!("create {label}: {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("write {label}: {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("sync {label}: {}", path.display()))?;
    Ok(())
}

fn write_jsonl(path: &Path, rows: &[Value], row_schema: &str, hash_field: &str) -> Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    for row in rows {
        ensure!(
            text(object(row, "campaign-output JSONL row")?, "schemaVersion")? == row_schema
                && canonical_sha256_without_object_field(row, hash_field)?
                    == sha_field(object(row, "campaign-output JSONL row")?, hash_field)?,
            "campaign-output JSONL row is invalid"
        );
        file.write_all(&canonical_json_line(row)?)?;
    }
    file.sync_all()?;
    Ok(())
}

fn promote_staged_file(staging: &Path, output_root: &Path, relative: &str) -> Result<()> {
    ensure!(
        !relative.contains('/') && !relative.contains('\\'),
        "campaign-output promoted path must be a fixed leaf"
    );
    let source = existing_file(&staging.join(relative), "campaign-output staged artifact")?;
    let destination = output_root.join(relative);
    if destination.exists() {
        ensure!(
            fs::metadata(&source)?.len() == fs::metadata(&destination)?.len()
                && sha_file(&source)? == sha_file(&destination)?,
            "campaign-output partial commit differs from recomputed artifact"
        );
        fs::remove_file(source)?;
        return Ok(());
    }
    fs::rename(source, &destination)?;
    sync_directory(output_root)
}

fn absolute_file_path(value: &str, label: &str) -> Result<PathBuf> {
    let path = PathBuf::from(value);
    ensure!(path.is_absolute(), "{label} path must be absolute");
    existing_file(&path, label)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHA_A: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const SHA_B: &str = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const SHA_C: &str = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    #[test]
    fn partial_staging_is_discarded_before_synced_index_rewrite() -> Result<()> {
        let root = tempfile::tempdir()?;
        let staging = root.path().join(".campaign-output.staging");
        fs::create_dir(&staging)?;
        fs::write(staging.join(DIRECTIONAL_INDEX_PATH), b"partial")?;
        fs::write(staging.join(".p123.456.tmp"), b"orphan")?;

        reset_staging_directory(&staging)?;
        assert_eq!(fs::read_dir(&staging)?.count(), 0);
        let index = staging.join(DIRECTIONAL_INDEX_PATH);
        write_new_synced_file(&index, b"complete\n", "test tail-result index")?;
        assert_eq!(fs::read(index)?, b"complete\n");
        Ok(())
    }

    fn write_value(path: &Path, value: &Value, newline: bool) -> Result<()> {
        let bytes = if newline {
            canonical_json_line(value)?
        } else {
            canonical_json_bytes(value)?
        };
        fs::write(path, bytes)?;
        Ok(())
    }

    fn semantic_descriptor(path: &Path, row_schema: &str) -> Result<Value> {
        Ok(json!({
            "rawSha256": sha_file(path)?,
            "sizeBytes": fs::metadata(path)?.len(),
            "recordCount": 1,
            "rowSchema": row_schema,
        }))
    }

    fn checkpoint_fixture(root: &Path) -> Result<Value> {
        fs::create_dir_all(root)?;
        let input_path = root.join("external-input.json");
        let gateway_path = root.join("external-gateway.json");
        fs::write(&input_path, b"{}\n")?;
        fs::write(&gateway_path, b"{}\n")?;

        let mut index = json!({
            "schemaVersion": DIRECTIONAL_INDEX_SCHEMA,
            "authorityId": SHA_A,
            "authoritySha256": SHA_A,
            "taskMatrixSha256": SHA_B,
            "taskManifestSha256": SHA_C,
            "checkpointSha256": SHA_A,
            "taskCount": 1,
            "funnelProjectionIncluded": true,
            "sourceResultBlobBytes": 1,
            "entries": [],
        });
        add_identity(&mut index, "tailResultIndexSha256")?;
        let index_path = root.join(DIRECTIONAL_INDEX_PATH);
        write_value(&index_path, &index, false)?;

        let member = json!({
            "schemaVersion": EVALUATED_MEMBER_SCHEMA,
            "candidate": {
                "candidateId": "candidate-1",
                "candidateIdentitySha256": SHA_A,
                "programSha256": SHA_B,
                "profileSnapshotSha256": SHA_C,
            },
        });
        let members_path = root.join(temporal_qd_tail_reducer::MEMBERS_PATH);
        write_value(&members_path, &member, true)?;

        let mut bundle = json!({
            "schemaVersion": PANEL_BUNDLE_SCHEMA,
            "candidateId": "candidate-1",
            "candidateIdentitySha256": SHA_A,
            "programSha256": SHA_B,
            "rotatingEvidenceSha256": SHA_C,
            "panelId": "panel-1",
            "windowEvidence": [],
        });
        add_identity(&mut bundle, "bundleSha256")?;
        let bundles_path = root.join(PANEL_BUNDLES_PATH);
        write_value(&bundles_path, &bundle, true)?;

        let campaign_input = json!({
            "checkpointSha256": SHA_A,
            "cohortPopulationSha256": SHA_B,
            "authorityId": SHA_C,
            "evaluationIdentitySha256": SHA_A,
            "campaignSha256": SHA_B,
            "taskMatrixSha256": SHA_C,
            "candidateCount": 1,
            "windowCount": 1,
            "taskCount": 1,
        });
        let mut campaign_seal_document = json!({
            "schemaVersion": CAMPAIGN_SEAL_SCHEMA,
            "fixture": true,
        });
        add_identity(&mut campaign_seal_document, "campaignSealSha256")?;
        let campaign_seal = json!({
            "directionalTailAuthoritySha256": SHA_A,
            "campaignSealSha256": campaign_seal_document["campaignSealSha256"],
            "tailResultIndexSha256": index["tailResultIndexSha256"],
            "tailTransactionSha256": SHA_C,
        });
        let evaluated_members = semantic_descriptor(&members_path, EVALUATED_MEMBER_SCHEMA)?;
        let candidate_panel_bundles = semantic_descriptor(&bundles_path, PANEL_BUNDLE_SCHEMA)?;
        let mut checkpoint = json!({
            "schemaVersion": CHECKPOINT_SCHEMA,
            "contractVersion": CONTRACT_VERSION,
            "generationIndex": 1,
            "campaignRole": "proposal_current_panel",
            "panelId": "panel-1",
            "rotatingEvidenceSha256": SHA_C,
            "cohortSource": {
                "kind": "proposal_evaluation_population",
                "sourceSemanticSha256": SHA_A,
                "candidateCount": 1,
                "selectionSha256": Value::Null,
            },
            "campaignInput": campaign_input,
            "campaignSeal": campaign_seal,
            "campaignSealDocument": campaign_seal_document,
            "evaluatedMembers": evaluated_members,
            "candidatePanelBundles": candidate_panel_bundles,
        });
        let semantic_receipt_sha256 = canonical_sha256(&checkpoint)?;
        let execution_bindings = json!({
            "campaignInputCheckpoint": execution_descriptor(&input_path)?,
            "gatewayExecutionReceipt": execution_descriptor(&gateway_path)?,
            "tailResultIndex": execution_descriptor(&index_path)?,
            "evaluatedMembersJsonl": execution_descriptor(&members_path)?,
            "candidatePanelBundlesJsonl": execution_descriptor(&bundles_path)?,
        });
        let map = checkpoint.as_object_mut().expect("checkpoint object");
        map.insert(
            "semanticReceiptSha256".into(),
            Value::String(semantic_receipt_sha256),
        );
        map.insert("manifestSha256".into(), Value::String(SHA_A.into()));
        map.insert("runtimeAuthoritySha256".into(), Value::String(SHA_B.into()));
        map.insert("gatewayReceiptSha256".into(), Value::String(SHA_C.into()));
        map.insert(
            "gatewaySemanticReceiptSha256".into(),
            Value::String(SHA_A.into()),
        );
        map.insert("executionBindings".into(), execution_bindings);
        let receipt_sha256 = canonical_sha256(&checkpoint)?;
        checkpoint
            .as_object_mut()
            .expect("checkpoint object")
            .insert("receiptSha256".into(), Value::String(receipt_sha256));
        Ok(checkpoint)
    }

    #[test]
    fn campaign_output_checkpoint_is_constant_size_and_restartable() -> Result<()> {
        let directory = tempfile::TempDir::new()?;
        let root = directory.path();
        let checkpoint = checkpoint_fixture(root)?;
        let checkpoint_path = root.join(CHECKPOINT_PATH);
        write_value(&checkpoint_path, &checkpoint, true)?;

        let opened = open_checkpoint(&checkpoint_path)?;
        assert_eq!(opened.task_count, 1);
        assert_eq!(opened.evaluated_member_count, 1);
        assert_eq!(opened.panel_bundle_count, 1);
        assert_eq!(
            fs::read_dir(root)?
                .filter_map(std::result::Result::ok)
                .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
                .count(),
            6,
        );

        // The committed checkpoint reopens only its three scientific payloads;
        // raw gateway/input control files are not part of restart work.
        fs::remove_file(root.join("external-input.json"))?;
        fs::remove_file(root.join("external-gateway.json"))?;
        open_checkpoint(&checkpoint_path)?;
        assert_eq!(
            fs::read_dir(root)?
                .filter_map(std::result::Result::ok)
                .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
                .count(),
            4,
        );

        let original = fs::read(root.join(PANEL_BUNDLES_PATH))?;
        fs::write(root.join(PANEL_BUNDLES_PATH), b"{}\n")?;
        assert!(open_checkpoint(&checkpoint_path).is_err());
        fs::write(root.join(PANEL_BUNDLES_PATH), original)?;
        open_checkpoint(&checkpoint_path)?;
        Ok(())
    }

    #[test]
    fn strict_analysis_opener_refuses_a_compact_checkpoint_without_its_source_graph() -> Result<()>
    {
        let directory = tempfile::TempDir::new()?;
        let root = directory.path();
        let checkpoint = checkpoint_fixture(root)?;
        let checkpoint_path = root.join(CHECKPOINT_PATH);
        write_value(&checkpoint_path, &checkpoint, true)?;

        // Compact restart remains intentionally valid, but post-run analysis
        // cannot claim authentication without the manifest/input/gateway graph.
        open_checkpoint(&checkpoint_path)?;
        assert!(authenticate_output_graph(&checkpoint_path).is_err());
        Ok(())
    }

    #[test]
    fn packed_gateway_journal_rejects_offset_and_semantic_drift() -> Result<()> {
        let directory = tempfile::TempDir::new()?;
        let journal_path = directory.path().join("completion-journal.jsonl");
        let record = json!({
            "resultSha256": SHA_A,
            "candidateId": "candidate-1",
            "resultCodec": "gzip-json-v1",
            "resultSemanticSha256": SHA_A,
            "resultSemanticSizeBytes": 1,
            "resultUncompressedSha256": SHA_B,
            "resultUncompressedSizeBytes": 1,
            "resultBlobSha256": SHA_C,
            "resultBlobSizeBytes": 7,
            "resultPackOffsetBytes": 0,
            "resultPackLengthBytes": 7,
        });
        let completion_sha256 = canonical_sha256(&record)?;
        let mut row = json!({
            "schemaVersion": "temporal_qd_native_gateway_completion_journal_entry_v1",
            "ordinal": 0,
            "taskId": "task-1",
            "taskSha256": SHA_B,
            "completionSha256": completion_sha256,
            "record": record,
        });
        add_identity(&mut row, "entrySha256")?;
        write_value(&journal_path, &row, true)?;
        let semantic_rows = Value::Array(vec![json!({
            "taskId": "task-1",
            "taskSha256": SHA_B,
            "completionSha256": row["completionSha256"],
            "resultSemanticSha256": SHA_A,
            "outcome": "admitted",
        })]);
        let receipt = Map::from_iter([
            ("resultCount".into(), Value::from(1_u64)),
            ("taskCount".into(), Value::from(1_u64)),
            ("resultPackSizeBytes".into(), Value::from(7_u64)),
            (
                "resultSetSemanticSha256".into(),
                Value::String(canonical_sha256(&semantic_rows)?),
            ),
        ]);
        assert_eq!(
            load_packed_gateway_completions(&journal_path, &receipt)?.len(),
            1
        );

        let mut tampered = row;
        tampered["record"]["resultPackOffsetBytes"] = Value::from(1_u64);
        add_identity(&mut tampered, "entrySha256")?;
        write_value(&journal_path, &tampered, true)?;
        assert!(load_packed_gateway_completions(&journal_path, &receipt).is_err());
        Ok(())
    }

    #[test]
    fn campaign_output_accepts_distinct_freezer_and_sealer_runtime_authorities() -> Result<()> {
        let manifest = CampaignOutputManifest {
            manifest_sha256: SHA_A.into(),
            runtime_authority_sha256: SHA_B.into(),
            campaign_input_checkpoint_path: PathBuf::from("campaign-input-checkpoint.json"),
            campaign_input_checkpoint_sha256: SHA_C.into(),
            gateway_execution_receipt_path: PathBuf::from("execution-receipt.json"),
            gateway_execution_receipt_sha256: SHA_A.into(),
            generation_index: 1,
            campaign_role: "proposal_current_panel".into(),
            panel_id: "panel-1".into(),
            rotating_evidence_sha256: SHA_B.into(),
            panel: json!({"panelId": "panel-1"}),
            cohort_source: json!({}),
            minimum_total_trades: 0,
            minimum_trades_per_window: 0,
            cap_trades: 0,
            provisional_limit: 1,
        };
        let input = V5CampaignInputCheckpoint {
            value: json!({}),
            root: PathBuf::new(),
            manifest_sha256: SHA_A.into(),
            native_runtime_authority_sha256: SHA_C.into(),
            generation_index: 1,
            campaign_role: "proposal_current_panel".into(),
            panel_id: "panel-1".into(),
            authority_id: SHA_A.into(),
            campaign_sha256: SHA_A.into(),
            evaluation_identity_sha256: SHA_A.into(),
            task_matrix_sha256: SHA_A.into(),
            candidate_count: 1,
            window_count: 1,
            task_count: 1,
            cohort_population_sha256: SHA_A.into(),
            checkpoint_sha256: SHA_C.into(),
            task_pack_path: PathBuf::new(),
            task_pack_raw_sha256: SHA_A.into(),
            task_pack_size_bytes: 0,
            cohort_population_path: PathBuf::new(),
            cohort_population_raw_sha256: SHA_A.into(),
            cohort_population_size_bytes: 0,
        };

        validate_input_binding(&manifest, &input)
    }
}
