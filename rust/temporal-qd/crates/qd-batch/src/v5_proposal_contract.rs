//! Closed outer contract for one native v5 proposal-construction transaction.
//!
//! The v5 kernel owns candidate construction and admission.  This envelope
//! deliberately carries frozen data once, binds the one batch executable that
//! is allowed to consume it, and contains no interpreter/JSONL authority or
//! per-candidate command.  A caller can only resume the exact immutable
//! transaction or fail closed; it cannot silently route a missing v5 feature
//! through the historical Python construction path.

use anyhow::{Context, Result, anyhow, bail};
use sha2::Digest;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Component, Path};
use temporal_qd_contract::{
    CONTRACT_VERSION, Map, Value, canonical_json_line, canonical_sha256,
    canonical_sha256_without_object_field,
};
use temporal_qd_kernel::v5::V5_SHARED_AUTHORITY_SCHEMA;

pub const V5_PROPOSAL_MANIFEST_SCHEMA: &str =
    "temporal_qd_native_v5_proposal_construction_manifest_v1";
pub const V5_PROPOSAL_RESULT_SCHEMA: &str = "temporal_qd_native_v5_proposal_construction_result_v5";
pub const V5_PROPOSAL_RECEIPT_SCHEMA: &str =
    "temporal_qd_native_v5_proposal_construction_receipt_v5";
pub const V5_PROPOSAL_EXECUTION_AUTHORITY_SCHEMA: &str =
    "temporal_qd_native_v5_proposal_execution_authority_v1";
/// Deterministic, receipt-sealed construction accounting.  Mutable execution
/// measurements are deliberately excluded and appear only in stdout-only
/// adoption evidence.
pub const V5_PROPOSAL_CONSTRUCTION_SUMMARY_SCHEMA: &str =
    "temporal_qd_native_v5_proposal_construction_summary_v1";
pub const V5_PROPOSAL_OUTPUT_INVENTORY_SCHEMA: &str =
    "temporal_qd_native_v5_proposal_output_inventory_v2";
pub const V5_PROPOSAL_OUTPUT_ROOT_SCHEMA: &str = "temporal_qd_native_v5_proposal_output_root_v1";
pub const V5_PROPOSAL_OBJECT_STORE_INVENTORY_SCHEMA: &str =
    "temporal_qd_native_v5_proposal_object_store_closure_v2";
pub const V5_PROPOSAL_OBJECT_INVENTORY_DESCRIPTOR_SCHEMA: &str =
    "temporal_qd_native_v5_proposal_object_inventory_descriptor_v1";
pub const V5_PROPOSAL_OBJECT_INVENTORY_ROW_SCHEMA: &str =
    "temporal_qd_native_v5_proposal_object_inventory_row_v1";
pub const V5_PROPOSAL_OBJECT_INVENTORY_PATH: &str = "v5-native/object-inventory.jsonl";
pub const V5_PROPOSAL_ADOPTION_EVIDENCE_SCHEMA: &str =
    "temporal_qd_native_v5_proposal_adoption_evidence_v2";
pub const V5_PROPOSAL_ADOPTION_TELEMETRY_SCHEMA: &str =
    "temporal_qd_native_v5_proposal_adoption_telemetry_v2";
/// The later-generation transaction/publication boundary has deliberately
/// separate outer seal schemas.  It must never be accepted as a sparse G0
/// bootstrap result merely because both executions share an invocation
/// manifest transport.
pub const V5_EVOLVED_PROPOSAL_RESULT_SCHEMA: &str =
    "temporal_qd_native_v5_evolved_construction_result_v3";
pub const V5_EVOLVED_PROPOSAL_RECEIPT_SCHEMA: &str =
    "temporal_qd_native_v5_evolved_construction_receipt_v3";
pub const V5_EVOLVED_PROPOSAL_CONSTRUCTION_SUMMARY_SCHEMA: &str =
    "temporal_qd_native_v5_evolved_construction_summary_v1";
pub const V5_EVOLVED_PROPOSAL_ADOPTION_EVIDENCE_SCHEMA: &str =
    "temporal_qd_native_v5_evolved_adoption_evidence_v1";
pub const V5_EVOLVED_PROPOSAL_ADOPTION_TELEMETRY_SCHEMA: &str =
    "temporal_qd_native_v5_evolved_adoption_telemetry_v1";
pub const V5_PROPOSAL_OPERATION: &str = "native_v5_proposal_construction";
pub const V5_PROPOSAL_RESULT_PATH: &str = "v5-proposal-result.json";
pub const V5_PROPOSAL_INPUTS_SCHEMA: &str = "temporal_qd_native_v5_proposal_inputs_v1";
pub const V5_PROPOSAL_INPUT_BINDING_SCHEMA: &str =
    "temporal_qd_native_v5_proposal_input_binding_v1";

const V5_G0_REQUIRED_OUTPUT_ARTIFACTS: [(&str, &str, Option<&str>); 11] = [
    (
        "attemptJournal",
        "v5-native/attempt-journal-root.json",
        Some("attemptJournalSha256"),
    ),
    (
        "attemptRows",
        "v5-native/attempts.jsonl",
        Some("attemptJournalSha256"),
    ),
    (
        "compactJournal",
        "v5-native/accepted-records.jsonl",
        Some("compactJournalSha256"),
    ),
    (
        "identityLedger",
        "v5-native/identity-ledger.json",
        Some("identityLedgerSha256"),
    ),
    (
        "selectedProjectionIndex",
        "v5-native/selected-projections.jsonl",
        Some("selectedProjectionIndexSha256"),
    ),
    (
        "sharedAuthority",
        "v5-native/authority/shared-authority.json",
        Some("expectedAuthoritySha256"),
    ),
    (
        "g0FunnelProjectionStream",
        "v5-native/g0-funnel-projections.jsonl",
        Some("g0FunnelProjectionStreamReceiptSha256"),
    ),
    ("evaluationPopulation", "evaluation-population.json", None),
    ("generationJournal", "generation-journal.json", None),
    ("pairConfig", "pair-config.json", None),
    ("population", "population.json", None),
];

const V5_G0_BOOTSTRAP_REQUIRED_OUTPUT_ARTIFACTS: [(&str, &str, Option<&str>); 3] = [
    ("g0AcceptedPool", "g0-bootstrap/accepted-pool.json", None),
    (
        "g0CampaignConstructionLedger",
        "g0-bootstrap/campaign-construction-ledger.json",
        None,
    ),
    ("g0Selection", "g0-bootstrap/selection.json", None),
];

/// Later generations do not publish G0’s compact journal/bootstrap files.
/// Their closed durable transaction lives in the content-addressed object
/// store, while this fixed public handoff contains only core-owned streaming
/// artifacts plus the next-generation identity-ledger facade.
const V5_EVOLVED_REQUIRED_OUTPUT_ARTIFACTS: [(&str, &str, Option<&str>); 5] = [
    ("evaluationPopulation", "evaluation-population.json", None),
    ("generationJournal", "generation-journal.json", None),
    (
        "identityLedger",
        "v5-native/identity-ledger.json",
        Some("identityLedgerSha256"),
    ),
    ("pairConfig", "pair-config.json", None),
    ("population", "population.json", None),
];

fn required_output_artifacts(
    generation_kind: &str,
) -> Result<Vec<(&'static str, &'static str, Option<&'static str>)>> {
    match generation_kind {
        "g0" => {
            let mut artifacts = V5_G0_REQUIRED_OUTPUT_ARTIFACTS.to_vec();
            artifacts.extend(V5_G0_BOOTSTRAP_REQUIRED_OUTPUT_ARTIFACTS);
            Ok(artifacts)
        }
        "evolved" => Ok(V5_EVOLVED_REQUIRED_OUTPUT_ARTIFACTS.to_vec()),
        _ => bail!("native v5 proposal output inventory generation kind is incompatible"),
    }
}

#[derive(Clone, Debug)]
pub struct V5ProposalManifest {
    pub authority_sha256: String,
    pub execution_authority: Value,
    pub frozen_authority: Value,
    pub expected_authority_sha256: String,
    pub output_root: String,
    pub final_newline: String,
    pub generation_config: Value,
    pub generation_config_sha256: String,
    pub generation_index: u64,
    pub generation_kind: String,
    pub requested_count: u64,
    pub evaluation_population_size: u64,
    pub max_proposal_attempts: u64,
    pub thread_cap: u64,
    pub inputs: Value,
    pub result_path: String,
    pub manifest_sha256: String,
}

#[derive(Clone, Debug)]
pub struct V5ProposalResult {
    pub value: Value,
}

/// Separate raw result wrapper for a completed later-generation transaction.
/// It is intentionally not interchangeable with [`V5ProposalResult`]: G0’s
/// selected projections/bootstrap roots do not exist in this sealed family.
#[derive(Clone, Debug)]
pub struct V5EvolvedProposalResult {
    pub value: Value,
}

/// File identity for one fixed public output artifact.  The outer contract
/// owns the fixed kind/path inventory; callers supply only the bytes that
/// were already staged by the typed kernel publisher and the kernel's
/// semantic root for that artifact.
#[derive(Clone, Debug)]
pub struct V5OutputArtifactIdentity {
    pub kind: String,
    pub relative_path: String,
    pub file_sha256: String,
    pub byte_length: u64,
    pub semantic_sha256: String,
}

/// File identity for one content-addressed object.  Its public path is not a
/// caller-controlled field: it is always derived from `object_sha256` as
/// `sha256/<digest>.json`, preventing inventory path aliases at the outer
/// contract boundary.
#[derive(Clone, Debug)]
pub struct V5ObjectStoreIdentity {
    pub object_sha256: String,
    pub file_sha256: String,
    pub byte_length: u64,
}

/// Re-iterable, bounded-row encoder for the full object closure.  The outer
/// receipt retains only its descriptor; production stages these rows directly
/// to a private file and never assembles the JSONL sidecar as one byte vector.
#[derive(Clone, Debug)]
pub struct V5ObjectInventorySidecar {
    objects: Vec<V5ObjectStoreIdentity>,
}

impl V5ObjectInventorySidecar {
    fn row(&self, ordinal: usize, object: &V5ObjectStoreIdentity) -> Result<Vec<u8>> {
        let digest = &object.object_sha256["sha256:".len()..];
        let mut row = Map::from_iter([
            (
                "schemaVersion".to_owned(),
                Value::String(V5_PROPOSAL_OBJECT_INVENTORY_ROW_SCHEMA.to_owned()),
            ),
            ("ordinal".to_owned(), Value::from(ordinal as u64)),
            (
                "relativePath".to_owned(),
                Value::String(format!("sha256/{digest}.json")),
            ),
            (
                "objectSha256".to_owned(),
                Value::String(object.object_sha256.clone()),
            ),
            (
                "fileSha256".to_owned(),
                Value::String(object.file_sha256.clone()),
            ),
            ("byteLength".to_owned(), Value::from(object.byte_length)),
        ]);
        let row_sha256 = canonical_sha256(&Value::Object(row.clone()))?;
        row.insert("rowSha256".to_owned(), Value::String(row_sha256));
        canonical_json_line(&Value::Object(row)).map_err(Into::into)
    }

    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        for (ordinal, object) in self.objects.iter().enumerate() {
            writer
                .write_all(&self.row(ordinal, object)?)
                .context("write canonical native v5 object inventory row")?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn encoded_bytes(&self) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.write_to(&mut bytes)?;
        Ok(bytes)
    }
}

/// Typed inputs for constructing the immutable outer receipt and its tiny
/// invocation result.  This is intentionally just an inventory/receipt
/// envelope: compact transaction replay, selected materialization, and all
/// publication semantics stay in qd-kernel.
#[derive(Clone, Debug)]
pub struct V5ProposalReceiptBuildInput {
    pub attempt_count: u64,
    pub attempt_journal_sha256: String,
    pub publication_request_sha256: String,
    pub publication_plan_sha256: String,
    /// The core-owned, self-hashed compact G0 funnel receipt.  It is stored
    /// as the exact core binding in the immutable object store, while this
    /// outer root makes it discoverable without a directory scan.
    pub g0_funnel_fragments_sha256: String,
    /// The core-owned receipt for the fixed public all-attempt JSONL stream.
    /// Its exact binding is stored in the immutable object store and this
    /// root cross-binds those semantics to the streamed artifact inventory.
    pub g0_funnel_projection_stream_receipt_sha256: String,
    pub compact_journal_sha256: String,
    pub identity_ledger_sha256: String,
    pub selected_projection_index_sha256: String,
    pub construction_summary: Value,
    pub artifacts: Vec<V5OutputArtifactIdentity>,
    pub objects: Vec<V5ObjectStoreIdentity>,
}

/// Typed inputs for the later-generation outer inventory/receipt envelope.
/// Candidate construction, scheduler replay, snapshots, and public document
/// semantics remain in qd-kernel; batch supplies only already-staged byte
/// identities and the typed durable roots to cross-bind them.
#[derive(Clone, Debug)]
pub struct V5EvolvedProposalReceiptBuildInput {
    pub attempt_count: u64,
    pub transaction_sha256: String,
    pub parent_archive_input_binding_sha256: String,
    pub identity_ledger_input_binding_sha256: String,
    pub publication_request_sha256: String,
    pub publication_plan_sha256: String,
    pub publication_receipt_sha256: String,
    /// Typed `temporal_qd_v5_evolved_publication_fragments_v2` root.  The
    /// bytes live only as a content-addressed immutable object; the outer
    /// envelope intentionally transports its identity rather than duplicating
    /// the attempt-stream receipt payload.
    pub publication_fragments_sha256: String,
    pub identity_ledger_sha256: String,
    pub construction_summary: Value,
    pub artifacts: Vec<V5OutputArtifactIdentity>,
    pub objects: Vec<V5ObjectStoreIdentity>,
}

fn exact_keys(map: &Map<String, Value>, expected: &[&str], label: &str) -> Result<()> {
    if map.len() != expected.len() || expected.iter().any(|key| !map.contains_key(*key)) {
        bail!("{label} fields are not exact");
    }
    Ok(())
}

fn object<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| anyhow!("{label} must be an object"))
}

fn field<'a>(map: &'a Map<String, Value>, key: &str, label: &str) -> Result<&'a Value> {
    map.get(key).ok_or_else(|| anyhow!("{label} lacks {key}"))
}

fn string(map: &Map<String, Value>, key: &str, label: &str) -> Result<String> {
    field(map, key, label)?
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("{label} {key} must be a nonempty string"))
}

fn sha(value: &str, label: &str) -> Result<()> {
    if value.len() != 71
        || !value.starts_with("sha256:")
        || !value.as_bytes()[7..]
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("{label} must be a lowercase SHA-256 identity");
    }
    Ok(())
}

fn sha_field(map: &Map<String, Value>, key: &str, label: &str) -> Result<String> {
    let value = string(map, key, label)?;
    sha(&value, &format!("{label} {key}"))?;
    Ok(value)
}

fn positive(map: &Map<String, Value>, key: &str, label: &str) -> Result<u64> {
    field(map, key, label)?
        .as_u64()
        .filter(|value| *value > 0)
        .ok_or_else(|| anyhow!("{label} {key} must be a positive integer"))
}

fn count(value: &Value, label: &str) -> Result<u64> {
    value
        .as_u64()
        .ok_or_else(|| anyhow!("{label} must be a nonnegative integer"))
}

fn count_field(map: &Map<String, Value>, key: &str, label: &str) -> Result<u64> {
    count(field(map, key, label)?, &format!("{label} {key}"))
}

fn count_map(value: &Value, label: &str) -> Result<(Map<String, Value>, u64)> {
    let values = object(value, label)?;
    if values.is_empty()
        || values
            .iter()
            .any(|(key, value)| key.trim().is_empty() || count(value, label).is_err())
    {
        bail!("{label} must contain nonempty disposition/reason counts");
    }
    let mut total = 0_u64;
    for value in values.values() {
        total = total
            .checked_add(count(value, label)?)
            .ok_or_else(|| anyhow!("{label} overflows u64"))?;
    }
    Ok((values.clone(), total))
}

fn safe_relative_output_path(value: &Value, label: &str) -> Result<String> {
    let path = value
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("{label} must be a nonempty relative path"))?;
    if path.contains('\\')
        || path.contains(':')
        || path.starts_with('/')
        || path
            .split('/')
            .any(|component| matches!(component, "" | "." | ".."))
    {
        bail!("{label} must be a safe canonical relative path")
    }
    Ok(path.to_owned())
}

fn safe_absolute_output_root(value: &str) -> Result<()> {
    let path = Path::new(value);
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        bail!("native v5 proposal outputRoot must be a safe absolute path");
    }
    Ok(())
}

fn output_root_sha256(output_root: &str) -> Result<String> {
    canonical_sha256(&serde_json::json!({
        "schemaVersion": V5_PROPOSAL_OUTPUT_ROOT_SCHEMA,
        "absolutePath": output_root,
    }))
    .map_err(|error| anyhow!("encode native v5 proposal output-root identity: {error}"))
}

/// Build the shared file-transport inventory used by both native v5 result
/// families.  The inventory is deliberately agnostic to construction kind;
/// each distinct receipt validator supplies its closed required artifact set
/// and semantic roots afterwards.
fn build_v5_output_inventory(
    manifest: &V5ProposalManifest,
    supplied_artifacts: &[V5OutputArtifactIdentity],
    supplied_objects: &[V5ObjectStoreIdentity],
    required_roots: &[(&str, &str)],
) -> Result<(Value, String, V5ObjectInventorySidecar)> {
    let mut artifacts = supplied_artifacts.to_vec();
    artifacts.sort_by(|left, right| left.kind.cmp(&right.kind));
    let artifact_values = artifacts
        .iter()
        .map(|artifact| {
            Value::Object(Map::from_iter([
                ("kind".to_owned(), Value::String(artifact.kind.clone())),
                (
                    "relativePath".to_owned(),
                    Value::String(artifact.relative_path.clone()),
                ),
                (
                    "fileSha256".to_owned(),
                    Value::String(artifact.file_sha256.clone()),
                ),
                ("byteLength".to_owned(), Value::from(artifact.byte_length)),
                (
                    "semanticSha256".to_owned(),
                    Value::String(artifact.semantic_sha256.clone()),
                ),
            ]))
        })
        .collect::<Vec<_>>();

    let mut objects = supplied_objects.to_vec();
    objects.sort_by(|left, right| left.object_sha256.cmp(&right.object_sha256));
    let mut object_bytes = 0_u64;
    let sidecar = V5ObjectInventorySidecar {
        objects: objects.clone(),
    };
    let mut sidecar_digest = sha2::Sha256::new();
    let mut sidecar_byte_length = 0_u64;
    for (ordinal, object) in objects.iter().enumerate() {
        sha(&object.object_sha256, "native v5 object semantic identity")?;
        sha(&object.file_sha256, "native v5 object file identity")?;
        object_bytes = object_bytes
            .checked_add(object.byte_length)
            .ok_or_else(|| anyhow!("native v5 object-store byte count overflows"))?;
        let row = sidecar.row(ordinal, object)?;
        sidecar_digest.update(&row);
        sidecar_byte_length = sidecar_byte_length
            .checked_add(row.len() as u64)
            .ok_or_else(|| anyhow!("native v5 object inventory byte count overflows"))?;
    }
    let mut descriptor = Map::from_iter([
        (
            "schemaVersion".to_owned(),
            Value::String(V5_PROPOSAL_OBJECT_INVENTORY_DESCRIPTOR_SCHEMA.to_owned()),
        ),
        (
            "rowSchemaVersion".to_owned(),
            Value::String(V5_PROPOSAL_OBJECT_INVENTORY_ROW_SCHEMA.to_owned()),
        ),
        (
            "relativePath".to_owned(),
            Value::String(V5_PROPOSAL_OBJECT_INVENTORY_PATH.to_owned()),
        ),
        (
            "fileSha256".to_owned(),
            Value::String(format!("sha256:{:x}", sidecar_digest.finalize())),
        ),
        ("byteLength".to_owned(), Value::from(sidecar_byte_length)),
        ("objectCount".to_owned(), Value::from(objects.len() as u64)),
        ("objectByteCount".to_owned(), Value::from(object_bytes)),
    ]);
    let descriptor_sha256 = canonical_sha256(&Value::Object(descriptor.clone()))?;
    descriptor.insert(
        "descriptorSha256".to_owned(),
        Value::String(descriptor_sha256),
    );

    let mut roots = Vec::with_capacity(required_roots.len());
    let mut prior_role: Option<&str> = None;
    for (role, root_sha256) in required_roots {
        if prior_role.is_some_and(|prior| *role <= prior) {
            bail!("native v5 required object roles are not strictly ordered");
        }
        prior_role = Some(role);
        let object = objects
            .iter()
            .find(|object| object.object_sha256 == *root_sha256)
            .ok_or_else(|| anyhow!("native v5 required object root {role} is absent"))?;
        let digest = &object.object_sha256["sha256:".len()..];
        roots.push(Value::Object(Map::from_iter([
            ("role".to_owned(), Value::String((*role).to_owned())),
            (
                "relativePath".to_owned(),
                Value::String(format!("sha256/{digest}.json")),
            ),
            (
                "objectSha256".to_owned(),
                Value::String(object.object_sha256.clone()),
            ),
            (
                "fileSha256".to_owned(),
                Value::String(object.file_sha256.clone()),
            ),
            ("byteLength".to_owned(), Value::from(object.byte_length)),
        ])));
    }
    let object_store_material = Value::Object(Map::from_iter([
        (
            "schemaVersion".to_owned(),
            Value::String(V5_PROPOSAL_OBJECT_STORE_INVENTORY_SCHEMA.to_owned()),
        ),
        (
            "relativeRoot".to_owned(),
            Value::String("v5-native/objects".to_owned()),
        ),
        ("inventory".to_owned(), Value::Object(descriptor)),
        ("roots".to_owned(), Value::Array(roots)),
    ]));
    let object_store_sha256 = canonical_sha256(&object_store_material)?;
    let mut object_store = object_store_material
        .as_object()
        .expect("constructed native v5 object-store inventory")
        .clone();
    object_store.insert(
        "objectStoreSha256".to_owned(),
        Value::String(object_store_sha256),
    );

    let inventory_material = Value::Object(Map::from_iter([
        (
            "schemaVersion".to_owned(),
            Value::String(V5_PROPOSAL_OUTPUT_INVENTORY_SCHEMA.to_owned()),
        ),
        (
            "outputRoot".to_owned(),
            Value::String(manifest.output_root.clone()),
        ),
        (
            "outputRootSha256".to_owned(),
            Value::String(output_root_sha256(&manifest.output_root)?),
        ),
        ("artifacts".to_owned(), Value::Array(artifact_values)),
        ("objectStore".to_owned(), Value::Object(object_store)),
    ]));
    let output_inventory_sha256 = canonical_sha256(&inventory_material)?;
    let mut inventory = inventory_material
        .as_object()
        .expect("constructed native v5 output inventory")
        .clone();
    inventory.insert(
        "outputInventorySha256".to_owned(),
        Value::String(output_inventory_sha256.clone()),
    );
    Ok((Value::Object(inventory), output_inventory_sha256, sidecar))
}

/// Build the immutable inventory, receipt, and invocation result from
/// already-staged file identities.  Receipt/result bytes are intentionally
/// excluded from the inventory to avoid a self-referential seal.  The caller
/// must still stage and pre-validate the returned canonical documents before
/// linking anything; this helper has no filesystem side effects.
pub fn build_v5_proposal_receipt_and_result(
    manifest: &V5ProposalManifest,
    input: &V5ProposalReceiptBuildInput,
) -> Result<(V5ObjectInventorySidecar, Value, V5ProposalResult)> {
    let roots = [
        (
            "g0FunnelFragments",
            input.g0_funnel_fragments_sha256.as_str(),
        ),
        (
            "g0FunnelProjectionStreamReceipt",
            input.g0_funnel_projection_stream_receipt_sha256.as_str(),
        ),
        ("publicationPlan", input.publication_plan_sha256.as_str()),
    ];
    let (inventory, output_inventory_sha256, object_inventory_sidecar) =
        build_v5_output_inventory(manifest, &input.artifacts, &input.objects, &roots)?;

    let native_batch_authority_sha256 = manifest
        .execution_authority
        .get("nativeBatchAuthoritySha256")
        .cloned()
        .ok_or_else(|| anyhow!("native v5 execution authority lacks batch identity"))?;
    let receipt_material = Value::Object(Map::from_iter([
        (
            "schemaVersion".to_owned(),
            Value::String(V5_PROPOSAL_RECEIPT_SCHEMA.to_owned()),
        ),
        (
            "authoritySha256".to_owned(),
            Value::String(manifest.authority_sha256.clone()),
        ),
        (
            "manifestSha256".to_owned(),
            Value::String(manifest.manifest_sha256.clone()),
        ),
        (
            "expectedAuthoritySha256".to_owned(),
            Value::String(manifest.expected_authority_sha256.clone()),
        ),
        (
            "generationConfigSha256".to_owned(),
            Value::String(manifest.generation_config_sha256.clone()),
        ),
        (
            "generationIndex".to_owned(),
            Value::from(manifest.generation_index),
        ),
        (
            "requestedCount".to_owned(),
            Value::from(manifest.requested_count),
        ),
        (
            "acceptedRecordCount".to_owned(),
            Value::from(manifest.requested_count),
        ),
        ("attemptCount".to_owned(), Value::from(input.attempt_count)),
        (
            "attemptJournalSha256".to_owned(),
            Value::String(input.attempt_journal_sha256.clone()),
        ),
        (
            "publicationRequestSha256".to_owned(),
            Value::String(input.publication_request_sha256.clone()),
        ),
        (
            "publicationPlanSha256".to_owned(),
            Value::String(input.publication_plan_sha256.clone()),
        ),
        (
            "g0FunnelFragmentsSha256".to_owned(),
            Value::String(input.g0_funnel_fragments_sha256.clone()),
        ),
        (
            "g0FunnelProjectionStreamReceiptSha256".to_owned(),
            Value::String(input.g0_funnel_projection_stream_receipt_sha256.clone()),
        ),
        (
            "evaluationPopulationSize".to_owned(),
            Value::from(manifest.evaluation_population_size),
        ),
        (
            "compactJournalSha256".to_owned(),
            Value::String(input.compact_journal_sha256.clone()),
        ),
        (
            "identityLedgerSha256".to_owned(),
            Value::String(input.identity_ledger_sha256.clone()),
        ),
        (
            "selectedProjectionIndexSha256".to_owned(),
            Value::String(input.selected_projection_index_sha256.clone()),
        ),
        ("outputInventory".to_owned(), inventory),
        (
            "outputInventorySha256".to_owned(),
            Value::String(output_inventory_sha256),
        ),
        (
            "nativeBatchAuthoritySha256".to_owned(),
            native_batch_authority_sha256,
        ),
        ("threadCap".to_owned(), Value::from(manifest.thread_cap)),
        (
            "constructionSummary".to_owned(),
            input.construction_summary.clone(),
        ),
    ]));
    let receipt_sha256 = canonical_sha256(&receipt_material)?;
    let mut receipt = receipt_material
        .as_object()
        .expect("constructed native v5 proposal receipt")
        .clone();
    receipt.insert("receiptSha256".to_owned(), Value::String(receipt_sha256));
    let receipt = Value::Object(receipt);
    let result = build_v5_proposal_result_from_receipt(manifest, &receipt)?;
    Ok((object_inventory_sidecar, receipt, result))
}

/// Build the separate later-generation inventory, outer receipt, and tiny
/// invocation result from already-staged file identities.  It retains no
/// candidate/public population bytes and does not reconstruct transaction
/// semantics; qd-batch performs that typed prepublication gate before any
/// link is published.
pub fn build_v5_evolved_proposal_receipt_and_result(
    manifest: &V5ProposalManifest,
    input: &V5EvolvedProposalReceiptBuildInput,
) -> Result<(V5ObjectInventorySidecar, Value, V5EvolvedProposalResult)> {
    if manifest.generation_kind != "evolved" || manifest.generation_index < 2 {
        bail!("native v5 evolved receipt requires a later-generation manifest");
    }
    let roots = [
        (
            "publicationFragments",
            input.publication_fragments_sha256.as_str(),
        ),
        ("publicationPlan", input.publication_plan_sha256.as_str()),
        (
            "publicationReceipt",
            input.publication_receipt_sha256.as_str(),
        ),
        ("transaction", input.transaction_sha256.as_str()),
    ];
    let (inventory, output_inventory_sha256, object_inventory_sidecar) =
        build_v5_output_inventory(manifest, &input.artifacts, &input.objects, &roots)?;
    let native_batch_authority_sha256 = manifest
        .execution_authority
        .get("nativeBatchAuthoritySha256")
        .cloned()
        .ok_or_else(|| anyhow!("native v5 execution authority lacks batch identity"))?;
    let receipt_material = Value::Object(Map::from_iter([
        (
            "schemaVersion".to_owned(),
            Value::String(V5_EVOLVED_PROPOSAL_RECEIPT_SCHEMA.to_owned()),
        ),
        (
            "authoritySha256".to_owned(),
            Value::String(manifest.authority_sha256.clone()),
        ),
        (
            "manifestSha256".to_owned(),
            Value::String(manifest.manifest_sha256.clone()),
        ),
        (
            "expectedAuthoritySha256".to_owned(),
            Value::String(manifest.expected_authority_sha256.clone()),
        ),
        (
            "generationConfigSha256".to_owned(),
            Value::String(manifest.generation_config_sha256.clone()),
        ),
        (
            "generationIndex".to_owned(),
            Value::from(manifest.generation_index),
        ),
        (
            "requestedCount".to_owned(),
            Value::from(manifest.requested_count),
        ),
        (
            "acceptedRecordCount".to_owned(),
            Value::from(manifest.requested_count),
        ),
        ("attemptCount".to_owned(), Value::from(input.attempt_count)),
        (
            "transactionSha256".to_owned(),
            Value::String(input.transaction_sha256.clone()),
        ),
        (
            "parentArchiveInputBindingSha256".to_owned(),
            Value::String(input.parent_archive_input_binding_sha256.clone()),
        ),
        (
            "identityLedgerInputBindingSha256".to_owned(),
            Value::String(input.identity_ledger_input_binding_sha256.clone()),
        ),
        (
            "publicationRequestSha256".to_owned(),
            Value::String(input.publication_request_sha256.clone()),
        ),
        (
            "publicationPlanSha256".to_owned(),
            Value::String(input.publication_plan_sha256.clone()),
        ),
        (
            "publicationReceiptSha256".to_owned(),
            Value::String(input.publication_receipt_sha256.clone()),
        ),
        (
            "publicationFragmentsSha256".to_owned(),
            Value::String(input.publication_fragments_sha256.clone()),
        ),
        (
            "evaluationPopulationSize".to_owned(),
            Value::from(manifest.evaluation_population_size),
        ),
        (
            "identityLedgerSha256".to_owned(),
            Value::String(input.identity_ledger_sha256.clone()),
        ),
        ("outputInventory".to_owned(), inventory),
        (
            "outputInventorySha256".to_owned(),
            Value::String(output_inventory_sha256),
        ),
        (
            "nativeBatchAuthoritySha256".to_owned(),
            native_batch_authority_sha256,
        ),
        ("threadCap".to_owned(), Value::from(manifest.thread_cap)),
        (
            "constructionSummary".to_owned(),
            input.construction_summary.clone(),
        ),
    ]));
    let receipt_sha256 = canonical_sha256(&receipt_material)?;
    let mut receipt = receipt_material
        .as_object()
        .expect("constructed native v5 evolved proposal receipt")
        .clone();
    receipt.insert("receiptSha256".to_owned(), Value::String(receipt_sha256));
    let receipt = Value::Object(receipt);
    let result = build_v5_evolved_proposal_result_from_receipt(manifest, &receipt)?;
    Ok((object_inventory_sidecar, receipt, result))
}

fn validate_object_store_inventory(value: &Value) -> Result<String> {
    let fields = object(value, "native v5 proposal object-store inventory")?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "relativeRoot",
            "inventory",
            "roots",
            "objectStoreSha256",
        ],
        "native v5 proposal object-store inventory",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some(V5_PROPOSAL_OBJECT_STORE_INVENTORY_SCHEMA)
        || fields.get("relativeRoot").and_then(Value::as_str) != Some("v5-native/objects")
    {
        bail!("native v5 proposal object-store inventory is incompatible");
    }
    let descriptor_value = field(
        fields,
        "inventory",
        "native v5 proposal object-store closure",
    )?;
    let descriptor = object(
        descriptor_value,
        "native v5 proposal object inventory descriptor",
    )?;
    exact_keys(
        descriptor,
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
        "native v5 proposal object inventory descriptor",
    )?;
    if descriptor.get("schemaVersion").and_then(Value::as_str)
        != Some(V5_PROPOSAL_OBJECT_INVENTORY_DESCRIPTOR_SCHEMA)
        || descriptor.get("rowSchemaVersion").and_then(Value::as_str)
            != Some(V5_PROPOSAL_OBJECT_INVENTORY_ROW_SCHEMA)
        || descriptor.get("relativePath").and_then(Value::as_str)
            != Some(V5_PROPOSAL_OBJECT_INVENTORY_PATH)
    {
        bail!("native v5 proposal object inventory descriptor is incompatible");
    }
    sha_field(
        descriptor,
        "fileSha256",
        "native v5 proposal object inventory descriptor",
    )?;
    count_field(
        descriptor,
        "byteLength",
        "native v5 proposal object inventory descriptor",
    )?;
    count_field(
        descriptor,
        "objectCount",
        "native v5 proposal object inventory descriptor",
    )?;
    count_field(
        descriptor,
        "objectByteCount",
        "native v5 proposal object inventory descriptor",
    )?;
    let descriptor_sha = sha_field(
        descriptor,
        "descriptorSha256",
        "native v5 proposal object inventory descriptor",
    )?;
    if canonical_sha256_without_object_field(descriptor_value, "descriptorSha256")?
        != descriptor_sha
    {
        bail!("native v5 proposal object inventory descriptor identity mismatch");
    }
    let entries = field(fields, "roots", "native v5 proposal object-store closure")?
        .as_array()
        .ok_or_else(|| anyhow!("native v5 proposal object-store roots must be an array"))?;
    if entries.len() > 4 {
        bail!("native v5 proposal object-store root projection is unbounded");
    }
    let mut prior_role: Option<String> = None;
    for entry in entries {
        let entry = object(entry, "native v5 proposal object-store root")?;
        exact_keys(
            entry,
            &[
                "role",
                "relativePath",
                "objectSha256",
                "fileSha256",
                "byteLength",
            ],
            "native v5 proposal object-store root",
        )?;
        let role = string(entry, "role", "native v5 proposal object-store root")?;
        if role.contains('/')
            || role.contains('\\')
            || prior_role.as_ref().is_some_and(|prior| role <= *prior)
        {
            bail!("native v5 proposal object-store roots are not strictly ordered");
        }
        prior_role = Some(role);
        let path = safe_relative_output_path(
            field(
                entry,
                "relativePath",
                "native v5 proposal object-store root",
            )?,
            "native v5 proposal object-store root path",
        )?;
        let object_sha = sha_field(
            entry,
            "objectSha256",
            "native v5 proposal object-store root",
        )?;
        let expected_path = format!("sha256/{}.json", &object_sha["sha256:".len()..]);
        if path != expected_path {
            bail!("native v5 proposal object-store path/semantic identity drifted");
        }
        sha_field(entry, "fileSha256", "native v5 proposal object-store root")?;
        count_field(entry, "byteLength", "native v5 proposal object-store root")?;
    }
    let supplied = sha_field(
        fields,
        "objectStoreSha256",
        "native v5 proposal object-store inventory",
    )?;
    if canonical_sha256_without_object_field(value, "objectStoreSha256")? != supplied {
        bail!("native v5 proposal object-store inventory identity mismatch");
    }
    Ok(supplied)
}

/// The publication plan is a core-owned, self-hashed authority rather than a
/// batch schema.  The outer receipt nevertheless has to prove that its plan
/// root resolves to one real immutable object: otherwise a result could name
/// a cap-free plan SHA which is never available to typed adoption replay.
fn require_object_store_semantic_object(
    value: &Value,
    role: &str,
    object_sha256: &str,
    label: &str,
) -> Result<()> {
    sha(object_sha256, label)?;
    let expected_path = format!("sha256/{}.json", &object_sha256["sha256:".len()..]);
    let entries = field(
        object(value, "native v5 proposal object-store inventory")?,
        "roots",
        "native v5 proposal object-store inventory",
    )?
    .as_array()
    .ok_or_else(|| anyhow!("native v5 proposal object-store entries must be an array"))?;
    let found = entries.iter().any(|entry| {
        entry.as_object().is_some_and(|fields| {
            fields.get("relativePath").and_then(Value::as_str) == Some(expected_path.as_str())
                && fields.get("objectSha256").and_then(Value::as_str) == Some(object_sha256)
                && fields.get("role").and_then(Value::as_str) == Some(role)
        })
    });
    if !found {
        bail!("{label} does not resolve to a real immutable object-store entry");
    }
    Ok(())
}

fn validate_output_inventory(
    value: &Value,
    semantic_roots: &Map<String, Value>,
    output_root: &str,
    generation_kind: &str,
) -> Result<String> {
    let fields = object(value, "native v5 proposal output inventory")?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "outputRoot",
            "outputRootSha256",
            "artifacts",
            "objectStore",
            "outputInventorySha256",
        ],
        "native v5 proposal output inventory",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some(V5_PROPOSAL_OUTPUT_INVENTORY_SCHEMA)
    {
        bail!("native v5 proposal output inventory schema is incompatible");
    }
    let expected_root_sha = output_root_sha256(output_root)?;
    if fields.get("outputRoot").and_then(Value::as_str) != Some(output_root)
        || fields.get("outputRootSha256").and_then(Value::as_str)
            != Some(expected_root_sha.as_str())
    {
        bail!("native v5 proposal output inventory root binding drifted");
    }
    let artifacts = field(fields, "artifacts", "native v5 proposal output inventory")?
        .as_array()
        .ok_or_else(|| anyhow!("native v5 proposal output artifacts must be an array"))?;
    let required_artifacts = required_output_artifacts(generation_kind)?;
    let required_artifact_count = required_artifacts.len();
    if artifacts.len() != required_artifact_count {
        bail!("native v5 proposal output inventory artifact set is not exact");
    }
    let mut by_kind = BTreeMap::<String, (String, String)>::new();
    let mut paths = BTreeSet::<String>::new();
    let mut prior_kind: Option<String> = None;
    for artifact in artifacts {
        let artifact = object(artifact, "native v5 proposal output artifact")?;
        exact_keys(
            artifact,
            &[
                "kind",
                "relativePath",
                "fileSha256",
                "byteLength",
                "semanticSha256",
            ],
            "native v5 proposal output artifact",
        )?;
        let kind = string(artifact, "kind", "native v5 proposal output artifact")?;
        if kind.contains('/') || kind.contains('\\') {
            bail!("native v5 proposal output artifact kind is invalid");
        }
        if prior_kind
            .as_ref()
            .is_some_and(|prior| kind.as_str() <= prior.as_str())
        {
            bail!("native v5 proposal output artifact kinds are not strictly ordered");
        }
        prior_kind = Some(kind.clone());
        let path = safe_relative_output_path(
            field(
                artifact,
                "relativePath",
                "native v5 proposal output artifact",
            )?,
            "native v5 proposal output artifact path",
        )?;
        if !paths.insert(path.clone()) {
            bail!("native v5 proposal output artifact paths are not unique");
        }
        sha_field(artifact, "fileSha256", "native v5 proposal output artifact")?;
        let semantic = sha_field(
            artifact,
            "semanticSha256",
            "native v5 proposal output artifact",
        )?;
        count_field(artifact, "byteLength", "native v5 proposal output artifact")?;
        if by_kind.insert(kind, (path, semantic)).is_some() {
            bail!("native v5 proposal output artifact kinds are not unique");
        }
    }
    for (kind, required_path, semantic_key) in required_artifacts {
        let Some((path, semantic)) = by_kind.get(kind) else {
            bail!("native v5 proposal output artifact {kind} is missing");
        };
        if path != required_path
            || semantic_key.is_some_and(|key| {
                semantic_roots.get(key).and_then(Value::as_str) != Some(semantic.as_str())
            })
        {
            bail!("native v5 proposal output artifact {kind} binding drifted");
        }
    }
    if by_kind.len() != required_artifact_count {
        bail!("native v5 proposal output inventory contains an undeclared artifact kind");
    }
    let object_store = field(fields, "objectStore", "native v5 proposal output inventory")?;
    validate_object_store_inventory(object_store)?;
    let root_roles = field(
        object(object_store, "native v5 proposal object-store closure")?,
        "roots",
        "native v5 proposal object-store closure",
    )?
    .as_array()
    .ok_or_else(|| anyhow!("native v5 proposal object-store roots are invalid"))?
    .iter()
    .map(|root| {
        root.get("role")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 proposal object-store root lacks role"))
    })
    .collect::<Result<Vec<_>>>()?;
    let expected_roles: &[&str] = match generation_kind {
        "g0" => &[
            "g0FunnelFragments",
            "g0FunnelProjectionStreamReceipt",
            "publicationPlan",
        ],
        "evolved" => &[
            "publicationFragments",
            "publicationPlan",
            "publicationReceipt",
            "transaction",
        ],
        _ => bail!("native v5 proposal object-store generation kind is incompatible"),
    };
    if root_roles != expected_roles {
        bail!("native v5 proposal object-store root roles are not the exact current closure");
    }
    let publication_plan_sha256 = semantic_roots
        .get("publicationPlanSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            anyhow!("native v5 proposal output inventory lacks publication plan root")
        })?;
    require_object_store_semantic_object(
        object_store,
        "publicationPlan",
        publication_plan_sha256,
        "native v5 publication plan root",
    )?;
    let supplied = sha_field(
        fields,
        "outputInventorySha256",
        "native v5 proposal output inventory",
    )?;
    if canonical_sha256_without_object_field(value, "outputInventorySha256")? != supplied {
        bail!("native v5 proposal output inventory identity mismatch");
    }
    Ok(supplied)
}

fn validate_construction_summary(
    value: &Value,
    attempt_count: u64,
    accepted_record_count: u64,
) -> Result<()> {
    let fields = object(value, "native v5 proposal construction summary")?;
    exact_keys(
        fields,
        &["schemaVersion", "bytes", "attempts", "uniqueCounts"],
        "native v5 proposal construction summary",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some(V5_PROPOSAL_CONSTRUCTION_SUMMARY_SCHEMA)
    {
        bail!("native v5 proposal construction summary schema is incompatible");
    }
    let byte_counts = object(
        field(fields, "bytes", "native v5 proposal construction summary")?,
        "native v5 proposal construction-summary bytes",
    )?;
    exact_keys(
        byte_counts,
        &[
            "compactJournalBytes",
            "staticAuthorityBytes",
            "objectStoreBytes",
            "selectedProjectionBytes",
        ],
        "native v5 proposal construction-summary bytes",
    )?;
    for key in [
        "compactJournalBytes",
        "staticAuthorityBytes",
        "objectStoreBytes",
        "selectedProjectionBytes",
    ] {
        count_field(
            byte_counts,
            key,
            "native v5 proposal construction-summary bytes",
        )?;
    }
    let attempts = object(
        field(
            fields,
            "attempts",
            "native v5 proposal construction summary",
        )?,
        "native v5 proposal construction-summary attempts",
    )?;
    exact_keys(
        attempts,
        &["byDisposition", "byReason"],
        "native v5 proposal construction-summary attempts",
    )?;
    let (by_disposition, disposition_total) = count_map(
        field(
            attempts,
            "byDisposition",
            "native v5 proposal construction-summary attempts",
        )?,
        "native v5 proposal construction-summary dispositions",
    )?;
    let (_, reason_total) = count_map(
        field(
            attempts,
            "byReason",
            "native v5 proposal construction-summary attempts",
        )?,
        "native v5 proposal construction-summary reasons",
    )?;
    if disposition_total != attempt_count
        || reason_total != attempt_count
        || by_disposition.get("accepted").and_then(Value::as_u64) != Some(accepted_record_count)
    {
        bail!("native v5 proposal construction summary attempt counts drifted");
    }
    let unique = object(
        field(
            fields,
            "uniqueCounts",
            "native v5 proposal construction summary",
        )?,
        "native v5 proposal construction-summary unique counts",
    )?;
    exact_keys(
        unique,
        &[
            "candidateCount",
            "programCount",
            "topologyCount",
            "resourceCount",
        ],
        "native v5 proposal construction-summary unique counts",
    )?;
    for key in [
        "candidateCount",
        "programCount",
        "topologyCount",
        "resourceCount",
    ] {
        count_field(
            unique,
            key,
            "native v5 proposal construction-summary unique counts",
        )?;
    }
    if unique.get("candidateCount").and_then(Value::as_u64) != Some(accepted_record_count) {
        bail!(
            "native v5 proposal construction summary unique candidate count drifts from accepted records"
        );
    }
    Ok(())
}

fn canonical_line(raw: &[u8], label: &str) -> Result<Value> {
    let value: Value = serde_json::from_slice(raw)
        .map_err(|error| anyhow!("{label} must be UTF-8 JSON: {error}"))?;
    if raw != canonical_json_line(&value)? {
        bail!("{label} must be one canonical JSON document followed by exactly one LF");
    }
    Ok(value)
}

fn validate_native_batch_authority(value: &Value) -> Result<String> {
    let fields = object(value, "native v5 batch authority")?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "contractVersion",
            "crateVersion",
            "binaryName",
            "buildProfile",
            "executableSha256",
            "sourceSha256",
            "authoritySha256",
        ],
        "native v5 batch authority",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some("temporal_qd_native_authority_v1")
        || fields.get("contractVersion").and_then(Value::as_str) != Some(CONTRACT_VERSION)
        || fields.get("binaryName").and_then(Value::as_str) != Some("temporal-qd-batch")
        || fields.get("buildProfile").and_then(Value::as_str) != Some("release")
    {
        bail!("native v5 batch authority is incompatible");
    }
    if fields
        .get("crateVersion")
        .and_then(Value::as_str)
        .map(|value| value.trim().is_empty())
        != Some(false)
    {
        bail!("native v5 batch authority crateVersion is invalid");
    }
    for key in ["executableSha256", "sourceSha256", "authoritySha256"] {
        sha_field(fields, key, "native v5 batch authority")?;
    }
    let authority = sha_field(fields, "authoritySha256", "native v5 batch authority")?;
    if canonical_sha256_without_object_field(value, "authoritySha256")? != authority {
        bail!("native v5 batch authority identity mismatch");
    }
    Ok(authority)
}

fn validate_frozen_authority(value: &Value) -> Result<String> {
    let fields = object(value, "native v5 frozen authority")?;
    exact_keys(
        fields,
        &["schemaVersion", "authority", "authoritySha256"],
        "native v5 frozen authority",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str) != Some(V5_SHARED_AUTHORITY_SCHEMA)
        || !fields.get("authority").is_some_and(Value::is_object)
    {
        bail!("native v5 frozen authority is incompatible");
    }
    let supplied = sha_field(fields, "authoritySha256", "native v5 frozen authority")?;
    if canonical_sha256(field(fields, "authority", "native v5 frozen authority")?)? != supplied {
        bail!("native v5 frozen authority identity mismatch");
    }
    Ok(supplied)
}

fn validate_execution_authority(
    value: &Value,
    expected_authority_sha256: &str,
    generation_config_sha256: &str,
    frozen_authority_sha256: &str,
) -> Result<String> {
    let fields = object(value, "native v5 proposal execution authority")?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "nativeBatchAuthority",
            "nativeBatchAuthoritySha256",
            "expectedAuthoritySha256",
            "frozenAuthoritySha256",
            "generationConfigSha256",
            "authoritySha256",
        ],
        "native v5 proposal execution authority",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some(V5_PROPOSAL_EXECUTION_AUTHORITY_SCHEMA)
    {
        bail!("native v5 proposal execution authority schema is incompatible");
    }
    for key in [
        "nativeBatchAuthoritySha256",
        "expectedAuthoritySha256",
        "frozenAuthoritySha256",
        "generationConfigSha256",
        "authoritySha256",
    ] {
        sha_field(fields, key, "native v5 proposal execution authority")?;
    }
    let batch = field(
        fields,
        "nativeBatchAuthority",
        "native v5 proposal execution authority",
    )?;
    let batch_sha = validate_native_batch_authority(batch)?;
    if fields
        .get("nativeBatchAuthoritySha256")
        .and_then(Value::as_str)
        != Some(batch_sha.as_str())
        || fields
            .get("expectedAuthoritySha256")
            .and_then(Value::as_str)
            != Some(expected_authority_sha256)
        || fields.get("frozenAuthoritySha256").and_then(Value::as_str)
            != Some(frozen_authority_sha256)
        || fields.get("generationConfigSha256").and_then(Value::as_str)
            != Some(generation_config_sha256)
    {
        bail!("native v5 proposal execution authority binding drifted");
    }
    let supplied = sha_field(
        fields,
        "authoritySha256",
        "native v5 proposal execution authority",
    )?;
    if canonical_sha256_without_object_field(value, "authoritySha256")? != supplied {
        bail!("native v5 proposal execution authority identity mismatch");
    }
    Ok(supplied)
}

fn validate_inputs(value: &Value, generation_kind: &str) -> Result<()> {
    let fields = object(value, "native v5 proposal inputs")?;
    exact_keys(
        fields,
        &["schemaVersion", "parentArchive", "identityLedger"],
        "native v5 proposal inputs",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str) != Some(V5_PROPOSAL_INPUTS_SCHEMA) {
        bail!("native v5 proposal inputs schema is incompatible");
    }
    // The initial v5 transaction has no parents and starts with an empty
    // ledger. Later generations bind files/semantics exactly; a generic map
    // is not an authority and may not be interpreted as one.
    if generation_kind == "g0"
        && (!fields.get("parentArchive").is_some_and(Value::is_null)
            || !fields.get("identityLedger").is_some_and(Value::is_null))
    {
        bail!("native v5 G0 inputs must not carry legacy parent/ledger files");
    }
    if generation_kind == "evolved" {
        validate_input_binding(
            field(fields, "parentArchive", "native v5 proposal inputs")?,
            "parentArchive",
        )?;
        validate_input_binding(
            field(fields, "identityLedger", "native v5 proposal inputs")?,
            "identityLedger",
        )?;
    }
    Ok(())
}

fn validate_input_binding(value: &Value, expected_kind: &str) -> Result<()> {
    let fields = object(value, "native v5 proposal input binding")?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "kind",
            "absolutePath",
            "fileSha256",
            "semanticSha256",
            "bindingSha256",
        ],
        "native v5 proposal input binding",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str) != Some(V5_PROPOSAL_INPUT_BINDING_SCHEMA)
        || fields.get("kind").and_then(Value::as_str) != Some(expected_kind)
    {
        bail!("native v5 proposal input binding schema/kind is incompatible");
    }
    let path = string(fields, "absolutePath", "native v5 proposal input binding")?;
    let normalized = path.replace('\\', "/");
    let is_windows_absolute = normalized.len() >= 3
        && normalized.as_bytes()[0].is_ascii_alphabetic()
        && normalized.as_bytes()[1] == b':'
        && normalized.as_bytes()[2] == b'/';
    if !(normalized.starts_with('/') || normalized.starts_with("//") || is_windows_absolute)
        || normalized
            .split('/')
            .any(|component| matches!(component, "." | ".."))
    {
        bail!("native v5 proposal input binding path is not safely absolute");
    }
    for key in ["fileSha256", "semanticSha256", "bindingSha256"] {
        sha_field(fields, key, "native v5 proposal input binding")?;
    }
    let binding = sha_field(fields, "bindingSha256", "native v5 proposal input binding")?;
    if canonical_sha256_without_object_field(value, "bindingSha256")? != binding {
        bail!("native v5 proposal input binding identity mismatch");
    }
    Ok(())
}

pub fn parse_v5_proposal_manifest(raw: &[u8]) -> Result<V5ProposalManifest> {
    let value = canonical_line(raw, "native v5 proposal manifest")?;
    validate_v5_proposal_manifest(&value)
}

pub fn validate_v5_proposal_manifest(value: &Value) -> Result<V5ProposalManifest> {
    let fields = object(value, "native v5 proposal manifest")?;
    exact_keys(
        fields,
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
        "native v5 proposal manifest",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str) != Some(V5_PROPOSAL_MANIFEST_SCHEMA)
        || fields.get("contractVersion").and_then(Value::as_str) != Some(CONTRACT_VERSION)
        || fields.get("operation").and_then(Value::as_str) != Some(V5_PROPOSAL_OPERATION)
        || fields.get("resultPath").and_then(Value::as_str) != Some(V5_PROPOSAL_RESULT_PATH)
    {
        bail!("native v5 proposal manifest is incompatible");
    }
    let generation_kind = string(fields, "generationKind", "native v5 proposal manifest")?;
    if !matches!(generation_kind.as_str(), "g0" | "evolved") {
        bail!("native v5 proposal manifest generationKind is unsupported");
    }
    let generation_config =
        field(fields, "generationConfig", "native v5 proposal manifest")?.clone();
    let config_fields = object(&generation_config, "native v5 proposal generation config")?;
    let generation_config_sha256 = sha_field(
        fields,
        "generationConfigSha256",
        "native v5 proposal manifest",
    )?;
    if config_fields.get("configSha256").and_then(Value::as_str)
        != Some(generation_config_sha256.as_str())
        || canonical_sha256_without_object_field(&generation_config, "configSha256")?
            != generation_config_sha256
    {
        bail!("native v5 proposal generation config identity mismatch");
    }
    let generation_index = positive(fields, "generationIndex", "native v5 proposal manifest")?;
    let requested_count = positive(fields, "requestedCount", "native v5 proposal manifest")?;
    let evaluation_population_size = positive(
        fields,
        "evaluationPopulationSize",
        "native v5 proposal manifest",
    )?;
    let max_proposal_attempts =
        positive(fields, "maxProposalAttempts", "native v5 proposal manifest")?;
    let thread_cap = positive(fields, "threadCap", "native v5 proposal manifest")?;
    if thread_cap > 8
        || evaluation_population_size > requested_count
        || max_proposal_attempts < requested_count
        || config_fields.get("generationIndex").and_then(Value::as_u64) != Some(generation_index)
        || config_fields
            .get("targetUniqueCandidates")
            .and_then(Value::as_u64)
            != Some(requested_count)
        || config_fields
            .get("maxProposalAttempts")
            .and_then(Value::as_u64)
            != Some(max_proposal_attempts)
    {
        bail!("native v5 proposal manifest/config dimensions are inconsistent");
    }
    if generation_kind == "g0" && generation_index != 1 {
        bail!("native v5 G0 proposal manifest must bind generation 1");
    }
    let final_newline = string(fields, "finalNewline", "native v5 proposal manifest")?;
    if final_newline != "lf" {
        bail!("native v5 proposal manifest finalNewline is invalid");
    }
    let output_root = string(fields, "outputRoot", "native v5 proposal manifest")?;
    safe_absolute_output_root(output_root.as_str())?;
    let frozen_authority = field(fields, "frozenAuthority", "native v5 proposal manifest")?.clone();
    let frozen_authority_sha256 = validate_frozen_authority(&frozen_authority)?;
    let expected_authority_sha256 = sha_field(
        fields,
        "expectedAuthoritySha256",
        "native v5 proposal manifest",
    )?;
    if expected_authority_sha256 != frozen_authority_sha256 {
        bail!("native v5 proposal expected authority differs from frozen authority");
    }
    let execution_authority =
        field(fields, "executionAuthority", "native v5 proposal manifest")?.clone();
    let authority_sha256 = validate_execution_authority(
        &execution_authority,
        expected_authority_sha256.as_str(),
        generation_config_sha256.as_str(),
        frozen_authority_sha256.as_str(),
    )?;
    if fields.get("authoritySha256").and_then(Value::as_str) != Some(authority_sha256.as_str()) {
        bail!("native v5 proposal manifest execution authority binding drifted");
    }
    let inputs = field(fields, "inputs", "native v5 proposal manifest")?.clone();
    validate_inputs(&inputs, generation_kind.as_str())?;
    let manifest_sha256 = sha_field(fields, "manifestSha256", "native v5 proposal manifest")?;
    if canonical_sha256_without_object_field(value, "manifestSha256")? != manifest_sha256 {
        bail!("native v5 proposal manifest identity mismatch");
    }
    Ok(V5ProposalManifest {
        authority_sha256,
        execution_authority,
        frozen_authority,
        expected_authority_sha256,
        output_root,
        final_newline,
        generation_config,
        generation_config_sha256,
        generation_index,
        generation_kind,
        requested_count,
        evaluation_population_size,
        max_proposal_attempts,
        thread_cap,
        inputs,
        result_path: V5_PROPOSAL_RESULT_PATH.to_owned(),
        manifest_sha256,
    })
}

pub fn validate_v5_proposal_result(
    value: &Value,
    manifest: &V5ProposalManifest,
) -> Result<V5ProposalResult> {
    if manifest.generation_kind != "g0" || manifest.generation_index != 1 {
        bail!("native v5 proposal result is incompatible with a non-G0 manifest");
    }
    let fields = object(value, "native v5 proposal result")?;
    exact_keys(
        fields,
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
        "native v5 proposal result",
    )?;
    for key in [
        "authoritySha256",
        "manifestSha256",
        "expectedAuthoritySha256",
        "generationConfigSha256",
        "attemptJournalSha256",
        "publicationRequestSha256",
        "publicationPlanSha256",
        "g0FunnelFragmentsSha256",
        "g0FunnelProjectionStreamReceiptSha256",
        "compactJournalSha256",
        "identityLedgerSha256",
        "selectedProjectionIndexSha256",
        "outputInventorySha256",
        "receiptSha256",
        "resultSha256",
    ] {
        sha_field(fields, key, "native v5 proposal result")?;
    }
    if fields.get("schemaVersion").and_then(Value::as_str) != Some(V5_PROPOSAL_RESULT_SCHEMA)
        || fields.get("contractVersion").and_then(Value::as_str) != Some(CONTRACT_VERSION)
        || fields.get("operation").and_then(Value::as_str) != Some(V5_PROPOSAL_OPERATION)
        || fields.get("status").and_then(Value::as_str) != Some("completed")
        || fields.get("authoritySha256").and_then(Value::as_str)
            != Some(manifest.authority_sha256.as_str())
        || fields.get("manifestSha256").and_then(Value::as_str)
            != Some(manifest.manifest_sha256.as_str())
        || fields
            .get("expectedAuthoritySha256")
            .and_then(Value::as_str)
            != Some(manifest.expected_authority_sha256.as_str())
        || fields.get("generationConfigSha256").and_then(Value::as_str)
            != Some(manifest.generation_config_sha256.as_str())
        || fields.get("generationIndex").and_then(Value::as_u64) != Some(manifest.generation_index)
        || fields.get("requestedCount").and_then(Value::as_u64) != Some(manifest.requested_count)
        || fields.get("acceptedRecordCount").and_then(Value::as_u64)
            != Some(manifest.requested_count)
        || fields
            .get("attemptCount")
            .and_then(Value::as_u64)
            .is_none_or(|count| {
                count < manifest.requested_count || count > manifest.max_proposal_attempts
            })
        || fields
            .get("evaluationPopulationSize")
            .and_then(Value::as_u64)
            != Some(manifest.evaluation_population_size)
    {
        bail!("native v5 proposal result is incompatible with its manifest");
    }
    validate_v5_proposal_receipt(
        field(fields, "receipt", "native v5 proposal result")?,
        manifest,
        fields,
    )?;
    let receipt_sha = sha_field(fields, "receiptSha256", "native v5 proposal result")?;
    if canonical_sha256_without_object_field(
        field(fields, "receipt", "native v5 proposal result")?,
        "receiptSha256",
    )? != receipt_sha
    {
        bail!("native v5 proposal result receipt identity mismatch");
    }
    let result_sha = sha_field(fields, "resultSha256", "native v5 proposal result")?;
    if canonical_sha256_without_object_field(value, "resultSha256")? != result_sha {
        bail!("native v5 proposal result identity mismatch");
    }
    Ok(V5ProposalResult {
        value: value.clone(),
    })
}

/// Recreate the small invocation result from the immutable output-tree
/// receipt.  The receipt is the durable transaction seal; the invocation
/// result is only the control-plane commit marker that lets the Python bridge
/// discover that seal.  Keeping this derivation here means a crash after the
/// receipt link but before the invocation-result link can converge by
/// authenticating the already-sealed tree instead of rerunning construction.
///
/// This function deliberately copies only the exact outer-contract fields.
/// It does not parse or rebuild a candidate, program, population, or G0
/// record.  `validate_v5_proposal_result` is still the final gate, including
/// receipt/inventory binding and every self-hash.
pub fn build_v5_proposal_result_from_receipt(
    manifest: &V5ProposalManifest,
    receipt: &Value,
) -> Result<V5ProposalResult> {
    let receipt_fields = object(receipt, "native v5 proposal receipt")?;
    let mut result = Map::from_iter([
        (
            "schemaVersion".to_owned(),
            Value::String(V5_PROPOSAL_RESULT_SCHEMA.to_owned()),
        ),
        (
            "contractVersion".to_owned(),
            Value::String(CONTRACT_VERSION.to_owned()),
        ),
        (
            "operation".to_owned(),
            Value::String(V5_PROPOSAL_OPERATION.to_owned()),
        ),
        ("status".to_owned(), Value::String("completed".to_owned())),
        (
            "authoritySha256".to_owned(),
            Value::String(manifest.authority_sha256.clone()),
        ),
        (
            "manifestSha256".to_owned(),
            Value::String(manifest.manifest_sha256.clone()),
        ),
        (
            "expectedAuthoritySha256".to_owned(),
            Value::String(manifest.expected_authority_sha256.clone()),
        ),
        (
            "generationConfigSha256".to_owned(),
            Value::String(manifest.generation_config_sha256.clone()),
        ),
        (
            "generationIndex".to_owned(),
            Value::from(manifest.generation_index),
        ),
        (
            "requestedCount".to_owned(),
            Value::from(manifest.requested_count),
        ),
        (
            "acceptedRecordCount".to_owned(),
            field(
                receipt_fields,
                "acceptedRecordCount",
                "native v5 proposal receipt",
            )?
            .clone(),
        ),
        (
            "attemptCount".to_owned(),
            field(receipt_fields, "attemptCount", "native v5 proposal receipt")?.clone(),
        ),
        (
            "attemptJournalSha256".to_owned(),
            field(
                receipt_fields,
                "attemptJournalSha256",
                "native v5 proposal receipt",
            )?
            .clone(),
        ),
        (
            "publicationPlanSha256".to_owned(),
            field(
                receipt_fields,
                "publicationPlanSha256",
                "native v5 proposal receipt",
            )?
            .clone(),
        ),
        (
            "publicationRequestSha256".to_owned(),
            field(
                receipt_fields,
                "publicationRequestSha256",
                "native v5 proposal receipt",
            )?
            .clone(),
        ),
        (
            "g0FunnelFragmentsSha256".to_owned(),
            field(
                receipt_fields,
                "g0FunnelFragmentsSha256",
                "native v5 proposal receipt",
            )?
            .clone(),
        ),
        (
            "g0FunnelProjectionStreamReceiptSha256".to_owned(),
            field(
                receipt_fields,
                "g0FunnelProjectionStreamReceiptSha256",
                "native v5 proposal receipt",
            )?
            .clone(),
        ),
        (
            "evaluationPopulationSize".to_owned(),
            field(
                receipt_fields,
                "evaluationPopulationSize",
                "native v5 proposal receipt",
            )?
            .clone(),
        ),
        (
            "compactJournalSha256".to_owned(),
            field(
                receipt_fields,
                "compactJournalSha256",
                "native v5 proposal receipt",
            )?
            .clone(),
        ),
        (
            "identityLedgerSha256".to_owned(),
            field(
                receipt_fields,
                "identityLedgerSha256",
                "native v5 proposal receipt",
            )?
            .clone(),
        ),
        (
            "selectedProjectionIndexSha256".to_owned(),
            field(
                receipt_fields,
                "selectedProjectionIndexSha256",
                "native v5 proposal receipt",
            )?
            .clone(),
        ),
        (
            "outputInventorySha256".to_owned(),
            field(
                receipt_fields,
                "outputInventorySha256",
                "native v5 proposal receipt",
            )?
            .clone(),
        ),
        ("receipt".to_owned(), receipt.clone()),
        (
            "receiptSha256".to_owned(),
            field(
                receipt_fields,
                "receiptSha256",
                "native v5 proposal receipt",
            )?
            .clone(),
        ),
    ]);
    let result_sha256 = canonical_sha256(&Value::Object(result.clone()))?;
    result.insert("resultSha256".to_owned(), Value::String(result_sha256));
    let result = Value::Object(result);
    validate_v5_proposal_result(&result, manifest)
}

fn evolved_manifest_input_binding_sha(manifest: &V5ProposalManifest, key: &str) -> Result<String> {
    let inputs = object(&manifest.inputs, "native v5 evolved manifest inputs")?;
    let binding = object(
        field(inputs, key, "native v5 evolved manifest inputs")?,
        "native v5 evolved input binding",
    )?;
    sha_field(binding, "bindingSha256", "native v5 evolved input binding")
}

/// Recreate the tiny later-generation invocation marker from the immutable
/// output-tree seal.  It is intentionally a deterministic projection of the
/// already-sealed receipt, so receipt-present/result-absent recovery never
/// invokes construction, scheduler replay, or public materialization.
pub fn build_v5_evolved_proposal_result_from_receipt(
    manifest: &V5ProposalManifest,
    receipt: &Value,
) -> Result<V5EvolvedProposalResult> {
    if manifest.generation_kind != "evolved" || manifest.generation_index < 2 {
        bail!("native v5 evolved result recovery requires a later-generation manifest");
    }
    let receipt_fields = object(receipt, "native v5 evolved proposal receipt")?;
    let mut result = Map::from_iter([
        (
            "schemaVersion".to_owned(),
            Value::String(V5_EVOLVED_PROPOSAL_RESULT_SCHEMA.to_owned()),
        ),
        (
            "contractVersion".to_owned(),
            Value::String(CONTRACT_VERSION.to_owned()),
        ),
        (
            "operation".to_owned(),
            Value::String(V5_PROPOSAL_OPERATION.to_owned()),
        ),
        ("status".to_owned(), Value::String("completed".to_owned())),
        (
            "authoritySha256".to_owned(),
            Value::String(manifest.authority_sha256.clone()),
        ),
        (
            "manifestSha256".to_owned(),
            Value::String(manifest.manifest_sha256.clone()),
        ),
        (
            "expectedAuthoritySha256".to_owned(),
            Value::String(manifest.expected_authority_sha256.clone()),
        ),
        (
            "generationConfigSha256".to_owned(),
            Value::String(manifest.generation_config_sha256.clone()),
        ),
        (
            "generationIndex".to_owned(),
            Value::from(manifest.generation_index),
        ),
        (
            "requestedCount".to_owned(),
            Value::from(manifest.requested_count),
        ),
        (
            "acceptedRecordCount".to_owned(),
            field(
                receipt_fields,
                "acceptedRecordCount",
                "native v5 evolved proposal receipt",
            )?
            .clone(),
        ),
        (
            "attemptCount".to_owned(),
            field(
                receipt_fields,
                "attemptCount",
                "native v5 evolved proposal receipt",
            )?
            .clone(),
        ),
        (
            "transactionSha256".to_owned(),
            field(
                receipt_fields,
                "transactionSha256",
                "native v5 evolved proposal receipt",
            )?
            .clone(),
        ),
        (
            "parentArchiveInputBindingSha256".to_owned(),
            field(
                receipt_fields,
                "parentArchiveInputBindingSha256",
                "native v5 evolved proposal receipt",
            )?
            .clone(),
        ),
        (
            "identityLedgerInputBindingSha256".to_owned(),
            field(
                receipt_fields,
                "identityLedgerInputBindingSha256",
                "native v5 evolved proposal receipt",
            )?
            .clone(),
        ),
        (
            "publicationPlanSha256".to_owned(),
            field(
                receipt_fields,
                "publicationPlanSha256",
                "native v5 evolved proposal receipt",
            )?
            .clone(),
        ),
        (
            "publicationRequestSha256".to_owned(),
            field(
                receipt_fields,
                "publicationRequestSha256",
                "native v5 evolved proposal receipt",
            )?
            .clone(),
        ),
        (
            "publicationReceiptSha256".to_owned(),
            field(
                receipt_fields,
                "publicationReceiptSha256",
                "native v5 evolved proposal receipt",
            )?
            .clone(),
        ),
        (
            "publicationFragmentsSha256".to_owned(),
            field(
                receipt_fields,
                "publicationFragmentsSha256",
                "native v5 evolved proposal receipt",
            )?
            .clone(),
        ),
        (
            "evaluationPopulationSize".to_owned(),
            field(
                receipt_fields,
                "evaluationPopulationSize",
                "native v5 evolved proposal receipt",
            )?
            .clone(),
        ),
        (
            "identityLedgerSha256".to_owned(),
            field(
                receipt_fields,
                "identityLedgerSha256",
                "native v5 evolved proposal receipt",
            )?
            .clone(),
        ),
        (
            "outputInventorySha256".to_owned(),
            field(
                receipt_fields,
                "outputInventorySha256",
                "native v5 evolved proposal receipt",
            )?
            .clone(),
        ),
        ("receipt".to_owned(), receipt.clone()),
        (
            "receiptSha256".to_owned(),
            field(
                receipt_fields,
                "receiptSha256",
                "native v5 evolved proposal receipt",
            )?
            .clone(),
        ),
    ]);
    let result_sha256 = canonical_sha256(&Value::Object(result.clone()))?;
    result.insert("resultSha256".to_owned(), Value::String(result_sha256));
    validate_v5_evolved_proposal_result(&Value::Object(result), manifest)
}

pub fn validate_v5_evolved_proposal_result(
    value: &Value,
    manifest: &V5ProposalManifest,
) -> Result<V5EvolvedProposalResult> {
    if manifest.generation_kind != "evolved" || manifest.generation_index < 2 {
        bail!("native v5 evolved result is incompatible with a non-evolved manifest");
    }
    let fields = object(value, "native v5 evolved proposal result")?;
    exact_keys(
        fields,
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
        "native v5 evolved proposal result",
    )?;
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
        "receiptSha256",
        "resultSha256",
    ] {
        sha_field(fields, key, "native v5 evolved proposal result")?;
    }
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some(V5_EVOLVED_PROPOSAL_RESULT_SCHEMA)
        || fields.get("contractVersion").and_then(Value::as_str) != Some(CONTRACT_VERSION)
        || fields.get("operation").and_then(Value::as_str) != Some(V5_PROPOSAL_OPERATION)
        || fields.get("status").and_then(Value::as_str) != Some("completed")
        || fields.get("authoritySha256").and_then(Value::as_str)
            != Some(manifest.authority_sha256.as_str())
        || fields.get("manifestSha256").and_then(Value::as_str)
            != Some(manifest.manifest_sha256.as_str())
        || fields
            .get("expectedAuthoritySha256")
            .and_then(Value::as_str)
            != Some(manifest.expected_authority_sha256.as_str())
        || fields.get("generationConfigSha256").and_then(Value::as_str)
            != Some(manifest.generation_config_sha256.as_str())
        || fields.get("generationIndex").and_then(Value::as_u64) != Some(manifest.generation_index)
        || fields.get("requestedCount").and_then(Value::as_u64) != Some(manifest.requested_count)
        || fields.get("acceptedRecordCount").and_then(Value::as_u64)
            != Some(manifest.requested_count)
        || fields
            .get("attemptCount")
            .and_then(Value::as_u64)
            .is_none_or(|count| {
                count < manifest.requested_count || count > manifest.max_proposal_attempts
            })
        || fields
            .get("evaluationPopulationSize")
            .and_then(Value::as_u64)
            != Some(manifest.evaluation_population_size)
        || fields
            .get("parentArchiveInputBindingSha256")
            .and_then(Value::as_str)
            != Some(evolved_manifest_input_binding_sha(manifest, "parentArchive")?.as_str())
        || fields
            .get("identityLedgerInputBindingSha256")
            .and_then(Value::as_str)
            != Some(evolved_manifest_input_binding_sha(manifest, "identityLedger")?.as_str())
    {
        bail!("native v5 evolved proposal result is incompatible with its manifest");
    }
    validate_v5_evolved_proposal_receipt(
        field(fields, "receipt", "native v5 evolved proposal result")?,
        manifest,
        fields,
    )?;
    let receipt_sha = sha_field(fields, "receiptSha256", "native v5 evolved proposal result")?;
    if canonical_sha256_without_object_field(
        field(fields, "receipt", "native v5 evolved proposal result")?,
        "receiptSha256",
    )? != receipt_sha
    {
        bail!("native v5 evolved proposal result receipt identity mismatch");
    }
    let result_sha = sha_field(fields, "resultSha256", "native v5 evolved proposal result")?;
    if canonical_sha256_without_object_field(value, "resultSha256")? != result_sha {
        bail!("native v5 evolved proposal result identity mismatch");
    }
    Ok(V5EvolvedProposalResult {
        value: value.clone(),
    })
}

fn validate_v5_evolved_construction_summary(
    value: &Value,
    attempt_count: u64,
    accepted_record_count: u64,
) -> Result<()> {
    let fields = object(value, "native v5 evolved construction summary")?;
    exact_keys(
        fields,
        &["schemaVersion", "bytes", "attempts", "uniqueCounts"],
        "native v5 evolved construction summary",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some(V5_EVOLVED_PROPOSAL_CONSTRUCTION_SUMMARY_SCHEMA)
    {
        bail!("native v5 evolved construction summary schema is incompatible");
    }
    let bytes = object(
        field(fields, "bytes", "native v5 evolved construction summary")?,
        "native v5 evolved construction-summary bytes",
    )?;
    exact_keys(
        bytes,
        &["durableObjectBytes", "publicArtifactBytes"],
        "native v5 evolved construction-summary bytes",
    )?;
    for key in ["durableObjectBytes", "publicArtifactBytes"] {
        count_field(bytes, key, "native v5 evolved construction-summary bytes")?;
    }
    let attempts = object(
        field(fields, "attempts", "native v5 evolved construction summary")?,
        "native v5 evolved construction-summary attempts",
    )?;
    exact_keys(
        attempts,
        &["byDisposition", "byReason"],
        "native v5 evolved construction-summary attempts",
    )?;
    for key in ["byDisposition", "byReason"] {
        let (_, total) = count_map(
            field(
                attempts,
                key,
                "native v5 evolved construction-summary attempts",
            )?,
            "native v5 evolved construction-summary attempt counts",
        )?;
        if total != attempt_count {
            bail!("native v5 evolved construction summary {key} total drifts from attempts");
        }
    }
    let unique = object(
        field(
            fields,
            "uniqueCounts",
            "native v5 evolved construction summary",
        )?,
        "native v5 evolved construction-summary unique counts",
    )?;
    exact_keys(
        unique,
        &[
            "candidateIdentityCount",
            "executableSemanticCount",
            "pairIdentityCount",
        ],
        "native v5 evolved construction-summary unique counts",
    )?;
    for key in [
        "candidateIdentityCount",
        "executableSemanticCount",
        "pairIdentityCount",
    ] {
        if count_field(
            unique,
            key,
            "native v5 evolved construction-summary unique counts",
        )? != accepted_record_count
        {
            bail!("native v5 evolved construction summary {key} drifts from accepted records");
        }
    }
    Ok(())
}

fn validate_v5_evolved_proposal_receipt(
    value: &Value,
    manifest: &V5ProposalManifest,
    result: &Map<String, Value>,
) -> Result<()> {
    let fields = object(value, "native v5 evolved proposal receipt")?;
    exact_keys(
        fields,
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
        "native v5 evolved proposal receipt",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some(V5_EVOLVED_PROPOSAL_RECEIPT_SCHEMA)
        || fields.get("authoritySha256").and_then(Value::as_str)
            != Some(manifest.authority_sha256.as_str())
        || fields.get("manifestSha256").and_then(Value::as_str)
            != Some(manifest.manifest_sha256.as_str())
        || fields
            .get("expectedAuthoritySha256")
            .and_then(Value::as_str)
            != Some(manifest.expected_authority_sha256.as_str())
        || fields.get("generationConfigSha256").and_then(Value::as_str)
            != Some(manifest.generation_config_sha256.as_str())
        || fields.get("generationIndex").and_then(Value::as_u64) != Some(manifest.generation_index)
        || fields.get("requestedCount").and_then(Value::as_u64) != Some(manifest.requested_count)
        || fields.get("acceptedRecordCount").and_then(Value::as_u64)
            != Some(manifest.requested_count)
        || fields
            .get("attemptCount")
            .and_then(Value::as_u64)
            .is_none_or(|count| {
                count < manifest.requested_count || count > manifest.max_proposal_attempts
            })
        || fields
            .get("evaluationPopulationSize")
            .and_then(Value::as_u64)
            != Some(manifest.evaluation_population_size)
        || fields.get("threadCap").and_then(Value::as_u64) != Some(manifest.thread_cap)
        || fields
            .get("parentArchiveInputBindingSha256")
            .and_then(Value::as_str)
            != Some(evolved_manifest_input_binding_sha(manifest, "parentArchive")?.as_str())
        || fields
            .get("identityLedgerInputBindingSha256")
            .and_then(Value::as_str)
            != Some(evolved_manifest_input_binding_sha(manifest, "identityLedger")?.as_str())
    {
        bail!("native v5 evolved proposal receipt is incompatible with its manifest");
    }
    let batch = manifest
        .execution_authority
        .get("nativeBatchAuthoritySha256")
        .and_then(Value::as_str)
        .expect("validated execution authority has batch SHA-256");
    for key in [
        "transactionSha256",
        "parentArchiveInputBindingSha256",
        "identityLedgerInputBindingSha256",
        "publicationRequestSha256",
        "publicationPlanSha256",
        "publicationReceiptSha256",
        "publicationFragmentsSha256",
        "identityLedgerSha256",
    ] {
        if fields.get(key) != result.get(key) {
            bail!("native v5 evolved proposal receipt {key} differs from result");
        }
        sha_field(fields, key, "native v5 evolved proposal receipt")?;
    }
    let mut semantic_roots = result.clone();
    semantic_roots.insert(
        "expectedAuthoritySha256".to_owned(),
        Value::String(manifest.expected_authority_sha256.clone()),
    );
    let output_inventory = validate_output_inventory(
        field(
            fields,
            "outputInventory",
            "native v5 evolved proposal receipt",
        )?,
        &semantic_roots,
        &manifest.output_root,
        "evolved",
    )?;
    if fields.get("outputInventorySha256").and_then(Value::as_str)
        != Some(output_inventory.as_str())
        || result.get("outputInventorySha256").and_then(Value::as_str)
            != Some(output_inventory.as_str())
    {
        bail!("native v5 evolved proposal output inventory binding drifted");
    }
    let object_store = field(
        object(
            field(
                fields,
                "outputInventory",
                "native v5 evolved proposal receipt",
            )?,
            "native v5 evolved proposal output inventory",
        )?,
        "objectStore",
        "native v5 evolved proposal output inventory",
    )?;
    for (key, role, label) in [
        (
            "transactionSha256",
            "transaction",
            "native v5 evolved transaction root",
        ),
        (
            "publicationReceiptSha256",
            "publicationReceipt",
            "native v5 evolved publication receipt root",
        ),
        (
            "publicationFragmentsSha256",
            "publicationFragments",
            "native v5 evolved publication fragments root",
        ),
    ] {
        require_object_store_semantic_object(
            object_store,
            role,
            fields
                .get(key)
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("native v5 evolved proposal receipt lacks {key}"))?,
            label,
        )?;
    }
    if fields
        .get("nativeBatchAuthoritySha256")
        .and_then(Value::as_str)
        != Some(batch)
    {
        bail!("native v5 evolved proposal receipt native batch authority drifted");
    }
    validate_v5_evolved_construction_summary(
        field(
            fields,
            "constructionSummary",
            "native v5 evolved proposal receipt",
        )?,
        fields
            .get("attemptCount")
            .and_then(Value::as_u64)
            .expect("validated native v5 evolved receipt attempt count"),
        fields
            .get("acceptedRecordCount")
            .and_then(Value::as_u64)
            .expect("validated native v5 evolved receipt accepted record count"),
    )?;
    let supplied = sha_field(
        fields,
        "receiptSha256",
        "native v5 evolved proposal receipt",
    )?;
    if canonical_sha256_without_object_field(value, "receiptSha256")? != supplied {
        bail!("native v5 evolved proposal receipt identity mismatch");
    }
    Ok(())
}

fn validate_v5_proposal_receipt(
    value: &Value,
    manifest: &V5ProposalManifest,
    result: &Map<String, Value>,
) -> Result<()> {
    let fields = object(value, "native v5 proposal receipt")?;
    exact_keys(
        fields,
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
        "native v5 proposal receipt",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str) != Some(V5_PROPOSAL_RECEIPT_SCHEMA)
        || fields.get("authoritySha256").and_then(Value::as_str)
            != Some(manifest.authority_sha256.as_str())
        || fields.get("manifestSha256").and_then(Value::as_str)
            != Some(manifest.manifest_sha256.as_str())
        || fields
            .get("expectedAuthoritySha256")
            .and_then(Value::as_str)
            != Some(manifest.expected_authority_sha256.as_str())
        || fields.get("generationConfigSha256").and_then(Value::as_str)
            != Some(manifest.generation_config_sha256.as_str())
        || fields.get("generationIndex").and_then(Value::as_u64) != Some(manifest.generation_index)
        || fields.get("requestedCount").and_then(Value::as_u64) != Some(manifest.requested_count)
        || fields.get("acceptedRecordCount").and_then(Value::as_u64)
            != Some(manifest.requested_count)
        || fields
            .get("attemptCount")
            .and_then(Value::as_u64)
            .is_none_or(|count| {
                count < manifest.requested_count || count > manifest.max_proposal_attempts
            })
        || fields
            .get("evaluationPopulationSize")
            .and_then(Value::as_u64)
            != Some(manifest.evaluation_population_size)
        || fields.get("threadCap").and_then(Value::as_u64) != Some(manifest.thread_cap)
    {
        bail!("native v5 proposal receipt is incompatible with its manifest");
    }
    let batch = manifest
        .execution_authority
        .get("nativeBatchAuthoritySha256")
        .and_then(Value::as_str)
        .expect("validated execution authority has batch SHA-256");
    for key in [
        "attemptJournalSha256",
        "publicationRequestSha256",
        "publicationPlanSha256",
        "g0FunnelFragmentsSha256",
        "g0FunnelProjectionStreamReceiptSha256",
        "compactJournalSha256",
        "identityLedgerSha256",
        "selectedProjectionIndexSha256",
    ] {
        if fields.get(key) != result.get(key) {
            bail!("native v5 proposal receipt {key} differs from result");
        }
        sha_field(fields, key, "native v5 proposal receipt")?;
    }
    let mut semantic_roots = result.clone();
    semantic_roots.insert(
        "expectedAuthoritySha256".to_owned(),
        Value::String(manifest.expected_authority_sha256.clone()),
    );
    let output_inventory = validate_output_inventory(
        field(fields, "outputInventory", "native v5 proposal receipt")?,
        &semantic_roots,
        &manifest.output_root,
        &manifest.generation_kind,
    )?;
    if fields.get("outputInventorySha256").and_then(Value::as_str)
        != Some(output_inventory.as_str())
        || result.get("outputInventorySha256").and_then(Value::as_str)
            != Some(output_inventory.as_str())
    {
        bail!("native v5 proposal output inventory binding drifted");
    }
    let object_store = field(
        object(
            field(fields, "outputInventory", "native v5 proposal receipt")?,
            "native v5 proposal output inventory",
        )?,
        "objectStore",
        "native v5 proposal output inventory",
    )?;
    require_object_store_semantic_object(
        object_store,
        "g0FunnelFragments",
        fields
            .get("g0FunnelFragmentsSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 proposal receipt lacks G0 funnel root"))?,
        "native v5 G0 funnel fragments root",
    )?;
    require_object_store_semantic_object(
        object_store,
        "g0FunnelProjectionStreamReceipt",
        fields
            .get("g0FunnelProjectionStreamReceiptSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                anyhow!("native v5 proposal receipt lacks G0 funnel projection-stream root")
            })?,
        "native v5 G0 funnel projection-stream receipt root",
    )?;
    if fields
        .get("nativeBatchAuthoritySha256")
        .and_then(Value::as_str)
        != Some(batch)
    {
        bail!("native v5 proposal receipt native batch authority drifted");
    }
    validate_construction_summary(
        field(fields, "constructionSummary", "native v5 proposal receipt")?,
        fields
            .get("attemptCount")
            .and_then(Value::as_u64)
            .expect("validated native v5 receipt attempt count"),
        fields
            .get("acceptedRecordCount")
            .and_then(Value::as_u64)
            .expect("validated native v5 receipt accepted record count"),
    )?;
    let supplied = sha_field(fields, "receiptSha256", "native v5 proposal receipt")?;
    if canonical_sha256_without_object_field(value, "receiptSha256")? != supplied {
        bail!("native v5 proposal receipt identity mismatch");
    }
    Ok(())
}

fn validate_adoption_telemetry_with_schema(
    value: &Value,
    manifest: &V5ProposalManifest,
    expected_schema: &str,
    label: &str,
) -> Result<()> {
    let fields = object(value, label)?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "outputAuthentication",
            "publicArtifactBytesRead",
            "objectStoreBytesRead",
            "authenticatedFileCount",
            "proposalReconstructionCount",
            "legacyRichExpansionCount",
            "processTree",
            "threadCap",
        ],
        label,
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str) != Some(expected_schema)
        || fields.get("threadCap").and_then(Value::as_u64) != Some(manifest.thread_cap)
    {
        bail!("native v5 proposal adoption telemetry is incompatible");
    }
    let timing = object(
        field(fields, "outputAuthentication", label)?,
        "native v5 proposal adoption output authentication",
    )?;
    exact_keys(
        timing,
        &["wallMilliseconds"],
        "native v5 proposal adoption output authentication",
    )?;
    count_field(
        timing,
        "wallMilliseconds",
        "native v5 proposal adoption output authentication",
    )?;
    for key in [
        "publicArtifactBytesRead",
        "objectStoreBytesRead",
        "authenticatedFileCount",
    ] {
        count_field(fields, key, label)?;
    }
    for key in ["proposalReconstructionCount", "legacyRichExpansionCount"] {
        if count_field(fields, key, label)? != 0 {
            bail!("native v5 proposal adoption telemetry records nonzero {key}");
        }
    }
    let process_tree = object(
        field(fields, "processTree", label)?,
        "native v5 proposal adoption process-tree evidence",
    )?;
    exact_keys(
        process_tree,
        &[
            "measurement",
            "peakRssBytes",
            "peakPrivateBytes",
            "pythonChildCount",
            "dashboardChildCount",
        ],
        "native v5 proposal adoption process-tree evidence",
    )?;
    match process_tree.get("measurement").and_then(Value::as_str) {
        Some("windows_peak_process_memory_v1") => {
            for key in ["peakRssBytes", "peakPrivateBytes"] {
                if count_field(
                    process_tree,
                    key,
                    "native v5 proposal adoption process-tree evidence",
                )? == 0
                {
                    bail!("native v5 proposal Windows process metric {key} must be nonzero");
                }
            }
        }
        Some("unavailable_non_windows_v1") => {
            for key in ["peakRssBytes", "peakPrivateBytes"] {
                if process_tree.get(key) != Some(&Value::Null) {
                    bail!("native v5 proposal non-Windows process metric {key} must be null");
                }
            }
        }
        _ => bail!("native v5 proposal process-tree measurement is incompatible"),
    }
    for key in ["pythonChildCount", "dashboardChildCount"] {
        if count_field(
            process_tree,
            key,
            "native v5 proposal adoption process-tree evidence",
        )? != 0
        {
            bail!("native v5 proposal adoption process-tree records nonzero {key}");
        }
    }
    Ok(())
}

fn validate_adoption_telemetry(value: &Value, manifest: &V5ProposalManifest) -> Result<()> {
    validate_adoption_telemetry_with_schema(
        value,
        manifest,
        V5_PROPOSAL_ADOPTION_TELEMETRY_SCHEMA,
        "native v5 proposal adoption telemetry",
    )
}

pub fn validate_v5_proposal_adoption_evidence(
    value: &Value,
    manifest: &V5ProposalManifest,
    immutable_result: &V5ProposalResult,
) -> Result<()> {
    let fields = object(value, "native v5 proposal adoption evidence")?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "operation",
            "status",
            "authoritySha256",
            "expectedAuthoritySha256",
            "manifestSha256",
            "immutableResultSha256",
            "outputInventorySha256",
            "nativeBatchAuthoritySha256",
            "nativeExecutableSha256",
            "nativeSourceSha256",
            "telemetry",
            "adoptionEvidenceSha256",
        ],
        "native v5 proposal adoption evidence",
    )?;
    let result = object(
        &immutable_result.value,
        "native v5 immutable proposal result",
    )?;
    let execution = object(
        &manifest.execution_authority,
        "native v5 proposal execution authority",
    )?;
    let batch = object(
        field(
            execution,
            "nativeBatchAuthority",
            "native v5 proposal execution authority",
        )?,
        "native v5 batch authority",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some(V5_PROPOSAL_ADOPTION_EVIDENCE_SCHEMA)
        || fields.get("operation").and_then(Value::as_str) != Some(V5_PROPOSAL_OPERATION)
        || fields.get("status").and_then(Value::as_str) != Some("adopted")
        || fields.get("authoritySha256").and_then(Value::as_str)
            != Some(manifest.authority_sha256.as_str())
        || fields
            .get("expectedAuthoritySha256")
            .and_then(Value::as_str)
            != Some(manifest.expected_authority_sha256.as_str())
        || fields.get("manifestSha256").and_then(Value::as_str)
            != Some(manifest.manifest_sha256.as_str())
        || fields.get("immutableResultSha256").and_then(Value::as_str)
            != result.get("resultSha256").and_then(Value::as_str)
        || fields.get("outputInventorySha256").and_then(Value::as_str)
            != result.get("outputInventorySha256").and_then(Value::as_str)
        || fields
            .get("nativeBatchAuthoritySha256")
            .and_then(Value::as_str)
            != execution
                .get("nativeBatchAuthoritySha256")
                .and_then(Value::as_str)
        || fields.get("nativeExecutableSha256").and_then(Value::as_str)
            != batch.get("executableSha256").and_then(Value::as_str)
        || fields.get("nativeSourceSha256").and_then(Value::as_str)
            != batch.get("sourceSha256").and_then(Value::as_str)
    {
        bail!("native v5 proposal adoption evidence binding drifted");
    }
    for key in [
        "authoritySha256",
        "expectedAuthoritySha256",
        "manifestSha256",
        "immutableResultSha256",
        "outputInventorySha256",
        "nativeBatchAuthoritySha256",
        "nativeExecutableSha256",
        "nativeSourceSha256",
        "adoptionEvidenceSha256",
    ] {
        sha_field(fields, key, "native v5 proposal adoption evidence")?;
    }
    validate_adoption_telemetry(
        field(fields, "telemetry", "native v5 proposal adoption evidence")?,
        manifest,
    )?;
    let supplied = sha_field(
        fields,
        "adoptionEvidenceSha256",
        "native v5 proposal adoption evidence",
    )?;
    if canonical_sha256_without_object_field(value, "adoptionEvidenceSha256")? != supplied {
        bail!("native v5 proposal adoption evidence identity mismatch");
    }
    Ok(())
}

/// Validate stdout-only later-generation adoption evidence.  It intentionally
/// shares the execution telemetry shape with G0 but has a distinct schema and
/// requires the distinct evolved immutable result wrapper.
pub fn validate_v5_evolved_proposal_adoption_evidence(
    value: &Value,
    manifest: &V5ProposalManifest,
    immutable_result: &V5EvolvedProposalResult,
) -> Result<()> {
    let fields = object(value, "native v5 evolved proposal adoption evidence")?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "operation",
            "status",
            "authoritySha256",
            "expectedAuthoritySha256",
            "manifestSha256",
            "immutableResultSha256",
            "outputInventorySha256",
            "nativeBatchAuthoritySha256",
            "nativeExecutableSha256",
            "nativeSourceSha256",
            "telemetry",
            "adoptionEvidenceSha256",
        ],
        "native v5 evolved proposal adoption evidence",
    )?;
    let result = object(
        &immutable_result.value,
        "native v5 evolved immutable proposal result",
    )?;
    let execution = object(
        &manifest.execution_authority,
        "native v5 evolved proposal execution authority",
    )?;
    let batch = object(
        field(
            execution,
            "nativeBatchAuthority",
            "native v5 evolved proposal execution authority",
        )?,
        "native v5 evolved batch authority",
    )?;
    if manifest.generation_kind != "evolved"
        || fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_EVOLVED_PROPOSAL_ADOPTION_EVIDENCE_SCHEMA)
        || fields.get("operation").and_then(Value::as_str) != Some(V5_PROPOSAL_OPERATION)
        || fields.get("status").and_then(Value::as_str) != Some("adopted")
        || fields.get("authoritySha256").and_then(Value::as_str)
            != Some(manifest.authority_sha256.as_str())
        || fields
            .get("expectedAuthoritySha256")
            .and_then(Value::as_str)
            != Some(manifest.expected_authority_sha256.as_str())
        || fields.get("manifestSha256").and_then(Value::as_str)
            != Some(manifest.manifest_sha256.as_str())
        || fields.get("immutableResultSha256").and_then(Value::as_str)
            != result.get("resultSha256").and_then(Value::as_str)
        || fields.get("outputInventorySha256").and_then(Value::as_str)
            != result.get("outputInventorySha256").and_then(Value::as_str)
        || fields
            .get("nativeBatchAuthoritySha256")
            .and_then(Value::as_str)
            != execution
                .get("nativeBatchAuthoritySha256")
                .and_then(Value::as_str)
        || fields.get("nativeExecutableSha256").and_then(Value::as_str)
            != batch.get("executableSha256").and_then(Value::as_str)
        || fields.get("nativeSourceSha256").and_then(Value::as_str)
            != batch.get("sourceSha256").and_then(Value::as_str)
    {
        bail!("native v5 evolved proposal adoption evidence binding drifted");
    }
    for key in [
        "authoritySha256",
        "expectedAuthoritySha256",
        "manifestSha256",
        "immutableResultSha256",
        "outputInventorySha256",
        "nativeBatchAuthoritySha256",
        "nativeExecutableSha256",
        "nativeSourceSha256",
        "adoptionEvidenceSha256",
    ] {
        sha_field(fields, key, "native v5 evolved proposal adoption evidence")?;
    }
    validate_adoption_telemetry_with_schema(
        field(
            fields,
            "telemetry",
            "native v5 evolved proposal adoption evidence",
        )?,
        manifest,
        V5_EVOLVED_PROPOSAL_ADOPTION_TELEMETRY_SCHEMA,
        "native v5 evolved proposal adoption telemetry",
    )?;
    let supplied = sha_field(
        fields,
        "adoptionEvidenceSha256",
        "native v5 evolved proposal adoption evidence",
    )?;
    if canonical_sha256_without_object_field(value, "adoptionEvidenceSha256")? != supplied {
        bail!("native v5 evolved proposal adoption evidence identity mismatch");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use temporal_qd_contract::canonical_sha256;

    use super::*;

    fn sha_token(byte: char) -> String {
        format!("sha256:{}", byte.to_string().repeat(64))
    }

    fn fixture() -> Value {
        let batch = json!({
            "schemaVersion": "temporal_qd_native_authority_v1",
            "contractVersion": CONTRACT_VERSION,
            "crateVersion": "0.1.0",
            "binaryName": "temporal-qd-batch",
            "buildProfile": "release",
            "executableSha256": sha_token('a'),
            "sourceSha256": sha_token('b'),
        });
        let mut batch = batch.as_object().unwrap().clone();
        batch.insert(
            "authoritySha256".to_owned(),
            Value::String(canonical_sha256(&Value::Object(batch.clone())).unwrap()),
        );
        let frozen_payload =
            json!({"budget": {"maxStates": 1}, "longContext": {}, "shortContext": {}});
        let frozen = json!({
            "schemaVersion": V5_SHARED_AUTHORITY_SCHEMA,
            "authority": frozen_payload,
            "authoritySha256": canonical_sha256(&frozen_payload).unwrap(),
        });
        let expected = frozen
            .get("authoritySha256")
            .and_then(Value::as_str)
            .unwrap()
            .to_owned();
        let config_without_sha = json!({
            "schemaVersion": "temporal_qd_pair_generation_v2",
            "generationIndex": 1,
            "targetUniqueCandidates": 2,
            "maxProposalAttempts": 2,
        });
        let config_sha = canonical_sha256(&config_without_sha).unwrap();
        let mut config = config_without_sha.as_object().unwrap().clone();
        config.insert("configSha256".to_owned(), Value::String(config_sha.clone()));
        let batch_sha = batch
            .get("authoritySha256")
            .and_then(Value::as_str)
            .unwrap()
            .to_owned();
        let execution_without_sha = json!({
            "schemaVersion": V5_PROPOSAL_EXECUTION_AUTHORITY_SCHEMA,
            "nativeBatchAuthority": Value::Object(batch),
            "nativeBatchAuthoritySha256": batch_sha,
            "expectedAuthoritySha256": expected,
            "frozenAuthoritySha256": expected,
            "generationConfigSha256": config_sha,
        });
        let execution_sha = canonical_sha256(&execution_without_sha).unwrap();
        let mut execution = execution_without_sha.as_object().unwrap().clone();
        execution.insert(
            "authoritySha256".to_owned(),
            Value::String(execution_sha.clone()),
        );
        let manifest_without_sha = json!({
            "schemaVersion": V5_PROPOSAL_MANIFEST_SCHEMA,
            "contractVersion": CONTRACT_VERSION,
            "operation": V5_PROPOSAL_OPERATION,
            "authoritySha256": execution_sha,
            "executionAuthority": Value::Object(execution),
            "frozenAuthority": frozen,
            "expectedAuthoritySha256": expected,
            "outputRoot": "C:\\fixture\\output",
            "finalNewline": "lf",
            "generationConfig": Value::Object(config),
            "generationConfigSha256": config_sha,
            "generationIndex": 1,
            "generationKind": "g0",
            "requestedCount": 2,
            "evaluationPopulationSize": 1,
            "maxProposalAttempts": 2,
            "threadCap": 1,
            "inputs": {
                "schemaVersion": V5_PROPOSAL_INPUTS_SCHEMA,
                "parentArchive": null,
                "identityLedger": null,
            },
            "resultPath": V5_PROPOSAL_RESULT_PATH,
        });
        let mut manifest = manifest_without_sha.as_object().unwrap().clone();
        manifest.insert(
            "manifestSha256".to_owned(),
            Value::String(canonical_sha256(&Value::Object(manifest.clone())).unwrap()),
        );
        Value::Object(manifest)
    }

    fn completed_result_fixture() -> (Value, Value) {
        let manifest = fixture();
        let parsed = validate_v5_proposal_manifest(&manifest).unwrap();
        let semantic_roots = json!({
            "attemptJournal": sha_token('1'),
            "attemptRows": sha_token('1'),
            "publicationRequest": sha_token('d'),
            "publicationPlan": sha_token('c'),
            "g0FunnelFragments": sha_token('0'),
            "g0FunnelProjectionStream": sha_token('e'),
            "compactJournal": sha_token('2'),
            "identityLedger": sha_token('3'),
            "selectedProjectionIndex": sha_token('4'),
            "sharedAuthority": parsed.expected_authority_sha256,
            "evaluationPopulation": sha_token('5'),
            "generationJournal": sha_token('6'),
            "g0AcceptedPool": sha_token('7'),
            "g0CampaignConstructionLedger": sha_token('8'),
            "g0Selection": sha_token('9'),
            "pairConfig": sha_token('a'),
            "population": sha_token('b'),
        });
        let artifacts = [
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
        ]
        .into_iter()
        .map(|(kind, path)| {
            json!({
                "kind": kind,
                "relativePath": path,
                "fileSha256": canonical_sha256(&Value::String(format!("raw:{path}"))).unwrap(),
                "byteLength": path.len() as u64,
                "semanticSha256": semantic_roots.get(kind).unwrap(),
            })
        })
        .collect::<Vec<_>>();
        let publication_plan_object = json!({
            "relativePath": format!(
                "sha256/{}.json",
                &semantic_roots["publicationPlan"]
                    .as_str()
                    .expect("fixture publication plan SHA")["sha256:".len()..],
            ),
            "objectSha256": semantic_roots["publicationPlan"],
            "fileSha256": sha_token('e'),
            "byteLength": 1,
        });
        let g0_funnel_object = json!({
            "relativePath": format!(
                "sha256/{}.json",
                &semantic_roots["g0FunnelFragments"]
                    .as_str()
                    .expect("fixture G0 funnel SHA")["sha256:".len()..],
            ),
            "objectSha256": semantic_roots["g0FunnelFragments"],
            "fileSha256": sha_token('f'),
            "byteLength": 1,
        });
        let g0_funnel_stream_receipt_object = json!({
            "relativePath": format!(
                "sha256/{}.json",
                &semantic_roots["g0FunnelProjectionStream"]
                    .as_str()
                    .expect("fixture G0 funnel projection-stream receipt SHA")["sha256:".len()..],
            ),
            "objectSha256": semantic_roots["g0FunnelProjectionStream"],
            "fileSha256": sha_token('a'),
            "byteLength": 1,
        });
        let mut object_values = vec![
            publication_plan_object,
            g0_funnel_object,
            g0_funnel_stream_receipt_object,
        ];
        object_values.sort_by(|left, right| {
            left["relativePath"]
                .as_str()
                .cmp(&right["relativePath"].as_str())
        });
        let construction_summary = json!({
            "schemaVersion": V5_PROPOSAL_CONSTRUCTION_SUMMARY_SCHEMA,
            "bytes": {
                "compactJournalBytes": 1, "staticAuthorityBytes": 1,
                "objectStoreBytes": 0, "selectedProjectionBytes": 1,
            },
            "attempts": {
                "byDisposition": {"accepted": 2},
                "byReason": {"accepted": 2},
            },
            "uniqueCounts": {
                "candidateCount": 2, "programCount": 2,
                "topologyCount": 2, "resourceCount": 2,
            },
        });
        let input = V5ProposalReceiptBuildInput {
            attempt_count: parsed.requested_count,
            attempt_journal_sha256: semantic_roots["attemptJournal"]
                .as_str()
                .unwrap()
                .to_owned(),
            publication_request_sha256: semantic_roots["publicationRequest"]
                .as_str()
                .unwrap()
                .to_owned(),
            publication_plan_sha256: semantic_roots["publicationPlan"]
                .as_str()
                .unwrap()
                .to_owned(),
            g0_funnel_fragments_sha256: semantic_roots["g0FunnelFragments"]
                .as_str()
                .unwrap()
                .to_owned(),
            g0_funnel_projection_stream_receipt_sha256: semantic_roots["g0FunnelProjectionStream"]
                .as_str()
                .unwrap()
                .to_owned(),
            compact_journal_sha256: semantic_roots["compactJournal"]
                .as_str()
                .unwrap()
                .to_owned(),
            identity_ledger_sha256: semantic_roots["identityLedger"]
                .as_str()
                .unwrap()
                .to_owned(),
            selected_projection_index_sha256: semantic_roots["selectedProjectionIndex"]
                .as_str()
                .unwrap()
                .to_owned(),
            construction_summary,
            artifacts: artifacts
                .iter()
                .map(|artifact| V5OutputArtifactIdentity {
                    kind: artifact["kind"].as_str().unwrap().to_owned(),
                    relative_path: artifact["relativePath"].as_str().unwrap().to_owned(),
                    file_sha256: artifact["fileSha256"].as_str().unwrap().to_owned(),
                    byte_length: artifact["byteLength"].as_u64().unwrap(),
                    semantic_sha256: artifact["semanticSha256"].as_str().unwrap().to_owned(),
                })
                .collect(),
            objects: object_values
                .iter()
                .map(|object| V5ObjectStoreIdentity {
                    object_sha256: object["objectSha256"].as_str().unwrap().to_owned(),
                    file_sha256: object["fileSha256"].as_str().unwrap().to_owned(),
                    byte_length: object["byteLength"].as_u64().unwrap(),
                })
                .collect(),
        };
        let (_, _, result) = build_v5_proposal_receipt_and_result(&parsed, &input).unwrap();
        return (manifest, result.value);
    }

    fn rehash_result_inventory_chain(result: &mut Value) {
        let receipt = result
            .get_mut("receipt")
            .and_then(Value::as_object_mut)
            .unwrap();
        let inventory = receipt
            .get_mut("outputInventory")
            .and_then(Value::as_object_mut)
            .unwrap();
        let mut inventory_material = inventory.clone();
        inventory_material.remove("outputInventorySha256");
        let inventory_sha = canonical_sha256(&Value::Object(inventory_material)).unwrap();
        inventory.insert(
            "outputInventorySha256".to_owned(),
            Value::String(inventory_sha.clone()),
        );
        receipt.insert(
            "outputInventorySha256".to_owned(),
            Value::String(inventory_sha.clone()),
        );
        let mut receipt_material = receipt.clone();
        receipt_material.remove("receiptSha256");
        let receipt_sha = canonical_sha256(&Value::Object(receipt_material)).unwrap();
        receipt.insert(
            "receiptSha256".to_owned(),
            Value::String(receipt_sha.clone()),
        );
        let result_fields = result.as_object_mut().unwrap();
        result_fields.insert(
            "outputInventorySha256".to_owned(),
            Value::String(inventory_sha),
        );
        result_fields.insert("receiptSha256".to_owned(), Value::String(receipt_sha));
        let mut result_material = result_fields.clone();
        result_material.remove("resultSha256");
        result_fields.insert(
            "resultSha256".to_owned(),
            Value::String(canonical_sha256(&Value::Object(result_material)).unwrap()),
        );
    }

    #[test]
    fn manifest_binds_one_native_batch_and_compact_frozen_authority() {
        let manifest = fixture();
        let parsed = validate_v5_proposal_manifest(&manifest).unwrap();
        assert_eq!(parsed.requested_count, 2);
        assert_eq!(parsed.evaluation_population_size, 1);
        assert_eq!(parsed.generation_kind, "g0");
        assert!(parsed.frozen_authority.get("authority").is_some());
    }

    #[test]
    fn g0_rejects_legacy_parent_and_noncanonical_manifest() {
        let mut manifest = fixture();
        manifest
            .get_mut("inputs")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("parentArchive".to_owned(), json!({"path": "legacy"}));
        manifest.as_object_mut().unwrap().remove("manifestSha256");
        let sha = canonical_sha256(&manifest).unwrap();
        manifest
            .as_object_mut()
            .unwrap()
            .insert("manifestSha256".to_owned(), Value::String(sha));
        assert!(validate_v5_proposal_manifest(&manifest).is_err());

        let raw = serde_json::to_vec(&fixture()).unwrap();
        assert!(parse_v5_proposal_manifest(&raw).is_err());
    }

    #[test]
    fn result_binds_all_canonical_public_output_paths() {
        let (manifest, result) = completed_result_fixture();
        let parsed = validate_v5_proposal_manifest(&manifest).unwrap();
        assert_eq!(
            canonical_sha256_without_object_field(&result["receipt"], "receiptSha256").unwrap(),
            result["receiptSha256"].as_str().unwrap(),
        );
        if let Err(error) = validate_v5_proposal_result(&result, &parsed) {
            panic!("valid v5 output-inventory fixture rejected: {error:#}");
        }

        // The receipt seal is intentionally deterministic: timing, process
        // measurements, and the execution cap belong only to stdout evidence.
        let mut mutable_summary = result.clone();
        mutable_summary["receipt"]["constructionSummary"]
            .as_object_mut()
            .unwrap()
            .insert("threadCap".to_owned(), Value::from(1_u64));
        rehash_result_inventory_chain(&mut mutable_summary);
        assert!(validate_v5_proposal_result(&mutable_summary, &parsed).is_err());

        let mut relocated = result.clone();
        let artifacts = relocated["receipt"]["outputInventory"]["artifacts"]
            .as_array_mut()
            .unwrap();
        artifacts
            .iter_mut()
            .find(|artifact| artifact["kind"] == "compactJournal")
            .unwrap()["relativePath"] = Value::String("supervisor-state.json".to_owned());
        rehash_result_inventory_chain(&mut relocated);
        assert!(validate_v5_proposal_result(&relocated, &parsed).is_err());

        // An inventory cannot authorize an otherwise arbitrary file below a
        // Rust-owned namespace.  New transaction artifacts require a
        // deliberate contract addition with a fixed kind and path.
        let mut with_extra = result.clone();
        with_extra["receipt"]["outputInventory"]["artifacts"]
            .as_array_mut()
            .unwrap()
            .push(json!({
                "kind": "zUntrusted",
                "relativePath": "v5-native/untrusted.json",
                "fileSha256": sha_token('f'),
                "byteLength": 1,
                "semanticSha256": sha_token('e'),
            }));
        rehash_result_inventory_chain(&mut with_extra);
        assert!(validate_v5_proposal_result(&with_extra, &parsed).is_err());

        // A plan SHA in the receipt is not sufficient by itself: typed
        // restart must be able to open the exact immutable plan object.
        let mut missing_plan_object = result.clone();
        let object_store = missing_plan_object["receipt"]["outputInventory"]["objectStore"]
            .as_object_mut()
            .unwrap();
        object_store
            .get_mut("roots")
            .and_then(Value::as_array_mut)
            .unwrap()
            .retain(|root| root["role"] != "publicationPlan");
        object_store.remove("objectStoreSha256");
        object_store.insert(
            "objectStoreSha256".to_owned(),
            Value::String(canonical_sha256(&Value::Object(object_store.clone())).unwrap()),
        );
        rehash_result_inventory_chain(&mut missing_plan_object);
        assert!(validate_v5_proposal_result(&missing_plan_object, &parsed).is_err());

        let mut foreign_root = result.clone();
        let object_store = foreign_root["receipt"]["outputInventory"]["objectStore"]
            .as_object_mut()
            .unwrap();
        object_store
            .get_mut("roots")
            .and_then(Value::as_array_mut)
            .unwrap()
            .push(json!({
                "role": "zForeign",
                "relativePath": format!("sha256/{}.json", "f".repeat(64)),
                "objectSha256": sha_token('f'),
                "fileSha256": sha_token('e'),
                "byteLength": 1,
            }));
        object_store.remove("objectStoreSha256");
        object_store.insert(
            "objectStoreSha256".to_owned(),
            Value::String(canonical_sha256(&Value::Object(object_store.clone())).unwrap()),
        );
        rehash_result_inventory_chain(&mut foreign_root);
        assert!(validate_v5_proposal_result(&foreign_root, &parsed).is_err());
    }

    #[test]
    fn g0_result_family_rejects_a_later_generation_manifest() {
        let (manifest, result) = completed_result_fixture();
        let mut parsed = validate_v5_proposal_manifest(&manifest).unwrap();
        parsed.generation_kind = "evolved".to_owned();
        parsed.generation_index = 2;
        assert!(validate_v5_proposal_result(&result, &parsed).is_err());
    }

    #[test]
    fn receipt_builder_derives_the_exact_inventory_and_invocation_result() {
        let (manifest, expected_result) = completed_result_fixture();
        let parsed = validate_v5_proposal_manifest(&manifest).unwrap();
        let receipt = &expected_result["receipt"];
        let inventory = &receipt["outputInventory"];
        let artifacts = inventory["artifacts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|artifact| V5OutputArtifactIdentity {
                kind: artifact["kind"].as_str().unwrap().to_owned(),
                relative_path: artifact["relativePath"].as_str().unwrap().to_owned(),
                file_sha256: artifact["fileSha256"].as_str().unwrap().to_owned(),
                byte_length: artifact["byteLength"].as_u64().unwrap(),
                semantic_sha256: artifact["semanticSha256"].as_str().unwrap().to_owned(),
            })
            .collect::<Vec<_>>();
        let objects = inventory["objectStore"]["roots"]
            .as_array()
            .unwrap()
            .iter()
            .map(|object| V5ObjectStoreIdentity {
                object_sha256: object["objectSha256"].as_str().unwrap().to_owned(),
                file_sha256: object["fileSha256"].as_str().unwrap().to_owned(),
                byte_length: object["byteLength"].as_u64().unwrap(),
            })
            .collect::<Vec<_>>();
        let input = V5ProposalReceiptBuildInput {
            attempt_count: receipt["attemptCount"].as_u64().unwrap(),
            attempt_journal_sha256: receipt["attemptJournalSha256"].as_str().unwrap().to_owned(),
            publication_request_sha256: receipt["publicationRequestSha256"]
                .as_str()
                .unwrap()
                .to_owned(),
            publication_plan_sha256: receipt["publicationPlanSha256"]
                .as_str()
                .unwrap()
                .to_owned(),
            g0_funnel_fragments_sha256: receipt["g0FunnelFragmentsSha256"]
                .as_str()
                .unwrap()
                .to_owned(),
            g0_funnel_projection_stream_receipt_sha256:
                receipt["g0FunnelProjectionStreamReceiptSha256"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            compact_journal_sha256: receipt["compactJournalSha256"].as_str().unwrap().to_owned(),
            identity_ledger_sha256: receipt["identityLedgerSha256"].as_str().unwrap().to_owned(),
            selected_projection_index_sha256: receipt["selectedProjectionIndexSha256"]
                .as_str()
                .unwrap()
                .to_owned(),
            construction_summary: receipt["constructionSummary"].clone(),
            artifacts,
            objects,
        };
        let (_object_inventory, rebuilt_receipt, rebuilt_result) =
            build_v5_proposal_receipt_and_result(&parsed, &input).unwrap();
        assert_eq!(rebuilt_receipt, expected_result["receipt"]);
        assert_eq!(rebuilt_result.value, expected_result);
    }

    #[test]
    fn sealed_receipt_deterministically_recreates_the_invocation_result() {
        let (manifest, result) = completed_result_fixture();
        let parsed = validate_v5_proposal_manifest(&manifest).unwrap();
        let rebuilt = build_v5_proposal_result_from_receipt(&parsed, &result["receipt"])
            .expect("rebuild invocation result from the sealed receipt");
        assert_eq!(rebuilt.value, result);

        let mut drifted = result["receipt"].clone();
        drifted["attemptCount"] = Value::from(0_u64);
        // The constructor is deliberately not a permissive receipt parser:
        // even self-hashing a changed receipt cannot make it an adoptable
        // result for this immutable manifest.
        let receipt = drifted.as_object_mut().unwrap();
        receipt.remove("receiptSha256");
        receipt.insert(
            "receiptSha256".to_owned(),
            Value::String(canonical_sha256(&Value::Object(receipt.clone())).unwrap()),
        );
        assert!(build_v5_proposal_result_from_receipt(&parsed, &drifted).is_err());
    }

    #[test]
    fn current_v5_outer_inventory_is_bounded_for_128_and_4000_objects() {
        let manifest = validate_v5_proposal_manifest(&fixture()).unwrap();
        let build = |count: usize, roots: &[(&str, &str)]| {
            let objects = (0..count)
                .map(|ordinal| V5ObjectStoreIdentity {
                    object_sha256: format!("sha256:{ordinal:064x}"),
                    file_sha256: format!("sha256:{:064x}", ordinal + count + 1),
                    byte_length: ordinal as u64 + 1,
                })
                .collect::<Vec<_>>();
            build_v5_output_inventory(&manifest, &[], &objects, roots).unwrap()
        };
        let g0_roots = [
            (
                "g0FunnelFragments",
                "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            ),
            (
                "g0FunnelProjectionStreamReceipt",
                "sha256:0000000000000000000000000000000000000000000000000000000000000001",
            ),
            (
                "publicationPlan",
                "sha256:0000000000000000000000000000000000000000000000000000000000000002",
            ),
        ];
        let evolved_roots = [
            (
                "publicationFragments",
                "sha256:0000000000000000000000000000000000000000000000000000000000000000",
            ),
            (
                "publicationPlan",
                "sha256:0000000000000000000000000000000000000000000000000000000000000001",
            ),
            (
                "publicationReceipt",
                "sha256:0000000000000000000000000000000000000000000000000000000000000002",
            ),
            (
                "transaction",
                "sha256:0000000000000000000000000000000000000000000000000000000000000003",
            ),
        ];
        for roots in [&g0_roots[..], &evolved_roots[..]] {
            let (small, _, small_sidecar) = build(128, roots);
            let (large, _, large_sidecar) = build(4000, roots);
            let small_outer = canonical_json_line(&small).unwrap().len();
            let large_outer = canonical_json_line(&large).unwrap().len();
            assert!(small_outer < 8 * 1024 && large_outer < 8 * 1024);
            assert!(large_outer.abs_diff(small_outer) < 128);
            assert!(
                large_sidecar.encoded_bytes().unwrap().len()
                    > small_sidecar.encoded_bytes().unwrap().len() * 20
            );
            assert!(small["objectStore"].get("objects").is_none());
            assert!(large["objectStore"]["inventory"]["objectCount"] == 4000);
        }
    }
}
