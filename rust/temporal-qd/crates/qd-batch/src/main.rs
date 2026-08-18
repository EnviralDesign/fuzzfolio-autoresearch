//! Standalone, one-shot Temporal QD native batch executable.
//!
//! The foundation probe validates a closed manifest and publishes one tiny
//! immutable result. Coarse pair generation composes the admitted kernel and
//! runtime crates behind a second exact, self-hashed manifest.

mod g0_funnel_contract;
mod generation_contract;
mod v5_fast_ephemeral;
mod v5_proposal_contract;

use std::collections::{BTreeMap, BTreeSet};
use std::env;
#[cfg(windows)]
use std::ffi::c_void;
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use sha2::{Digest, Sha256};
use temporal_qd_contract::{
    CONTRACT_VERSION, FoundationResult, JsonNewline, Map, NativeProgress, NativeProgressHandle,
    NativeProgressSection, NativeProgressSpec, NativeVersion, Value, canonical_json_line,
    canonical_sha256, parse_foundation_manifest, python_pretty_json_line,
};
use temporal_qd_kernel::v5::{
    V5AttemptJournal, V5AttemptOutcomeAudit, V5CompactAcceptedRecord, V5SelectedProjection,
    V5SharedConstructionAuthority, parent_reference_from_v5_compact_record,
    verify_v5_evolved_parent_reference,
};
use temporal_qd_kernel::{
    factory::ParentReference,
    g0_funnel::{
        DEFAULT_G0_ADMISSION_THREAD_CAP, G0FunnelOutcome, G0FunnelRequest,
        MAX_G0_ADMISSION_THREAD_CAP, finalize_g0,
    },
    generation::{GenerateGenerationRequest, generate_generation},
    journal::FinalNewline,
    proposal::{IdentityLedger, ParentSelector},
    publication::PublicationPolicy,
    schedule::RotatingParentSchedule,
    v5_evolved_durable::{
        V5EvolvedDurableObjectBinding, reconstruct_v5_evolved_transaction_from_durable_objects,
    },
    v5_evolved_publication::{
        V5_EVOLVED_IDENTITY_LEDGER_RELATIVE_PATH, V5EvolvedIdentityLedger,
        V5EvolvedPublicationFragmentKind, V5EvolvedPublicationFragmentSink,
        V5EvolvedPublicationFragmentSource, V5EvolvedPublicationFragments,
        V5EvolvedPublicationInputs, V5EvolvedPublicationPlan, V5EvolvedPublicationReceipt,
        V5EvolvedPublicationStream, prepare_v5_evolved_publication_stream,
        verify_v5_evolved_publication_adoption,
    },
    v5_evolved_transaction::{
        V5EvolvedTransactionRequest, V5EvolvedTransactionResult,
        execute_v5_evolved_transaction_with_progress, reconstruct_selected_parent_references,
    },
    v5_g0_funnel::{
        V5_G0_FUNNEL_PROJECTION_STREAM_PATH, V5G0FunnelFragmentReceiptObjectBinding,
        V5G0FunnelProjectionStreamReceiptObjectBinding, build_v5_g0_funnel_fragments,
        verify_v5_g0_funnel_fragment_receipt, verify_v5_g0_funnel_projection_stream,
        write_v5_g0_funnel_projection_stream,
    },
    v5_publication::{
        V5G0PublicationFragmentKind, V5G0PublicationFragmentSink, V5G0PublicationFragmentSource,
        V5G0PublicationFragments, V5G0PublicationInputs, V5G0PublicationReceipt,
        V5G0PublicationStream, prepare_v5_g0_publication_stream_from_fresh_transaction,
        verify_v5_g0_publication_adoption,
    },
    v5_transaction::{
        V5_G0_CONSTRUCTION_PREFETCH_MULTIPLIER, V5G0CompactIdentityLedger,
        V5G0DurableObjectBinding, V5G0DurableObjectKind, V5G0TransactionRequest,
        V5G0TransactionResult, execute_v5_g0_transaction_with_progress,
        reconstruct_v5_g0_transaction_from_artifacts, verify_v5_g0_transaction_replay,
    },
};
use temporal_qd_runtime::{
    DashboardPairAuthority, RuntimeManifest, RuntimeParentSelector,
    archive_bootstrap_inputs_from_manifest, bootstrap_global_identity_ledger_inputs,
    global_identity_ledger_from_public,
};

use crate::g0_funnel_contract::{G0_FUNNEL_MANIFEST_SCHEMA, parse_g0_funnel_manifest};
use crate::generation_contract::{
    FRONT_GENERATION_PROGRESS_SCHEMA, GENERATION_MANIFEST_SCHEMA, assemble_runtime_manifest_owned,
    build_generation_result, parse_generation_manifest, validate_generation_result,
};
use crate::v5_proposal_contract::{
    V5_EVOLVED_PROPOSAL_ADOPTION_EVIDENCE_SCHEMA, V5_EVOLVED_PROPOSAL_ADOPTION_TELEMETRY_SCHEMA,
    V5_EVOLVED_PROPOSAL_CONSTRUCTION_SUMMARY_SCHEMA, V5_PROPOSAL_ADOPTION_EVIDENCE_SCHEMA,
    V5_PROPOSAL_ADOPTION_TELEMETRY_SCHEMA, V5_PROPOSAL_CONSTRUCTION_SUMMARY_SCHEMA,
    V5_PROPOSAL_MANIFEST_SCHEMA, V5_PROPOSAL_OBJECT_INVENTORY_DESCRIPTOR_SCHEMA,
    V5_PROPOSAL_OBJECT_INVENTORY_PATH, V5_PROPOSAL_OBJECT_INVENTORY_ROW_SCHEMA,
    V5_PROPOSAL_OPERATION, V5_PROPOSAL_RESULT_PATH, V5EvolvedProposalReceiptBuildInput,
    V5EvolvedProposalResult, V5ObjectStoreIdentity, V5OutputArtifactIdentity, V5ProposalManifest,
    V5ProposalReceiptBuildInput, V5ProposalResult, build_v5_evolved_proposal_receipt_and_result,
    build_v5_evolved_proposal_result_from_receipt, build_v5_proposal_receipt_and_result,
    build_v5_proposal_result_from_receipt, parse_v5_proposal_manifest,
    validate_v5_evolved_proposal_adoption_evidence, validate_v5_evolved_proposal_result,
    validate_v5_proposal_adoption_evidence, validate_v5_proposal_result,
};

fn main() {
    match run() {
        Ok(()) => {}
        Err(error) => {
            eprintln!("ERROR: {error:#}");
            std::process::exit(2);
        }
    }
}

fn run() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() == 2 && args[1] == "--version-json" {
        return write_stdout_json(&serde_json::to_value(NativeVersion::current())?);
    }
    if args.len() == 3 && args[1] == "--manifest" {
        return execute_manifest(Path::new(&args[2]), V5BatchExecutionMode::Durable);
    }
    if args.len() == 5
        && args[1] == "--manifest"
        && args[3] == "--execution-mode"
        && args[4] == "fast-ephemeral-v1"
    {
        return execute_manifest(Path::new(&args[2]), V5BatchExecutionMode::FastEphemeralV1);
    }
    bail!(
        "usage: temporal-qd-batch --version-json | --manifest PATH [--execution-mode fast-ephemeral-v1]"
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum V5BatchExecutionMode {
    Durable,
    FastEphemeralV1,
}

fn execute_manifest(manifest_path: &Path, execution_mode: V5BatchExecutionMode) -> Result<()> {
    let manifest_path = safe_existing_file(manifest_path, "manifest")?;
    let raw = fs::read(&manifest_path)
        .with_context(|| format!("read manifest: {}", manifest_path.display()))?;
    let value: Value = serde_json::from_slice(&raw).context("parse manifest dispatch envelope")?;
    match value.get("schemaVersion").and_then(Value::as_str) {
        Some(V5_PROPOSAL_MANIFEST_SCHEMA) => {
            execute_v5_proposal(&manifest_path, &raw, execution_mode)
        }
        Some(_) if execution_mode == V5BatchExecutionMode::FastEphemeralV1 => {
            bail!("fast-ephemeral-v1 is admitted only for native v5 proposal construction")
        }
        Some(GENERATION_MANIFEST_SCHEMA) => execute_generation(&manifest_path, &raw),
        Some(G0_FUNNEL_MANIFEST_SCHEMA) => execute_g0_funnel(&manifest_path, &raw),
        _ => execute_foundation_bytes(&manifest_path, &raw),
    }
}

const V5_VALIDATION_MODE_ENV: &str = "TEMPORAL_QD_V5_VALIDATION_MODE";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum V5ValidationMode {
    Balanced,
    Strict,
}

impl V5ValidationMode {
    fn from_environment() -> Result<Self> {
        match env::var(V5_VALIDATION_MODE_ENV) {
            Ok(value) if value == "balanced" => Ok(Self::Balanced),
            Ok(value) if value == "strict" => Ok(Self::Strict),
            Ok(value) => {
                bail!("{V5_VALIDATION_MODE_ENV} must be exactly balanced or strict, not {value:?}")
            }
            Err(env::VarError::NotPresent) => Ok(Self::Balanced),
            Err(error) => Err(error).context(format!("read {V5_VALIDATION_MODE_ENV} as Unicode")),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Balanced => "balanced",
            Self::Strict => "strict",
        }
    }

    fn is_strict(self) -> bool {
        self == Self::Strict
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum V5ExecutionPath {
    Fresh,
    SealedRestart,
    ReceiptRecovery,
}

impl V5ExecutionPath {
    fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::SealedRestart => "sealed_restart",
            Self::ReceiptRecovery => "receipt_recovery",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum V5AuthenticationStrategy {
    FreshPublicationProof,
    ReceiptBoundContent,
    StrictDeepReplay,
}

impl V5AuthenticationStrategy {
    fn as_str(self) -> &'static str {
        match self {
            Self::FreshPublicationProof => "fresh_publication_proof",
            Self::ReceiptBoundContent => "receipt_bound_content",
            Self::StrictDeepReplay => "strict_deep_replay",
        }
    }
}

#[derive(Default, Clone, Copy)]
struct V5AdoptionBytes {
    public_artifact_bytes: u64,
    object_store_bytes: u64,
    authenticated_file_count: u64,
}

impl V5AdoptionBytes {
    fn total_bytes(self) -> Result<u64> {
        self.public_artifact_bytes
            .checked_add(self.object_store_bytes)
            .ok_or_else(|| anyhow!("native v5 adoption byte total overflows"))
    }
}

#[derive(Default, Clone, Copy)]
struct V5PhaseDurations {
    static_authority: Duration,
    construction: Duration,
    post_construction: Duration,
    staging: Duration,
    prepublication_validation: Duration,
    publication: Duration,
    output_authentication: Duration,
}

#[derive(Default, Clone, Copy)]
struct V5IoTelemetry {
    files_reopened: u64,
    bytes_read: u64,
    bytes_hashed: u64,
    bytes_written: u64,
    json_rows_parsed: u64,
}

#[derive(Default, Clone, Copy)]
struct V5ValidationPasses {
    constructor_replay: u64,
    redundant_fresh_replay: u64,
    publication_prepare_replay: u64,
    staged_semantic_replay: u64,
    staged_final_rehash: u64,
    receipt_bound_content_authentication: u64,
    deep_output_replay: u64,
}

impl V5IoTelemetry {
    fn merge(&mut self, other: Self) -> Result<()> {
        self.files_reopened = self
            .files_reopened
            .checked_add(other.files_reopened)
            .ok_or_else(|| anyhow!("native v5 telemetry file-open count overflows"))?;
        self.bytes_read = self
            .bytes_read
            .checked_add(other.bytes_read)
            .ok_or_else(|| anyhow!("native v5 telemetry read bytes overflow"))?;
        self.bytes_hashed = self
            .bytes_hashed
            .checked_add(other.bytes_hashed)
            .ok_or_else(|| anyhow!("native v5 telemetry hashed bytes overflow"))?;
        self.bytes_written = self
            .bytes_written
            .checked_add(other.bytes_written)
            .ok_or_else(|| anyhow!("native v5 telemetry written bytes overflow"))?;
        self.json_rows_parsed = self
            .json_rows_parsed
            .checked_add(other.json_rows_parsed)
            .ok_or_else(|| anyhow!("native v5 telemetry JSON-row count overflows"))?;
        Ok(())
    }
}

impl V5ValidationPasses {
    fn merge(&mut self, other: Self) -> Result<()> {
        macro_rules! add {
            ($field:ident) => {
                self.$field = self.$field.checked_add(other.$field).ok_or_else(|| {
                    anyhow!(concat!(
                        "native v5 validation-pass ",
                        stringify!($field),
                        " overflows"
                    ))
                })?;
            };
        }
        add!(constructor_replay);
        add!(redundant_fresh_replay);
        add!(publication_prepare_replay);
        add!(staged_semantic_replay);
        add!(staged_final_rehash);
        add!(receipt_bound_content_authentication);
        add!(deep_output_replay);
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct V5ExecutionMeasurements {
    path: V5ExecutionPath,
    mode: V5ValidationMode,
    strategy: V5AuthenticationStrategy,
    phases: V5PhaseDurations,
    io: V5IoTelemetry,
    passes: V5ValidationPasses,
    parallel_authentication_workers: u64,
    process_cpu: Option<Duration>,
    total: Duration,
}

fn record_v5_progress_sections(
    progress: &NativeProgressHandle,
    phases: V5PhaseDurations,
    io: V5IoTelemetry,
    parallel_authentication_workers: u64,
    requested_count: u64,
) {
    for (name, wall, work, bytes, files, workers) in [
        (
            "static_authority",
            phases.static_authority,
            None,
            None,
            None,
            Some(1),
        ),
        (
            "construction",
            phases.construction,
            Some(requested_count),
            None,
            None,
            None,
        ),
        (
            "post_construction",
            phases.post_construction,
            None,
            None,
            None,
            Some(1),
        ),
        (
            "staging",
            phases.staging,
            None,
            Some(io.bytes_written),
            None,
            None,
        ),
        (
            "prepublication_validation",
            phases.prepublication_validation,
            None,
            Some(io.bytes_hashed),
            Some(io.files_reopened),
            None,
        ),
        (
            "publication",
            phases.publication,
            None,
            Some(io.bytes_written),
            None,
            None,
        ),
        (
            "output_authentication",
            phases.output_authentication,
            None,
            Some(io.bytes_read),
            Some(io.files_reopened),
            (parallel_authentication_workers > 0).then_some(parallel_authentication_workers),
        ),
    ] {
        if wall.is_zero() {
            continue;
        }
        progress.record_section(NativeProgressSection {
            name: name.to_owned(),
            wall,
            completed_work_units: work,
            bytes_processed: bytes,
            files_processed: files,
            parallel_workers: workers,
            ..NativeProgressSection::default()
        });
    }
}

struct V5FreshExecution {
    bytes: V5AdoptionBytes,
    phases: V5PhaseDurations,
    passes: V5ValidationPasses,
}

struct V5Authentication {
    bytes: V5AdoptionBytes,
    elapsed: Duration,
    strategy: V5AuthenticationStrategy,
    parallel_workers: u64,
    io: V5IoTelemetry,
    passes: V5ValidationPasses,
}

struct V5RecoveredResult<T> {
    result: T,
    authentication: V5Authentication,
    publication: Duration,
}

#[derive(Debug)]
struct V5PublishDurations {
    validation: Duration,
    publication: Duration,
}

fn duration_milliseconds(duration: Duration) -> Result<u64> {
    u64::try_from(duration.as_millis()).context("convert native v5 phase milliseconds")
}

#[derive(Clone, Debug)]
struct V5InventoryFile {
    relative_path: String,
    file_sha256: String,
    byte_length: u64,
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("sha256:{:x}", digest.finalize())
}

fn v5_inventory_file(parent: &Path, relative: &str, label: &str) -> Result<Vec<u8>> {
    v5_safe_relative_output_path(relative, label)?;
    read_stable_existing_file(&parent.join(relative), label)
}

fn v5_inventory_file_identity(
    artifact: &Map<String, Value>,
    label: &str,
) -> Result<V5InventoryFile> {
    Ok(V5InventoryFile {
        relative_path: artifact
            .get("relativePath")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow!("{label} lacks relativePath"))?,
        file_sha256: artifact
            .get("fileSha256")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow!("{label} lacks fileSha256"))?,
        byte_length: artifact
            .get("byteLength")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("{label} lacks byteLength"))?,
    })
}

fn v5_read_authenticated_compact_file(
    output_root: &Path,
    artifact: &V5InventoryFile,
    label: &str,
) -> Result<Vec<u8>> {
    let raw = v5_inventory_file(output_root, &artifact.relative_path, label)?;
    if raw.len() as u64 != artifact.byte_length || sha256_bytes(&raw) != artifact.file_sha256 {
        bail!("{label} bytes differ from the authenticated inventory entry");
    }
    Ok(raw)
}

fn v5_read_authenticated_compact_document(
    output_root: &Path,
    artifact: &V5InventoryFile,
    label: &str,
) -> Result<Value> {
    let raw = v5_read_authenticated_compact_file(output_root, artifact, label)?;
    parse_v5_canonical_document_bytes(&raw, label)
}

fn open_v5_inventory_stream(
    output_root: &Path,
    artifact: &V5InventoryFile,
    label: &str,
) -> Result<fs::File> {
    v5_safe_relative_output_path(&artifact.relative_path, label)?;
    let path = output_root.join(&artifact.relative_path);
    let path = safe_existing_file(&path, label)?;
    let expected_identity = identity_from_path(&path)
        .with_context(|| format!("identify {label}: {}", path.display()))?;
    let file = fs::OpenOptions::new()
        .read(true)
        .open(&path)
        .with_context(|| format!("open {label}: {}", path.display()))?;
    let actual_identity = identity_from_file(&file)
        .with_context(|| format!("identify opened {label}: {}", path.display()))?;
    require_same_identity(actual_identity, expected_identity, label, &path)?;
    Ok(file)
}

fn v5_object_inventory_descriptor(inventory: &Map<String, Value>) -> Result<&Map<String, Value>> {
    let object_store = inventory
        .get("objectStore")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("native v5 output inventory lacks object-store closure"))?;
    object_store
        .get("inventory")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("native v5 object-store closure lacks inventory descriptor"))
}

/// Authenticate the full immutable-object closure from the canonical JSONL
/// sidecar.  Rows must be ordinal-contiguous and strictly ordered by semantic
/// SHA, so duplicate/reordered/foreign entries fail without retaining the
/// population-sized inventory in the outer result/receipt.
fn verify_v5_object_inventory_sidecar(
    output_root: &Path,
    inventory: &Map<String, Value>,
    observed: &mut V5AdoptionBytes,
    allowed: &mut BTreeSet<String>,
) -> Result<BTreeMap<String, V5InventoryFile>> {
    let descriptor = v5_object_inventory_descriptor(inventory)?;
    let relative = descriptor
        .get("relativePath")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native v5 object inventory descriptor lacks path"))?;
    if relative != V5_OUTPUT_INVENTORY_PATH
        || descriptor.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_PROPOSAL_OBJECT_INVENTORY_DESCRIPTOR_SCHEMA)
        || descriptor.get("rowSchemaVersion").and_then(Value::as_str)
            != Some(V5_PROPOSAL_OBJECT_INVENTORY_ROW_SCHEMA)
    {
        bail!("native v5 object inventory descriptor is incompatible");
    }
    if !allowed.insert(relative.to_owned()) {
        bail!("native v5 object inventory sidecar aliases another output artifact");
    }
    let expected_sha = descriptor
        .get("fileSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native v5 object inventory descriptor lacks file SHA-256"))?;
    let expected_length = descriptor
        .get("byteLength")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("native v5 object inventory descriptor lacks byte length"))?;
    let expected_count = descriptor
        .get("objectCount")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("native v5 object inventory descriptor lacks object count"))?;
    let expected_object_bytes = descriptor
        .get("objectByteCount")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("native v5 object inventory descriptor lacks object byte count"))?;
    let sidecar = safe_existing_file(
        &output_root.join(relative),
        "native v5 object inventory sidecar",
    )?;
    let mut reader = BufReader::new(
        fs::OpenOptions::new()
            .read(true)
            .open(&sidecar)
            .with_context(|| format!("open native v5 object inventory: {}", sidecar.display()))?,
    );
    let mut digest = Sha256::new();
    let mut encoded_bytes = 0_u64;
    let mut object_bytes = 0_u64;
    let mut ordinal = 0_u64;
    let mut prior_sha: Option<String> = None;
    let mut objects = BTreeMap::new();
    loop {
        let mut line = Vec::new();
        let read = reader
            .read_until(b'\n', &mut line)
            .context("stream native v5 object inventory row")?;
        if read == 0 {
            break;
        }
        if line.len() > 16 * 1024 || line.last() != Some(&b'\n') {
            bail!("native v5 object inventory row is not bounded canonical JSONL");
        }
        digest.update(&line);
        encoded_bytes = encoded_bytes
            .checked_add(line.len() as u64)
            .ok_or_else(|| anyhow!("native v5 object inventory byte count overflows"))?;
        let value: Value =
            serde_json::from_slice(&line).context("parse native v5 object inventory row")?;
        if canonical_json_line(&value)? != line {
            bail!("native v5 object inventory row is not canonical JSONL");
        }
        let row = value
            .as_object()
            .ok_or_else(|| anyhow!("native v5 object inventory row is not an object"))?;
        let exact = [
            "schemaVersion",
            "ordinal",
            "relativePath",
            "objectSha256",
            "fileSha256",
            "byteLength",
            "rowSha256",
        ];
        if row.len() != exact.len() || exact.iter().any(|key| !row.contains_key(*key)) {
            bail!("native v5 object inventory row fields are not exact");
        }
        if row.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_PROPOSAL_OBJECT_INVENTORY_ROW_SCHEMA)
            || row.get("ordinal").and_then(Value::as_u64) != Some(ordinal)
        {
            bail!("native v5 object inventory row ordinal/schema drifted");
        }
        let object_sha = row
            .get("objectSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 object inventory row lacks semantic SHA-256"))?;
        let file_sha = row
            .get("fileSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 object inventory row lacks file SHA-256"))?;
        let valid_sha = |value: &str| {
            value.len() == 71
                && value.starts_with("sha256:")
                && value[7..]
                    .bytes()
                    .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        };
        if !valid_sha(object_sha)
            || !valid_sha(file_sha)
            || prior_sha
                .as_deref()
                .is_some_and(|prior| object_sha <= prior)
        {
            bail!("native v5 object inventory identities are invalid or not strictly ordered");
        }
        prior_sha = Some(object_sha.to_owned());
        let expected_relative = format!("sha256/{}.json", &object_sha[7..]);
        if row.get("relativePath").and_then(Value::as_str) != Some(expected_relative.as_str()) {
            bail!("native v5 object inventory path/semantic identity drifted");
        }
        let byte_length = row
            .get("byteLength")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("native v5 object inventory row lacks byte length"))?;
        let supplied_row_sha = row
            .get("rowSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 object inventory row lacks self hash"))?;
        let mut material = row.clone();
        material.remove("rowSha256");
        if !valid_sha(supplied_row_sha)
            || canonical_sha256(&Value::Object(material))? != supplied_row_sha
        {
            bail!("native v5 object inventory row self hash drifted");
        }
        let full_relative = format!("v5-native/objects/{expected_relative}");
        if !allowed.insert(full_relative.clone()) {
            bail!("native v5 object inventory repeats or aliases an output path");
        }
        let object_artifact = Map::from_iter([
            (
                "relativePath".to_owned(),
                Value::String(full_relative.clone()),
            ),
            ("fileSha256".to_owned(), Value::String(file_sha.to_owned())),
            ("byteLength".to_owned(), Value::from(byte_length)),
            (
                "objectSha256".to_owned(),
                Value::String(object_sha.to_owned()),
            ),
        ]);
        verify_v5_inventory_artifact_streaming(output_root, &object_artifact, observed, true)?;
        object_bytes = object_bytes
            .checked_add(byte_length)
            .ok_or_else(|| anyhow!("native v5 object inventory object bytes overflow"))?;
        objects.insert(
            object_sha.to_owned(),
            V5InventoryFile {
                relative_path: full_relative,
                file_sha256: file_sha.to_owned(),
                byte_length,
            },
        );
        ordinal += 1;
    }
    let actual_sha = format!("sha256:{:x}", digest.finalize());
    if encoded_bytes != expected_length
        || actual_sha != expected_sha
        || ordinal != expected_count
        || object_bytes != expected_object_bytes
    {
        bail!("native v5 object inventory sidecar descriptor binding drifted");
    }
    observed.public_artifact_bytes = observed
        .public_artifact_bytes
        .checked_add(encoded_bytes)
        .ok_or_else(|| anyhow!("native v5 authenticated byte count overflows"))?;
    observed.authenticated_file_count += 1;
    Ok(objects)
}

fn v5_expected_adoption_bytes(immutable_result: &Value) -> Result<V5AdoptionBytes> {
    let result = immutable_result
        .as_object()
        .ok_or_else(|| anyhow!("native v5 immutable result is invalid"))?;
    let receipt = result
        .get("receipt")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("native v5 immutable result lacks receipt"))?;
    let inventory = receipt
        .get("outputInventory")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("native v5 immutable receipt lacks output inventory"))?;
    let artifacts = inventory
        .get("artifacts")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("native v5 immutable output inventory lacks artifacts"))?;
    let descriptor = v5_object_inventory_descriptor(inventory)?;
    let receipt_bytes = u64::try_from(canonical_json_line(&Value::Object(receipt.clone()))?.len())
        .context("convert native v5 receipt byte length")?;
    let public_artifact_bytes = artifacts
        .iter()
        .try_fold(receipt_bytes, |total, artifact| {
            total
                .checked_add(
                    artifact
                        .get("byteLength")
                        .and_then(Value::as_u64)
                        .ok_or_else(|| {
                            anyhow!("native v5 output inventory artifact lacks byte length")
                        })?,
                )
                .ok_or_else(|| anyhow!("native v5 public artifact byte total overflows"))
        })?;
    let public_artifact_bytes = public_artifact_bytes
        .checked_add(
            descriptor
                .get("byteLength")
                .and_then(Value::as_u64)
                .ok_or_else(|| anyhow!("native v5 object inventory lacks byte length"))?,
        )
        .ok_or_else(|| anyhow!("native v5 public artifact byte total overflows"))?;
    let object_store_bytes = descriptor
        .get("objectByteCount")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("native v5 object inventory lacks object byte count"))?;
    let object_count = descriptor
        .get("objectCount")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("native v5 object inventory lacks object count"))?;
    let authenticated_file_count = 2_u64
        .checked_add(artifacts.len() as u64)
        .and_then(|count| count.checked_add(object_count))
        .ok_or_else(|| anyhow!("native v5 authenticated file count overflows"))?;
    Ok(V5AdoptionBytes {
        public_artifact_bytes,
        object_store_bytes,
        authenticated_file_count,
    })
}

/// Recover exact object paths and file digests from the receipt-bound sidecar.
/// Balanced adoption hashes the sidecar itself but deliberately does not repeat
/// canonical reserialization or semantic object replay that was already proven
/// before the receipt-last publication barrier. Strict mode retains that replay.
fn read_v5_receipt_bound_object_inventory(
    output_root: &Path,
    inventory: &Map<String, Value>,
    allowed: &mut BTreeSet<String>,
) -> Result<(Vec<V5InventoryFile>, u64)> {
    let descriptor = v5_object_inventory_descriptor(inventory)?;
    let relative = descriptor
        .get("relativePath")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native v5 object inventory descriptor lacks path"))?;
    if relative != V5_OUTPUT_INVENTORY_PATH
        || descriptor.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_PROPOSAL_OBJECT_INVENTORY_DESCRIPTOR_SCHEMA)
        || descriptor.get("rowSchemaVersion").and_then(Value::as_str)
            != Some(V5_PROPOSAL_OBJECT_INVENTORY_ROW_SCHEMA)
    {
        bail!("native v5 receipt-bound object inventory descriptor is incompatible");
    }
    if !allowed.insert(relative.to_owned()) {
        bail!("native v5 object inventory sidecar aliases another output artifact");
    }
    let expected_sha = descriptor
        .get("fileSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native v5 object inventory descriptor lacks file SHA-256"))?;
    let expected_length = descriptor
        .get("byteLength")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("native v5 object inventory descriptor lacks byte length"))?;
    let expected_count = descriptor
        .get("objectCount")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("native v5 object inventory descriptor lacks object count"))?;
    let expected_object_bytes = descriptor
        .get("objectByteCount")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("native v5 object inventory descriptor lacks object byte count"))?;
    let sidecar = safe_existing_file(
        &output_root.join(relative),
        "native v5 receipt-bound object inventory sidecar",
    )?;
    let mut reader = BufReader::new(
        fs::OpenOptions::new()
            .read(true)
            .open(&sidecar)
            .with_context(|| format!("open native v5 object inventory: {}", sidecar.display()))?,
    );
    let mut digest = Sha256::new();
    let mut encoded_bytes = 0_u64;
    let mut object_bytes = 0_u64;
    let mut ordinal = 0_u64;
    let mut prior_sha: Option<String> = None;
    let mut objects = Vec::new();
    loop {
        let mut line = Vec::new();
        let read = reader
            .read_until(b'\n', &mut line)
            .context("stream receipt-bound native v5 object inventory row")?;
        if read == 0 {
            break;
        }
        if line.len() > 16 * 1024 || line.last() != Some(&b'\n') {
            bail!("native v5 receipt-bound object inventory row is not bounded JSONL");
        }
        digest.update(&line);
        encoded_bytes = encoded_bytes
            .checked_add(line.len() as u64)
            .ok_or_else(|| anyhow!("native v5 object inventory byte count overflows"))?;
        let value: Value = serde_json::from_slice(&line)
            .context("parse receipt-bound native v5 object inventory row")?;
        let row = value
            .as_object()
            .ok_or_else(|| anyhow!("native v5 object inventory row is not an object"))?;
        let exact = [
            "schemaVersion",
            "ordinal",
            "relativePath",
            "objectSha256",
            "fileSha256",
            "byteLength",
            "rowSha256",
        ];
        if row.len() != exact.len()
            || exact.iter().any(|key| !row.contains_key(*key))
            || row.get("schemaVersion").and_then(Value::as_str)
                != Some(V5_PROPOSAL_OBJECT_INVENTORY_ROW_SCHEMA)
            || row.get("ordinal").and_then(Value::as_u64) != Some(ordinal)
        {
            bail!("native v5 receipt-bound object inventory row fields/ordinal drifted");
        }
        let object_sha = row
            .get("objectSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 object inventory row lacks object SHA-256"))?;
        let file_sha = row
            .get("fileSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 object inventory row lacks file SHA-256"))?;
        let valid_sha = |value: &str| {
            value.len() == 71
                && value.starts_with("sha256:")
                && value[7..]
                    .bytes()
                    .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        };
        if !valid_sha(object_sha)
            || !valid_sha(file_sha)
            || prior_sha
                .as_deref()
                .is_some_and(|prior| object_sha <= prior)
        {
            bail!("native v5 receipt-bound object inventory identities are invalid");
        }
        let relative = format!("sha256/{}.json", &object_sha[7..]);
        if row.get("relativePath").and_then(Value::as_str) != Some(relative.as_str()) {
            bail!("native v5 receipt-bound object inventory path drifted");
        }
        let byte_length = row
            .get("byteLength")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("native v5 object inventory row lacks byte length"))?;
        object_bytes = object_bytes
            .checked_add(byte_length)
            .ok_or_else(|| anyhow!("native v5 object inventory object bytes overflow"))?;
        let full_relative = format!("v5-native/objects/{relative}");
        if !allowed.insert(full_relative.clone()) {
            bail!("native v5 receipt-bound object inventory repeats an output path");
        }
        objects.push(V5InventoryFile {
            relative_path: full_relative,
            file_sha256: file_sha.to_owned(),
            byte_length,
        });
        prior_sha = Some(object_sha.to_owned());
        ordinal += 1;
    }
    let actual_sha = format!("sha256:{:x}", digest.finalize());
    if encoded_bytes != expected_length
        || actual_sha != expected_sha
        || ordinal != expected_count
        || object_bytes != expected_object_bytes
    {
        bail!("native v5 receipt-bound object inventory descriptor binding drifted");
    }
    Ok((objects, ordinal))
}

fn authenticate_v5_inventory_files_parallel(
    output_root: &Path,
    files: &[(V5InventoryFile, bool)],
    thread_cap: u64,
    progress: Option<&NativeProgressHandle>,
) -> Result<u64> {
    if files.is_empty() {
        return Ok(0);
    }
    let workers = usize::min(thread_cap as usize, files.len()).max(1);
    let chunk_size = files.len().div_ceil(workers);
    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::new();
        for chunk in files.chunks(chunk_size) {
            handles.push(scope.spawn(move || -> Result<()> {
                if let Some(progress) = progress {
                    progress.worker_started();
                }
                let result = (|| -> Result<()> {
                    for (file, object_store) in chunk {
                        v5_safe_relative_output_path(
                            &file.relative_path,
                            "native v5 receipt-bound output artifact",
                        )?;
                        require_v5_file_digest(
                            &output_root.join(&file.relative_path),
                            file.byte_length,
                            &file.file_sha256,
                            if *object_store {
                                "native v5 receipt-bound object-store artifact"
                            } else {
                                "native v5 receipt-bound public artifact"
                            },
                        )?;
                        if let Some(progress) = progress {
                            progress.advance_completed(1);
                            progress.add_files(1);
                            progress.add_bytes(file.byte_length);
                        }
                    }
                    Ok(())
                })();
                if let Some(progress) = progress {
                    progress.worker_finished();
                }
                result
            }));
        }
        for handle in handles {
            handle
                .join()
                .map_err(|_| anyhow!("native v5 receipt-bound authentication worker panicked"))??;
        }
        Ok(())
    })?;
    Ok(workers as u64)
}

fn authenticate_v5_receipt_bound_content(
    output_root: &Path,
    manifest: &V5ProposalManifest,
    immutable_result: &Value,
    progress: Option<&NativeProgressHandle>,
) -> Result<V5Authentication> {
    let started = Instant::now();
    let result = immutable_result
        .as_object()
        .ok_or_else(|| anyhow!("native v5 immutable result is invalid"))?;
    let receipt = result
        .get("receipt")
        .cloned()
        .ok_or_else(|| anyhow!("native v5 immutable result lacks receipt"))?;
    let persisted = read_optional_v5_canonical_document(
        &output_root.join(V5_OUTPUT_RECEIPT_PATH),
        "native v5 receipt-bound output receipt",
    )?
    .ok_or_else(|| anyhow!("native v5 receipt-bound output receipt is missing"))?;
    if persisted != receipt {
        bail!("native v5 persisted output receipt differs from the immutable result");
    }
    let receipt_fields = receipt
        .as_object()
        .ok_or_else(|| anyhow!("native v5 immutable receipt is invalid"))?;
    let inventory = receipt_fields
        .get("outputInventory")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("native v5 immutable receipt lacks output inventory"))?;
    let artifacts = inventory
        .get("artifacts")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("native v5 output inventory lacks artifacts"))?;
    let mut allowed = BTreeSet::from([V5_OUTPUT_RECEIPT_PATH.to_owned()]);
    let mut files = Vec::<(V5InventoryFile, bool)>::new();
    for artifact in artifacts {
        let artifact = artifact
            .as_object()
            .ok_or_else(|| anyhow!("native v5 output inventory artifact is invalid"))?;
        let file = v5_inventory_file_identity(artifact, "native v5 output inventory artifact")?;
        v5_safe_relative_output_path(&file.relative_path, "native v5 output inventory artifact")?;
        if !allowed.insert(file.relative_path.clone()) {
            bail!("native v5 output inventory repeats an artifact path");
        }
        files.push((file, false));
    }
    let (objects, object_rows) =
        read_v5_receipt_bound_object_inventory(output_root, inventory, &mut allowed)?;
    files.extend(objects.into_iter().map(|file| (file, true)));
    require_v5_owned_namespace_file_set(output_root, &allowed)
        .context("verify receipt-bound native v5 owned output namespaces before hashing")?;
    if let Some(progress) = progress {
        progress.begin_phase(
            "output_authentication",
            "hash_receipt_bound_files",
            "file",
            Some(files.len() as u64),
            false,
            Some(manifest.thread_cap),
            None,
        );
    }
    let workers = authenticate_v5_inventory_files_parallel(
        output_root,
        &files,
        manifest.thread_cap,
        progress,
    )?;
    require_v5_owned_namespace_file_set(output_root, &allowed)
        .context("verify receipt-bound native v5 owned output namespaces after hashing")?;
    let bytes = v5_expected_adoption_bytes(immutable_result)?;
    let receipt_bytes = u64::try_from(canonical_json_line(&receipt)?.len())
        .context("convert native v5 receipt byte length")?;
    let hashed_files_bytes = files.iter().try_fold(0_u64, |total, (file, _)| {
        total
            .checked_add(file.byte_length)
            .ok_or_else(|| anyhow!("native v5 receipt-bound hashed byte total overflows"))
    })?;
    let descriptor_bytes = v5_object_inventory_descriptor(inventory)?
        .get("byteLength")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("native v5 object inventory lacks byte length"))?;
    let io = V5IoTelemetry {
        files_reopened: files.len() as u64 + 2,
        bytes_read: receipt_bytes
            .checked_add(descriptor_bytes)
            .and_then(|total| total.checked_add(hashed_files_bytes))
            .ok_or_else(|| anyhow!("native v5 receipt-bound read byte total overflows"))?,
        bytes_hashed: descriptor_bytes
            .checked_add(hashed_files_bytes)
            .ok_or_else(|| anyhow!("native v5 receipt-bound hash byte total overflows"))?,
        bytes_written: 0,
        json_rows_parsed: object_rows,
    };
    Ok(V5Authentication {
        bytes,
        elapsed: started.elapsed(),
        strategy: V5AuthenticationStrategy::ReceiptBoundContent,
        parallel_workers: workers,
        io,
        passes: V5ValidationPasses {
            receipt_bound_content_authentication: 1,
            ..V5ValidationPasses::default()
        },
    })
}

fn authenticate_v5_proposal_output(
    output_root: &Path,
    manifest: &V5ProposalManifest,
    result: &V5ProposalResult,
    mode: V5ValidationMode,
    progress: Option<&NativeProgressHandle>,
) -> Result<V5Authentication> {
    if !mode.is_strict() {
        return authenticate_v5_receipt_bound_content(
            output_root,
            manifest,
            &result.value,
            progress,
        );
    }
    if let Some(progress) = progress {
        progress.begin_phase(
            "output_authentication",
            "strict_deep_replay",
            "artifact",
            None,
            false,
            Some(1),
            Some("strict_replay_work_total_not_exposed"),
        );
    }
    let started = Instant::now();
    let bytes = verify_v5_output_inventory(output_root, manifest, result)?;
    let total = bytes.total_bytes()?;
    Ok(V5Authentication {
        bytes,
        elapsed: started.elapsed(),
        strategy: V5AuthenticationStrategy::StrictDeepReplay,
        parallel_workers: 1,
        io: V5IoTelemetry {
            files_reopened: bytes.authenticated_file_count,
            bytes_read: total,
            bytes_hashed: total,
            bytes_written: 0,
            json_rows_parsed: 0,
        },
        passes: V5ValidationPasses {
            deep_output_replay: 1,
            ..V5ValidationPasses::default()
        },
    })
}

fn authenticate_v5_evolved_proposal_output(
    output_root: &Path,
    manifest: &V5ProposalManifest,
    result: &V5EvolvedProposalResult,
    mode: V5ValidationMode,
    progress: Option<&NativeProgressHandle>,
) -> Result<V5Authentication> {
    if !mode.is_strict() {
        return authenticate_v5_receipt_bound_content(
            output_root,
            manifest,
            &result.value,
            progress,
        );
    }
    if let Some(progress) = progress {
        progress.begin_phase(
            "output_authentication",
            "strict_deep_replay",
            "artifact",
            None,
            false,
            Some(1),
            Some("strict_replay_work_total_not_exposed"),
        );
    }
    let started = Instant::now();
    let bytes = verify_v5_evolved_output_inventory(output_root, manifest, result)?;
    let total = bytes.total_bytes()?;
    Ok(V5Authentication {
        bytes,
        elapsed: started.elapsed(),
        strategy: V5AuthenticationStrategy::StrictDeepReplay,
        parallel_workers: 1,
        io: V5IoTelemetry {
            files_reopened: bytes.authenticated_file_count,
            bytes_read: total,
            bytes_hashed: total,
            bytes_written: 0,
            json_rows_parsed: 0,
        },
        passes: V5ValidationPasses {
            deep_output_replay: 1,
            ..V5ValidationPasses::default()
        },
    })
}

fn v5_safe_relative_output_path(relative: &str, label: &str) -> Result<()> {
    if relative.contains('\\')
        || relative.contains(':')
        || relative.starts_with('/')
        || relative
            .split('/')
            .any(|component| matches!(component, "" | "." | ".."))
    {
        bail!("{label} has an unsafe relative path")
    }
    Ok(())
}

#[cfg(test)]
fn verify_v5_inventory_artifact(
    parent: &Path,
    artifact: &Map<String, Value>,
    bytes: &mut V5AdoptionBytes,
    object_store: bool,
) -> Result<Vec<u8>> {
    let relative = artifact
        .get("relativePath")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native v5 output inventory artifact lacks relativePath"))?;
    let expected_file_sha = artifact
        .get("fileSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native v5 output inventory artifact lacks fileSha256"))?;
    let expected_length = artifact
        .get("byteLength")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("native v5 output inventory artifact lacks byteLength"))?;
    let payload = v5_inventory_file(parent, relative, "native v5 output inventory artifact")?;
    if payload.len() as u64 != expected_length || sha256_bytes(&payload) != expected_file_sha {
        bail!("native v5 output inventory artifact bytes drifted: {relative}");
    }
    if object_store {
        bytes.object_store_bytes = bytes
            .object_store_bytes
            .checked_add(payload.len() as u64)
            .ok_or_else(|| anyhow!("native v5 object-store byte counter overflows"))?;
    } else {
        bytes.public_artifact_bytes = bytes
            .public_artifact_bytes
            .checked_add(payload.len() as u64)
            .ok_or_else(|| anyhow!("native v5 public artifact byte counter overflows"))?;
    }
    bytes.authenticated_file_count = bytes
        .authenticated_file_count
        .checked_add(1)
        .ok_or_else(|| anyhow!("native v5 authenticated file counter overflows"))?;
    Ok(payload)
}

/// Authenticate one inventory file without retaining its payload.  This is
/// the production adoption path for every artifact, including the potentially
/// multi-gigabyte `population.json`; callers may separately reopen only the
/// compact typed documents that a replay gate actually needs.
fn verify_v5_inventory_artifact_streaming(
    parent: &Path,
    artifact: &Map<String, Value>,
    bytes: &mut V5AdoptionBytes,
    object_store: bool,
) -> Result<V5StableFileIdentity> {
    let relative = artifact
        .get("relativePath")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native v5 output inventory artifact lacks relativePath"))?;
    let expected_file_sha = artifact
        .get("fileSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native v5 output inventory artifact lacks fileSha256"))?;
    let expected_length = artifact
        .get("byteLength")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("native v5 output inventory artifact lacks byteLength"))?;
    v5_safe_relative_output_path(relative, "native v5 output inventory artifact")?;
    let identity = require_v5_file_digest(
        &parent.join(relative),
        expected_length,
        expected_file_sha,
        "native v5 output inventory artifact",
    )?;
    let counter = if object_store {
        &mut bytes.object_store_bytes
    } else {
        &mut bytes.public_artifact_bytes
    };
    *counter = counter
        .checked_add(expected_length)
        .ok_or_else(|| anyhow!("native v5 authenticated byte counter overflows"))?;
    bytes.authenticated_file_count = bytes
        .authenticated_file_count
        .checked_add(1)
        .ok_or_else(|| anyhow!("native v5 authenticated file counter overflows"))?;
    Ok(identity)
}

fn enumerate_v5_real_files(
    root: &Path,
    relative: &Path,
    files: &mut BTreeSet<String>,
) -> Result<()> {
    let directory = root.join(relative);
    let metadata = fs::symlink_metadata(&directory).with_context(|| {
        format!(
            "inspect native v5 output directory: {}",
            directory.display()
        )
    })?;
    if is_link_or_reparse(&metadata) || !metadata.is_dir() {
        bail!(
            "native v5 output tree contains an unsafe directory: {}",
            directory.display()
        );
    }
    for entry in fs::read_dir(&directory)
        .with_context(|| format!("list native v5 output directory: {}", directory.display()))?
    {
        let entry = entry
            .with_context(|| format!("read native v5 output entry: {}", directory.display()))?;
        let name = entry.file_name();
        let child_relative = relative.join(name);
        let child = root.join(&child_relative);
        let metadata = fs::symlink_metadata(&child)
            .with_context(|| format!("inspect native v5 output entry: {}", child.display()))?;
        if is_link_or_reparse(&metadata) {
            bail!(
                "native v5 output tree contains a symlink/reparse point: {}",
                child.display()
            );
        }
        if metadata.is_dir() {
            enumerate_v5_real_files(root, &child_relative, files)?;
        } else if metadata.is_file() {
            let normalized = child_relative.to_string_lossy().replace('\\', "/");
            if !files.insert(normalized) {
                bail!("native v5 output tree contains duplicate file names");
            }
        } else {
            bail!(
                "native v5 output tree contains a non-regular entry: {}",
                child.display()
            );
        }
    }
    Ok(())
}

fn require_v5_exact_file_set(parent: &Path, allowed: &BTreeSet<String>) -> Result<()> {
    let mut observed_files = BTreeSet::new();
    enumerate_v5_real_files(parent, Path::new(""), &mut observed_files)?;
    if &observed_files != allowed {
        bail!("native v5 output tree is missing or contains unsealed files");
    }
    Ok(())
}

/// Verify only namespaces whose contents are owned by the one native v5
/// transaction.  A proposal directory also contains supervisor configuration,
/// telemetry, and state; treating its whole root as Rust-owned would either
/// reject legitimate state or invite a later Python copy step.  Root-level
/// worker artifacts are individually sealed by the inventory, while every
/// file below these native-owned namespaces must be named by it.
fn require_v5_owned_namespace_file_set(
    output_root: &Path,
    allowed: &BTreeSet<String>,
) -> Result<()> {
    const OWNED_NAMESPACES: [&str; 3] = ["v5-native", "g0-bootstrap", "internal/v5-proposal"];

    let mut expected = BTreeSet::new();
    let mut observed = BTreeSet::new();
    for namespace in OWNED_NAMESPACES {
        let prefix = format!("{namespace}/");
        let namespace_expected = allowed
            .iter()
            .filter(|path| path.starts_with(&prefix))
            .cloned()
            .collect::<BTreeSet<_>>();
        expected.extend(namespace_expected.iter().cloned());

        let directory = output_root.join(namespace);
        match fs::symlink_metadata(&directory) {
            Ok(metadata) => {
                if is_link_or_reparse(&metadata) || !metadata.is_dir() {
                    bail!(
                        "native v5 owned output namespace is unsafe: {}",
                        directory.display()
                    );
                }
                enumerate_v5_real_files(output_root, Path::new(namespace), &mut observed)?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if !namespace_expected.is_empty() {
                    bail!(
                        "native v5 owned output namespace is missing: {}",
                        directory.display()
                    );
                }
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "inspect native v5 owned output namespace: {}",
                        directory.display()
                    )
                });
            }
        }
    }
    if observed != expected {
        bail!("native v5 owned output namespaces are missing or contain unsealed files");
    }
    Ok(())
}

fn verify_v5_static_authorities(
    invocation_root: &Path,
    manifest: &V5ProposalManifest,
    manifest_bytes: &[u8],
) -> Result<()> {
    let execution = manifest
        .execution_authority
        .as_object()
        .ok_or_else(|| anyhow!("native v5 execution authority is invalid"))?;
    let batch = execution
        .get("nativeBatchAuthority")
        .ok_or_else(|| anyhow!("native v5 execution authority lacks batch authority"))?;
    let expected_binary_sha = batch
        .get("executableSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native v5 batch authority lacks executable SHA-256"))?;
    let current = std::env::current_exe().context("discover native v5 batch executable")?;
    if sha256_file(&current, "native v5 batch executable")? != expected_binary_sha {
        bail!("native v5 batch executable authority drifted");
    }
    for (relative, expected, label) in [
        (
            "authority.json",
            batch,
            "native v5 batch authority artifact",
        ),
        (
            "frozen-authority.json",
            &manifest.frozen_authority,
            "native v5 frozen authority artifact",
        ),
    ] {
        let raw = v5_inventory_file(invocation_root, relative, label)?;
        let value: Value =
            serde_json::from_slice(&raw).with_context(|| format!("parse {label}"))?;
        if raw != canonical_json_line(&value)? || &value != expected {
            bail!("{label} differs from the sealed manifest authority");
        }
    }
    let persisted_manifest = v5_inventory_file(
        invocation_root,
        "manifest.json",
        "native v5 manifest artifact",
    )?;
    if persisted_manifest != manifest_bytes {
        bail!("native v5 manifest artifact changed after dispatch");
    }
    let shared = V5SharedConstructionAuthority::from_shared_object(&manifest.frozen_authority)
        .context("open complete sealed v5 shared construction authority")?;
    if shared.shared_authority_sha256 != manifest.expected_authority_sha256 {
        bail!("native v5 shared construction authority binding drifted");
    }
    Ok(())
}

/// Read the three compact v5 journals exactly as they were sealed.  JSONL is
/// intentionally not treated as whitespace-tolerant transport: rows are
/// independently canonical JSON values, every file has one final LF, and
/// there are no blank/CRLF rows.  This lets restart authenticate the compact
/// transaction without expanding a legacy-rich candidate payload.
fn parse_v5_canonical_jsonl(raw: &[u8], label: &str) -> Result<Vec<Value>> {
    if raw.is_empty() || raw.last() != Some(&b'\n') || raw.contains(&b'\r') {
        bail!("{label} must be non-empty canonical JSONL with exactly LF line endings");
    }
    let body = &raw[..raw.len() - 1];
    let mut rows = Vec::new();
    for (index, line) in body.split(|byte| *byte == b'\n').enumerate() {
        if line.is_empty() {
            bail!("{label} contains a blank JSONL row");
        }
        let value: Value = serde_json::from_slice(line)
            .with_context(|| format!("parse {label} JSONL row {index}"))?;
        let canonical = canonical_json_line(&value)
            .with_context(|| format!("canonicalize {label} JSONL row {index}"))?;
        if canonical.len() != line.len() + 1
            || canonical.last() != Some(&b'\n')
            || canonical[..canonical.len() - 1] != *line
        {
            bail!("{label} JSONL row {index} is not canonical");
        }
        rows.push(value);
    }
    if rows.is_empty() {
        bail!("{label} must contain at least one JSONL row");
    }
    Ok(rows)
}

/// Resolve every compact outcome audit named by a durable attempt journal.
/// Object-store inventory authentication happens before this function; this
/// adds typed semantic admission of the evidence objects themselves.
fn verify_v5_attempt_outcome_audit_replay(
    journal: &V5AttemptJournal,
    objects: &BTreeMap<String, Vec<u8>>,
) -> Result<()> {
    let mut outcome_audits = Vec::with_capacity(journal.attempts.len());
    for attempt in &journal.attempts {
        let raw = objects.get(&attempt.outcome_audit_sha256).ok_or_else(|| {
            anyhow!(
                "native v5 object store lacks outcome audit {}",
                attempt.outcome_audit_sha256
            )
        })?;
        let value: Value = serde_json::from_slice(raw).with_context(|| {
            format!(
                "parse native v5 outcome audit {}",
                attempt.outcome_audit_sha256
            )
        })?;
        if raw != &canonical_json_line(&value)? {
            bail!(
                "native v5 outcome audit is not canonical: {}",
                attempt.outcome_audit_sha256
            );
        }
        outcome_audits.push(V5AttemptOutcomeAudit::from_value(&value).with_context(|| {
            format!(
                "parse native v5 outcome audit {}",
                attempt.outcome_audit_sha256
            )
        })?);
    }
    journal
        .verify_outcome_audit_replay(outcome_audits)
        .context("replay native v5 attempt outcome audits")
}

/// Re-admit the compact, typed journals that now have a stable kernel ABI.
/// The identity ledger/object reference mapping and the public G0/evaluation
/// replay remain deliberately outside this function until their corresponding
/// typed kernel APIs land.  In particular, this never asks Python to recreate
/// programs, profiles, or legacy-rich candidate records.
fn verify_v5_compact_journal_replay(
    manifest: &V5ProposalManifest,
    result: &V5ProposalResult,
    artifacts: &BTreeMap<String, Vec<u8>>,
    objects: &BTreeMap<String, Vec<u8>>,
) -> Result<()> {
    let result_fields = result
        .value
        .as_object()
        .ok_or_else(|| anyhow!("native v5 immutable result is invalid"))?;
    let result_count = |name: &str| {
        result_fields
            .get(name)
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("native v5 immutable result lacks {name}"))
    };
    let result_sha = |name: &str| {
        result_fields
            .get(name)
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 immutable result lacks {name}"))
    };
    let artifact = |kind: &str| {
        artifacts
            .get(kind)
            .map(Vec::as_slice)
            .ok_or_else(|| anyhow!("native v5 output inventory lacks {kind} bytes"))
    };

    let attempt_rows =
        parse_v5_canonical_jsonl(artifact("attemptRows")?, "native v5 attempt rows")?;
    let journal = V5AttemptJournal::from_rows(
        manifest.generation_index,
        &manifest.generation_config_sha256,
        &manifest.expected_authority_sha256,
        &attempt_rows,
    )
    .context("replay native v5 contiguous attempt journal")?;
    if journal.attempts.len() as u64 != result_count("attemptCount")?
        || journal.attempt_journal_sha256()? != result_sha("attemptJournalSha256")?
    {
        bail!("native v5 attempt journal/result binding drifted");
    }
    // The JSONL is the durable attempt stream, but its companion root is a
    // separate public artifact.  Re-admit it too: accepting only a matching
    // result hash would allow an independently self-rehashed root to outlive
    // a changed/reordered stream.  This parses compact facts only; it never
    // reconstructs a candidate or expands a legacy-rich record.
    let attempt_root_raw = artifact("attemptJournal")?;
    let attempt_root: Value =
        serde_json::from_slice(attempt_root_raw).context("parse native v5 attempt-journal root")?;
    if attempt_root_raw != canonical_json_line(&attempt_root)? {
        bail!("native v5 attempt-journal root is not canonical");
    }
    let persisted_journal = V5AttemptJournal::from_value(&attempt_root)
        .context("parse native v5 attempt-journal root")?;
    if persisted_journal != journal {
        bail!("native v5 attempt-journal root differs from its JSONL rows");
    }
    // Every attempt names a separate, content-addressed outcome audit.  These
    // are intentionally compact evidence objects, not candidate payloads: a
    // rejection/no-op/duplicate must survive restart with the exact reason,
    // plan stage, and ledger effect that caused the retry.  Other static
    // program/profile objects stay opaque until their typed mapping ABI lands.
    // The object store can also carry the typed G0 schedule/state receipt
    // and other compact transaction facts.  Outcome-audit replay must remain
    // exact for the audit subset named by attempts, without mistaking those
    // additional typed objects for an audit injection.
    let audit_object_ids = journal
        .attempts
        .iter()
        .map(|attempt| attempt.outcome_audit_sha256.clone())
        .collect::<BTreeSet<_>>();
    let audit_objects = objects
        .iter()
        .filter(|(object_sha, _)| audit_object_ids.contains(*object_sha))
        .map(|(object_sha, bytes)| (object_sha.clone(), bytes.clone()))
        .collect::<BTreeMap<_, _>>();
    verify_v5_attempt_outcome_audit_replay(&journal, &audit_objects)?;

    let accepted_rows = parse_v5_canonical_jsonl(
        artifact("compactJournal")?,
        "native v5 compact accepted-record journal",
    )?;
    // Preserve the JSONL order for the kernel's typed replay.  The map is
    // only the selected-projection lookup index; sorting it would erase the
    // accepted-slot sequence that binds attempts to birth ordinals.
    let mut records = BTreeMap::<String, V5CompactAcceptedRecord>::new();
    let mut ordered_records = Vec::<V5CompactAcceptedRecord>::new();
    for value in &accepted_rows {
        let record = V5CompactAcceptedRecord::from_value(value)
            .context("parse native v5 compact accepted record")?;
        if record.generation_index != manifest.generation_index
            || record.shared_authority_sha256 != manifest.expected_authority_sha256
        {
            bail!("native v5 compact accepted record authority/generation drifted");
        }
        let record_sha = record.record_sha256()?;
        if records.insert(record_sha, record.clone()).is_some() {
            bail!("native v5 compact accepted journal repeats a record");
        }
        ordered_records.push(record);
    }
    if records.len() as u64 != result_count("acceptedRecordCount")? {
        bail!("native v5 compact accepted journal count drifted");
    }
    journal
        .verify_accepted_record_replay(ordered_records)
        .context("replay native v5 accepted-record references")?;

    let selected_rows = parse_v5_canonical_jsonl(
        artifact("selectedProjectionIndex")?,
        "native v5 selected-projection journal",
    )?;
    if selected_rows.len() as u64 != result_count("evaluationPopulationSize")? {
        bail!("native v5 selected-projection journal count drifted");
    }
    let mut selected_records = BTreeSet::new();
    let mut selected_candidates = BTreeSet::new();
    for value in &selected_rows {
        let projection = V5SelectedProjection::from_value(value)
            .context("parse native v5 selected projection")?;
        if projection.generation_index != manifest.generation_index
            || projection.shared_authority_sha256 != manifest.expected_authority_sha256
        {
            bail!("native v5 selected projection authority/generation drifted");
        }
        let record = records.get(&projection.record_sha256).ok_or_else(|| {
            anyhow!("native v5 selected projection references an unaccepted record")
        })?;
        projection
            .verify_against_record(record)
            .context("bind native v5 selected projection to compact record")?;
        if !selected_records.insert(projection.record_sha256.clone())
            || !selected_candidates.insert(projection.candidate_identity_sha256.clone())
        {
            bail!("native v5 selected-projection journal repeats a selected candidate");
        }
    }

    Ok(())
}

/// Compact-only replay material reconstructed from an authenticated durable
/// output tree.  This deliberately contains neither public population bytes
/// nor any rich candidate: those stay file-streamed through qd-kernel's
/// receipt verifier after this compact gate succeeds.
struct V5TypedReplay {
    transaction: V5G0TransactionResult,
    publication_receipt_value: Value,
}

fn v5_required_string<'a>(value: &'a Value, field: &str, label: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("{label} lacks a nonempty {field}"))
}

fn v5_public_document<'a>(documents: &'a BTreeMap<String, Value>, kind: &str) -> Result<&'a Value> {
    documents
        .get(kind)
        .ok_or_else(|| anyhow!("native v5 typed replay lacks public {kind} document"))
}

fn v5_object_value<'a>(
    objects: &'a BTreeMap<String, Value>,
    object_sha256: &str,
    label: &str,
) -> Result<&'a Value> {
    objects.get(object_sha256).ok_or_else(|| {
        anyhow!("native v5 typed replay lacks immutable {label} object {object_sha256}")
    })
}

fn v5_bind_public_document_to_object(
    public: &Value,
    objects: &BTreeMap<String, Value>,
    object_sha256: &str,
    label: &str,
) -> Result<()> {
    let object = v5_object_value(objects, object_sha256, label)?;
    if object != public {
        bail!("native v5 public {label} differs from its immutable object");
    }
    Ok(())
}

fn v5_object_with_self_hash_field<'a>(
    objects: &'a BTreeMap<String, Value>,
    self_hash_field: &str,
    label: &str,
) -> Result<(&'a String, &'a Value)> {
    let mut matches = objects.iter().filter(|(object_sha256, value)| {
        value.get(self_hash_field).and_then(Value::as_str) == Some(object_sha256.as_str())
    });
    let first = matches
        .next()
        .ok_or_else(|| anyhow!("native v5 typed replay lacks immutable {label} object"))?;
    if matches.next().is_some() {
        bail!("native v5 typed replay has multiple immutable {label} objects");
    }
    Ok(first)
}

fn v5_reconstruct_typed_transaction(
    manifest: &V5ProposalManifest,
    result: &V5ProposalResult,
    request: &V5G0TransactionRequest,
    public_documents: &BTreeMap<String, Value>,
    attempt_rows: &[Value],
    accepted_records: &[Value],
    selected_projections: &[Value],
    objects: &BTreeMap<String, Value>,
) -> Result<V5TypedReplay> {
    let result_fields = result
        .value
        .as_object()
        .ok_or_else(|| anyhow!("native v5 immutable result is invalid"))?;
    let result_sha = |field: &str| {
        result_fields
            .get(field)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("native v5 immutable result lacks {field}"))
    };
    let attempt_journal = v5_public_document(public_documents, "attemptJournal")?;
    let attempt_journal_sha256 = result_sha("attemptJournalSha256")?;
    v5_bind_public_document_to_object(
        attempt_journal,
        objects,
        attempt_journal_sha256,
        "attempt journal",
    )?;
    let compact_journal = v5_object_value(
        objects,
        result_sha("compactJournalSha256")?,
        "compact accepted journal",
    )?;
    let identity_ledger = v5_public_document(public_documents, "identityLedger")?;
    v5_bind_public_document_to_object(
        identity_ledger,
        objects,
        result_sha("identityLedgerSha256")?,
        "identity ledger",
    )?;
    let selected_projection_index = v5_object_value(
        objects,
        result_sha("selectedProjectionIndexSha256")?,
        "selected projection index",
    )?;
    let (schedule_state_sha256, schedule_state_receipt) = v5_object_with_self_hash_field(
        objects,
        "scheduleStateReceiptSha256",
        "schedule/state receipt",
    )?;
    if schedule_state_sha256.is_empty() || schedule_state_receipt.is_null() {
        bail!("native v5 typed replay has an invalid schedule/state receipt");
    }

    let mut outcome_audits = Vec::with_capacity(attempt_rows.len());
    let mut attempt_deltas = Vec::with_capacity(attempt_rows.len());
    for row in attempt_rows {
        let audit_sha256 = v5_required_string(row, "outcomeAuditSha256", "native v5 attempt row")?;
        outcome_audits
            .push(v5_object_value(objects, audit_sha256, "attempt outcome audit")?.clone());
        match row.get("proposalDeltaSha256") {
            Some(Value::Null) => attempt_deltas.push(Value::Null),
            Some(Value::String(delta_sha256)) if !delta_sha256.is_empty() => attempt_deltas
                .push(v5_object_value(objects, delta_sha256, "attempt proposal delta")?.clone()),
            _ => bail!("native v5 attempt row has an invalid proposalDeltaSha256"),
        }
    }
    let mut accepted_deltas = Vec::with_capacity(accepted_records.len());
    for record in accepted_records {
        let record_sha256 =
            v5_required_string(record, "recordSha256", "native v5 compact accepted record")?;
        v5_bind_public_document_to_object(
            record,
            objects,
            record_sha256,
            "compact accepted record",
        )?;
        let delta_sha256 = v5_required_string(
            record,
            "proposalDeltaSha256",
            "native v5 compact accepted record",
        )?;
        accepted_deltas
            .push(v5_object_value(objects, delta_sha256, "accepted proposal delta")?.clone());
    }

    let accepted_pool = v5_public_document(public_documents, "g0AcceptedPool")?;
    let accepted_pool_sha256 = v5_required_string(
        accepted_pool,
        "acceptedPoolSha256",
        "native v5 G0 accepted pool",
    )?;
    v5_bind_public_document_to_object(
        accepted_pool,
        objects,
        accepted_pool_sha256,
        "G0 accepted pool",
    )?;
    let campaign_ledger = v5_public_document(public_documents, "g0CampaignConstructionLedger")?;
    let campaign_ledger_sha256 = v5_required_string(
        campaign_ledger,
        "ledgerSha256",
        "native v5 G0 campaign ledger",
    )?;
    v5_bind_public_document_to_object(
        campaign_ledger,
        objects,
        campaign_ledger_sha256,
        "G0 campaign ledger",
    )?;
    let g0_selection = v5_public_document(public_documents, "g0Selection")?;
    let g0_selection_sha256 =
        v5_required_string(g0_selection, "selectionSha256", "native v5 G0 selection")?;
    v5_bind_public_document_to_object(g0_selection, objects, g0_selection_sha256, "G0 selection")?;
    let publication_plan = v5_object_value(
        objects,
        result_sha("publicationPlanSha256")?,
        "publication plan",
    )?;

    let transaction = reconstruct_v5_g0_transaction_from_artifacts(
        request,
        attempt_journal,
        attempt_rows,
        &outcome_audits,
        accepted_records,
        compact_journal,
        &attempt_deltas,
        &accepted_deltas,
        identity_ledger,
        schedule_state_receipt,
        Some(accepted_pool),
        Some(campaign_ledger),
        Some(g0_selection),
        Some(selected_projection_index),
        publication_plan,
    )
    .context("typed replay native v5 durable G0 transaction")?;
    verify_v5_g0_transaction_replay(request, &transaction)
        .context("verify typed native v5 durable G0 transaction replay")?;
    if transaction.attempt_journal.attempt_journal_sha256()? != attempt_journal_sha256
        || transaction
            .compact_accepted_journal
            .compact_journal_sha256()?
            != result_sha("compactJournalSha256")?
        || transaction.identity_ledger.identity_ledger_sha256()?
            != result_sha("identityLedgerSha256")?
        || transaction.publication_plan_sha256()? != result_sha("publicationPlanSha256")?
    {
        bail!("native v5 typed replay root differs from the immutable result");
    }
    let index = transaction
        .selected_projection_index
        .as_ref()
        .ok_or_else(|| anyhow!("native v5 typed G0 replay lacks selected projection index"))?;
    if index.selected_projection_index_sha256()? != result_sha("selectedProjectionIndexSha256")? {
        bail!("native v5 selected projection index/result binding drifted");
    }
    let expected_selected = index
        .projections
        .iter()
        .map(|projection| projection.to_value().map_err(anyhow::Error::from))
        .collect::<Result<Vec<_>>>()?;
    if expected_selected != selected_projections {
        bail!("native v5 selected-projection JSONL differs from its immutable index object");
    }
    if selected_projections.len() as u64 != manifest.evaluation_population_size {
        bail!("native v5 selected projection count differs from the manifest");
    }

    let publication_request_sha256 = result_sha("publicationRequestSha256")?;
    let publication_plan_sha256 = result_sha("publicationPlanSha256")?;
    let mut receipt_candidates = objects.iter().filter(|(object_sha256, value)| {
        value
            .get("publicationReceiptSha256")
            .and_then(Value::as_str)
            == Some(object_sha256.as_str())
            && value.get("publicationPlanSha256").and_then(Value::as_str)
                == Some(publication_plan_sha256)
            && value
                .get("publicationRequestSha256")
                .and_then(Value::as_str)
                == Some(publication_request_sha256)
    });
    let (_, publication_receipt_value) = receipt_candidates.next().ok_or_else(|| {
        anyhow!("native v5 typed replay lacks receipt-bound immutable publication object")
    })?;
    if receipt_candidates.next().is_some() {
        bail!("native v5 typed replay has multiple receipt-bound publication objects");
    }
    let mut expected_object_sha256s = transaction
        .durable_object_bindings()
        .context("enumerate typed native v5 replay objects")?
        .into_iter()
        .map(|binding| binding.object_sha256)
        .collect::<BTreeSet<_>>();
    let receipt_sha256 = v5_required_string(
        publication_receipt_value,
        "publicationReceiptSha256",
        "native v5 publication receipt object",
    )?;
    expected_object_sha256s.insert(receipt_sha256.to_owned());
    // This root names the exact core binding wrapper, not a caller-authored
    // alias of its inner compact receipt.  The persisted verifier below
    // performs the typed replay once the public receipt is opened.
    expected_object_sha256s.insert(result_sha("g0FunnelFragmentsSha256")?.to_owned());
    expected_object_sha256s.insert(result_sha("g0FunnelProjectionStreamReceiptSha256")?.to_owned());
    let observed_object_sha256s = objects.keys().cloned().collect::<BTreeSet<_>>();
    if observed_object_sha256s != expected_object_sha256s {
        bail!("native v5 immutable object inventory is not the exact typed durable closure");
    }
    Ok(V5TypedReplay {
        transaction,
        publication_receipt_value: publication_receipt_value.clone(),
    })
}

fn v5_verify_publication_receipt_inventory(
    result: &V5ProposalResult,
    receipt: &V5G0PublicationReceipt,
) -> Result<()> {
    let result_fields = result
        .value
        .as_object()
        .ok_or_else(|| anyhow!("native v5 immutable result is invalid"))?;
    if result_fields
        .get("publicationPlanSha256")
        .and_then(Value::as_str)
        != Some(receipt.publication_plan_sha256.as_str())
        || result_fields
            .get("publicationRequestSha256")
            .and_then(Value::as_str)
            != Some(receipt.publication_request_sha256.as_str())
    {
        bail!("native v5 publication receipt/result root binding drifted");
    }
    let inventory = result_fields
        .get("receipt")
        .and_then(Value::as_object)
        .and_then(|receipt| receipt.get("outputInventory"))
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("native v5 immutable result lacks output inventory"))?;
    let artifacts = inventory
        .get("artifacts")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("native v5 immutable output inventory lacks artifacts"))?;
    let expected = [
        ("pairConfig", "pair-config.json", &receipt.pair_config),
        ("population", "population.json", &receipt.population),
        (
            "evaluationPopulation",
            "evaluation-population.json",
            &receipt.evaluation_population,
        ),
        (
            "generationJournal",
            "generation-journal.json",
            &receipt.generation_journal,
        ),
    ];
    for (kind, path, streamed) in expected {
        let artifact = artifacts
            .iter()
            .find(|value| {
                value.as_object().is_some_and(|artifact| {
                    artifact.get("kind").and_then(Value::as_str) == Some(kind)
                })
            })
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow!("native v5 output inventory lacks {kind} artifact"))?;
        if artifact.get("relativePath").and_then(Value::as_str) != Some(path)
            || artifact.get("semanticSha256").and_then(Value::as_str)
                != Some(streamed.semantic_sha256.as_str())
            || artifact.get("fileSha256").and_then(Value::as_str)
                != Some(streamed.file_sha256.as_str())
            || artifact.get("byteLength").and_then(Value::as_u64) != Some(streamed.encoded_bytes)
        {
            bail!("native v5 {kind} inventory identity differs from the publication receipt");
        }
    }
    Ok(())
}

fn verify_v5_output_inventory(
    output_root: &Path,
    manifest: &V5ProposalManifest,
    result: &V5ProposalResult,
) -> Result<V5AdoptionBytes> {
    let result_fields = result
        .value
        .as_object()
        .ok_or_else(|| anyhow!("native v5 immutable result is invalid"))?;
    let receipt = result_fields
        .get("receipt")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("native v5 immutable result lacks receipt"))?;
    let inventory = receipt
        .get("outputInventory")
        .ok_or_else(|| anyhow!("native v5 immutable receipt lacks output inventory"))?;
    let inventory_fields = inventory
        .as_object()
        .ok_or_else(|| anyhow!("native v5 output inventory is invalid"))?;
    let inventory_sha = inventory_fields
        .get("outputInventorySha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native v5 output inventory lacks identity"))?;
    if result_fields
        .get("outputInventorySha256")
        .and_then(Value::as_str)
        != Some(inventory_sha)
        || receipt.get("outputInventorySha256").and_then(Value::as_str) != Some(inventory_sha)
    {
        bail!("native v5 output inventory/result binding drifted");
    }
    let mut observed = V5AdoptionBytes::default();
    let raw_receipt = v5_inventory_file(
        output_root,
        "internal/v5-proposal/receipt.json",
        "native v5 receipt artifact",
    )?;
    let persisted_receipt: Value =
        serde_json::from_slice(&raw_receipt).context("parse native v5 receipt artifact")?;
    if raw_receipt != canonical_json_line(&persisted_receipt)?
        || persisted_receipt != Value::Object(receipt.clone())
    {
        bail!("native v5 receipt artifact differs from immutable result");
    }
    observed.public_artifact_bytes += raw_receipt.len() as u64;
    observed.authenticated_file_count += 1;
    let artifacts = inventory_fields
        .get("artifacts")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("native v5 output inventory lacks artifacts"))?;
    let mut allowed = BTreeSet::from(["internal/v5-proposal/receipt.json".to_owned()]);
    // Authenticate every byte stream before opening the typed replay.  The
    // only later buffered reads are compact journals/objects; population and
    // evaluation documents are handed back to the core as held file streams.
    let mut artifact_files = BTreeMap::<String, V5InventoryFile>::new();
    let mut artifact_kinds = BTreeSet::new();
    for artifact in artifacts {
        let artifact = artifact
            .as_object()
            .ok_or_else(|| anyhow!("native v5 output inventory artifact is invalid"))?;
        let kind = artifact
            .get("kind")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 output inventory artifact lacks kind"))?;
        let relative = artifact
            .get("relativePath")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 output inventory artifact lacks path"))?;
        if !allowed.insert(relative.to_owned()) {
            bail!("native v5 output inventory aliases a static artifact: {relative}");
        }
        verify_v5_inventory_artifact_streaming(output_root, artifact, &mut observed, false)?;
        if !artifact_kinds.insert(kind.to_owned()) {
            bail!("native v5 output inventory repeats an artifact kind: {kind}");
        }
        if artifact_files
            .insert(
                kind.to_owned(),
                v5_inventory_file_identity(artifact, "native v5 output inventory artifact")?,
            )
            .is_some()
        {
            bail!("native v5 output inventory repeats an artifact identity: {kind}");
        }
    }
    let object_files = verify_v5_object_inventory_sidecar(
        output_root,
        inventory_fields,
        &mut observed,
        &mut allowed,
    )?;
    require_v5_owned_namespace_file_set(output_root, &allowed)?;

    let compact_artifact = |kind: &str| {
        artifact_files
            .get(kind)
            .ok_or_else(|| anyhow!("native v5 output inventory lacks {kind} artifact"))
    };
    let mut public_documents = BTreeMap::new();
    for (kind, label) in [
        ("attemptJournal", "native v5 attempt-journal root"),
        ("identityLedger", "native v5 identity ledger"),
        ("sharedAuthority", "native v5 shared authority"),
        ("g0AcceptedPool", "native v5 G0 accepted pool"),
        (
            "g0CampaignConstructionLedger",
            "native v5 G0 campaign construction ledger",
        ),
        ("g0Selection", "native v5 G0 selection"),
    ] {
        public_documents.insert(
            kind.to_owned(),
            v5_read_authenticated_compact_document(output_root, compact_artifact(kind)?, label)?,
        );
    }
    if v5_public_document(&public_documents, "sharedAuthority")? != &manifest.frozen_authority {
        bail!("native v5 public shared authority differs from the sealed manifest authority");
    }
    let attempt_rows = parse_v5_canonical_jsonl(
        &v5_read_authenticated_compact_file(
            output_root,
            compact_artifact("attemptRows")?,
            "native v5 attempt rows",
        )?,
        "native v5 attempt rows",
    )?;
    let accepted_records = parse_v5_canonical_jsonl(
        &v5_read_authenticated_compact_file(
            output_root,
            compact_artifact("compactJournal")?,
            "native v5 compact accepted-record journal",
        )?,
        "native v5 compact accepted-record journal",
    )?;
    let selected_projections = parse_v5_canonical_jsonl(
        &v5_read_authenticated_compact_file(
            output_root,
            compact_artifact("selectedProjectionIndex")?,
            "native v5 selected-projection journal",
        )?,
        "native v5 selected-projection journal",
    )?;
    let mut object_values = BTreeMap::new();
    for (object_sha256, object) in &object_files {
        let value = v5_read_authenticated_compact_document(
            output_root,
            object,
            "native v5 immutable durable object",
        )?;
        if object_values.insert(object_sha256.clone(), value).is_some() {
            bail!("native v5 output inventory repeats immutable object identity");
        }
    }
    let request = v5_g0_transaction_request(manifest)?;
    let replay = v5_reconstruct_typed_transaction(
        manifest,
        result,
        &request,
        &public_documents,
        &attempt_rows,
        &accepted_records,
        &selected_projections,
        &object_values,
    )?;
    let mut pair = open_v5_inventory_stream(
        output_root,
        compact_artifact("pairConfig")?,
        "native v5 public pair config",
    )?;
    let mut population = open_v5_inventory_stream(
        output_root,
        compact_artifact("population")?,
        "native v5 public population",
    )?;
    let mut evaluation = open_v5_inventory_stream(
        output_root,
        compact_artifact("evaluationPopulation")?,
        "native v5 public evaluation population",
    )?;
    let mut journal = open_v5_inventory_stream(
        output_root,
        compact_artifact("generationJournal")?,
        "native v5 public generation journal",
    )?;
    let publication_receipt = verify_v5_g0_publication_adoption(
        &request,
        &replay.transaction,
        &replay.publication_receipt_value,
        &mut pair,
        &mut population,
        &mut evaluation,
        &mut journal,
    )
    .context("verify native v5 sealed public bundle without rich reconstruction")?;
    if publication_receipt.to_value()? != replay.publication_receipt_value {
        bail!("native v5 immutable publication receipt object differs from typed adoption");
    }
    v5_verify_publication_receipt_inventory(result, &publication_receipt)?;
    let g0_funnel_fragments_sha256 = result_fields
        .get("g0FunnelFragmentsSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native v5 immutable result lacks G0 funnel receipt identity"))?;
    let g0_funnel_binding_value =
        object_values
            .get(g0_funnel_fragments_sha256)
            .ok_or_else(|| {
                anyhow!("native v5 immutable object inventory lacks G0 funnel receipt binding")
            })?;
    let g0_funnel = verify_v5_g0_funnel_fragment_receipt(
        &request,
        &replay.transaction,
        &publication_receipt,
        g0_funnel_binding_value,
    )
    .context("verify native v5 sealed G0 funnel receipt without private fragments")?;
    if g0_funnel
        .funnel_fragments_sha256()
        .context("identify native v5 sealed G0 funnel receipt")?
        != g0_funnel_fragments_sha256
    {
        bail!("native v5 sealed G0 funnel receipt/result root binding drifted");
    }
    let g0_funnel_stream_receipt_sha256 = result_fields
        .get("g0FunnelProjectionStreamReceiptSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            anyhow!("native v5 immutable result lacks G0 funnel projection-stream identity")
        })?;
    let g0_funnel_stream_binding_value = object_values
        .get(g0_funnel_stream_receipt_sha256)
        .ok_or_else(|| {
            anyhow!(
                "native v5 immutable object inventory lacks G0 funnel projection-stream receipt binding"
            )
        })?;
    let g0_funnel_stream_binding =
        V5G0FunnelProjectionStreamReceiptObjectBinding::from_value(g0_funnel_stream_binding_value)
            .context("parse native v5 sealed G0 funnel projection-stream receipt binding")?;
    if g0_funnel_stream_binding.g0_funnel_projection_stream_receipt_sha256
        != g0_funnel_stream_receipt_sha256
    {
        bail!("native v5 sealed G0 funnel projection-stream result root drifted");
    }
    let mut g0_funnel_stream = open_v5_inventory_stream(
        output_root,
        compact_artifact("g0FunnelProjectionStream")?,
        "native v5 public G0 funnel projection stream",
    )?;
    let verified_stream_receipt = verify_v5_g0_funnel_projection_stream(
        &g0_funnel,
        &g0_funnel_stream_binding.value,
        &mut g0_funnel_stream,
    )
    .context("verify native v5 sealed G0 funnel projection stream")?;
    if verified_stream_receipt
        .projection_stream_receipt_sha256()
        .context("identify native v5 sealed G0 funnel projection-stream receipt")?
        != g0_funnel_stream_receipt_sha256
    {
        bail!("native v5 sealed G0 funnel projection-stream receipt/result binding drifted");
    }
    Ok(observed)
}

/// Authenticate a sealed later-generation output tree without reopening the
/// parent archive/ledger or performing structural replay.  The typed durable
/// object closure is cross-bound byte-for-byte, while the core’s receipt-only
/// verifier streams the five public artifacts through bounded buffers.
fn verify_v5_evolved_output_inventory(
    output_root: &Path,
    manifest: &V5ProposalManifest,
    result: &V5EvolvedProposalResult,
) -> Result<V5AdoptionBytes> {
    let result_fields = result
        .value
        .as_object()
        .ok_or_else(|| anyhow!("native v5 evolved immutable result is invalid"))?;
    let receipt = result_fields
        .get("receipt")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("native v5 evolved immutable result lacks receipt"))?;
    let inventory = receipt
        .get("outputInventory")
        .ok_or_else(|| anyhow!("native v5 evolved immutable receipt lacks output inventory"))?;
    let inventory_fields = inventory
        .as_object()
        .ok_or_else(|| anyhow!("native v5 evolved output inventory is invalid"))?;
    let inventory_sha = inventory_fields
        .get("outputInventorySha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native v5 evolved output inventory lacks identity"))?;
    if result_fields
        .get("outputInventorySha256")
        .and_then(Value::as_str)
        != Some(inventory_sha)
        || receipt.get("outputInventorySha256").and_then(Value::as_str) != Some(inventory_sha)
    {
        bail!("native v5 evolved output inventory/result binding drifted");
    }
    let mut observed = V5AdoptionBytes::default();
    let receipt_path = "internal/v5-proposal/receipt.json";
    let raw_receipt = v5_inventory_file(
        output_root,
        receipt_path,
        "native v5 evolved receipt artifact",
    )?;
    let persisted_receipt: Value =
        serde_json::from_slice(&raw_receipt).context("parse native v5 evolved receipt artifact")?;
    if raw_receipt != canonical_json_line(&persisted_receipt)?
        || persisted_receipt != Value::Object(receipt.clone())
    {
        bail!("native v5 evolved receipt artifact differs from immutable result");
    }
    observed.public_artifact_bytes = observed
        .public_artifact_bytes
        .checked_add(raw_receipt.len() as u64)
        .ok_or_else(|| anyhow!("native v5 evolved public byte counter overflows"))?;
    observed.authenticated_file_count = observed
        .authenticated_file_count
        .checked_add(1)
        .ok_or_else(|| anyhow!("native v5 evolved file counter overflows"))?;

    let artifacts = inventory_fields
        .get("artifacts")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("native v5 evolved output inventory lacks artifacts"))?;
    let mut allowed = BTreeSet::from([receipt_path.to_owned()]);
    let mut artifact_files = BTreeMap::<String, V5InventoryFile>::new();
    for artifact in artifacts {
        let fields = artifact
            .as_object()
            .ok_or_else(|| anyhow!("native v5 evolved output inventory artifact is invalid"))?;
        let kind = fields
            .get("kind")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 evolved output inventory artifact lacks kind"))?;
        let relative = fields
            .get("relativePath")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 evolved output inventory artifact lacks path"))?;
        if !allowed.insert(relative.to_owned()) {
            bail!("native v5 evolved output inventory aliases a static artifact: {relative}");
        }
        verify_v5_inventory_artifact_streaming(output_root, fields, &mut observed, false)?;
        if artifact_files
            .insert(
                kind.to_owned(),
                v5_inventory_file_identity(fields, "native v5 evolved output inventory artifact")?,
            )
            .is_some()
        {
            bail!("native v5 evolved output inventory repeats an artifact kind: {kind}");
        }
    }
    let object_files = verify_v5_object_inventory_sidecar(
        output_root,
        inventory_fields,
        &mut observed,
        &mut allowed,
    )?;
    require_v5_owned_namespace_file_set(output_root, &allowed)?;

    let artifact = |kind: &str| {
        artifact_files
            .get(kind)
            .ok_or_else(|| anyhow!("native v5 evolved output inventory lacks {kind} artifact"))
    };
    let object = |sha256: &str| {
        object_files
            .get(sha256)
            .ok_or_else(|| anyhow!("native v5 evolved object store lacks {sha256}"))
    };
    let transaction_sha256 = result_fields
        .get("transactionSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native v5 evolved immutable result lacks transaction identity"))?;
    let transaction_value = v5_read_authenticated_compact_document(
        output_root,
        object(transaction_sha256)?,
        "native v5 evolved transaction root",
    )?;
    let transaction = V5EvolvedTransactionResult::from_value(&transaction_value)
        .context("parse native v5 evolved transaction root")?;
    if transaction
        .transaction_sha256()
        .context("identify native v5 evolved transaction root")?
        != transaction_sha256
    {
        bail!("native v5 evolved transaction root identity drifted");
    }
    transaction
        .verify_replay()
        .context("validate native v5 evolved transaction root")?;
    let request = v5_evolved_adoption_request(manifest, &transaction)?;

    let plan_sha256 = result_fields
        .get("publicationPlanSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            anyhow!("native v5 evolved immutable result lacks publication plan identity")
        })?;
    let plan_value = v5_read_authenticated_compact_document(
        output_root,
        object(plan_sha256)?,
        "native v5 evolved publication plan object",
    )?;
    let plan = V5EvolvedPublicationPlan::from_value(&plan_value)
        .context("parse native v5 evolved publication plan object")?;
    let plan_binding = plan
        .object_binding()
        .context("bind native v5 evolved publication plan object")?;
    if plan_binding.publication_plan_sha256 != plan_sha256
        || object(plan_sha256)?.relative_path != plan_binding.relative_path
    {
        bail!("native v5 evolved publication plan object path/identity drifted");
    }
    let publication_receipt_sha256 = result_fields
        .get("publicationReceiptSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            anyhow!("native v5 evolved immutable result lacks publication receipt identity")
        })?;
    let publication_receipt_value = v5_read_authenticated_compact_document(
        output_root,
        object(publication_receipt_sha256)?,
        "native v5 evolved publication receipt object",
    )?;
    let publication_receipt = V5EvolvedPublicationReceipt::from_value(&publication_receipt_value)
        .context("parse native v5 evolved publication receipt object")?;
    let publication_receipt_binding = publication_receipt
        .object_binding()
        .context("bind native v5 evolved publication receipt object")?;
    if publication_receipt_binding.publication_receipt_sha256 != publication_receipt_sha256
        || object(publication_receipt_sha256)?.relative_path
            != publication_receipt_binding.relative_path
    {
        bail!("native v5 evolved publication receipt object path/identity drifted");
    }
    v5_verify_evolved_publication_receipt_inventory(result, &publication_receipt)?;

    let publication_fragments_sha256 = result_fields
        .get("publicationFragmentsSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            anyhow!("native v5 evolved immutable result lacks publication-fragments identity")
        })?;
    let publication_fragments_value = v5_read_authenticated_compact_document(
        output_root,
        object(publication_fragments_sha256)?,
        "native v5 evolved publication-fragments receipt object",
    )?;
    let publication_fragments =
        V5EvolvedPublicationFragments::from_value(&publication_fragments_value)
            .context("parse native v5 evolved publication-fragments receipt object")?;
    let publication_fragments_binding = publication_fragments
        .object_binding()
        .context("bind native v5 evolved publication-fragments receipt object")?;
    publication_fragments_binding
        .validate()
        .context("validate native v5 evolved publication-fragments receipt object")?;
    if publication_fragments_binding.fragment_bundle_sha256 != publication_fragments_sha256
        || object(publication_fragments_sha256)?.relative_path
            != publication_fragments_binding.relative_path
        || publication_fragments_binding.value != publication_fragments_value
        || publication_fragments.accepted_candidate_count
            != transaction.accepted_records.len() as u64
        || publication_fragments.proposal_attempt_count != transaction.attempts.len() as u64
    {
        bail!("native v5 evolved publication-fragments object path/identity/count drifted");
    }

    // Closed object inventory: all transaction durable bindings plus the
    // separately typed publication plan/receipt/fragments objects, with no
    // hidden snapshot or alias entry permitted by the outer envelope.
    let mut expected_objects = BTreeMap::<String, (String, Value)>::new();
    for binding in transaction
        .durable_object_bindings()
        .context("enumerate native v5 evolved durable object closure")?
    {
        binding
            .validate()
            .context("validate native v5 evolved durable object binding")?;
        if expected_objects
            .insert(
                binding.object_sha256.clone(),
                (binding.relative_path.clone(), binding.value.clone()),
            )
            .is_some()
        {
            bail!("native v5 evolved durable object closure repeats a semantic identity");
        }
    }
    for (sha256, relative, value) in [
        (
            plan_binding.publication_plan_sha256.clone(),
            plan_binding.relative_path.clone(),
            plan_value.clone(),
        ),
        (
            publication_receipt_binding
                .publication_receipt_sha256
                .clone(),
            publication_receipt_binding.relative_path.clone(),
            publication_receipt_value.clone(),
        ),
        (
            publication_fragments_binding.fragment_bundle_sha256.clone(),
            publication_fragments_binding.relative_path.clone(),
            publication_fragments_value.clone(),
        ),
    ] {
        if expected_objects.insert(sha256, (relative, value)).is_some() {
            bail!("native v5 evolved publication object aliases a durable object");
        }
    }
    if object_files.len() != expected_objects.len() {
        bail!("native v5 evolved object-store inventory is not the exact typed closure");
    }
    for (sha256, (relative, expected_value)) in expected_objects {
        let inventory_file = object(&sha256)?;
        if inventory_file.relative_path != relative {
            bail!("native v5 evolved typed object path drifted for {sha256}");
        }
        let observed_value = v5_read_authenticated_compact_document(
            output_root,
            inventory_file,
            "native v5 evolved typed durable object",
        )?;
        if observed_value != expected_value {
            bail!("native v5 evolved typed object bytes drifted for {sha256}");
        }
    }

    let mut pair =
        open_v5_inventory_stream(output_root, artifact("pairConfig")?, "evolved pair config")?;
    let mut identity_ledger = open_v5_inventory_stream(
        output_root,
        artifact("identityLedger")?,
        "evolved public identity ledger",
    )?;
    let mut population =
        open_v5_inventory_stream(output_root, artifact("population")?, "evolved population")?;
    let mut evaluation = open_v5_inventory_stream(
        output_root,
        artifact("evaluationPopulation")?,
        "evolved evaluation population",
    )?;
    let mut journal = open_v5_inventory_stream(
        output_root,
        artifact("generationJournal")?,
        "evolved generation journal",
    )?;
    let verified_receipt = verify_v5_evolved_publication_adoption(
        &request,
        &transaction,
        &plan_value,
        &publication_receipt_value,
        &mut pair,
        &mut identity_ledger,
        &mut population,
        &mut evaluation,
        &mut journal,
    )
    .context("verify sealed native v5 evolved publication bundle without rich reconstruction")?;
    drop((pair, identity_ledger, population, evaluation, journal));
    if verified_receipt != publication_receipt {
        bail!("native v5 evolved publication receipt differs from receipt-only verifier");
    }

    // Defend against a post-authentication replacement before the caller emits
    // evidence.  These rehashes are deliberately not counted as a second
    // adoption pass; the evidence describes the primary authenticated read.
    for file in artifact_files.values().chain(object_files.values()) {
        v5_safe_relative_output_path(&file.relative_path, "native v5 evolved reauthentication")?;
        require_v5_file_digest(
            &output_root.join(&file.relative_path),
            file.byte_length,
            &file.file_sha256,
            "native v5 evolved sealed output artifact",
        )?;
    }
    let descriptor = v5_object_inventory_descriptor(inventory_fields)?;
    require_v5_file_digest(
        &output_root.join(V5_OUTPUT_INVENTORY_PATH),
        descriptor
            .get("byteLength")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("native v5 evolved object inventory lacks byte length"))?,
        descriptor
            .get("fileSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 evolved object inventory lacks file SHA-256"))?,
        "native v5 evolved object inventory",
    )?;
    require_v5_file_digest(
        &output_root.join(receipt_path),
        raw_receipt.len() as u64,
        &sha256_bytes(&raw_receipt),
        "native v5 evolved output receipt",
    )?;
    Ok(observed)
}

/// Windows' `PROCESS_MEMORY_COUNTERS_EX` matches the Win32 declaration
/// exactly.  It is kept local rather than introducing an otherwise-unused
/// platform crate: this executable already owns the process being measured.
#[cfg(windows)]
#[repr(C)]
struct V5ProcessMemoryCountersEx {
    cb: u32,
    page_fault_count: u32,
    peak_working_set_size: usize,
    working_set_size: usize,
    quota_peak_paged_pool_usage: usize,
    quota_paged_pool_usage: usize,
    quota_peak_non_paged_pool_usage: usize,
    quota_non_paged_pool_usage: usize,
    pagefile_usage: usize,
    peak_pagefile_usage: usize,
    private_usage: usize,
}

#[cfg(windows)]
#[repr(C)]
struct V5FileTime {
    low_date_time: u32,
    high_date_time: u32,
}

#[cfg(windows)]
#[link(name = "kernel32")]
unsafe extern "system" {
    fn GetCurrentProcess() -> *mut c_void;
    fn GetProcessTimes(
        process: *mut c_void,
        creation: *mut V5FileTime,
        exit: *mut V5FileTime,
        kernel: *mut V5FileTime,
        user: *mut V5FileTime,
    ) -> i32;
}

#[cfg(windows)]
#[link(name = "psapi")]
unsafe extern "system" {
    fn GetProcessMemoryInfo(
        process: *mut c_void,
        counters: *mut V5ProcessMemoryCountersEx,
        counters_size: u32,
    ) -> i32;
}

#[cfg(windows)]
fn v5_process_cpu_duration() -> Result<Option<Duration>> {
    let mut creation = V5FileTime {
        low_date_time: 0,
        high_date_time: 0,
    };
    let mut exit = V5FileTime {
        low_date_time: 0,
        high_date_time: 0,
    };
    let mut kernel = V5FileTime {
        low_date_time: 0,
        high_date_time: 0,
    };
    let mut user = V5FileTime {
        low_date_time: 0,
        high_date_time: 0,
    };
    let queried = unsafe {
        GetProcessTimes(
            GetCurrentProcess(),
            &mut creation,
            &mut exit,
            &mut kernel,
            &mut user,
        )
    };
    if queried == 0 {
        bail!("query native v5 Windows process CPU time with GetProcessTimes");
    }
    let ticks = |value: &V5FileTime| {
        (u64::from(value.high_date_time) << 32) | u64::from(value.low_date_time)
    };
    let total_100ns = ticks(&kernel)
        .checked_add(ticks(&user))
        .ok_or_else(|| anyhow!("native v5 Windows process CPU time overflows"))?;
    Ok(Some(Duration::from_nanos(
        total_100ns
            .checked_mul(100)
            .ok_or_else(|| anyhow!("native v5 Windows process CPU nanoseconds overflow"))?,
    )))
}

#[cfg(not(windows))]
fn v5_process_cpu_duration() -> Result<Option<Duration>> {
    Ok(None)
}

fn elapsed_process_cpu(started: Option<Duration>, finished: Option<Duration>) -> Option<Duration> {
    match (started, finished) {
        (Some(started), Some(finished)) => finished.checked_sub(started),
        _ => None,
    }
}

/// Execution-only process-tree evidence. It is never put in the immutable
/// receipt: timings and memory are mutable observations, not scientific
/// output semantics.
#[cfg(windows)]
fn v5_execution_process_tree_evidence() -> Result<Value> {
    let size = u32::try_from(std::mem::size_of::<V5ProcessMemoryCountersEx>())
        .context("size native v5 Windows process-memory counters")?;
    let mut counters = V5ProcessMemoryCountersEx {
        cb: size,
        page_fault_count: 0,
        peak_working_set_size: 0,
        working_set_size: 0,
        quota_peak_paged_pool_usage: 0,
        quota_paged_pool_usage: 0,
        quota_peak_non_paged_pool_usage: 0,
        quota_non_paged_pool_usage: 0,
        pagefile_usage: 0,
        peak_pagefile_usage: 0,
        private_usage: 0,
    };
    // `GetCurrentProcess` returns a pseudo handle valid for this call; the
    // counter buffer is initialized with its exact C-layout byte size.
    let queried = unsafe {
        GetProcessMemoryInfo(
            GetCurrentProcess(),
            &mut counters as *mut V5ProcessMemoryCountersEx,
            size,
        )
    };
    if queried == 0 {
        bail!("query native v5 Windows process memory with GetProcessMemoryInfo");
    }
    let peak_rss = u64::try_from(counters.peak_working_set_size)
        .context("convert native v5 Windows peak working set")?;
    let peak_private = u64::try_from(counters.peak_pagefile_usage.max(counters.private_usage))
        .context("convert native v5 Windows peak private commit")?;
    if peak_rss == 0 || peak_private == 0 {
        bail!("native v5 Windows process-memory query reported an unusable zero peak");
    }
    Ok(serde_json::json!({
        "measurement": "windows_peak_process_memory_v1",
        "peakRssBytes": peak_rss,
        "peakPrivateBytes": peak_private,
        // The one-shot native batch has no Python or Dashboard subprocess.
        "pythonChildCount": 0_u64,
        "dashboardChildCount": 0_u64,
    }))
}

#[cfg(not(windows))]
fn v5_execution_process_tree_evidence() -> Result<Value> {
    // Do not publish pretend zero measurements on platforms where this batch
    // has no equivalent supported peak sampler.
    Ok(serde_json::json!({
        "measurement": "unavailable_non_windows_v1",
        "peakRssBytes": Value::Null,
        "peakPrivateBytes": Value::Null,
        "pythonChildCount": 0_u64,
        "dashboardChildCount": 0_u64,
    }))
}

fn v5_adoption_evidence_with_schemas(
    manifest: &V5ProposalManifest,
    immutable_result: &Value,
    evidence_schema: &str,
    telemetry_schema: &str,
    bytes: V5AdoptionBytes,
    measurements: V5ExecutionMeasurements,
) -> Result<Value> {
    let process_tree = v5_execution_process_tree_evidence()?;
    let process_cpu_milliseconds = measurements
        .process_cpu
        .map(duration_milliseconds)
        .transpose()?;
    let total_milliseconds = duration_milliseconds(measurements.total)?;
    let cpu_utilization_milli_cores = match (process_cpu_milliseconds, total_milliseconds) {
        (Some(cpu), total) if total > 0 => Some(
            cpu.checked_mul(1_000)
                .ok_or_else(|| anyhow!("native v5 CPU utilization overflows"))?
                / total,
        ),
        _ => None,
    };
    let result_fields = immutable_result
        .as_object()
        .ok_or_else(|| anyhow!("native v5 immutable result is invalid"))?;
    let execution = manifest
        .execution_authority
        .as_object()
        .ok_or_else(|| anyhow!("native v5 execution authority is invalid"))?;
    let batch = execution
        .get("nativeBatchAuthority")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("native v5 batch authority is invalid"))?;
    let phases = measurements.phases;
    let passes = measurements.passes;
    let io = measurements.io;
    let mut value = Map::from_iter([
        (
            "schemaVersion".to_owned(),
            Value::String(evidence_schema.to_owned()),
        ),
        (
            "operation".to_owned(),
            Value::String(V5_PROPOSAL_OPERATION.to_owned()),
        ),
        ("status".to_owned(), Value::String("adopted".to_owned())),
        (
            "authoritySha256".to_owned(),
            Value::String(manifest.authority_sha256.clone()),
        ),
        (
            "expectedAuthoritySha256".to_owned(),
            Value::String(manifest.expected_authority_sha256.clone()),
        ),
        (
            "manifestSha256".to_owned(),
            Value::String(manifest.manifest_sha256.clone()),
        ),
        (
            "immutableResultSha256".to_owned(),
            result_fields
                .get("resultSha256")
                .cloned()
                .ok_or_else(|| anyhow!("native v5 immutable result lacks identity"))?,
        ),
        (
            "outputInventorySha256".to_owned(),
            result_fields
                .get("outputInventorySha256")
                .cloned()
                .ok_or_else(|| anyhow!("native v5 immutable result lacks output inventory"))?,
        ),
        (
            "nativeBatchAuthoritySha256".to_owned(),
            execution
                .get("nativeBatchAuthoritySha256")
                .cloned()
                .ok_or_else(|| anyhow!("native v5 execution authority lacks batch identity"))?,
        ),
        (
            "nativeExecutableSha256".to_owned(),
            batch
                .get("executableSha256")
                .cloned()
                .ok_or_else(|| anyhow!("native v5 batch authority lacks executable identity"))?,
        ),
        (
            "nativeSourceSha256".to_owned(),
            batch
                .get("sourceSha256")
                .cloned()
                .ok_or_else(|| anyhow!("native v5 batch authority lacks source identity"))?,
        ),
        (
            "telemetry".to_owned(),
            serde_json::json!({
                "schemaVersion": telemetry_schema,
                "executionPath": measurements.path.as_str(),
                "validationMode": measurements.mode.as_str(),
                "authenticationStrategy": measurements.strategy.as_str(),
                "phases": {
                    "staticAuthorityMilliseconds": duration_milliseconds(phases.static_authority)?,
                    "constructionMilliseconds": duration_milliseconds(phases.construction)?,
                    "stagingMilliseconds": duration_milliseconds(phases.staging)?,
                    "prepublicationValidationMilliseconds": duration_milliseconds(phases.prepublication_validation)?,
                    "publicationMilliseconds": duration_milliseconds(phases.publication)?,
                    "outputAuthenticationMilliseconds": duration_milliseconds(phases.output_authentication)?,
                    "totalMilliseconds": total_milliseconds,
                },
                "processCpuMilliseconds": process_cpu_milliseconds,
                "cpuUtilizationMilliCores": cpu_utilization_milli_cores,
                "publicArtifactBytesRead": bytes.public_artifact_bytes,
                "objectStoreBytesRead": bytes.object_store_bytes,
                "authenticatedFileCount": bytes.authenticated_file_count,
                "io": {
                    "filesReopened": io.files_reopened,
                    "bytesRead": io.bytes_read,
                    "bytesHashed": io.bytes_hashed,
                    "bytesWritten": io.bytes_written,
                    "jsonRowsParsed": io.json_rows_parsed,
                },
                "validationPasses": {
                    "constructorReplay": passes.constructor_replay,
                    "redundantFreshReplay": passes.redundant_fresh_replay,
                    "publicationPrepareReplay": passes.publication_prepare_replay,
                    "stagedSemanticReplay": passes.staged_semantic_replay,
                    "stagedFinalRehash": passes.staged_final_rehash,
                    "receiptBoundContentAuthentication": passes.receipt_bound_content_authentication,
                    "deepOutputReplay": passes.deep_output_replay,
                },
                "parallelAuthenticationWorkers": measurements.parallel_authentication_workers,
                "proposalReconstructionCount": 0_u64,
                "legacyRichExpansionCount": 0_u64,
                "processTree": process_tree,
                "threadCap": manifest.thread_cap,
                "constructionPrefetchMultiplier": V5_G0_CONSTRUCTION_PREFETCH_MULTIPLIER,
            }),
        ),
    ]);
    let identity = canonical_sha256(&Value::Object(value.clone()))?;
    value.insert("adoptionEvidenceSha256".to_owned(), Value::String(identity));
    Ok(Value::Object(value))
}

fn v5_adoption_evidence(
    manifest: &V5ProposalManifest,
    result: &V5ProposalResult,
    bytes: V5AdoptionBytes,
    measurements: V5ExecutionMeasurements,
) -> Result<Value> {
    v5_adoption_evidence_with_schemas(
        manifest,
        &result.value,
        V5_PROPOSAL_ADOPTION_EVIDENCE_SCHEMA,
        V5_PROPOSAL_ADOPTION_TELEMETRY_SCHEMA,
        bytes,
        measurements,
    )
}

fn v5_evolved_adoption_evidence(
    manifest: &V5ProposalManifest,
    result: &V5EvolvedProposalResult,
    bytes: V5AdoptionBytes,
    measurements: V5ExecutionMeasurements,
) -> Result<Value> {
    v5_adoption_evidence_with_schemas(
        manifest,
        &result.value,
        V5_EVOLVED_PROPOSAL_ADOPTION_EVIDENCE_SCHEMA,
        V5_EVOLVED_PROPOSAL_ADOPTION_TELEMETRY_SCHEMA,
        bytes,
        measurements,
    )
}

/// Open an optional canonical immutable document without treating a missing
/// final marker as an error.  The caller still gets the full held-handle,
/// component-safe read path when the entry exists, so a symlink/reparse point
/// cannot turn a receipt/result absence check into an unsafe adoption.
fn read_optional_v5_canonical_document(path: &Path, label: &str) -> Result<Option<Value>> {
    match fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("inspect {label}: {}", path.display()));
        }
    }
    let raw = read_stable_existing_file(path, label)?;
    let value: Value = serde_json::from_slice(&raw).with_context(|| format!("parse {label}"))?;
    if raw != canonical_json_line(&value)? {
        bail!("{label} is not canonical");
    }
    Ok(Some(value))
}

/// The outer receipt is the last sealed artifact in the proposal output tree.
/// If a crash occurs after that link but before the tiny invocation result is
/// linked, re-adopt exactly those durable bytes and rebuild the deterministic
/// control-plane marker.  No fresh construction is allowed from this branch.
fn recover_v5_invocation_result_from_sealed_receipt(
    output_root: &Path,
    invocation_root: &Path,
    result_path: &Path,
    manifest: &V5ProposalManifest,
    validation_mode: V5ValidationMode,
    progress: Option<&NativeProgressHandle>,
) -> Result<Option<V5RecoveredResult<V5ProposalResult>>> {
    let receipt_path = output_root.join("internal/v5-proposal/receipt.json");
    let Some(receipt) =
        read_optional_v5_canonical_document(&receipt_path, "native v5 sealed proposal receipt")?
    else {
        return Ok(None);
    };
    let result = build_v5_proposal_result_from_receipt(manifest, &receipt)
        .context("rebuild native v5 invocation result from sealed receipt")?;

    // Authenticate every output artifact *before* creating the control-plane
    // completion marker.  This makes receipt-present/result-absent recovery a
    // true adoption pass, not a shortcut that could bless a truncated or
    // swapped output tree.
    let authentication =
        authenticate_v5_proposal_output(output_root, manifest, &result, validation_mode, progress)
            .context("authenticate sealed native v5 output tree before result recovery")?;
    let publication_started = Instant::now();
    let result_bytes = canonical_json_line(&result.value)
        .context("encode recovered native v5 invocation result")?;
    publish_once(result_path, &result_bytes)
        .context("publish recovered native v5 invocation result")?;
    let recovered =
        read_optional_v5_canonical_document(result_path, "recovered native v5 immutable result")?
            .ok_or_else(|| anyhow!("recovered native v5 invocation result vanished"))?;
    let recovered = validate_v5_proposal_result(&recovered, manifest)
        .context("validate recovered native v5 immutable result")?;
    if recovered.value != result.value {
        bail!("recovered native v5 invocation result drifted from its sealed receipt");
    }
    // The private invocation root can only become complete after the result
    // marker is present.  Do not tolerate any unsealed control files during
    // this recovery branch.
    let allowed = BTreeSet::from([
        "authority.json".to_owned(),
        "frozen-authority.json".to_owned(),
        "manifest.json".to_owned(),
        V5_PROPOSAL_RESULT_PATH.to_owned(),
    ]);
    require_v5_exact_file_set(invocation_root, &allowed)
        .context("verify recovered native v5 invocation file set")?;
    Ok(Some(V5RecoveredResult {
        result: recovered,
        authentication,
        publication: publication_started.elapsed(),
    }))
}

/// Evolved counterpart to receipt-present/result-absent recovery.  It only
/// authenticates the sealed output tree and recreates the deterministic tiny
/// control result; construction, input reopening, and fragment materialization
/// are prohibited on this path.
fn recover_v5_evolved_invocation_result_from_sealed_receipt(
    output_root: &Path,
    invocation_root: &Path,
    result_path: &Path,
    manifest: &V5ProposalManifest,
    validation_mode: V5ValidationMode,
    progress: Option<&NativeProgressHandle>,
) -> Result<Option<V5RecoveredResult<V5EvolvedProposalResult>>> {
    let receipt_path = output_root.join(V5_OUTPUT_RECEIPT_PATH);
    let Some(receipt) = read_optional_v5_canonical_document(
        &receipt_path,
        "native v5 sealed evolved proposal receipt",
    )?
    else {
        return Ok(None);
    };
    let result = build_v5_evolved_proposal_result_from_receipt(manifest, &receipt)
        .context("rebuild native v5 evolved invocation result from sealed receipt")?;
    let authentication = authenticate_v5_evolved_proposal_output(
        output_root,
        manifest,
        &result,
        validation_mode,
        progress,
    )
    .context("authenticate sealed native v5 evolved output tree before result recovery")?;
    let publication_started = Instant::now();
    let result_bytes = canonical_json_line(&result.value)
        .context("encode recovered native v5 evolved invocation result")?;
    publish_once(result_path, &result_bytes)
        .context("publish recovered native v5 evolved invocation result")?;
    let recovered = read_optional_v5_canonical_document(
        result_path,
        "recovered native v5 evolved immutable result",
    )?
    .ok_or_else(|| anyhow!("recovered native v5 evolved invocation result vanished"))?;
    let recovered = validate_v5_evolved_proposal_result(&recovered, manifest)
        .context("validate recovered native v5 evolved immutable result")?;
    if recovered.value != result.value {
        bail!("recovered native v5 evolved invocation result drifted from its sealed receipt");
    }
    let allowed = BTreeSet::from([
        "authority.json".to_owned(),
        "frozen-authority.json".to_owned(),
        "manifest.json".to_owned(),
        V5_PROPOSAL_RESULT_PATH.to_owned(),
    ]);
    require_v5_exact_file_set(invocation_root, &allowed)
        .context("verify recovered native v5 evolved invocation file set")?;
    Ok(Some(V5RecoveredResult {
        result: recovered,
        authentication,
        publication: publication_started.elapsed(),
    }))
}

/// Translate only authenticated manifest fields into the typed core request.
/// The publication plan is deliberately absent: qd-kernel derives and hashes
/// it from this request, keeping `threadCap` outside the semantic plan/request
/// identity while retaining it as a bounded execution control.
fn v5_g0_transaction_request(manifest: &V5ProposalManifest) -> Result<V5G0TransactionRequest> {
    if manifest.generation_kind != "g0" {
        bail!("native v5 typed transaction currently requires a G0 manifest");
    }
    let final_newline = Value::String(manifest.final_newline.clone());
    let publication_inputs = V5G0PublicationInputs::from_manifest_values(
        &final_newline,
        &manifest.execution_authority,
        &manifest.inputs,
    )
    .context("parse native v5 core publication inputs from sealed manifest")?;
    let request = V5G0TransactionRequest {
        shared_authority: manifest.frozen_authority.clone(),
        generation_config: manifest.generation_config.clone(),
        generation_config_sha256: manifest.generation_config_sha256.clone(),
        generation_index: manifest.generation_index,
        target_accepted: manifest.requested_count,
        max_attempts: manifest.max_proposal_attempts,
        evaluation_width: manifest.evaluation_population_size,
        thread_cap: manifest.thread_cap,
        publication_inputs,
    };
    request
        .validate()
        .context("validate sealed native v5 G0 transaction request")?;
    Ok(request)
}

/// One manifest-bound later-generation input after its transport binding has
/// been reauthenticated from disk.  The semantic value remains parsed only
/// long enough to build the typed selector/ledger below; no path is retained
/// by the core transaction or its recovery path.
struct V5EvolvedInputDocument {
    value: Value,
    binding_sha256: String,
    semantic_sha256: String,
    path: PathBuf,
}

fn read_v5_evolved_input_document(
    manifest: &V5ProposalManifest,
    key: &str,
    label: &str,
) -> Result<V5EvolvedInputDocument> {
    if manifest.generation_kind != "evolved" {
        bail!("native v5 later-generation input is only valid for an evolved manifest");
    }
    let inputs = manifest
        .inputs
        .as_object()
        .ok_or_else(|| anyhow!("sealed native v5 evolved inputs are not an object"))?;
    let binding = inputs
        .get(key)
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("sealed native v5 evolved inputs lack {key}"))?;
    let path = binding
        .get("absolutePath")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("sealed native v5 evolved {label} input lacks absolutePath"))?;
    let expected_file_sha256 = binding
        .get("fileSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("sealed native v5 evolved {label} input lacks fileSha256"))?;
    let semantic_sha256 = binding
        .get("semanticSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("sealed native v5 evolved {label} input lacks semanticSha256"))?
        .to_owned();
    let binding_sha256 = binding
        .get("bindingSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("sealed native v5 evolved {label} input lacks bindingSha256"))?
        .to_owned();
    let path = safe_existing_file(Path::new(path), &format!("native v5 evolved {label} input"))?;
    let raw = read_stable_existing_file(&path, &format!("native v5 evolved {label} input"))?;
    if sha256_bytes(&raw) != expected_file_sha256 {
        bail!("sealed native v5 evolved {label} input bytes drifted from its binding");
    }
    let value: Value = serde_json::from_slice(&raw)
        .with_context(|| format!("parse sealed native v5 evolved {label} input"))?;
    Ok(V5EvolvedInputDocument {
        value,
        binding_sha256,
        semantic_sha256,
        path,
    })
}

fn native_v5_object_path(root: &Path, identity: &str, label: &str) -> Result<PathBuf> {
    let digest = identity
        .strip_prefix("sha256:")
        .filter(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        })
        .ok_or_else(|| anyhow!("{label} is not a lowercase SHA-256 identity"))?;
    safe_existing_file(&root.join(format!("{digest}.json")), label)
}

/// Reopen only the retained G0 compact records/deltas addressed by the
/// committed archive. The archive self-hash binds each proposal entry; the
/// compact record binds its delta; and the sealed compiler independently
/// reconstructs the exact opaque parent payload before it reaches selection.
fn native_v5_g0_parent_references(
    manifest: &V5ProposalManifest,
    archive: &V5EvolvedInputDocument,
) -> Result<BTreeMap<String, ParentReference>> {
    if manifest.generation_index != 2 {
        bail!("native v5 G0 parent recovery is valid only for generation two");
    }
    let finalization_root = archive
        .path
        .parent()
        .filter(|path| {
            path.file_name().and_then(|value| value.to_str()) == Some("native-finalization")
        })
        .ok_or_else(|| anyhow!("native v5 G0 parent archive is outside native-finalization"))?;
    if archive.path.file_name().and_then(|value| value.to_str()) != Some("archive.json") {
        bail!("native v5 G0 parent archive has an unexpected filename");
    }
    let generation_root = finalization_root
        .parent()
        .ok_or_else(|| anyhow!("native v5 G0 parent archive lacks a generation root"))?;
    let object_root = generation_root.join("proposal/v5-native/objects/sha256");
    let authority = V5SharedConstructionAuthority::from_shared_object(&manifest.frozen_authority)
        .context("open sealed native v5 G0 parent reconstruction authority")?;
    let cells = archive
        .value
        .get("cells")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("native v5 G0 parent archive cells are invalid"))?;
    let mut references = BTreeMap::new();
    for cell in cells {
        let members = cell
            .get("members")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("native v5 G0 parent archive cell members are invalid"))?;
        for member in members {
            let candidate = member
                .get("candidate")
                .and_then(Value::as_object)
                .ok_or_else(|| anyhow!("native v5 G0 parent member lacks candidate"))?;
            let candidate_id = candidate
                .get("candidateId")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("native v5 G0 parent candidate lacks candidateId"))?;
            let record_sha256 = candidate
                .get("proposalEntrySha256")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    anyhow!("native v5 G0 parent candidate lacks proposalEntrySha256")
                })?;
            let record_path = native_v5_object_path(
                &object_root,
                record_sha256,
                "native v5 G0 compact parent record",
            )?;
            let record_raw =
                read_stable_existing_file(&record_path, "native v5 G0 compact parent record")?;
            let record_value: Value = serde_json::from_slice(&record_raw)
                .context("parse native v5 G0 compact parent record")?;
            let record = V5CompactAcceptedRecord::from_value(&record_value)
                .context("validate native v5 G0 compact parent record")?;
            if record.record_sha256()? != record_sha256
                || record.candidate_id != candidate_id
                || candidate
                    .get("candidateIdentitySha256")
                    .and_then(Value::as_str)
                    != Some(record.candidate_identity_sha256.as_str())
                || candidate.get("programSha256").and_then(Value::as_str)
                    != Some(record.compiled.program_sha256.as_str())
                || candidate.get("sourceProfileSha256").and_then(Value::as_str)
                    != Some(record.compiled.raw_pair_sha256.as_str())
                || candidate
                    .get("profileSnapshotSha256")
                    .and_then(Value::as_str)
                    != Some(record.compiled.profile_snapshot_sha256.as_str())
            {
                bail!("native v5 G0 archive candidate drifts from its compact record");
            }
            let delta_path = native_v5_object_path(
                &object_root,
                &record.proposal_delta_sha256,
                "native v5 G0 compact parent delta",
            )?;
            let delta_raw =
                read_stable_existing_file(&delta_path, "native v5 G0 compact parent delta")?;
            let delta: Value = serde_json::from_slice(&delta_raw)
                .context("parse native v5 G0 compact parent delta")?;
            let reference = parent_reference_from_v5_compact_record(&authority, &delta, &record)
                .context("reconstruct native v5 G0 parent material")?;
            if references
                .insert(candidate_id.to_owned(), reference)
                .is_some()
            {
                bail!("native v5 G0 parent archive repeats a candidate");
            }
        }
    }
    if references.is_empty() {
        bail!("native v5 G0 parent archive has no retained compact material");
    }
    Ok(references)
}

fn native_v5_archive_candidate_ids(archive: &Value) -> Result<BTreeSet<String>> {
    let cells = archive
        .get("cells")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("native v5 parent archive cells are invalid"))?;
    let mut candidate_ids = BTreeSet::new();
    for cell in cells {
        let members = cell
            .get("members")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("native v5 parent archive cell members are invalid"))?;
        for member in members {
            let candidate_id = member
                .get("candidate")
                .and_then(Value::as_object)
                .and_then(|candidate| candidate.get("candidateId"))
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("native v5 parent archive member lacks candidateId"))?;
            if !candidate_ids.insert(candidate_id.to_owned()) {
                bail!("native v5 parent archive repeats a candidate");
            }
        }
    }
    Ok(candidate_ids)
}

/// Open the single-file rolling parent handoff emitted by fast-ephemeral
/// publication. The stream may contain the current proposal plus parents
/// carried from the preceding archive, but only candidates named by the
/// committed archive are recompiled and admitted into the next selector.
fn native_v5_fast_ephemeral_parent_references(
    manifest: &V5ProposalManifest,
    archive: &V5EvolvedInputDocument,
) -> Result<BTreeMap<String, v5_fast_ephemeral::ParentMaterialEntry>> {
    if manifest.generation_index < 2 {
        bail!("fast-ephemeral parent recovery requires generation two or later");
    }
    let finalization_root = archive
        .path
        .parent()
        .filter(|path| {
            path.file_name().and_then(|value| value.to_str()) == Some("native-finalization")
        })
        .ok_or_else(|| anyhow!("fast-ephemeral parent archive is outside native-finalization"))?;
    if archive.path.file_name().and_then(|value| value.to_str()) != Some("archive.json") {
        bail!("fast-ephemeral parent archive has an unexpected filename");
    }
    let generation_root = finalization_root
        .parent()
        .ok_or_else(|| anyhow!("fast-ephemeral parent archive lacks a generation root"))?;
    let stream_path = safe_existing_file(
        &generation_root.join(format!(
            "proposal/{}",
            v5_fast_ephemeral::PARENT_MATERIAL_PATH
        )),
        "fast-ephemeral parent material stream",
    )?;
    let authority = V5SharedConstructionAuthority::from_shared_object(&manifest.frozen_authority)
        .context("open sealed fast-ephemeral parent reconstruction authority")?;

    let mut archive_candidates = BTreeMap::<String, Value>::new();
    for cell in archive
        .value
        .get("cells")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("fast-ephemeral parent archive cells are invalid"))?
    {
        for member in cell
            .get("members")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("fast-ephemeral parent archive members are invalid"))?
        {
            let candidate = member
                .get("candidate")
                .and_then(Value::as_object)
                .ok_or_else(|| anyhow!("fast-ephemeral parent archive member lacks candidate"))?;
            let candidate_id = candidate
                .get("candidateId")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("fast-ephemeral parent archive candidate lacks ID"))?;
            if archive_candidates
                .insert(candidate_id.to_owned(), Value::Object(candidate.clone()))
                .is_some()
            {
                bail!("fast-ephemeral parent archive repeats a candidate");
            }
        }
    }
    if archive_candidates.is_empty() {
        return Ok(BTreeMap::new());
    }

    let file = fs::File::open(&stream_path).with_context(|| {
        format!(
            "open fast-ephemeral parent material stream: {}",
            stream_path.display()
        )
    })?;
    let mut reader = BufReader::new(file);
    let mut seen = BTreeSet::new();
    let mut references = BTreeMap::new();
    let mut raw = Vec::new();
    loop {
        raw.clear();
        let read = reader
            .by_ref()
            .take((v5_fast_ephemeral::MAX_PARENT_MATERIAL_ROW_BYTES + 1) as u64)
            .read_until(b'\n', &mut raw)
            .context("read fast-ephemeral parent material row")?;
        if read == 0 {
            break;
        }
        if raw.len() > v5_fast_ephemeral::MAX_PARENT_MATERIAL_ROW_BYTES {
            bail!("fast-ephemeral parent material row exceeds its byte budget");
        }
        if raw.last() != Some(&b'\n') || raw.contains(&b'\r') {
            bail!("fast-ephemeral parent material row is not canonical LF JSONL");
        }
        let value: Value =
            serde_json::from_slice(&raw).context("parse fast-ephemeral parent material row")?;
        if canonical_json_line(&value)? != raw {
            bail!("fast-ephemeral parent material row is not canonical JSON plus LF");
        }
        let fields = value
            .as_object()
            .ok_or_else(|| anyhow!("fast-ephemeral parent material row is not an object"))?;
        let expected = [
            "schemaVersion",
            "candidateId",
            "pairIdentitySha256",
            "proposalEntrySha256",
            "pairPayload",
            "rowSha256",
        ];
        if fields.len() != expected.len()
            || expected.iter().any(|key| !fields.contains_key(*key))
            || fields.get("schemaVersion").and_then(Value::as_str)
                != Some(v5_fast_ephemeral::PARENT_MATERIAL_ROW_SCHEMA)
        {
            bail!("fast-ephemeral parent material row shape is incompatible");
        }
        let candidate_id = fields
            .get("candidateId")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty() && *value == value.trim())
            .ok_or_else(|| anyhow!("fast-ephemeral parent material candidate ID is invalid"))?;
        if !seen.insert(candidate_id.to_owned()) {
            bail!("fast-ephemeral parent material repeats a candidate");
        }
        let supplied_row_sha256 = fields
            .get("rowSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("fast-ephemeral parent material lacks row identity"))?;
        let mut semantic = fields.clone();
        semantic.remove("rowSha256");
        if canonical_sha256(&Value::Object(semantic))? != supplied_row_sha256 {
            bail!("fast-ephemeral parent material row identity drifted");
        }

        let Some(archive_candidate) = archive_candidates.get(candidate_id) else {
            continue;
        };
        let pair_identity_sha256 = fields
            .get("pairIdentitySha256")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("fast-ephemeral parent material lacks pair identity"))?
            .to_owned();
        let proposal_entry_sha256 = fields
            .get("proposalEntrySha256")
            .and_then(Value::as_str)
            .filter(|value| {
                value.len() == 71
                    && value.starts_with("sha256:")
                    && value.as_bytes()[7..]
                        .iter()
                        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            })
            .ok_or_else(|| {
                anyhow!("fast-ephemeral parent material proposal entry identity is invalid")
            })?
            .to_owned();
        let pair_payload = fields
            .get("pairPayload")
            .cloned()
            .ok_or_else(|| anyhow!("fast-ephemeral parent material lacks pair payload"))?;
        let record_value = pair_payload
            .get("acceptedRecord")
            .ok_or_else(|| anyhow!("fast-ephemeral parent payload lacks accepted record"))?;
        let record = V5CompactAcceptedRecord::from_value(record_value)
            .context("validate fast-ephemeral parent compact record")?;
        if record.candidate_id != candidate_id
            || record.pair_identity_sha256 != pair_identity_sha256
            || archive_candidate
                .get("candidateIdentitySha256")
                .and_then(Value::as_str)
                != Some(record.candidate_identity_sha256.as_str())
            || archive_candidate
                .get("programSha256")
                .and_then(Value::as_str)
                != Some(record.compiled.program_sha256.as_str())
            || archive_candidate
                .get("sourceProfileSha256")
                .and_then(Value::as_str)
                != Some(record.compiled.raw_pair_sha256.as_str())
            || archive_candidate
                .get("profileSnapshotSha256")
                .and_then(Value::as_str)
                != Some(record.compiled.profile_snapshot_sha256.as_str())
            || archive_candidate
                .get("proposalEntrySha256")
                .and_then(Value::as_str)
                != Some(proposal_entry_sha256.as_str())
        {
            bail!("fast-ephemeral archive candidate drifts from its parent material");
        }
        let reference = ParentReference {
            pair_identity_sha256,
            candidate_id: candidate_id.to_owned(),
            pair_payload,
            selection_audit: None,
        };
        verify_v5_evolved_parent_reference(&authority, &reference)
            .context("recompile fast-ephemeral archive parent material")?;
        references.insert(
            candidate_id.to_owned(),
            v5_fast_ephemeral::ParentMaterialEntry {
                reference,
                proposal_entry_sha256,
            },
        );
    }
    if references.len() != archive_candidates.len()
        || archive_candidates
            .keys()
            .any(|candidate_id| !references.contains_key(candidate_id))
    {
        bail!("fast-ephemeral parent material stream lacks an archive candidate");
    }
    Ok(references)
}

fn validate_native_v5_self_hash(value: &Value, field: &str, label: &str) -> Result<String> {
    let supplied = value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("{label} lacks {field}"))?
        .to_owned();
    let mut semantic = value.clone();
    semantic
        .as_object_mut()
        .ok_or_else(|| anyhow!("{label} is not an object"))?
        .remove(field);
    if canonical_sha256(&semantic).with_context(|| format!("identify {label}"))? != supplied {
        bail!("{label} identity drifted");
    }
    Ok(supplied)
}

/// Reconstruct retained children from the immediately preceding evolved
/// proposal.  The final archive deliberately carries no genome payload; its
/// sibling state-application sidecar binds the exact prior proposal manifest
/// and receipt, while the kernel replays the sealed transaction/snapshots and
/// returns only the archive-selected opaque parent references.
fn native_v5_evolved_parent_references(
    manifest: &V5ProposalManifest,
    archive: &V5EvolvedInputDocument,
) -> Result<BTreeMap<String, ParentReference>> {
    if manifest.generation_index < 3 {
        bail!("native v5 evolved parent recovery requires generation three or later");
    }
    let selected_candidate_ids = native_v5_archive_candidate_ids(&archive.value)?;
    if selected_candidate_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let finalization_root = archive
        .path
        .parent()
        .filter(|path| {
            path.file_name().and_then(|value| value.to_str()) == Some("native-finalization")
        })
        .ok_or_else(|| {
            anyhow!("native v5 evolved parent archive is outside native-finalization")
        })?;
    if archive.path.file_name().and_then(|value| value.to_str()) != Some("archive.json") {
        bail!("native v5 evolved parent archive has an unexpected filename");
    }
    let generation_root = finalization_root
        .parent()
        .ok_or_else(|| anyhow!("native v5 evolved parent archive lacks a generation root"))?;

    let sidecar_path = safe_existing_file(
        &finalization_root.join("generation-state-application-sidecar.json"),
        "native v5 previous generation state-application sidecar",
    )?;
    let sidecar_raw = read_stable_existing_file(
        &sidecar_path,
        "native v5 previous generation state-application sidecar",
    )?;
    let sidecar: Value = serde_json::from_slice(&sidecar_raw)
        .context("parse native v5 previous generation state-application sidecar")?;
    if canonical_json_line(&sidecar)? != sidecar_raw
        || sidecar.get("schemaVersion").and_then(Value::as_str)
            != Some("temporal_qd_v5_generation_state_application_sidecar_v1")
        || sidecar.get("generationIndex").and_then(Value::as_u64)
            != Some(manifest.generation_index - 1)
    {
        bail!("native v5 previous generation state-application sidecar is incompatible");
    }
    validate_native_v5_self_hash(
        &sidecar,
        "sidecarSha256",
        "native v5 previous generation state-application sidecar",
    )?;
    let commit_path = safe_existing_file(
        &finalization_root.join("generation-commit.json"),
        "native v5 previous generation commit",
    )?;
    let commit_raw =
        read_stable_existing_file(&commit_path, "native v5 previous generation commit")?;
    let commit: Value = serde_json::from_slice(&commit_raw)
        .context("parse native v5 previous generation commit")?;
    if canonical_json_line(&commit)? != commit_raw
        || commit.get("schemaVersion").and_then(Value::as_str)
            != Some("temporal_qd_generation_commit_v1")
        || commit.get("generationIndex").and_then(Value::as_u64)
            != Some(manifest.generation_index - 1)
    {
        bail!("native v5 previous generation commit is incompatible");
    }
    let commit_sha256 = validate_native_v5_self_hash(
        &commit,
        "commitSha256",
        "native v5 previous generation commit",
    )?;
    if sidecar
        .get("finalization")
        .and_then(Value::as_object)
        .and_then(|value| value.get("commitSha256"))
        .and_then(Value::as_str)
        != Some(commit_sha256.as_str())
        || commit
            .get("parentArchive")
            .and_then(Value::as_object)
            .and_then(|value| value.get("path"))
            .and_then(Value::as_str)
            != Some("archive.json")
        || commit
            .get("parentArchive")
            .and_then(Value::as_object)
            .and_then(|value| value.get("archiveSha256"))
            .and_then(Value::as_str)
            != Some(archive.semantic_sha256.as_str())
    {
        bail!("native v5 previous generation commit/archive binding drifted");
    }
    let proposal_state = sidecar
        .get("proposalStateAuthority")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("native v5 previous generation sidecar lacks proposal authority"))?;
    let proposal_manifest_sha256 = proposal_state
        .get("proposalManifestSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native v5 previous generation sidecar lacks proposal manifest"))?;
    let proposal_receipt_sha256 = proposal_state
        .get("proposalReceiptSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native v5 previous generation sidecar lacks proposal receipt"))?;
    let proposal_manifest_digest = proposal_manifest_sha256
        .strip_prefix("sha256:")
        .filter(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        })
        .ok_or_else(|| anyhow!("native v5 previous proposal manifest identity is invalid"))?;
    let proposal_root = generation_root.join("proposal");
    let invocation_root = proposal_root
        .join("native-batch/v5-proposal")
        .join(proposal_manifest_digest);
    let previous_manifest_path = safe_existing_file(
        &invocation_root.join("manifest.json"),
        "native v5 previous evolved proposal manifest",
    )?;
    let previous_manifest_raw = read_stable_existing_file(
        &previous_manifest_path,
        "native v5 previous evolved proposal manifest",
    )?;
    let previous_manifest = parse_v5_proposal_manifest(&previous_manifest_raw)
        .context("parse native v5 previous evolved proposal manifest")?;
    if previous_manifest.manifest_sha256 != proposal_manifest_sha256
        || previous_manifest.generation_kind != "evolved"
        || previous_manifest.generation_index != manifest.generation_index - 1
    {
        bail!("native v5 previous evolved proposal manifest binding drifted");
    }
    let previous_result_path = safe_existing_file(
        &invocation_root.join(V5_PROPOSAL_RESULT_PATH),
        "native v5 previous evolved proposal result",
    )?;
    let previous_result_raw = read_stable_existing_file(
        &previous_result_path,
        "native v5 previous evolved proposal result",
    )?;
    let previous_result_value: Value = serde_json::from_slice(&previous_result_raw)
        .context("parse native v5 previous evolved proposal result")?;
    if canonical_json_line(&previous_result_value)? != previous_result_raw {
        bail!("native v5 previous evolved proposal result is not canonical JSON plus LF");
    }
    let previous_result =
        validate_v5_evolved_proposal_result(&previous_result_value, &previous_manifest)
            .context("validate native v5 previous evolved proposal result")?;
    if previous_result
        .value
        .get("proposalReceiptSha256")
        .and_then(Value::as_str)
        != Some(proposal_receipt_sha256)
    {
        bail!("native v5 previous evolved proposal receipt binding drifted");
    }
    let transaction_sha256 = previous_result
        .value
        .get("transactionSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native v5 previous evolved result lacks transaction root"))?;
    let transaction_path = native_v5_object_path(
        &proposal_root.join("v5-native/objects/sha256"),
        transaction_sha256,
        "native v5 previous evolved transaction root",
    )?;
    let transaction_raw = read_stable_existing_file(
        &transaction_path,
        "native v5 previous evolved transaction root",
    )?;
    let transaction_value: Value = serde_json::from_slice(&transaction_raw)
        .context("parse native v5 previous evolved transaction root")?;
    if canonical_json_line(&transaction_value)? != transaction_raw {
        bail!("native v5 previous evolved transaction root is not canonical JSON plus LF");
    }
    let transaction = V5EvolvedTransactionResult::from_value(&transaction_value)
        .context("validate native v5 previous evolved transaction root")?;
    if transaction.transaction_sha256()? != transaction_sha256 {
        bail!("native v5 previous evolved transaction identity drifted");
    }
    let request = v5_evolved_adoption_request(&previous_manifest, &transaction)
        .context("reconstruct native v5 previous evolved replay request")?;
    reconstruct_selected_parent_references(&request, &transaction, &selected_candidate_ids)
        .context("reconstruct native v5 selected evolved parent references")
}

/// Authenticate both later-generation input bindings and translate them into
/// the exact typed runtime state consumed by the write-neutral core.  G2 is
/// intentionally restored from the public compact G0 ledger; G3+ restores
/// only the dedicated evolved ledger facade.  Batch never falls back to a
/// generic JSON ledger or a previous generation's private schedule receipt.
fn v5_evolved_frozen_accepted_quotas(
    generation_config: &Value,
    target_accepted: u64,
) -> Result<(u64, u64)> {
    let allocation = generation_config
        .get("reproductionAllocation")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            anyhow!("native v5 evolved generation config lacks reproductionAllocation")
        })?;
    let accepted_terms = allocation.get("schemaVersion").and_then(Value::as_str)
        == Some("temporal_qd_reproduction_allocation_v2");
    let (offspring_key, immigrant_key, target_key) = if accepted_terms {
        (
            "desiredAcceptedOffspringCount",
            "desiredAcceptedImmigrantCount",
            "targetAcceptedCandidates",
        )
    } else {
        (
            "desiredEvaluatedOffspringCount",
            "desiredEvaluatedImmigrantCount",
            "targetEvaluatedCandidates",
        )
    };
    let offspring = allocation
        .get(offspring_key)
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            anyhow!("native v5 evolved reproduction allocation lacks offspring count")
        })?;
    let immigrants = allocation
        .get(immigrant_key)
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            anyhow!("native v5 evolved reproduction allocation lacks immigrant count")
        })?;
    if allocation.get(target_key).and_then(Value::as_u64) != Some(target_accepted)
        || offspring.checked_add(immigrants) != Some(target_accepted)
    {
        bail!("native v5 evolved reproduction allocation disagrees with targetAccepted");
    }
    if let Some(receipt) = generation_config
        .get("breedingConfidenceReceipt")
        .and_then(Value::as_object)
    {
        if receipt
            .get("desiredOffspringCandidateCount")
            .and_then(Value::as_u64)
            != Some(offspring)
            || receipt
                .get("desiredImmigrantCandidateCount")
                .and_then(Value::as_u64)
                != Some(immigrants)
        {
            bail!(
                "native v5 evolved breeding confidence receipt disagrees with reproduction allocation"
            );
        }
    }
    Ok((offspring, immigrants))
}

fn v5_evolved_transaction_request_with_parent_references(
    manifest: &V5ProposalManifest,
    fast_ephemeral: bool,
) -> Result<(
    V5EvolvedTransactionRequest,
    RuntimeParentSelector,
    temporal_qd_kernel::proposal::CandidateIdentityLedger,
    BTreeMap<String, ParentReference>,
    BTreeMap<String, v5_fast_ephemeral::ParentMaterialEntry>,
)> {
    if manifest.generation_kind != "evolved" || manifest.generation_index < 2 {
        bail!("native v5 typed later-generation transaction requires an evolved generation >= 2");
    }
    let parent_archive =
        read_v5_evolved_input_document(manifest, "parentArchive", "parent archive")?;
    let archive_sha256 = parent_archive
        .value
        .get("archiveSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("sealed native v5 evolved parent archive lacks archiveSha256"))?;
    if archive_sha256 != parent_archive.semantic_sha256 {
        bail!("sealed native v5 evolved parent archive semantic identity drifted from its binding");
    }
    let configured_parent_schedule =
        v5_evolved_configured_parent_schedule(manifest.generation_config.get("parentSchedule"))?;
    // A rotating v2 schedule records the selected robust reservoir before the
    // direction-aware parent projection is applied.  Its frozen
    // `unsupportedParentPolicy` explicitly converts unsupported parent share
    // to immigrants.  Open the selector with that empty-pool outcome admitted,
    // then bind the kernel to the exact supported count derived from the
    // authenticated archive.  This is especially important for a G0 frontier
    // whose cumulative economics are all direction-ineligible: it is a valid
    // all-immigrant G1 authority, not a corrupt archive.
    let (parent_references, prior_parent_material) = if fast_ephemeral {
        let entries = native_v5_fast_ephemeral_parent_references(manifest, &parent_archive)?;
        let references = entries
            .iter()
            .map(|(candidate_id, entry)| (candidate_id.clone(), entry.reference.clone()))
            .collect();
        Ok((references, entries))
    } else if manifest.generation_index == 2 {
        native_v5_g0_parent_references(manifest, &parent_archive)
            .map(|references| (references, BTreeMap::new()))
    } else {
        native_v5_evolved_parent_references(manifest, &parent_archive)
            .map(|references| (references, BTreeMap::new()))
    }
    .context("open typed native v5 evolved parent material")?;
    let parents = RuntimeParentSelector::from_native_v5_archive(
        &parent_archive.value,
        &parent_references,
        &manifest.generation_config_sha256,
        true,
    )
    .context("open typed native v5 evolved parent selector")?;
    let parent_schedule = v5_evolved_effective_parent_schedule(
        configured_parent_schedule,
        parents.eligible_parent_count(),
    )?;

    let input_ledger =
        read_v5_evolved_input_document(manifest, "identityLedger", "identity ledger")?;
    let ledger = if manifest.generation_index == 2 {
        let ledger = V5G0CompactIdentityLedger::from_value(&input_ledger.value)
            .context("parse typed native v5 G0 compact identity ledger for G2")?;
        if ledger
            .identity_ledger_sha256()
            .context("identify typed native v5 G0 compact identity ledger")?
            != input_ledger.semantic_sha256
        {
            bail!(
                "sealed native v5 G0 compact identity ledger semantic identity drifted from its binding"
            );
        }
        ledger
            .restore_candidate_identity_ledger()
            .context("restore typed native v5 G2 candidate identity ledger")?
    } else {
        let ledger = V5EvolvedIdentityLedger::from_value(&input_ledger.value)
            .context("parse typed native v5 evolved identity ledger for G3+")?;
        if ledger
            .identity_ledger_sha256()
            .context("identify typed native v5 evolved identity ledger")?
            != input_ledger.semantic_sha256
        {
            bail!(
                "sealed native v5 evolved identity ledger semantic identity drifted from its binding"
            );
        }
        ledger
            .restore_candidate_identity_ledger()
            .context("restore typed native v5 G3+ candidate identity ledger")?
    };
    let frozen_quotas =
        v5_evolved_frozen_accepted_quotas(&manifest.generation_config, manifest.requested_count)?;
    let request = V5EvolvedTransactionRequest {
        shared_authority: manifest.frozen_authority.clone(),
        generation_config_sha256: manifest.generation_config_sha256.clone(),
        parent_archive_input_binding_sha256: parent_archive.binding_sha256,
        identity_ledger_input_binding_sha256: input_ledger.binding_sha256,
        generation_index: manifest.generation_index,
        target_accepted: manifest.requested_count,
        max_attempts: manifest.max_proposal_attempts,
        evaluation_width: manifest.evaluation_population_size,
        thread_cap: manifest.thread_cap,
        desired_accepted_offspring: frozen_quotas.0,
        desired_accepted_immigrants: frozen_quotas.1,
        parent_schedule,
        parent_selector_state_sha256: canonical_sha256(&parents.compact_state())
            .context("identify native v5 evolved parent selector state")?,
        identity_ledger_identity_sha256: canonical_sha256(ledger.identity())
            .context("identify native v5 evolved identity ledger authority")?,
        identity_ledger_state_sha256: canonical_sha256(&ledger.compact_state())
            .context("identify native v5 evolved identity ledger state")?,
    };
    Ok((
        request,
        parents,
        ledger,
        parent_references,
        prior_parent_material,
    ))
}

fn v5_evolved_transaction_request(
    manifest: &V5ProposalManifest,
) -> Result<(
    V5EvolvedTransactionRequest,
    RuntimeParentSelector,
    temporal_qd_kernel::proposal::CandidateIdentityLedger,
)> {
    let (request, parents, ledger, _, _) =
        v5_evolved_transaction_request_with_parent_references(manifest, false)?;
    Ok((request, parents, ledger))
}

fn v5_fast_ephemeral_evolved_transaction_request(
    manifest: &V5ProposalManifest,
) -> Result<(
    V5EvolvedTransactionRequest,
    RuntimeParentSelector,
    temporal_qd_kernel::proposal::CandidateIdentityLedger,
    BTreeMap<String, v5_fast_ephemeral::ParentMaterialEntry>,
)> {
    let (request, parents, ledger, _parent_references, parent_material) =
        v5_evolved_transaction_request_with_parent_references(manifest, true)?;
    Ok((request, parents, ledger, parent_material))
}

/// Rebuild only the cap-bearing control request needed by the evolved public
/// adoption verifier.  Unlike fresh construction this deliberately never
/// reopens the parent archive or prior public identity-ledger: their transport
/// binding identities are already sealed in both the manifest and typed
/// transaction.  The initial compact selector/ledger identities come from the
/// typed schedule receipt, which is itself part of the authenticated durable
/// closure.
fn v5_evolved_adoption_request(
    manifest: &V5ProposalManifest,
    transaction: &V5EvolvedTransactionResult,
) -> Result<V5EvolvedTransactionRequest> {
    if manifest.generation_kind != "evolved" || manifest.generation_index < 2 {
        bail!("native v5 evolved adoption request requires an evolved generation >= 2");
    }
    let inputs = manifest
        .inputs
        .as_object()
        .ok_or_else(|| anyhow!("sealed native v5 evolved inputs are not an object"))?;
    let input_binding_sha = |key: &str| -> Result<String> {
        inputs
            .get(key)
            .and_then(Value::as_object)
            .and_then(|binding| binding.get("bindingSha256"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow!("sealed native v5 evolved inputs lack {key} bindingSha256"))
    };
    let parent_archive_input_binding_sha256 = input_binding_sha("parentArchive")?;
    let identity_ledger_input_binding_sha256 = input_binding_sha("identityLedger")?;
    if transaction.generation_index != manifest.generation_index
        || transaction.generation_config_sha256 != manifest.generation_config_sha256
        || transaction.parent_archive_input_binding_sha256 != parent_archive_input_binding_sha256
        || transaction.identity_ledger_input_binding_sha256 != identity_ledger_input_binding_sha256
        || transaction.target_accepted != manifest.requested_count
        || transaction.max_attempts != manifest.max_proposal_attempts
        || transaction.evaluation_width != manifest.evaluation_population_size
    {
        bail!("native v5 evolved transaction roots drift from the sealed adoption manifest");
    }
    let schedule = &transaction.schedule_state_receipt;
    let configured_parent_schedule =
        v5_evolved_configured_parent_schedule(manifest.generation_config.get("parentSchedule"))?;
    let parent_schedule = v5_evolved_adopted_parent_schedule(
        configured_parent_schedule,
        schedule.parent_schedule_sha256.as_deref(),
    )?;
    let frozen_quotas =
        v5_evolved_frozen_accepted_quotas(&manifest.generation_config, manifest.requested_count)?;
    let request = V5EvolvedTransactionRequest {
        shared_authority: manifest.frozen_authority.clone(),
        generation_config_sha256: manifest.generation_config_sha256.clone(),
        parent_archive_input_binding_sha256,
        identity_ledger_input_binding_sha256,
        generation_index: manifest.generation_index,
        target_accepted: manifest.requested_count,
        max_attempts: manifest.max_proposal_attempts,
        evaluation_width: manifest.evaluation_population_size,
        // This is invocation telemetry only and remains absent from every
        // core semantic root.  It is still validated by the typed core.
        thread_cap: manifest.thread_cap,
        desired_accepted_offspring: frozen_quotas.0,
        desired_accepted_immigrants: frozen_quotas.1,
        parent_schedule,
        parent_selector_state_sha256: canonical_sha256(&schedule.initial_parent_selector_state)
            .context("identify sealed native v5 evolved initial parent-selector state")?,
        identity_ledger_identity_sha256: canonical_sha256(&schedule.identity_ledger_identity)
            .context("identify sealed native v5 evolved identity-ledger authority")?,
        identity_ledger_state_sha256: canonical_sha256(&schedule.initial_identity_ledger_state)
            .context("identify sealed native v5 evolved initial identity-ledger state")?,
    };
    Ok(request)
}

/// Stage the complete typed later-generation durable closure before any
/// receipt/publication boundary exists.  The evolving core owns the object
/// family/order and the offline replay; batch only writes its canonical
/// values to private file-backed staging.  This intentionally has no outer
/// result or receipt yet: those will be supplied by the separate
/// `v5_evolved_publication` core authority rather than invented here.
#[allow(dead_code)]
fn stage_v5_evolved_durable_objects<O: PublicationIo>(
    ops: &O,
    output_root: &Path,
    staging: &V5PrivateStagingArea,
    request: &V5EvolvedTransactionRequest,
    transaction: &V5EvolvedTransactionResult,
) -> Result<Vec<V5StagedArtifact>> {
    let bindings = transaction
        .durable_object_bindings()
        .context("enumerate typed native v5 evolved durable objects")?;
    let reconstructed = reconstruct_v5_evolved_transaction_from_durable_objects(request, &bindings)
        .context("offline-replay typed native v5 evolved durable objects before staging")?;
    if reconstructed
        .to_value()
        .context("encode replayed native v5 evolved transaction")?
        != transaction
            .to_value()
            .context("encode native v5 evolved transaction for staging")?
    {
        bail!("native v5 evolved durable replay differs from the staged transaction");
    }

    let mut object_sha256s = BTreeSet::new();
    let mut relative_paths = BTreeSet::new();
    let mut staged = Vec::with_capacity(bindings.len());
    let stage_result = (|| -> Result<()> {
        for binding in bindings {
            binding
                .validate()
                .context("validate typed native v5 evolved durable object binding")?;
            if !object_sha256s.insert(binding.object_sha256.clone())
                || !relative_paths.insert(binding.relative_path.clone())
            {
                bail!("native v5 evolved durable object inventory repeats an immutable binding");
            }
            staged.push(stage_v5_canonical_value(
                ops,
                output_root,
                staging,
                &binding.relative_path,
                &binding.value,
                "native v5 staged evolved durable object",
            )?);
        }
        if staged.is_empty() {
            bail!("native v5 evolved durable object inventory is empty");
        }
        Ok(())
    })();
    if let Err(error) = stage_result {
        let cleanup = cleanup_staged_v5_artifacts(ops, &staged);
        return match cleanup {
            Ok(()) => Err(error),
            Err(cleanup) => Err(error.context(format!(
                "native v5 staged evolved durable objects could not be safely removed after staging failure: {cleanup:#}"
            ))),
        };
    }
    Ok(staged)
}

/// Fresh-prepublication deep audit for compact, typed evolved durable objects.
/// It proves private staged bytes form the core's exact ordered closure before
/// the first public link, then runs the core offline replay.  It never opens
/// an input archive, an identity-ledger source file, or a selected population.
///
/// This is deliberately *not* the normal sealed-adoption path: post-receipt
/// adoption must authenticate receipt/inventory/object/public-file bindings
/// through the future evolved-publication verifier without structural replay.
/// The deep replay remains separately available for explicit audit.
#[allow(dead_code)]
fn verify_staged_v5_evolved_durable_objects(
    request: &V5EvolvedTransactionRequest,
    transaction_sha256: &str,
    staged: &[V5StagedArtifact],
) -> Result<V5EvolvedTransactionResult> {
    if staged.is_empty() {
        bail!("native v5 staged evolved durable object inventory is empty");
    }
    let mut staged_by_relative = BTreeMap::new();
    let mut root_value = None;
    for artifact in staged {
        let Some(digest) = artifact
            .relative_path
            .strip_prefix("v5-native/objects/sha256/")
            .and_then(|path| path.strip_suffix(".json"))
        else {
            bail!(
                "native v5 staged evolved durable object escapes the immutable object store: {}",
                artifact.relative_path
            );
        };
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        {
            bail!(
                "native v5 staged evolved durable object path is incompatible: {}",
                artifact.relative_path
            );
        }
        let value = read_staged_v5_document(artifact, "native v5 staged evolved durable object")?;
        if value.get("transactionSha256").and_then(Value::as_str) == Some(transaction_sha256)
            && root_value.replace(value.clone()).is_some()
        {
            bail!(
                "native v5 staged evolved durable object inventory has multiple transaction roots"
            );
        }
        if staged_by_relative
            .insert(artifact.relative_path.clone(), value)
            .is_some()
        {
            bail!("native v5 staged evolved durable object inventory repeats a path");
        }
    }
    let root_value = root_value.ok_or_else(|| {
        anyhow!("native v5 staged evolved durable object inventory lacks its transaction root")
    })?;
    let transaction = V5EvolvedTransactionResult::from_value(&root_value)
        .context("parse native v5 staged evolved transaction root")?;
    if transaction
        .transaction_sha256()
        .context("identify native v5 staged evolved transaction root")?
        != transaction_sha256
    {
        bail!("native v5 staged evolved transaction root identity drifted");
    }
    let expected = transaction
        .durable_object_bindings()
        .context("enumerate native v5 staged evolved durable closure")?;
    if expected.len() != staged_by_relative.len() {
        bail!("native v5 staged evolved durable object inventory has an unexpected object count");
    }
    let mut observed = Vec::with_capacity(expected.len());
    for binding in expected {
        binding
            .validate()
            .context("validate expected native v5 evolved durable object binding")?;
        let value = staged_by_relative
            .remove(&binding.relative_path)
            .ok_or_else(|| {
                anyhow!(
                    "native v5 staged evolved durable object inventory lacks {}",
                    binding.relative_path
                )
            })?;
        let observed_binding = V5EvolvedDurableObjectBinding {
            kind: binding.kind,
            object_sha256: binding.object_sha256.clone(),
            relative_path: binding.relative_path.clone(),
            value,
        };
        observed_binding
            .validate()
            .context("validate staged native v5 evolved durable object binding")?;
        if observed_binding != binding {
            bail!("native v5 staged evolved durable object bytes drift from the typed closure");
        }
        observed.push(observed_binding);
    }
    if !staged_by_relative.is_empty() {
        bail!("native v5 staged evolved durable object inventory has an undeclared object");
    }
    let replayed = reconstruct_v5_evolved_transaction_from_durable_objects(request, &observed)
        .context("offline-replay staged native v5 evolved durable objects")?;
    if replayed
        .to_value()
        .context("encode replayed staged native v5 evolved transaction")?
        != root_value
    {
        bail!("native v5 staged evolved durable replay differs from its transaction root");
    }
    Ok(replayed)
}

fn stage_v5_canonical_value<O: PublicationIo>(
    ops: &O,
    output_root: &Path,
    staging: &V5PrivateStagingArea,
    relative: &str,
    value: &Value,
    label: &str,
) -> Result<V5StagedArtifact> {
    let bytes = canonical_json_line(value).with_context(|| format!("encode {label}"))?;
    stage_v5_relative_bytes_with(ops, output_root, staging, relative, &bytes, label)
}

const V5_FRESH_DURABLE_STAGE_BATCH_PER_WORKER: usize = 4;

fn stage_v5_fresh_durable_batch<O: PublicationIo + Sync>(
    ops: &O,
    output_root: &Path,
    staging: &V5PrivateStagingArea,
    bindings: &[V5G0DurableObjectBinding],
    thread_cap: u64,
    progress: Option<&NativeProgressHandle>,
) -> Result<Vec<V5StagedArtifact>> {
    if bindings.is_empty() {
        return Ok(Vec::new());
    }
    let workers = (thread_cap as usize).min(bindings.len()).max(1);
    let chunk_size = bindings.len().div_ceil(workers);
    let joined = thread::scope(|scope| {
        let mut handles = Vec::new();
        for (chunk_index, chunk) in bindings.chunks(chunk_size).enumerate() {
            handles.push(
                scope.spawn(move || -> Result<Vec<(usize, V5StagedArtifact)>> {
                    if let Some(progress) = progress {
                        progress.worker_started();
                    }
                    let mut staged = Vec::with_capacity(chunk.len());
                    let outcome = (|| -> Result<()> {
                        for (offset, binding) in chunk.iter().enumerate() {
                            let artifact = stage_v5_canonical_value(
                                ops,
                                output_root,
                                staging,
                                &binding.relative_path,
                                &binding.value,
                                "native v5 staged fresh durable object",
                            )?;
                            staged.push((chunk_index * chunk_size + offset, artifact));
                        }
                        Ok(())
                    })();
                    if let Some(progress) = progress {
                        progress.worker_finished();
                    }
                    if let Err(error) = outcome {
                        let cleanup = staged.iter().try_for_each(|(_, artifact)| {
                            discard_staged_v5_artifact(ops, artifact)
                        });
                        return match cleanup {
                            Ok(()) => Err(error),
                            Err(cleanup) => Err(error.context(format!(
                                "native v5 fresh durable staging cleanup failed: {cleanup:#}"
                            ))),
                        };
                    }
                    Ok(staged)
                }),
            );
        }

        let mut staged = Vec::with_capacity(bindings.len());
        let mut first_error = None;
        for handle in handles {
            match handle.join() {
                Ok(Ok(mut artifacts)) => staged.append(&mut artifacts),
                Ok(Err(error)) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
                Err(_) => {
                    if first_error.is_none() {
                        first_error =
                            Some(anyhow!("native v5 fresh durable staging worker panicked"));
                    }
                }
            }
        }
        if let Some(error) = first_error {
            let cleanup = staged
                .iter()
                .try_for_each(|(_, artifact)| discard_staged_v5_artifact(ops, artifact));
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.context(format!(
                    "native v5 staged durable objects could not be removed after worker failure: {cleanup:#}"
                ))),
            };
        }
        staged.sort_by_key(|(index, _)| *index);
        Ok(staged
            .into_iter()
            .map(|(_, artifact)| artifact)
            .collect::<Vec<_>>())
    })?;
    if let Some(progress) = progress {
        for artifact in &joined {
            progress.advance_completed(1);
            progress.add_files(1);
            progress.add_bytes(artifact.digest.byte_length);
        }
    }
    Ok(joined)
}

/// Stage the freshly constructed G0 durable closure in bounded groups.  The
/// typed transaction yields one binding at a time, so a 4,000-candidate run no
/// longer accumulates ~12,000 cloned JSON values before the first file write.
/// Canonical encoding, file write, and fsync are parallelized up to the sealed
/// construction cap while output ordering and all object identities remain
/// unchanged.
fn stage_v5_fresh_g0_durable_objects<O: PublicationIo + Sync>(
    ops: &O,
    output_root: &Path,
    staging: &V5PrivateStagingArea,
    transaction: &V5G0TransactionResult,
    thread_cap: u64,
    progress: Option<&NativeProgressHandle>,
) -> Result<(Vec<V5StagedArtifact>, BTreeMap<String, String>)> {
    if !(1..=8).contains(&thread_cap) {
        bail!("native v5 fresh durable staging thread cap must be in 1..=8");
    }
    ensure_safe_directory_tree(
        &output_root.join("v5-native/objects/sha256"),
        "native v5 durable object target directory",
    )?;
    let expected_count = transaction.fresh_durable_object_count();
    if let Some(progress) = progress {
        progress.begin_phase(
            "staging",
            "stage_durable_object_closure",
            "durable_object",
            Some(expected_count as u64),
            false,
            Some(thread_cap),
            None,
        );
    }
    let batch_width = (thread_cap as usize)
        .saturating_mul(V5_FRESH_DURABLE_STAGE_BATCH_PER_WORKER)
        .max(1);
    let mut pending = Vec::with_capacity(batch_width);
    let mut artifacts = Vec::with_capacity(expected_count);
    let mut roots = BTreeMap::new();
    let stage_result = transaction
        .try_for_each_fresh_durable_object_binding::<anyhow::Error, _>(|binding| {
            if roots
                .insert(
                    binding.kind.as_str().to_owned(),
                    binding.object_sha256.clone(),
                )
                .is_some()
                && !matches!(
                    binding.kind,
                    V5G0DurableObjectKind::AttemptOutcomeAudit
                        | V5G0DurableObjectKind::CompactProposalDelta
                        | V5G0DurableObjectKind::CompactAcceptedRecord
                )
            {
                bail!(
                    "native v5 durable object list repeats a singleton {} binding",
                    binding.kind.as_str()
                );
            }
            pending.push(binding);
            if pending.len() >= batch_width {
                artifacts.extend(stage_v5_fresh_durable_batch(
                    ops,
                    output_root,
                    staging,
                    &pending,
                    thread_cap,
                    progress,
                )?);
                pending.clear();
            }
            Ok(())
        })
        .context("stream fresh native v5 durable object bindings")
        .and_then(|()| {
            if !pending.is_empty() {
                artifacts.extend(stage_v5_fresh_durable_batch(
                    ops,
                    output_root,
                    staging,
                    &pending,
                    thread_cap,
                    progress,
                )?);
            }
            Ok(())
        });
    if let Err(error) = stage_result {
        let cleanup = cleanup_staged_v5_artifacts(ops, &artifacts);
        return match cleanup {
            Ok(()) => Err(error),
            Err(cleanup) => Err(error.context(format!(
                "native v5 prior durable batches could not be removed after staging failure: {cleanup:#}"
            ))),
        };
    }
    if artifacts.len() != expected_count {
        let cleanup = cleanup_staged_v5_artifacts(ops, &artifacts);
        let error = anyhow!(
            "native v5 fresh durable staging count drifted: expected {expected_count}, observed {}",
            artifacts.len()
        );
        return match cleanup {
            Ok(()) => Err(error),
            Err(cleanup) => Err(error.context(format!(
                "native v5 fresh durable count-drift cleanup failed: {cleanup:#}"
            ))),
        };
    }
    Ok((artifacts, roots))
}

fn v5_staged_output_identity(
    kind: &str,
    staged: &V5StagedArtifact,
    semantic_sha256: String,
) -> V5OutputArtifactIdentity {
    V5OutputArtifactIdentity {
        kind: kind.to_owned(),
        relative_path: staged.relative_path.clone(),
        file_sha256: staged.digest.file_sha256.clone(),
        byte_length: staged.digest.byte_length,
        semantic_sha256,
    }
}

fn v5_staged_object_identity(
    object_sha256: String,
    staged: &V5StagedArtifact,
) -> V5ObjectStoreIdentity {
    V5ObjectStoreIdentity {
        object_sha256,
        file_sha256: staged.digest.file_sha256.clone(),
        byte_length: staged.digest.byte_length,
    }
}

/// Convert private staged content-addressed object files into the exact outer
/// inventory identities.  The semantic object SHA is derived only from the
/// fixed core path; no caller-provided alias is accepted.
fn v5_staged_object_identities(staged: &[V5StagedArtifact]) -> Result<Vec<V5ObjectStoreIdentity>> {
    let mut object_sha256s = BTreeSet::new();
    let mut relative_paths = BTreeSet::new();
    staged
        .iter()
        .map(|artifact| {
            let digest = artifact
                .relative_path
                .strip_prefix("v5-native/objects/sha256/")
                .and_then(|value| value.strip_suffix(".json"))
                .ok_or_else(|| {
                    anyhow!(
                        "native v5 staged object path is incompatible: {}",
                        artifact.relative_path
                    )
                })?;
            if digest.len() != 64
                || !digest
                    .bytes()
                    .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
            {
                bail!(
                    "native v5 staged object path has an invalid digest: {}",
                    artifact.relative_path
                );
            }
            let object_sha256 = format!("sha256:{digest}");
            if !object_sha256s.insert(object_sha256.clone())
                || !relative_paths.insert(artifact.relative_path.clone())
            {
                bail!("native v5 staged object inventory repeats a path or semantic identity");
            }
            Ok(v5_staged_object_identity(object_sha256, artifact))
        })
        .collect()
}

fn v5_staged_by_relative<'a>(
    staged: &'a [V5StagedArtifact],
    relative: &str,
    label: &str,
) -> Result<&'a V5StagedArtifact> {
    staged
        .iter()
        .find(|artifact| artifact.relative_path == relative)
        .ok_or_else(|| anyhow!("native v5 staged transaction lacks {label}: {relative}"))
}

fn v5_durable_object_sha(
    roots: &BTreeMap<String, String>,
    kind: V5G0DurableObjectKind,
) -> Result<String> {
    roots.get(kind.as_str()).cloned().ok_or_else(|| {
        anyhow!(
            "native v5 typed transaction lacks durable {} object",
            kind.as_str()
        )
    })
}

fn v5_stage_jsonl_attempts<O: PublicationIo>(
    ops: &O,
    output_root: &Path,
    staging: &V5PrivateStagingArea,
    transaction: &V5G0TransactionResult,
) -> Result<V5StagedArtifact> {
    stage_v5_relative_with(
        ops,
        output_root,
        staging,
        "v5-native/attempts.jsonl",
        "native v5 staged attempt journal rows",
        |file| {
            for attempt in &transaction.attempts {
                let value = attempt
                    .to_value()
                    .context("encode native v5 compact attempt row")?;
                file.write_all(&canonical_json_line(&value)?)
                    .context("write native v5 compact attempt row")?;
            }
            Ok(())
        },
    )
}

fn v5_stage_jsonl_accepted_records<O: PublicationIo>(
    ops: &O,
    output_root: &Path,
    staging: &V5PrivateStagingArea,
    transaction: &V5G0TransactionResult,
) -> Result<V5StagedArtifact> {
    stage_v5_relative_with(
        ops,
        output_root,
        staging,
        "v5-native/accepted-records.jsonl",
        "native v5 staged compact accepted-record journal",
        |file| {
            for record in &transaction.accepted_records {
                let value = record
                    .to_value()
                    .context("encode native v5 compact accepted record")?;
                file.write_all(&canonical_json_line(&value)?)
                    .context("write native v5 compact accepted record")?;
            }
            Ok(())
        },
    )
}

fn v5_stage_jsonl_selected_projections<O: PublicationIo>(
    ops: &O,
    output_root: &Path,
    staging: &V5PrivateStagingArea,
    transaction: &V5G0TransactionResult,
) -> Result<V5StagedArtifact> {
    let index = transaction
        .selected_projection_index
        .as_ref()
        .ok_or_else(|| anyhow!("native v5 G0 transaction lacks selected projection index"))?;
    stage_v5_relative_with(
        ops,
        output_root,
        staging,
        "v5-native/selected-projections.jsonl",
        "native v5 staged selected-projection journal",
        |file| {
            for projection in &index.projections {
                let value = projection
                    .to_value()
                    .context("encode native v5 selected projection")?;
                file.write_all(&canonical_json_line(&value)?)
                    .context("write native v5 selected projection")?;
            }
            Ok(())
        },
    )
}

fn v5_construction_summary(
    transaction: &V5G0TransactionResult,
    public_artifacts: &[V5StagedArtifact],
    object_artifacts: &[V5StagedArtifact],
) -> Result<Value> {
    let mut by_disposition = BTreeMap::<String, u64>::new();
    let mut by_reason = BTreeMap::<String, u64>::new();
    for attempt in &transaction.attempts {
        *by_disposition
            .entry(attempt.disposition.clone())
            .or_default() += 1;
        *by_reason.entry(attempt.reason_code.clone()).or_default() += 1;
    }
    let mut program_sha256s = BTreeSet::new();
    let mut topology_sha256s = BTreeSet::new();
    let mut resource_sha256s = BTreeSet::new();
    for record in &transaction.accepted_records {
        program_sha256s.insert(record.compiled.program_sha256.clone());
        topology_sha256s.insert(record.long.semantic_topology_sha256.clone());
        topology_sha256s.insert(record.short.semantic_topology_sha256.clone());
        resource_sha256s.insert(record.long.resource_fingerprint_sha256.clone());
        resource_sha256s.insert(record.short.resource_fingerprint_sha256.clone());
    }
    let compact_journal_bytes = [
        "v5-native/attempt-journal-root.json",
        "v5-native/attempts.jsonl",
        "v5-native/accepted-records.jsonl",
        "v5-native/identity-ledger.json",
    ]
    .iter()
    .try_fold(0_u64, |total, relative| {
        total
            .checked_add(
                v5_staged_by_relative(public_artifacts, relative, "compact journal")?
                    .digest
                    .byte_length,
            )
            .ok_or_else(|| anyhow!("native v5 compact journal summary byte count overflows"))
    })?;
    let static_authority_bytes = v5_staged_by_relative(
        public_artifacts,
        "v5-native/authority/shared-authority.json",
        "shared authority",
    )?
    .digest
    .byte_length;
    let selected_projection_bytes = v5_staged_by_relative(
        public_artifacts,
        "v5-native/selected-projections.jsonl",
        "selected projection journal",
    )?
    .digest
    .byte_length;
    let object_store_bytes = object_artifacts.iter().try_fold(0_u64, |total, staged| {
        total
            .checked_add(staged.digest.byte_length)
            .ok_or_else(|| anyhow!("native v5 object-store summary byte count overflows"))
    })?;
    Ok(serde_json::json!({
        "schemaVersion": V5_PROPOSAL_CONSTRUCTION_SUMMARY_SCHEMA,
        "bytes": {
            "compactJournalBytes": compact_journal_bytes,
            "staticAuthorityBytes": static_authority_bytes,
            "objectStoreBytes": object_store_bytes,
            "selectedProjectionBytes": selected_projection_bytes,
        },
        "attempts": {
            "byDisposition": by_disposition,
            "byReason": by_reason,
        },
        "uniqueCounts": {
            "candidateCount": transaction.identity_ledger.candidate_identity_sha256s.len() as u64,
            "programCount": program_sha256s.len() as u64,
            "topologyCount": topology_sha256s.len() as u64,
            "resourceCount": resource_sha256s.len() as u64,
        },
    }))
}

/// Deterministic later-generation accounting sealed by the distinct evolved
/// outer receipt.  It contains only facts already present in the typed compact
/// transaction and staged file identities; process/timing/cap telemetry stays
/// in stdout-only adoption evidence.
fn v5_evolved_construction_summary(
    transaction: &V5EvolvedTransactionResult,
    public_artifacts: &[V5StagedArtifact],
    object_artifacts: &[V5StagedArtifact],
) -> Result<Value> {
    let mut by_disposition = BTreeMap::<String, u64>::new();
    let mut by_reason = BTreeMap::<String, u64>::new();
    for attempt in &transaction.attempts {
        *by_disposition
            .entry(attempt.disposition.clone())
            .or_default() += 1;
        *by_reason.entry(attempt.reason_code.clone()).or_default() += 1;
    }
    let mut candidate_identity_sha256s = BTreeSet::new();
    let mut executable_semantic_sha256s = BTreeSet::new();
    let mut pair_identity_sha256s = BTreeSet::new();
    for record in &transaction.accepted_records {
        candidate_identity_sha256s.insert(record.candidate_identity_sha256.clone());
        executable_semantic_sha256s.insert(record.executable_semantic_sha256.clone());
        pair_identity_sha256s.insert(record.pair_identity_sha256.clone());
    }
    let accepted_count = transaction.accepted_records.len();
    if candidate_identity_sha256s.len() != accepted_count
        || executable_semantic_sha256s.len() != accepted_count
        || pair_identity_sha256s.len() != accepted_count
    {
        bail!("native v5 evolved construction has duplicate accepted semantic identities");
    }
    let public_artifact_bytes = public_artifacts.iter().try_fold(0_u64, |total, staged| {
        total.checked_add(staged.digest.byte_length).ok_or_else(|| {
            anyhow!("native v5 evolved public-artifact summary byte count overflows")
        })
    })?;
    let durable_object_bytes = object_artifacts.iter().try_fold(0_u64, |total, staged| {
        total
            .checked_add(staged.digest.byte_length)
            .ok_or_else(|| anyhow!("native v5 evolved durable-object summary byte count overflows"))
    })?;
    Ok(serde_json::json!({
        "schemaVersion": V5_EVOLVED_PROPOSAL_CONSTRUCTION_SUMMARY_SCHEMA,
        "bytes": {
            "durableObjectBytes": durable_object_bytes,
            "publicArtifactBytes": public_artifact_bytes,
        },
        "attempts": {
            "byDisposition": by_disposition,
            "byReason": by_reason,
        },
        "uniqueCounts": {
            "candidateIdentityCount": candidate_identity_sha256s.len() as u64,
            "executableSemanticCount": executable_semantic_sha256s.len() as u64,
            "pairIdentityCount": pair_identity_sha256s.len() as u64,
        },
    }))
}

fn execute_v5_fresh_transaction(
    output_root: &Path,
    invocation_root: &Path,
    result_path: &Path,
    manifest: &V5ProposalManifest,
    validation_mode: V5ValidationMode,
    progress: Option<&NativeProgressHandle>,
) -> Result<V5FreshExecution> {
    let ops = StdPublicationIo;
    let request = v5_g0_transaction_request(manifest)?;
    if let Some(progress) = progress {
        progress.begin_phase(
            "construction",
            "construct_and_admit_g0",
            "accepted_candidate",
            Some(manifest.requested_count),
            true,
            Some(manifest.thread_cap),
            None,
        );
    }
    let construction_started = Instant::now();
    let transaction = execute_v5_g0_transaction_with_progress(request.clone(), progress)
        .context("execute sealed native v5 G0 transaction")?;
    let construction_elapsed = construction_started.elapsed();
    if let Some(progress) = progress {
        progress.begin_phase(
            "post_construction",
            "validate_and_prepare_publication",
            "transaction",
            None,
            false,
            Some(1),
            Some("post_construction_validation_total_not_exposed"),
        );
    }
    let post_construction_started = Instant::now();
    if validation_mode.is_strict() {
        verify_v5_g0_transaction_replay(&request, &transaction)
            .context("replay fresh sealed native v5 G0 transaction")?;
    }
    if !transaction.target_reached
        || transaction.accepted_records.len() as u64 != manifest.requested_count
        || transaction.selected_projection_index.is_none()
        || transaction.accepted_pool.is_none()
        || transaction.campaign_ledger.is_none()
        || transaction.g0_selection.is_none()
    {
        bail!("native v5 typed G0 transaction did not produce a complete publication bundle");
    }
    let stream = prepare_v5_g0_publication_stream_from_fresh_transaction(&request, &transaction)
        .context("prepare sealed native v5 G0 publication stream")?;
    let post_construction_elapsed = post_construction_started.elapsed();

    if let Some(progress) = progress {
        progress.begin_phase(
            "staging",
            "materialize_selected_publication_fragments",
            "selected_candidate",
            Some(stream.selected_count() as u64),
            false,
            Some(manifest.thread_cap),
            None,
        );
    }
    let staging_started = Instant::now();
    let staging = v5_private_staging_area(output_root, invocation_root, &manifest.manifest_sha256)?;
    let mut open_fragments = begin_v5_private_fragment_set(&ops, &staging)?;
    let core_fragments = {
        let mut sink = open_fragments.sink();
        stream
            .materialize_selected_fragments_parallel(manifest.thread_cap, progress, &mut sink)
            .context("materialize native v5 selected publication fragments")
    };
    let core_fragments = match core_fragments {
        Ok(fragments) => fragments,
        Err(error) => {
            let cleanup = open_fragments.discard(&ops);
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.context(format!(
                    "native v5 private fragments could not be removed after materialization failure: {cleanup:#}"
                ))),
            };
        }
    };
    let fragments = open_fragments
        .seal(&ops)
        .context("seal native v5 private publication fragments")?;
    fragments
        .verify_against_core(&core_fragments)
        .context("bind native v5 private fragments to typed core receipt")?;
    let staged_publication = stage_v5_publication_bundle_from_fragments(
        &ops,
        output_root,
        &staging,
        &stream,
        &fragments,
        &core_fragments,
    );
    let (mut public_artifacts, publication_receipt) = match staged_publication {
        Ok(staged) => staged,
        Err(error) => {
            let cleanup = fragments.discard(&ops);
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.context(format!(
                    "native v5 private fragments could not be removed after publication staging failure: {cleanup:#}"
                ))),
            };
        }
    };
    // This compact receipt is the only durable G0 funnel authority.  The
    // builder sees the ephemeral fragment receipt exactly once here; every
    // later prepublication/adoption check validates the persisted binding
    // against the sealed transaction/publication receipt without reopening
    // private fragments or rich candidates.
    let (g0_funnel_binding, g0_funnel_stream_artifact, g0_funnel_stream_receipt) =
        match (|| -> Result<_> {
            let funnel = build_v5_g0_funnel_fragments(
                &request,
                &transaction,
                &core_fragments,
                &publication_receipt,
            )
            .context("build native v5 G0 funnel receipt")?;
            let binding = funnel
                .object_binding()
                .context("bind native v5 G0 funnel receipt object")?;
            binding
                .validate()
                .context("validate native v5 G0 funnel receipt object binding")?;
            let (stream_artifact, stream_receipt) = stage_v5_relative_with_result(
                &ops,
                output_root,
                &staging,
                V5_G0_FUNNEL_PROJECTION_STREAM_PATH,
                "native v5 staged G0 funnel projection stream",
                |file| {
                    write_v5_g0_funnel_projection_stream(
                        &request,
                        &transaction,
                        &publication_receipt,
                        &funnel,
                        file,
                    )
                    .map_err(Into::into)
                },
            )?;
            Ok((binding, stream_artifact, stream_receipt))
        })() {
            Ok(staged) => staged,
            Err(error) => {
                let cleanup = fragments
                    .discard(&ops)
                    .and_then(|()| cleanup_staged_v5_artifacts(&ops, &public_artifacts));
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(error.context(format!(
                        "native v5 fragments/public artifacts could not be removed after G0 funnel receipt failure: {cleanup:#}"
                    ))),
                };
            }
        };
    public_artifacts.push(g0_funnel_stream_artifact);
    // The public documents have been authenticated against the fragment
    // receipts above.  Delete the private rich-element streams before any
    // outer inventory/receipt can be staged or linked; recovery never needs
    // them and uses the no-rich adoption verifier instead.
    fragments
        .discard(&ops)
        .context("remove sealed native v5 private publication fragments")?;
    let g0_funnel_stream_binding = g0_funnel_stream_receipt
        .object_binding()
        .context("bind native v5 G0 funnel projection-stream receipt object")?;
    g0_funnel_stream_binding
        .validate()
        .context("validate native v5 G0 funnel projection-stream receipt object binding")?;

    let mut object_artifacts = Vec::new();
    let compact_staging = (|| -> Result<BTreeMap<String, String>> {
        let attempt_journal_value = transaction
            .attempt_journal
            .to_value()
            .context("encode native v5 attempt-journal root")?;
        public_artifacts.push(stage_v5_canonical_value(
            &ops,
            output_root,
            &staging,
            "v5-native/attempt-journal-root.json",
            &attempt_journal_value,
            "native v5 staged attempt-journal root",
        )?);
        public_artifacts.push(v5_stage_jsonl_attempts(
            &ops,
            output_root,
            &staging,
            &transaction,
        )?);
        public_artifacts.push(v5_stage_jsonl_accepted_records(
            &ops,
            output_root,
            &staging,
            &transaction,
        )?);
        let identity_ledger = transaction
            .identity_ledger
            .to_value()
            .context("encode native v5 identity ledger")?;
        public_artifacts.push(stage_v5_canonical_value(
            &ops,
            output_root,
            &staging,
            "v5-native/identity-ledger.json",
            &identity_ledger,
            "native v5 staged identity ledger",
        )?);
        public_artifacts.push(v5_stage_jsonl_selected_projections(
            &ops,
            output_root,
            &staging,
            &transaction,
        )?);
        public_artifacts.push(stage_v5_canonical_value(
            &ops,
            output_root,
            &staging,
            "v5-native/authority/shared-authority.json",
            &manifest.frozen_authority,
            "native v5 staged shared authority",
        )?);
        for (relative, value, label) in [
            (
                "g0-bootstrap/accepted-pool.json",
                transaction
                    .accepted_pool
                    .as_ref()
                    .expect("checked complete G0 pool"),
                "native v5 staged G0 accepted pool",
            ),
            (
                "g0-bootstrap/campaign-construction-ledger.json",
                transaction
                    .campaign_ledger
                    .as_ref()
                    .expect("checked complete G0 campaign ledger"),
                "native v5 staged G0 campaign ledger",
            ),
            (
                "g0-bootstrap/selection.json",
                transaction
                    .g0_selection
                    .as_ref()
                    .expect("checked complete G0 selection"),
                "native v5 staged G0 selection",
            ),
        ] {
            public_artifacts.push(stage_v5_canonical_value(
                &ops,
                output_root,
                &staging,
                relative,
                value,
                label,
            )?);
        }

        let (mut durable_artifacts, roots) = stage_v5_fresh_g0_durable_objects(
            &ops,
            output_root,
            &staging,
            &transaction,
            manifest.thread_cap,
            progress,
        )
        .context("stage sealed native v5 fresh durable object closure")?;
        object_artifacts.reserve(durable_artifacts.len() + 3);
        object_artifacts.append(&mut durable_artifacts);
        let g0_funnel_binding_value = g0_funnel_binding
            .to_value()
            .context("encode native v5 G0 funnel receipt object binding")?;
        if object_artifacts
            .iter()
            .any(|artifact| artifact.relative_path == g0_funnel_binding.relative_path)
        {
            bail!("native v5 G0 funnel receipt object aliases a durable object");
        }
        object_artifacts.push(stage_v5_canonical_value(
            &ops,
            output_root,
            &staging,
            &g0_funnel_binding.relative_path,
            &g0_funnel_binding_value,
            "native v5 staged G0 funnel receipt object binding",
        )?);
        let g0_funnel_stream_binding_value = g0_funnel_stream_binding
            .to_value()
            .context("encode native v5 G0 funnel projection-stream receipt object binding")?;
        if object_artifacts
            .iter()
            .any(|artifact| artifact.relative_path == g0_funnel_stream_binding.relative_path)
        {
            bail!("native v5 G0 funnel projection-stream receipt object aliases another object");
        }
        object_artifacts.push(stage_v5_canonical_value(
            &ops,
            output_root,
            &staging,
            &g0_funnel_stream_binding.relative_path,
            &g0_funnel_stream_binding_value,
            "native v5 staged G0 funnel projection-stream receipt object binding",
        )?);
        let receipt_value = publication_receipt
            .to_value()
            .context("encode native v5 publication receipt object")?;
        let receipt_binding = publication_receipt
            .object_binding()
            .context("bind native v5 publication receipt object")?;
        let receipt_sha = publication_receipt
            .publication_receipt_sha256()
            .context("identify native v5 publication receipt object")?;
        if receipt_binding.publication_receipt_sha256 != receipt_sha {
            bail!("native v5 publication receipt object binding identity drifted");
        }
        object_artifacts.push(stage_v5_canonical_value(
            &ops,
            output_root,
            &staging,
            &receipt_binding.relative_path,
            &receipt_value,
            "native v5 staged publication receipt object",
        )?);
        Ok(roots)
    })();
    let durable_roots = match compact_staging {
        Ok(roots) => roots,
        Err(error) => {
            let cleanup = cleanup_staged_v5_artifacts(&ops, &public_artifacts)
                .and_then(|()| cleanup_staged_v5_artifacts(&ops, &object_artifacts));
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.context(format!(
                    "native v5 staged public artifacts could not be removed after compact staging failure: {cleanup:#}"
                ))),
            };
        }
    };
    let publication_plan_sha256 = transaction
        .publication_plan_sha256()
        .context("identify native v5 publication plan")?;
    let publication_request_sha256 = publication_receipt.publication_request_sha256.clone();
    if publication_receipt.publication_plan_sha256 != publication_plan_sha256 {
        bail!("native v5 publication receipt plan binding drifted");
    }
    let attempt_journal_sha256 = transaction
        .attempt_journal
        .attempt_journal_sha256()
        .context("identify native v5 attempt journal")?;
    let compact_journal_sha256 = transaction
        .compact_accepted_journal
        .compact_journal_sha256()
        .context("identify native v5 compact accepted journal")?;
    let identity_ledger_sha256 = transaction
        .identity_ledger
        .identity_ledger_sha256()
        .context("identify native v5 identity ledger")?;
    let selected_projection_index_sha256 = transaction
        .selected_projection_index
        .as_ref()
        .ok_or_else(|| anyhow!("native v5 G0 transaction lacks selected projection index"))?
        .selected_projection_index_sha256()
        .context("identify native v5 selected projection index")?;
    let g0_funnel_projection_stream_receipt_sha256 = g0_funnel_stream_receipt
        .projection_stream_receipt_sha256()
        .context("identify native v5 G0 funnel projection-stream receipt")?;
    if g0_funnel_stream_receipt.input_g0_funnel_fragments_sha256
        != g0_funnel_binding.g0_funnel_fragments_sha256
        || g0_funnel_stream_binding.g0_funnel_projection_stream_receipt_sha256
            != g0_funnel_projection_stream_receipt_sha256
    {
        bail!("native v5 G0 funnel projection-stream input root drifted");
    }
    if v5_durable_object_sha(&durable_roots, V5G0DurableObjectKind::PublicationPlan)?
        != publication_plan_sha256
        || v5_durable_object_sha(&durable_roots, V5G0DurableObjectKind::AttemptJournal)?
            != attempt_journal_sha256
        || v5_durable_object_sha(
            &durable_roots,
            V5G0DurableObjectKind::CompactAcceptedJournal,
        )? != compact_journal_sha256
        || v5_durable_object_sha(&durable_roots, V5G0DurableObjectKind::IdentityLedger)?
            != identity_ledger_sha256
        || v5_durable_object_sha(
            &durable_roots,
            V5G0DurableObjectKind::SelectedProjectionIndex,
        )? != selected_projection_index_sha256
    {
        let cleanup = cleanup_staged_v5_artifacts(&ops, &public_artifacts)
            .and_then(|()| cleanup_staged_v5_artifacts(&ops, &object_artifacts));
        if let Err(cleanup) = cleanup {
            return Err(anyhow!(
                "native v5 public roots do not resolve to their durable objects; cleanup failed: {cleanup:#}"
            ));
        }
        bail!("native v5 public roots do not resolve to their durable objects");
    }

    let output_artifacts = vec![
        v5_staged_output_identity(
            "attemptJournal",
            v5_staged_by_relative(
                &public_artifacts,
                "v5-native/attempt-journal-root.json",
                "attempt journal",
            )?,
            attempt_journal_sha256.clone(),
        ),
        v5_staged_output_identity(
            "attemptRows",
            v5_staged_by_relative(
                &public_artifacts,
                "v5-native/attempts.jsonl",
                "attempt rows",
            )?,
            attempt_journal_sha256.clone(),
        ),
        v5_staged_output_identity(
            "compactJournal",
            v5_staged_by_relative(
                &public_artifacts,
                "v5-native/accepted-records.jsonl",
                "compact journal",
            )?,
            compact_journal_sha256.clone(),
        ),
        v5_staged_output_identity(
            "identityLedger",
            v5_staged_by_relative(
                &public_artifacts,
                "v5-native/identity-ledger.json",
                "identity ledger",
            )?,
            identity_ledger_sha256.clone(),
        ),
        v5_staged_output_identity(
            "selectedProjectionIndex",
            v5_staged_by_relative(
                &public_artifacts,
                "v5-native/selected-projections.jsonl",
                "selected projection index",
            )?,
            selected_projection_index_sha256.clone(),
        ),
        v5_staged_output_identity(
            "sharedAuthority",
            v5_staged_by_relative(
                &public_artifacts,
                "v5-native/authority/shared-authority.json",
                "shared authority",
            )?,
            manifest.expected_authority_sha256.clone(),
        ),
        v5_staged_output_identity(
            "g0FunnelProjectionStream",
            v5_staged_by_relative(
                &public_artifacts,
                V5_G0_FUNNEL_PROJECTION_STREAM_PATH,
                "G0 funnel projection stream",
            )?,
            g0_funnel_projection_stream_receipt_sha256.clone(),
        ),
        v5_staged_output_identity(
            "pairConfig",
            v5_staged_by_relative(&public_artifacts, "pair-config.json", "pair config")?,
            publication_receipt.pair_config.semantic_sha256.clone(),
        ),
        v5_staged_output_identity(
            "population",
            v5_staged_by_relative(&public_artifacts, "population.json", "population")?,
            publication_receipt.population.semantic_sha256.clone(),
        ),
        v5_staged_output_identity(
            "evaluationPopulation",
            v5_staged_by_relative(
                &public_artifacts,
                "evaluation-population.json",
                "evaluation population",
            )?,
            publication_receipt
                .evaluation_population
                .semantic_sha256
                .clone(),
        ),
        v5_staged_output_identity(
            "generationJournal",
            v5_staged_by_relative(
                &public_artifacts,
                "generation-journal.json",
                "generation journal",
            )?,
            publication_receipt
                .generation_journal
                .semantic_sha256
                .clone(),
        ),
        v5_staged_output_identity(
            "g0AcceptedPool",
            v5_staged_by_relative(
                &public_artifacts,
                "g0-bootstrap/accepted-pool.json",
                "G0 accepted pool",
            )?,
            v5_durable_object_sha(&durable_roots, V5G0DurableObjectKind::AcceptedPool)?,
        ),
        v5_staged_output_identity(
            "g0CampaignConstructionLedger",
            v5_staged_by_relative(
                &public_artifacts,
                "g0-bootstrap/campaign-construction-ledger.json",
                "G0 campaign ledger",
            )?,
            v5_durable_object_sha(&durable_roots, V5G0DurableObjectKind::CampaignLedger)?,
        ),
        v5_staged_output_identity(
            "g0Selection",
            v5_staged_by_relative(
                &public_artifacts,
                "g0-bootstrap/selection.json",
                "G0 selection",
            )?,
            v5_durable_object_sha(&durable_roots, V5G0DurableObjectKind::G0Selection)?,
        ),
    ];
    let object_identities = object_artifacts
        .iter()
        .map(|staged| {
            let digest = staged
                .relative_path
                .strip_prefix("v5-native/objects/sha256/")
                .and_then(|value| value.strip_suffix(".json"))
                .ok_or_else(|| {
                    anyhow!(
                        "native v5 staged object path is incompatible: {}",
                        staged.relative_path
                    )
                })?;
            Ok(v5_staged_object_identity(
                format!("sha256:{digest}"),
                staged,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let construction_summary =
        v5_construction_summary(&transaction, &public_artifacts, &object_artifacts)?;
    let receipt_input = V5ProposalReceiptBuildInput {
        attempt_count: transaction.attempts.len() as u64,
        attempt_journal_sha256,
        publication_request_sha256,
        publication_plan_sha256,
        g0_funnel_fragments_sha256: g0_funnel_binding.g0_funnel_fragments_sha256.clone(),
        g0_funnel_projection_stream_receipt_sha256,
        compact_journal_sha256,
        identity_ledger_sha256,
        selected_projection_index_sha256,
        construction_summary,
        artifacts: output_artifacts,
        objects: object_identities,
    };
    let (object_inventory_sidecar, outer_receipt_value, outer_result) =
        build_v5_proposal_receipt_and_result(manifest, &receipt_input)
            .context("build native v5 immutable receipt/result")?;
    let staged_inventory = stage_v5_relative_with(
        &ops,
        output_root,
        &staging,
        V5_OUTPUT_INVENTORY_PATH,
        "native v5 staged object inventory",
        |writer| object_inventory_sidecar.write_to(writer),
    )?;
    let staged_receipt = stage_v5_canonical_value(
        &ops,
        output_root,
        &staging,
        V5_OUTPUT_RECEIPT_PATH,
        &outer_receipt_value,
        "native v5 staged output receipt",
    )?;
    let invocation_result = canonical_json_line(&outer_result.value)
        .context("encode native v5 immutable invocation result")?;
    let mut all_artifacts = public_artifacts;
    all_artifacts.extend(object_artifacts);
    let bytes = v5_expected_adoption_bytes(&outer_result.value)?;
    if let Some(progress) = progress {
        progress.begin_phase(
            "prepublication_and_publication",
            "validate_then_receipt_last_commit",
            "artifact",
            None,
            false,
            Some(manifest.thread_cap),
            Some("validation_and_publication_have_no_single_trustworthy_total"),
        );
    }
    let publish = validate_and_publish_v5_staged_receipt_last_with(
        &ops,
        output_root,
        manifest,
        &outer_result,
        &all_artifacts,
        &staged_inventory,
        &staged_receipt,
        result_path,
        &invocation_result,
        validation_mode,
        |bundle| {
            if validation_mode.is_strict() {
                v5_verify_staged_typed_bundle(bundle, manifest, &outer_result, &request)
            } else {
                v5_verify_staged_fresh_g0_bundle(
                    bundle,
                    manifest,
                    &outer_result,
                    &request,
                    &transaction,
                    &publication_receipt,
                    &g0_funnel_binding,
                    &g0_funnel_stream_binding,
                )
            }
        },
    )
    .context("publish native v5 receipt-last transaction")?;
    let staging_elapsed = staging_started
        .elapsed()
        .saturating_sub(publish.validation)
        .saturating_sub(publish.publication);
    Ok(V5FreshExecution {
        bytes,
        phases: V5PhaseDurations {
            construction: construction_elapsed,
            post_construction: post_construction_elapsed,
            staging: staging_elapsed,
            prepublication_validation: publish.validation,
            publication: publish.publication,
            ..V5PhaseDurations::default()
        },
        passes: V5ValidationPasses {
            constructor_replay: 0,
            redundant_fresh_replay: u64::from(validation_mode.is_strict()),
            publication_prepare_replay: 0,
            staged_semantic_replay: 1,
            staged_final_rehash: u64::from(validation_mode.is_strict()),
            ..V5ValidationPasses::default()
        },
    })
}

/// Fresh later-generation construction.  All core semantics are produced by
/// the evolved transaction/publication authorities; this layer only owns
/// authenticated input transport, bounded private files, and the outer
/// receipt-last publication protocol.
fn execute_v5_evolved_fresh_transaction(
    output_root: &Path,
    invocation_root: &Path,
    result_path: &Path,
    manifest: &V5ProposalManifest,
    validation_mode: V5ValidationMode,
    progress: Option<&NativeProgressHandle>,
) -> Result<V5FreshExecution> {
    let ops = StdPublicationIo;
    let (request, mut parents, mut identity_ledger) = v5_evolved_transaction_request(manifest)?;
    if let Some(progress) = progress {
        progress.begin_phase(
            "construction",
            "construct_and_admit_evolved",
            "accepted_candidate",
            Some(manifest.requested_count),
            true,
            Some(manifest.thread_cap),
            None,
        );
    }
    let construction_started = Instant::now();
    let transaction = execute_v5_evolved_transaction_with_progress(
        request.clone(),
        &mut parents,
        &mut identity_ledger,
        progress,
    )
    .context("execute sealed native v5 evolved transaction")?;
    if validation_mode.is_strict() {
        transaction
            .verify_replay()
            .context("validate fresh sealed native v5 evolved transaction")?;
    }
    let construction_elapsed = construction_started.elapsed();
    if !transaction.target_reached
        || transaction.accepted_records.len() as u64 != manifest.requested_count
        || transaction.attempts.len() as u64 > manifest.max_proposal_attempts
    {
        bail!("native v5 typed evolved transaction did not produce a complete publication bundle");
    }
    let transaction_sha256 = transaction
        .transaction_sha256()
        .context("identify native v5 evolved transaction")?;
    if let Some(progress) = progress {
        progress.begin_phase(
            "staging",
            "materialize_and_hash_evolved_publication_bundle",
            "artifact",
            None,
            false,
            Some(manifest.thread_cap),
            Some("staging_artifact_total_finalized_after_streaming"),
        );
    }
    let staging_started = Instant::now();
    let staging = v5_private_staging_area(output_root, invocation_root, &manifest.manifest_sha256)?;

    // A fresh bundle must prove the complete typed durable closure before the
    // selected streaming traversal and well before any output-root link.
    let mut object_artifacts =
        stage_v5_evolved_durable_objects(&ops, output_root, &staging, &request, &transaction)?;
    let replayed = match verify_staged_v5_evolved_durable_objects(
        &request,
        &transaction_sha256,
        &object_artifacts,
    ) {
        Ok(transaction) => transaction,
        Err(error) => {
            let cleanup = cleanup_staged_v5_artifacts(&ops, &object_artifacts);
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.context(format!(
                    "native v5 staged evolved durable objects could not be safely removed after replay failure: {cleanup:#}"
                ))),
            };
        }
    };
    if replayed
        .to_value()
        .context("encode replayed native v5 evolved transaction")?
        != transaction
            .to_value()
            .context("encode fresh native v5 evolved transaction")?
    {
        let cleanup = cleanup_staged_v5_artifacts(&ops, &object_artifacts);
        if let Err(cleanup) = cleanup {
            return Err(anyhow!(
                "native v5 staged evolved durable replay differs from fresh transaction; cleanup failed: {cleanup:#}"
            ));
        }
        bail!("native v5 staged evolved durable replay differs from fresh transaction");
    }

    let final_newline = Value::String(manifest.final_newline.clone());
    let publication_inputs = V5EvolvedPublicationInputs::from_manifest_values(
        &manifest.generation_config,
        &final_newline,
        &manifest.execution_authority,
        &manifest.inputs,
    )
    .context("parse sealed native v5 evolved publication inputs")?;
    let publication_plan = V5EvolvedPublicationPlan::derive(&request, &publication_inputs)
        .context("derive sealed native v5 evolved publication plan")?;
    let stream = prepare_v5_evolved_publication_stream(&request, &transaction, &publication_plan)
        .context("prepare sealed native v5 evolved publication stream")?;
    if stream.accepted_count() as u64 != manifest.requested_count {
        let cleanup = cleanup_staged_v5_artifacts(&ops, &object_artifacts);
        if let Err(cleanup) = cleanup {
            return Err(anyhow!(
                "native v5 evolved publication accepted count drifted; cleanup failed: {cleanup:#}"
            ));
        }
        bail!("native v5 evolved publication accepted count drifts from the manifest");
    }

    // Exactly one selected-material traversal enters the four file-backed
    // fragments.  Batch never receives a rich candidate or population Vec.
    let mut open_fragments = match begin_v5_private_fragment_set(&ops, &staging) {
        Ok(open) => open,
        Err(error) => {
            let cleanup = cleanup_staged_v5_artifacts(&ops, &object_artifacts);
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.context(format!(
                    "native v5 staged evolved durable objects could not be safely removed after fragment allocation failure: {cleanup:#}"
                ))),
            };
        }
    };
    let core_fragments = {
        let mut sink = open_fragments.sink();
        stream
            .materialize_accepted_fragments(&mut sink)
            .context("materialize native v5 evolved accepted publication fragments")
    };
    let core_fragments = match core_fragments {
        Ok(fragments) => fragments,
        Err(error) => {
            let fragment_cleanup = open_fragments.discard(&ops);
            let object_cleanup = cleanup_staged_v5_artifacts(&ops, &object_artifacts);
            return match (fragment_cleanup, object_cleanup) {
                (Ok(()), Ok(())) => Err(error),
                (fragments, objects) => Err(error.context(format!(
                    "native v5 private evolved fragments/durable objects could not be safely removed after materialization failure: fragments={fragments:?}; objects={objects:?}"
                ))),
            };
        }
    };
    let fragments = match open_fragments.seal(&ops) {
        Ok(fragments) => fragments,
        Err(error) => {
            let cleanup = cleanup_staged_v5_artifacts(&ops, &object_artifacts);
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.context(format!(
                    "native v5 staged evolved durable objects could not be safely removed after fragment seal failure: {cleanup:#}"
                ))),
            };
        }
    };
    if let Err(error) = fragments.verify_against_evolved_core(
        &core_fragments,
        stream.accepted_count() as u64,
        stream.proposal_attempt_count() as u64,
    ) {
        let fragment_cleanup = fragments.discard(&ops);
        let object_cleanup = cleanup_staged_v5_artifacts(&ops, &object_artifacts);
        return match (fragment_cleanup, object_cleanup) {
            (Ok(()), Ok(())) => Err(error),
            (fragments, objects) => Err(error.context(format!(
                "native v5 private evolved fragments/durable objects could not be safely removed after fragment binding failure: fragments={fragments:?}; objects={objects:?}"
            ))),
        };
    }
    // The core-owned v2 fragment receipt is the only compact object that
    // covers every proposal attempt, including rejected/no-op rows that are
    // intentionally absent from the three accepted-only fragment streams.
    // Persist the exact typed binding now; the outer evolved receipt roots its
    // identity later, while Python exposes only this immutable descriptor.
    let (publication_fragments_sha256, publication_fragments_value) =
        match (|| -> Result<(String, Value)> {
            let binding = core_fragments
                .object_binding()
                .context("bind native v5 evolved publication-fragments receipt object")?;
            binding
                .validate()
                .context("validate native v5 evolved publication-fragments receipt object")?;
            let _ = binding
                .to_value()
                .context("encode native v5 evolved publication-fragments object binding")?;
            if binding.fragment_bundle_sha256
                != core_fragments
                    .fragment_bundle_sha256()
                    .context("identify native v5 evolved publication-fragments receipt")?
                || binding.value
                    != core_fragments
                        .to_value()
                        .context("encode native v5 evolved publication-fragments receipt")?
            {
                bail!("native v5 evolved publication-fragments object binding drifted");
            }
            if object_artifacts
                .iter()
                .any(|staged| staged.relative_path == binding.relative_path)
            {
                bail!(
                    "native v5 evolved publication-fragments object aliases a durable object: {}",
                    binding.relative_path
                );
            }
            object_artifacts.push(stage_v5_canonical_value(
                &ops,
                output_root,
                &staging,
                &binding.relative_path,
                &binding.value,
                "native v5 staged evolved publication-fragments receipt object",
            )?);
            Ok((binding.fragment_bundle_sha256, binding.value))
        })() {
            Ok(binding) => binding,
            Err(error) => {
                let fragment_cleanup = fragments.discard(&ops);
                let object_cleanup = cleanup_staged_v5_artifacts(&ops, &object_artifacts);
                return match (fragment_cleanup, object_cleanup) {
                    (Ok(()), Ok(())) => Err(error),
                    (fragments, objects) => Err(error.context(format!(
                        "native v5 private evolved fragments/durable objects could not be safely removed after fragment-receipt staging failure: fragments={fragments:?}; objects={objects:?}"
                    ))),
                };
            }
        };
    let staged_publication = stage_v5_evolved_publication_bundle_from_fragments(
        &ops,
        output_root,
        &staging,
        &stream,
        &fragments,
        &core_fragments,
    );
    let (public_artifacts, publication_receipt) = match staged_publication {
        Ok(staged) => staged,
        Err(error) => {
            let fragment_cleanup = fragments.discard(&ops);
            let object_cleanup = cleanup_staged_v5_artifacts(&ops, &object_artifacts);
            return match (fragment_cleanup, object_cleanup) {
                (Ok(()), Ok(())) => Err(error),
                (fragments, objects) => Err(error.context(format!(
                    "native v5 private evolved fragments/durable objects could not be safely removed after publication staging failure: fragments={fragments:?}; objects={objects:?}"
                ))),
            };
        }
    };
    // Fragment bytes are private and must be gone before an inventory, receipt,
    // or output-root artifact can become eligible for publication.
    if let Err(error) = fragments.discard(&ops) {
        let cleanup = cleanup_staged_v5_artifacts(&ops, &public_artifacts)
            .and_then(|()| cleanup_staged_v5_artifacts(&ops, &object_artifacts));
        return match cleanup {
            Ok(()) => Err(error),
            Err(cleanup) => Err(error.context(format!(
                "native v5 staged evolved artifacts could not be safely removed after private-fragment cleanup failure: {cleanup:#}"
            ))),
        };
    }

    let publication_plan_sha256 = publication_plan
        .publication_plan_sha256()
        .context("identify native v5 evolved publication plan")?;
    let publication_receipt_sha256 = publication_receipt
        .publication_receipt_sha256()
        .context("identify native v5 evolved publication receipt")?;
    if publication_receipt.publication_plan_sha256 != publication_plan_sha256 {
        let cleanup = cleanup_staged_v5_artifacts(&ops, &public_artifacts)
            .and_then(|()| cleanup_staged_v5_artifacts(&ops, &object_artifacts));
        if let Err(cleanup) = cleanup {
            return Err(anyhow!(
                "native v5 evolved publication receipt plan binding drifted; cleanup failed: {cleanup:#}"
            ));
        }
        bail!("native v5 evolved publication receipt plan binding drifted");
    }
    let publication_plan_value = publication_plan
        .to_value()
        .context("encode native v5 evolved publication plan object")?;
    let publication_plan_binding = publication_plan
        .object_binding()
        .context("bind native v5 evolved publication plan object")?;
    let publication_receipt_value = publication_receipt
        .to_value()
        .context("encode native v5 evolved publication receipt object")?;
    let publication_receipt_binding = publication_receipt
        .object_binding()
        .context("bind native v5 evolved publication receipt object")?;
    let stage_publication_objects = (|| -> Result<()> {
        if publication_plan_binding.publication_plan_sha256 != publication_plan_sha256
            || publication_receipt_binding.publication_receipt_sha256 != publication_receipt_sha256
        {
            bail!("native v5 evolved publication object binding identity drifted");
        }
        // Call the tiny typed binding serializers as an explicit path/hash
        // validation before using their paths in batch staging.
        let _ = publication_plan_binding
            .to_value()
            .context("validate native v5 evolved publication plan object binding")?;
        let _ = publication_receipt_binding
            .to_value()
            .context("validate native v5 evolved publication receipt object binding")?;
        for (relative, value, label) in [
            (
                publication_plan_binding.relative_path.as_str(),
                &publication_plan_value,
                "native v5 staged evolved publication plan object",
            ),
            (
                publication_receipt_binding.relative_path.as_str(),
                &publication_receipt_value,
                "native v5 staged evolved publication receipt object",
            ),
        ] {
            if object_artifacts
                .iter()
                .any(|staged| staged.relative_path == relative)
            {
                bail!("native v5 evolved immutable object path repeats: {relative}");
            }
            object_artifacts.push(stage_v5_canonical_value(
                &ops,
                output_root,
                &staging,
                relative,
                value,
                label,
            )?);
        }
        Ok(())
    })();
    if let Err(error) = stage_publication_objects {
        let cleanup = cleanup_staged_v5_artifacts(&ops, &public_artifacts)
            .and_then(|()| cleanup_staged_v5_artifacts(&ops, &object_artifacts));
        return match cleanup {
            Ok(()) => Err(error),
            Err(cleanup) => Err(error.context(format!(
                "native v5 staged evolved artifacts could not be safely removed after publication-object staging failure: {cleanup:#}"
            ))),
        };
    }

    let staging_result = (|| -> Result<(V5EvolvedProposalResult, V5StagedArtifact, V5StagedArtifact, Vec<u8>)> {
        let output_artifacts = vec![
            v5_staged_output_identity(
                "pairConfig",
                v5_staged_by_relative(&public_artifacts, "pair-config.json", "evolved pair config")?,
                publication_receipt.pair_config.semantic_sha256.clone(),
            ),
            v5_staged_output_identity(
                "identityLedger",
                v5_staged_by_relative(
                    &public_artifacts,
                    V5_EVOLVED_IDENTITY_LEDGER_RELATIVE_PATH,
                    "evolved identity ledger",
                )?,
                publication_receipt.identity_ledger.semantic_sha256.clone(),
            ),
            v5_staged_output_identity(
                "population",
                v5_staged_by_relative(&public_artifacts, "population.json", "evolved population")?,
                publication_receipt.population.semantic_sha256.clone(),
            ),
            v5_staged_output_identity(
                "evaluationPopulation",
                v5_staged_by_relative(
                    &public_artifacts,
                    "evaluation-population.json",
                    "evolved evaluation population",
                )?,
                publication_receipt.evaluation_population.semantic_sha256.clone(),
            ),
            v5_staged_output_identity(
                "generationJournal",
                v5_staged_by_relative(
                    &public_artifacts,
                    "generation-journal.json",
                    "evolved generation journal",
                )?,
                publication_receipt.generation_journal.semantic_sha256.clone(),
            ),
        ];
        let construction_summary =
            v5_evolved_construction_summary(&transaction, &public_artifacts, &object_artifacts)?;
        let receipt_input = V5EvolvedProposalReceiptBuildInput {
            attempt_count: transaction.attempts.len() as u64,
            transaction_sha256: transaction_sha256.clone(),
            parent_archive_input_binding_sha256: transaction
                .parent_archive_input_binding_sha256
                .clone(),
            identity_ledger_input_binding_sha256: transaction
                .identity_ledger_input_binding_sha256
                .clone(),
            publication_request_sha256: publication_receipt.publication_request_sha256.clone(),
            publication_plan_sha256: publication_plan_sha256.clone(),
            publication_receipt_sha256: publication_receipt_sha256.clone(),
            publication_fragments_sha256: publication_fragments_sha256.clone(),
            identity_ledger_sha256: publication_receipt.identity_ledger.semantic_sha256.clone(),
            construction_summary,
            artifacts: output_artifacts,
            objects: v5_staged_object_identities(&object_artifacts)?,
        };
        let (object_inventory_sidecar, outer_receipt_value, outer_result) =
            build_v5_evolved_proposal_receipt_and_result(manifest, &receipt_input)
                .context("build native v5 evolved immutable receipt/result")?;
        let staged_inventory = stage_v5_relative_with(
            &ops,
            output_root,
            &staging,
            V5_OUTPUT_INVENTORY_PATH,
            "native v5 staged evolved object inventory",
            |writer| object_inventory_sidecar.write_to(writer),
        )?;
        let staged_receipt = match stage_v5_canonical_value(
            &ops,
            output_root,
            &staging,
            V5_OUTPUT_RECEIPT_PATH,
            &outer_receipt_value,
            "native v5 staged evolved output receipt",
        ) {
            Ok(staged) => staged,
            Err(error) => {
                let cleanup = discard_staged_v5_artifact(&ops, &staged_inventory);
                return match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(error.context(format!(
                        "native v5 staged evolved output inventory could not be safely removed after receipt staging failure: {cleanup:#}"
                    ))),
                };
            }
        };
        let invocation_result = canonical_json_line(&outer_result.value)
            .context("encode native v5 evolved immutable invocation result")?;
        Ok((outer_result, staged_inventory, staged_receipt, invocation_result))
    })();
    let (outer_result, staged_inventory, staged_receipt, invocation_result) = match staging_result {
        Ok(staged) => staged,
        Err(error) => {
            let cleanup = cleanup_staged_v5_artifacts(&ops, &public_artifacts)
                .and_then(|()| cleanup_staged_v5_artifacts(&ops, &object_artifacts));
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.context(format!(
                    "native v5 staged evolved artifacts could not be safely removed after outer seal staging failure: {cleanup:#}"
                ))),
            };
        }
    };
    let mut all_artifacts = public_artifacts;
    all_artifacts.append(&mut object_artifacts);
    let bytes = v5_expected_adoption_bytes(&outer_result.value)?;
    if let Some(progress) = progress {
        progress.begin_phase(
            "prepublication_and_publication",
            "validate_then_receipt_last_commit",
            "artifact",
            None,
            false,
            Some(manifest.thread_cap),
            Some("validation_and_publication_have_no_single_trustworthy_total"),
        );
    }
    let publish = validate_and_publish_v5_evolved_staged_receipt_last_with(
        &ops,
        output_root,
        manifest,
        &outer_result,
        &all_artifacts,
        &staged_inventory,
        &staged_receipt,
        result_path,
        &invocation_result,
        validation_mode,
        |bundle| {
            v5_verify_staged_evolved_typed_bundle(
                bundle,
                manifest,
                &outer_result,
                &request,
                &transaction_sha256,
                &publication_plan_value,
                &publication_receipt_value,
                &publication_fragments_value,
            )
        },
    )
    .context("publish native v5 evolved receipt-last transaction")?;
    let staging_elapsed = staging_started
        .elapsed()
        .saturating_sub(publish.validation)
        .saturating_sub(publish.publication);
    Ok(V5FreshExecution {
        bytes,
        phases: V5PhaseDurations {
            construction: construction_elapsed,
            staging: staging_elapsed,
            prepublication_validation: publish.validation,
            publication: publish.publication,
            ..V5PhaseDurations::default()
        },
        passes: V5ValidationPasses {
            constructor_replay: 1,
            redundant_fresh_replay: u64::from(validation_mode.is_strict()),
            publication_prepare_replay: 1,
            staged_semantic_replay: 1,
            staged_final_rehash: u64::from(validation_mode.is_strict()),
            ..V5ValidationPasses::default()
        },
    })
}

fn execute_v5_evolved_proposal(
    output_root: &Path,
    invocation_root: &Path,
    result_path: &Path,
    manifest: &V5ProposalManifest,
    started: Instant,
    process_cpu_started: Option<Duration>,
    static_authority_elapsed: Duration,
    validation_mode: V5ValidationMode,
    progress: NativeProgress,
) -> Result<()> {
    let progress_handle = progress.handle();
    if manifest.generation_kind != "evolved" || manifest.generation_index < 2 {
        bail!("native v5 evolved proposal execution requires generation index at least two");
    }
    let mut phases = V5PhaseDurations {
        static_authority: static_authority_elapsed,
        ..V5PhaseDurations::default()
    };
    let mut io = V5IoTelemetry::default();
    let mut passes = V5ValidationPasses::default();
    let mut parallel_workers = 0_u64;
    let (result, bytes, path, strategy) = match read_optional_v5_canonical_document(
        result_path,
        "native v5 evolved immutable result",
    )? {
        Some(result_value) => {
            let result = validate_v5_evolved_proposal_result(&result_value, manifest)
                .context("validate native v5 evolved immutable result")?;
            let authentication = authenticate_v5_evolved_proposal_output(
                output_root,
                manifest,
                &result,
                validation_mode,
                Some(&progress_handle),
            )?;
            phases.output_authentication = authentication.elapsed;
            io.merge(authentication.io)?;
            passes.merge(authentication.passes)?;
            parallel_workers = authentication.parallel_workers;
            (
                result,
                authentication.bytes,
                V5ExecutionPath::SealedRestart,
                authentication.strategy,
            )
        }
        None => {
            let static_allowed = BTreeSet::from([
                "authority.json".to_owned(),
                "frozen-authority.json".to_owned(),
                "manifest.json".to_owned(),
            ]);
            require_v5_exact_file_set(invocation_root, &static_allowed)
                .context("verify incomplete native v5 evolved invocation file set")?;
            match recover_v5_evolved_invocation_result_from_sealed_receipt(
                output_root,
                invocation_root,
                result_path,
                manifest,
                validation_mode,
                Some(&progress_handle),
            )? {
                Some(recovered) => {
                    phases.output_authentication = recovered.authentication.elapsed;
                    phases.publication = recovered.publication;
                    io.merge(recovered.authentication.io)?;
                    passes.merge(recovered.authentication.passes)?;
                    parallel_workers = recovered.authentication.parallel_workers;
                    (
                        recovered.result,
                        recovered.authentication.bytes,
                        V5ExecutionPath::ReceiptRecovery,
                        recovered.authentication.strategy,
                    )
                }
                None => {
                    let fresh = execute_v5_evolved_fresh_transaction(
                        output_root,
                        invocation_root,
                        result_path,
                        manifest,
                        validation_mode,
                        Some(&progress_handle),
                    )?;
                    let result_value = read_optional_v5_canonical_document(
                        result_path,
                        "fresh native v5 evolved immutable result",
                    )?
                    .ok_or_else(|| {
                        anyhow!(
                            "fresh native v5 evolved transaction did not publish its immutable result"
                        )
                    })?;
                    let result = validate_v5_evolved_proposal_result(&result_value, manifest)
                        .context("validate fresh native v5 evolved immutable result")?;
                    phases.construction = fresh.phases.construction;
                    phases.staging = fresh.phases.staging;
                    phases.prepublication_validation = fresh.phases.prepublication_validation;
                    phases.publication = fresh.phases.publication;
                    passes.merge(fresh.passes)?;
                    io.bytes_written = fresh.bytes.total_bytes()?;
                    if validation_mode.is_strict() {
                        let authentication = authenticate_v5_evolved_proposal_output(
                            output_root,
                            manifest,
                            &result,
                            validation_mode,
                            Some(&progress_handle),
                        )?;
                        phases.output_authentication = authentication.elapsed;
                        io.merge(authentication.io)?;
                        passes.merge(authentication.passes)?;
                        parallel_workers = authentication.parallel_workers;
                        (
                            result,
                            authentication.bytes,
                            V5ExecutionPath::Fresh,
                            authentication.strategy,
                        )
                    } else {
                        (
                            result,
                            fresh.bytes,
                            V5ExecutionPath::Fresh,
                            V5AuthenticationStrategy::FreshPublicationProof,
                        )
                    }
                }
            }
        }
    };
    let invocation_allowed = BTreeSet::from([
        "authority.json".to_owned(),
        "frozen-authority.json".to_owned(),
        "manifest.json".to_owned(),
        V5_PROPOSAL_RESULT_PATH.to_owned(),
    ]);
    require_v5_exact_file_set(invocation_root, &invocation_allowed)?;
    let measurements = V5ExecutionMeasurements {
        path,
        mode: validation_mode,
        strategy,
        phases,
        io,
        passes,
        parallel_authentication_workers: parallel_workers,
        process_cpu: elapsed_process_cpu(process_cpu_started, v5_process_cpu_duration()?),
        total: started.elapsed(),
    };
    let evidence = v5_evolved_adoption_evidence(manifest, &result, bytes, measurements)?;
    validate_v5_evolved_proposal_adoption_evidence(&evidence, manifest, &result)
        .context("validate native v5 evolved adoption evidence")?;
    record_v5_progress_sections(
        &progress_handle,
        measurements.phases,
        measurements.io,
        measurements.parallel_authentication_workers,
        manifest.requested_count,
    );
    progress.finish(measurements.process_cpu);
    write_stdout_json(&evidence)
}

fn execute_v5_proposal(
    manifest_path: &Path,
    manifest_bytes: &[u8],
    execution_mode: V5BatchExecutionMode,
) -> Result<()> {
    let started = Instant::now();
    let process_cpu_started = v5_process_cpu_duration()?;
    let validation_mode = V5ValidationMode::from_environment()?;
    let manifest = parse_v5_proposal_manifest(manifest_bytes)
        .context("validate native v5 proposal manifest")?;
    let mut progress_spec = NativeProgressSpec::new("proposal_construction", "static_authority");
    progress_spec.generation_kind = Some(manifest.generation_kind.clone());
    progress_spec.generation_index = Some(manifest.generation_index);
    progress_spec.subphase = "authenticate_manifest_and_runtime".to_owned();
    progress_spec.thread_cap = Some(manifest.thread_cap);
    let progress = NativeProgress::from_environment(progress_spec);
    let progress_handle = progress.handle();
    let invocation_root = manifest_path
        .parent()
        .ok_or_else(|| anyhow!("native v5 proposal manifest has no parent directory"))?;
    let invocation_root =
        resolved_safe_existing_directory(invocation_root, "native v5 proposal manifest parent")?;
    let sealed_output_root = PathBuf::from(&manifest.output_root);
    let output_root =
        resolved_safe_existing_directory(&sealed_output_root, "native v5 proposal output root")?;
    if output_root != sealed_output_root {
        bail!("native v5 proposal output root is not its exact resolved path");
    }
    if !invocation_root.starts_with(&output_root) {
        bail!("native v5 proposal manifest parent escapes its sealed output root");
    }
    let static_authority_started = Instant::now();
    verify_v5_static_authorities(&invocation_root, &manifest, manifest_bytes)?;
    let static_authority_elapsed = static_authority_started.elapsed();
    let result_path = invocation_root.join(&manifest.result_path);
    if execution_mode == V5BatchExecutionMode::FastEphemeralV1 {
        if manifest.generation_kind == "evolved" {
            return v5_fast_ephemeral::execute_evolved(
                &output_root,
                &result_path,
                &manifest,
                &progress_handle,
                progress,
                started,
                static_authority_elapsed,
            );
        }
        return v5_fast_ephemeral::execute_g0(
            &output_root,
            &invocation_root,
            &result_path,
            &manifest,
            &progress_handle,
            progress,
            started,
            static_authority_elapsed,
        );
    }
    if manifest.generation_kind == "evolved" {
        return execute_v5_evolved_proposal(
            &output_root,
            &invocation_root,
            &result_path,
            &manifest,
            started,
            process_cpu_started,
            static_authority_elapsed,
            validation_mode,
            progress,
        );
    }
    let mut phases = V5PhaseDurations {
        static_authority: static_authority_elapsed,
        ..V5PhaseDurations::default()
    };
    let mut io = V5IoTelemetry::default();
    let mut passes = V5ValidationPasses::default();
    let mut parallel_workers = 0_u64;
    let (result, bytes, path, strategy) =
        match read_optional_v5_canonical_document(&result_path, "native v5 immutable result")? {
            Some(result_value) => {
                let result = validate_v5_proposal_result(&result_value, &manifest)
                    .context("validate native v5 immutable result")?;
                let authentication = authenticate_v5_proposal_output(
                    &output_root,
                    &manifest,
                    &result,
                    validation_mode,
                    Some(&progress_handle),
                )?;
                phases.output_authentication = authentication.elapsed;
                io.merge(authentication.io)?;
                passes.merge(authentication.passes)?;
                parallel_workers = authentication.parallel_workers;
                (
                    result,
                    authentication.bytes,
                    V5ExecutionPath::SealedRestart,
                    authentication.strategy,
                )
            }
            None => {
                let static_allowed = BTreeSet::from([
                    "authority.json".to_owned(),
                    "frozen-authority.json".to_owned(),
                    "manifest.json".to_owned(),
                ]);
                require_v5_exact_file_set(&invocation_root, &static_allowed)
                    .context("verify incomplete native v5 invocation file set")?;
                match recover_v5_invocation_result_from_sealed_receipt(
                    &output_root,
                    &invocation_root,
                    &result_path,
                    &manifest,
                    validation_mode,
                    Some(&progress_handle),
                )? {
                    Some(recovered) => {
                        phases.output_authentication = recovered.authentication.elapsed;
                        phases.publication = recovered.publication;
                        io.merge(recovered.authentication.io)?;
                        passes.merge(recovered.authentication.passes)?;
                        parallel_workers = recovered.authentication.parallel_workers;
                        (
                            recovered.result,
                            recovered.authentication.bytes,
                            V5ExecutionPath::ReceiptRecovery,
                            recovered.authentication.strategy,
                        )
                    }
                    None => {
                        let fresh = execute_v5_fresh_transaction(
                            &output_root,
                            &invocation_root,
                            &result_path,
                            &manifest,
                            validation_mode,
                            Some(&progress_handle),
                        )?;
                        let result_value = read_optional_v5_canonical_document(
                            &result_path,
                            "fresh native v5 immutable result",
                        )?
                        .ok_or_else(|| {
                            anyhow!(
                                "fresh native v5 transaction did not publish its immutable result"
                            )
                        })?;
                        let result = validate_v5_proposal_result(&result_value, &manifest)
                            .context("validate fresh native v5 immutable result")?;
                        phases.construction = fresh.phases.construction;
                        phases.staging = fresh.phases.staging;
                        phases.prepublication_validation = fresh.phases.prepublication_validation;
                        phases.publication = fresh.phases.publication;
                        passes.merge(fresh.passes)?;
                        io.bytes_written = fresh.bytes.total_bytes()?;
                        if validation_mode.is_strict() {
                            let authentication = authenticate_v5_proposal_output(
                                &output_root,
                                &manifest,
                                &result,
                                validation_mode,
                                Some(&progress_handle),
                            )?;
                            phases.output_authentication = authentication.elapsed;
                            io.merge(authentication.io)?;
                            passes.merge(authentication.passes)?;
                            parallel_workers = authentication.parallel_workers;
                            (
                                result,
                                authentication.bytes,
                                V5ExecutionPath::Fresh,
                                authentication.strategy,
                            )
                        } else {
                            (
                                result,
                                fresh.bytes,
                                V5ExecutionPath::Fresh,
                                V5AuthenticationStrategy::FreshPublicationProof,
                            )
                        }
                    }
                }
            }
        };
    let invocation_allowed = BTreeSet::from([
        "authority.json".to_owned(),
        "frozen-authority.json".to_owned(),
        "manifest.json".to_owned(),
        V5_PROPOSAL_RESULT_PATH.to_owned(),
    ]);
    require_v5_exact_file_set(&invocation_root, &invocation_allowed)?;
    let measurements = V5ExecutionMeasurements {
        path,
        mode: validation_mode,
        strategy,
        phases,
        io,
        passes,
        parallel_authentication_workers: parallel_workers,
        process_cpu: elapsed_process_cpu(process_cpu_started, v5_process_cpu_duration()?),
        total: started.elapsed(),
    };
    let evidence = v5_adoption_evidence(&manifest, &result, bytes, measurements)?;
    validate_v5_proposal_adoption_evidence(&evidence, &manifest, &result)
        .context("validate native v5 adoption evidence")?;
    record_v5_progress_sections(
        &progress_handle,
        measurements.phases,
        measurements.io,
        measurements.parallel_authentication_workers,
        manifest.requested_count,
    );
    progress.finish(measurements.process_cpu);
    write_stdout_json(&evidence)
}

fn execute_g0_funnel(manifest_path: &Path, manifest_bytes: &[u8]) -> Result<()> {
    let manifest =
        parse_g0_funnel_manifest(manifest_bytes).context("validate native G0 funnel manifest")?;
    let parent = manifest_path
        .parent()
        .ok_or_else(|| anyhow!("G0 funnel manifest has no parent directory"))?;
    ensure_safe_existing_directory(parent, "G0 funnel manifest parent")?;
    verify_g0_execution_authority(parent, &manifest.execution_authority)
        .context("verify native G0 execution authority")?;
    let output_root =
        ensure_safe_directory_tree(Path::new(&manifest.output_root), "G0 funnel output root")?;
    let final_newline = generation_final_newline(&manifest.final_newline);
    let policy = parse_publication_policy(&manifest.publication_policy)
        .context("parse G0 funnel publication policy")?;
    let request = G0FunnelRequest {
        output_root,
        final_newline,
        request_sha256: manifest.manifest_sha256,
        authority_sha256: manifest.authority_sha256,
        config: manifest.generation_config,
        config_sha256: manifest.generation_config_sha256,
        generation_index: manifest.generation_index,
        construction_pool_size: manifest.construction_pool_size,
        evaluation_population_size: manifest.evaluation_population_size,
        max_proposal_attempts: manifest.max_proposal_attempts,
        admission_thread_cap: g0_admission_thread_cap()?,
        publication_policy: policy,
        execution_authority: manifest.execution_authority,
        identity_ledger: manifest.identity_ledger,
        global_identity_ledger: None,
        audit: manifest.audit,
    };
    let outcome = finalize_g0(&request).context("execute native G0 funnel")?;
    let result_value = outcome.result_value();
    let result_bytes = canonical_json_line(&result_value)
        .map_err(|error| anyhow!("encode canonical G0 funnel result: {error}"))?;
    // An incomplete construction is intentionally not an immutable result:
    // Python may append more proposal entries and invoke this same manifest
    // again.  Publishing it would make the later completed transaction appear
    // to conflict with an unrelated stale result.  An adopted receipt also
    // does not rewrite the original completed bridge result: its receipt is
    // the authoritative restart artifact, while its status is transient.
    if matches!(outcome, G0FunnelOutcome::Completed { .. }) {
        publish_once(&parent.join(&manifest.result_path), &result_bytes)?;
    }
    write_stdout_bytes(&result_bytes)
}

/// Operational-only cap for bounded G0 journal admission.  It is intentionally
/// not part of the sealed manifest: ordinal merge makes the output invariant,
/// while the caller records the chosen cap in external performance evidence.
fn g0_admission_thread_cap() -> Result<usize> {
    let Some(raw) = env::var_os("TEMPORAL_QD_G0_ADMISSION_THREAD_CAP") else {
        return Ok(DEFAULT_G0_ADMISSION_THREAD_CAP);
    };
    let raw = raw
        .into_string()
        .map_err(|_| anyhow!("G0 admission thread cap must be valid UTF-8"))?;
    let cap = raw
        .parse::<usize>()
        .map_err(|_| anyhow!("G0 admission thread cap must be an integer"))?;
    if !(1..=MAX_G0_ADMISSION_THREAD_CAP).contains(&cap) {
        bail!("G0 admission thread cap must be within 1..={MAX_G0_ADMISSION_THREAD_CAP}")
    }
    Ok(cap)
}

fn sha256_file(path: &Path, label: &str) -> Result<String> {
    let path = safe_existing_file(path, label)?;
    let before = fs::metadata(&path).with_context(|| format!("stat {label}"))?;
    let mut file = fs::File::open(&path).with_context(|| format!("open {label}"))?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("read {label}"))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    let after = fs::metadata(&path).with_context(|| format!("restat {label}"))?;
    if before.len() != after.len() || before.modified().ok() != after.modified().ok() {
        bail!("{label} changed while its authority hash was being read")
    }
    Ok(format!("sha256:{:x}", digest.finalize()))
}

fn verify_g0_execution_authority(parent: &Path, authority: &Value) -> Result<()> {
    let fields = authority
        .as_object()
        .ok_or_else(|| anyhow!("native G0 execution authority is invalid"))?;
    let batch = fields
        .get("nativeBatchAuthority")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("native G0 execution authority lacks batch authority"))?;
    let expected_binary_sha = batch
        .get("executableSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native G0 execution authority lacks executable hash"))?;
    let current = std::env::current_exe().context("discover current native batch executable")?;
    if sha256_file(&current, "native G0 batch executable")? != expected_binary_sha {
        bail!("native G0 batch executable authority drifted")
    }
    let authority_path = parent.join("authority.json");
    let authority_path = safe_existing_file(&authority_path, "native G0 authority artifact")?;
    let raw = read_stable_existing_file(&authority_path, "native G0 authority artifact")?;
    let persisted: Value =
        serde_json::from_slice(&raw).context("parse native G0 authority artifact")?;
    if raw != canonical_json_line(&persisted)? || persisted != Value::Object(batch.clone()) {
        bail!("native G0 authority artifact differs from manifest authority")
    }
    Ok(())
}

fn execute_foundation_bytes(manifest_path: &Path, manifest_bytes: &[u8]) -> Result<()> {
    let manifest = parse_foundation_manifest(manifest_bytes)
        .map_err(|error| anyhow!("validate manifest: {error}"))?;
    let parent = manifest_path
        .parent()
        .ok_or_else(|| anyhow!("manifest has no parent directory"))?;
    ensure_safe_existing_directory(parent, "manifest parent")?;
    let result_path = parent.join(&manifest.result_path);
    let result = FoundationResult::from_manifest(&manifest);
    result
        .validate()
        .map_err(|error| anyhow!("validate foundation result: {error}"))?;
    let result_value = serde_json::to_value(&result)?;
    let result_bytes = canonical_json_line(&result_value)
        .map_err(|error| anyhow!("encode canonical foundation result: {error}"))?;
    publish_once(&result_path, &result_bytes)?;
    write_stdout_bytes(&result_bytes)
}

fn execute_generation(manifest_path: &Path, manifest_bytes: &[u8]) -> Result<()> {
    let manifest = parse_generation_manifest(manifest_bytes, CONTRACT_VERSION)
        .context("validate native generation manifest")?;
    let parent = manifest_path
        .parent()
        .ok_or_else(|| anyhow!("generation manifest has no parent directory"))?;
    ensure_safe_existing_directory(parent, "generation manifest parent")?;

    let archive_path = safe_existing_file(
        Path::new(&manifest.parent_archive_path),
        "generation parent archive",
    )?;
    let archive_bytes = read_stable_existing_file(&archive_path, "generation parent archive")?;
    let archive: Value =
        serde_json::from_slice(&archive_bytes).context("parse generation parent archive")?;
    // `RuntimeManifest` retains its own strict archive projection.  The raw
    // file bytes are staging-only and can be substantial for real generations.
    drop(archive_bytes);
    let ledger_path = safe_existing_file(
        Path::new(&manifest.identity_ledger_path),
        "generation identity ledger",
    )?;
    let ledger_bytes = read_stable_existing_file(&ledger_path, "generation identity ledger")?;
    let final_newline = generation_final_newline(&manifest.final_newline);
    let identity_ledger = parse_python_pretty_json_document(
        &ledger_bytes,
        JsonNewline::Lf,
        "generation identity ledger",
    )?;
    let runtime_value = assemble_runtime_manifest_owned(&manifest, archive, identity_ledger)
        .context("assemble runtime manifest from verified path inputs")?;

    let output_root =
        ensure_safe_directory_tree(Path::new(&manifest.output_root), "generation output root")?;
    let runtime = RuntimeManifest::from_owned_value(runtime_value)
        .context("validate concrete runtime manifest")?;
    let mut parents =
        RuntimeParentSelector::from_manifest(&runtime, manifest.allow_empty_quality_bootstrap)
            .context("initialize verified parent selector")?;
    let mut authority = DashboardPairAuthority::from_manifest(&runtime)
        .context("initialize frozen Dashboard pair authority")?;
    let evidence_identity_context = runtime.evidence_identity_context.clone();
    let archive_bootstrap_inputs = archive_bootstrap_inputs_from_manifest(&runtime)
        .context("reduce verified parent archive to identity-ledger inputs")?;
    // Selector, authority, and bootstrap inputs are self-contained.  Consume
    // the validated public ledger and release the full runtime envelope before
    // proposal generation, avoiding both a duplicate ledger and a retained
    // parent archive.
    let mut ledger = global_identity_ledger_from_public(runtime.into_identity_ledger())
        .context("open global identity ledger")?;
    bootstrap_global_identity_ledger_inputs(&mut ledger, archive_bootstrap_inputs)
        .context("bootstrap global identity ledger from verified parent archive")?;
    let request = GenerateGenerationRequest {
        output_root: output_root.clone(),
        final_newline,
        pair_config: manifest.generation_config.clone(),
        config_sha256: manifest.generation_config_sha256.clone(),
        generation_index: manifest
            .generation_config
            .get("generationIndex")
            .and_then(Value::as_u64)
            .expect("generation contract checked generationIndex"),
        target_unique_candidates: manifest.target_unique_candidates,
        max_proposal_attempts: manifest.max_proposal_attempts,
        max_new_proposals: manifest.max_new_proposals,
        parent_schedule: parse_parent_schedule(manifest.parent_schedule.as_ref())?,
        expected_native_authority_sha256: manifest.native_proposal_authority_sha256.clone(),
        publication_policy: parse_publication_policy(&manifest.publication_policy)?,
        g0_evaluation_width: manifest.g0_evaluation_width,
        evidence_identity_context,
        frozen_construction_catalog: manifest.frozen_construction_catalog.clone(),
        factory_construction_policy: manifest
            .generation_config
            .get("immigrantConstructionPolicy")
            .cloned(),
    };
    let kernel_result = generate_generation(&request, &mut authority, &mut parents, &mut ledger)
        .context("execute native generation")?;
    let inner = if kernel_result.get("completed").and_then(Value::as_bool) == Some(true) {
        if kernel_result.get("schemaVersion").and_then(Value::as_str)
            != Some("temporal_qd_front_generation_result_v1")
        {
            bail!("completed kernel generation wrapper has an incompatible schema")
        }
        kernel_result
            .get("pairGenerationResult")
            .cloned()
            .ok_or_else(|| anyhow!("completed kernel generation lacks pairGenerationResult"))?
    } else if kernel_result.get("schemaVersion").and_then(Value::as_str)
        == Some(FRONT_GENERATION_PROGRESS_SCHEMA)
        && kernel_result.get("completed").and_then(Value::as_bool) == Some(false)
    {
        kernel_result
    } else {
        bail!("kernel generation returned an incompatible result")
    };
    let output_ledger_path = output_root.join("identity-ledger.json");
    let output_ledger_bytes = fs::read(&output_ledger_path).with_context(|| {
        format!(
            "read native output identity ledger: {}",
            output_ledger_path.display()
        )
    })?;
    let output_ledger = parse_python_pretty_json_document(
        &output_ledger_bytes,
        JsonNewline::Lf,
        "native output identity ledger",
    )?;
    let output_ledger_sha256 = output_ledger
        .get("ledgerSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native output identity ledger lacks ledgerSha256"))?
        .to_owned();
    let mut output_ledger_material = output_ledger.clone();
    output_ledger_material
        .as_object_mut()
        .ok_or_else(|| anyhow!("native output identity ledger must be an object"))?
        .remove("ledgerSha256");
    if canonical_sha256(&output_ledger_material)? != output_ledger_sha256 {
        bail!("native output identity ledger self-hash mismatch")
    }
    replace_mutable_ledger(
        &ledger_path,
        &ledger_bytes,
        &output_ledger_bytes,
        &manifest.identity_ledger_sha256,
    )?;
    let result = build_generation_result(&manifest, inner, output_ledger_sha256)?;
    validate_generation_result(&result.value, &manifest)?;
    let result_bytes = canonical_json_line(&result.value)
        .map_err(|error| anyhow!("encode canonical generation result: {error}"))?;
    publish_once(&parent.join(&manifest.result_path), &result_bytes)?;
    write_stdout_bytes(&result_bytes)
}

fn generation_final_newline(value: &str) -> FinalNewline {
    match value {
        "lf" => FinalNewline::Lf,
        "crlf" => FinalNewline::Crlf,
        _ => unreachable!("generation contract validated finalNewline"),
    }
}

fn parse_parent_schedule(value: Option<&Value>) -> Result<Option<RotatingParentSchedule>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let fields = value
        .as_object()
        .ok_or_else(|| anyhow!("parent schedule must be an object"))?;
    let breeder_width = fields
        .get("breederWidth")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("parent schedule breederWidth is invalid"))?;
    let breeder_parent_count = fields
        .get("breederParentCount")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("parent schedule breederParentCount is invalid"))?;
    let schema = fields.get("schemaVersion").and_then(Value::as_str);
    let schedule = if schema == Some("temporal_qd_rotating_parent_schedule_v1") {
        let numerator = fields
            .get("offspringNumerator")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("legacy parent schedule offspringNumerator is invalid"))?;
        let denominator = fields
            .get("offspringDenominator")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("legacy parent schedule offspringDenominator is invalid"))?;
        let expected = temporal_qd_kernel::schedule::RotatingParentSchedule::legacy_schedule_sha256(
            breeder_width,
            breeder_parent_count,
            numerator,
            denominator,
        );
        if fields.get("scheduleSha256").and_then(Value::as_str) != Some(expected.as_str()) {
            bail!("legacy parent schedule identity mismatch")
        }
        // v1 is recovery-only, but its self-hash is not sufficient: prove
        // the sealed sparse projection before mapping it to v2 allocation.
        RotatingParentSchedule::validated_legacy_fields(
            breeder_width,
            breeder_parent_count,
            numerator,
            denominator,
        )
        .context("validate legacy rotating parent schedule")?
    } else {
        if schema != Some("temporal_qd_rotating_parent_schedule_v2")
            || fields
                .get("minimumImmigrantNumerator")
                .and_then(Value::as_u64)
                != Some(1)
            || fields
                .get("minimumImmigrantDenominator")
                .and_then(Value::as_u64)
                != Some(5)
            || fields.get("parentSampling").and_then(Value::as_str)
                != Some("with_replacement_supported_parents_v1")
            || fields
                .get("unsupportedParentPolicy")
                .and_then(Value::as_str)
                != Some("immigrant_only_authority_bound_v1")
            || fields.get("schedulingMethod").and_then(Value::as_str)
                != Some("accepted_quota_prefix_balance_v1")
        {
            bail!("parent schedule v2 policy is invalid")
        }
        RotatingParentSchedule::from_counts(breeder_width, breeder_parent_count)
            .context("validate rotating parent schedule")?
    };
    if schema == Some("temporal_qd_rotating_parent_schedule_v2")
        && fields.get("scheduleSha256").and_then(Value::as_str)
            != Some(schedule.schedule_sha256().as_str())
    {
        bail!("parent schedule identity mismatch")
    }
    Ok(Some(schedule))
}

/// Translate the sealed rotating projection into the core execution shape.
/// A zero-parent projection is an explicit immigrant-only authority: the
/// archive may be empty and the core request must carry no selectable parent
/// schedule. Nonzero projections remain strict and must match the archive's
/// exact quality-eligible parent count during core admission.
fn v5_evolved_configured_parent_schedule(value: Option<&Value>) -> Result<RotatingParentSchedule> {
    parse_parent_schedule(value)?
        .ok_or_else(|| anyhow!("sealed native v5 evolved generation config lacks parentSchedule"))
}

fn v5_evolved_effective_parent_schedule(
    configured: RotatingParentSchedule,
    eligible_parent_count: usize,
) -> Result<Option<RotatingParentSchedule>> {
    let eligible_parent_count = u64::try_from(eligible_parent_count)
        .context("native v5 eligible parent count exceeds u64")?;
    if eligible_parent_count > configured.breeder_width {
        bail!("native v5 eligible parent count exceeds the frozen breeder width")
    }
    if eligible_parent_count == 0 {
        return Ok(None);
    }
    RotatingParentSchedule::from_counts(configured.breeder_width, eligible_parent_count)
        .context("derive native v5 supported-parent schedule")
        .map(Some)
}

fn v5_evolved_adopted_parent_schedule(
    configured: RotatingParentSchedule,
    sealed_schedule_sha256: Option<&str>,
) -> Result<Option<RotatingParentSchedule>> {
    let Some(sealed_schedule_sha256) = sealed_schedule_sha256 else {
        return Ok(None);
    };
    for eligible_parent_count in 1..=configured.breeder_width {
        let candidate =
            RotatingParentSchedule::from_counts(configured.breeder_width, eligible_parent_count)
                .context("reconstruct native v5 adopted parent schedule")?;
        if candidate.schedule_sha256() == sealed_schedule_sha256 {
            return Ok(Some(candidate));
        }
    }
    bail!("sealed native v5 evolved parent schedule is not derivable from its breeder width")
}

fn parse_python_pretty_json_document(
    raw: &[u8],
    newline: JsonNewline,
    label: &str,
) -> Result<Value> {
    let value: Value = serde_json::from_slice(raw).with_context(|| format!("parse {label}"))?;
    let expected = python_pretty_json_line(&value, newline)
        .with_context(|| format!("encode exact Python {label}"))?;
    if raw != expected {
        bail!("{label} must be exact Python pretty JSON")
    }
    Ok(value)
}

fn parse_publication_policy(value: &Value) -> Result<PublicationPolicy> {
    let fields = value
        .as_object()
        .ok_or_else(|| anyhow!("publication policy must be an object"))?;
    let required_string = |key: &str| {
        fields
            .get(key)
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow!("publication policy {key} is invalid"))
    };
    Ok(PublicationPolicy {
        qd_version: required_string("qdVersion")?,
        policy_name: required_string("policyName")?,
        policy_sha256: required_string("policySha256")?,
        pair_policy: fields
            .get("pairPolicy")
            .cloned()
            .ok_or_else(|| anyhow!("publication policy lacks pairPolicy"))?,
        operator_implementation_identity: fields
            .get("operatorImplementationIdentity")
            .cloned()
            .ok_or_else(|| anyhow!("publication policy lacks operatorImplementationIdentity"))?,
        predeclared_evidence_context_sha256: fields
            .get("predeclaredEvidenceContextSha256")
            .filter(|value| !value.is_null())
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        archive_policy_authority: fields
            .get("archivePolicyAuthority")
            .filter(|value| !value.is_null())
            .cloned(),
    })
}

fn write_stdout_json(value: &serde_json::Value) -> Result<()> {
    let bytes =
        canonical_json_line(value).map_err(|error| anyhow!("encode version JSON: {error}"))?;
    write_stdout_bytes(&bytes)
}

fn write_stdout_bytes(bytes: &[u8]) -> Result<()> {
    let mut stdout = io::stdout().lock();
    stdout.write_all(bytes)?;
    stdout.flush()?;
    Ok(())
}

/// Resolve an existing absolute file while rejecting POSIX symlinks and
/// Windows reparse points in every path component.
fn safe_existing_file(path: &Path, label: &str) -> Result<PathBuf> {
    if !path.is_absolute() {
        bail!("{label} path must be absolute")
    }
    ensure_safe_components(path, label, false)?;
    Ok(path.to_path_buf())
}

fn ensure_safe_existing_directory(path: &Path, label: &str) -> Result<()> {
    if !path.is_absolute() {
        bail!("{label} path must be absolute")
    }
    ensure_safe_components(path, label, true)
}

/// Resolve a real directory only after walking every existing component with
/// `symlink_metadata`.  Windows canonicalization normally adds a verbatim
/// `\\?\` prefix, so normalize that representation before comparing it to a
/// manifest's sealed absolute path.  The v5 bridge writes an already-resolved
/// path; accepting a lexical alias here would weaken the output-root binding.
fn resolved_safe_existing_directory(path: &Path, label: &str) -> Result<PathBuf> {
    ensure_safe_existing_directory(path, label)?;
    let resolved =
        fs::canonicalize(path).with_context(|| format!("resolve {label}: {}", path.display()))?;
    let resolved = normalized_canonical_path(resolved);
    ensure_safe_existing_directory(&resolved, label)?;
    Ok(resolved)
}

#[cfg(windows)]
fn normalized_canonical_path(path: PathBuf) -> PathBuf {
    let rendered = path.to_string_lossy();
    if let Some(remainder) = rendered.strip_prefix(r"\\?\UNC\") {
        PathBuf::from(format!(r"\\{remainder}"))
    } else if let Some(remainder) = rendered.strip_prefix(r"\\?\") {
        PathBuf::from(remainder)
    } else {
        path
    }
}

#[cfg(not(windows))]
fn normalized_canonical_path(path: PathBuf) -> PathBuf {
    path
}

fn ensure_safe_directory_tree(path: &Path, label: &str) -> Result<PathBuf> {
    if !path.is_absolute() {
        bail!("{label} path must be absolute")
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        use std::path::Component;
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                current.push(component.as_os_str())
            }
            Component::CurDir | Component::ParentDir => {
                bail!("{label} path contains an unsafe component")
            }
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if is_link_or_reparse(&metadata) || !metadata.is_dir() {
                    bail!(
                        "{label} contains a non-directory or reparse point: {}",
                        current.display()
                    )
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(&current)
                    .with_context(|| format!("create {label} component: {}", current.display()))?;
                let metadata = fs::symlink_metadata(&current)
                    .with_context(|| format!("inspect created {label}: {}", current.display()))?;
                if is_link_or_reparse(&metadata) || !metadata.is_dir() {
                    bail!(
                        "created {label} component is not a real directory: {}",
                        current.display()
                    )
                }
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("inspect {label} component: {}", current.display()));
            }
        }
    }
    Ok(path.to_path_buf())
}

fn ensure_safe_components(path: &Path, label: &str, final_is_directory: bool) -> Result<()> {
    // `std::fs::canonicalize` returns verbatim drive paths on Windows.  Their
    // first component renders as `\\?\C:` and is not itself a stat-able path;
    // inspection must begin at the rooted drive.  Normalize only the two
    // exact Windows verbatim spellings already handled by the resolved-path
    // authority, while returning and binding the caller's original path.
    let inspection_path = normalized_canonical_path(path.to_path_buf());
    let mut current = PathBuf::new();
    let components: Vec<_> = inspection_path.components().collect();
    for (index, component) in components.iter().enumerate() {
        use std::path::Component;
        match component {
            Component::Prefix(_) => {
                current.push(component.as_os_str());
                continue;
            }
            Component::RootDir | Component::Normal(_) => current.push(component.as_os_str()),
            Component::CurDir | Component::ParentDir => {
                bail!("{label} path contains an unsafe component")
            }
        }
        let metadata = fs::symlink_metadata(&current)
            .with_context(|| format!("inspect {label} component: {}", current.display()))?;
        if is_link_or_reparse(&metadata) {
            bail!(
                "{label} contains a symlink or Windows reparse point: {}",
                current.display()
            )
        }
        let is_final = index + 1 == components.len();
        if (!is_final || final_is_directory) && !metadata.is_dir() {
            bail!(
                "{label} parent is not a real directory: {}",
                current.display()
            )
        }
        if is_final && !final_is_directory && !metadata.is_file() {
            bail!("{label} is not a regular file: {}", current.display())
        }
    }
    Ok(())
}

fn is_link_or_reparse(metadata: &fs::Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes() & 0x0400 != 0
    }
    #[cfg(not(windows))]
    {
        false
    }
}

const SHARING_RETRY_DELAYS_MS: [u64; 5] = [5, 10, 20, 40, 80];
const TEMPORARY_CREATE_ATTEMPTS: usize = 16;

trait PublicationIo {
    fn create_new(&self, path: &Path) -> io::Result<fs::File>;
    fn write_all(&self, file: &mut fs::File, bytes: &[u8]) -> io::Result<()>;
    fn sync_file(&self, file: &fs::File) -> io::Result<()>;
    fn hard_link(&self, source: &Path, target: &Path) -> io::Result<()>;
    fn remove_file(&self, path: &Path) -> io::Result<()>;
    fn sync_directory(&self, path: &Path) -> io::Result<()>;
    fn is_sharing_violation(&self, error: &io::Error) -> bool;
    fn pause(&self, duration: Duration);
}

struct StdPublicationIo;

impl PublicationIo for StdPublicationIo {
    fn create_new(&self, path: &Path) -> io::Result<fs::File> {
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
    }

    fn write_all(&self, file: &mut fs::File, bytes: &[u8]) -> io::Result<()> {
        file.write_all(bytes)
    }

    fn sync_file(&self, file: &fs::File) -> io::Result<()> {
        file.sync_all()
    }

    fn hard_link(&self, source: &Path, target: &Path) -> io::Result<()> {
        fs::hard_link(source, target)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        sync_directory_platform(path)
    }

    fn is_sharing_violation(&self, error: &io::Error) -> bool {
        is_windows_sharing_violation(error)
    }

    fn pause(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

/// Persist a newly installed directory entry. POSIX exposes directory fsync
/// directly. Windows requires opening a directory with BACKUP_SEMANTICS; the
/// documented unsupported errors mean the already-fsynced payload is the
/// strongest primitive available on that filesystem and are a safe fallback.
#[cfg(not(windows))]
fn sync_directory_platform(path: &Path) -> io::Result<()> {
    fs::File::open(path)?.sync_all()
}

#[cfg(windows)]
fn sync_directory_platform(path: &Path) -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt;

    const GENERIC_READ: u32 = 0x8000_0000;
    const GENERIC_WRITE: u32 = 0x4000_0000;
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_SHARE_DELETE: u32 = 0x0000_0004;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;

    let mut options = fs::OpenOptions::new();
    options
        .access_mode(GENERIC_READ | GENERIC_WRITE)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
    let directory = match options.open(path) {
        Ok(directory) => directory,
        Err(error) if is_unsupported_windows_directory_sync(&error) => return Ok(()),
        Err(error) => return Err(error),
    };
    match directory.sync_all() {
        Err(error) if is_unsupported_windows_directory_sync(&error) => Ok(()),
        outcome => outcome,
    }
}

#[cfg(windows)]
fn is_unsupported_windows_directory_sync(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(1 | 5 | 50 | 87))
}

fn is_windows_sharing_violation(error: &io::Error) -> bool {
    #[cfg(windows)]
    {
        matches!(error.raw_os_error(), Some(32 | 33))
    }
    #[cfg(not(windows))]
    {
        let _ = error;
        false
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity(u128, u128);

#[cfg(unix)]
fn identity_from_metadata(metadata: &fs::Metadata) -> FileIdentity {
    use std::os::unix::fs::MetadataExt;
    FileIdentity(metadata.dev() as u128, metadata.ino() as u128)
}

#[cfg(unix)]
fn identity_from_file(file: &fs::File) -> io::Result<FileIdentity> {
    file.metadata()
        .map(|metadata| identity_from_metadata(&metadata))
}

#[cfg(unix)]
fn identity_from_path(path: &Path) -> io::Result<FileIdentity> {
    fs::symlink_metadata(path).map(|metadata| identity_from_metadata(&metadata))
}

#[cfg(windows)]
fn identity_from_file(file: &fs::File) -> io::Result<FileIdentity> {
    use std::ffi::c_void;
    use std::mem::MaybeUninit;
    use std::os::windows::io::AsRawHandle;

    #[repr(C)]
    struct FileTime {
        low: u32,
        high: u32,
    }

    #[repr(C)]
    struct ByHandleFileInformation {
        attributes: u32,
        creation_time: FileTime,
        last_access_time: FileTime,
        last_write_time: FileTime,
        volume_serial_number: u32,
        file_size_high: u32,
        file_size_low: u32,
        number_of_links: u32,
        file_index_high: u32,
        file_index_low: u32,
    }

    unsafe extern "system" {
        fn GetFileInformationByHandle(
            file: *mut c_void,
            information: *mut ByHandleFileInformation,
        ) -> i32;
    }

    let mut information = MaybeUninit::<ByHandleFileInformation>::uninit();
    // SAFETY: `file` owns a live Windows handle and `information` points to
    // writable storage with the exact Win32 structure layout.
    if unsafe { GetFileInformationByHandle(file.as_raw_handle(), information.as_mut_ptr()) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the successful call initialized the full structure.
    let information = unsafe { information.assume_init() };
    let file_index =
        ((information.file_index_high as u64) << 32) | information.file_index_low as u64;
    Ok(FileIdentity(
        information.volume_serial_number as u128,
        file_index as u128,
    ))
}

#[cfg(windows)]
fn identity_from_path(path: &Path) -> io::Result<FileIdentity> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_SHARE_DELETE: u32 = 0x0000_0004;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;

    let mut options = fs::OpenOptions::new();
    options
        .access_mode(0)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
    identity_from_file(&options.open(path)?)
}

fn require_same_identity(
    current: FileIdentity,
    expected: FileIdentity,
    label: &str,
    path: &Path,
) -> Result<()> {
    if current != expected {
        bail!("{label} changed identity: {}", path.display())
    }
    Ok(())
}

fn require_directory_identity(path: &Path, expected: FileIdentity, label: &str) -> Result<()> {
    ensure_safe_existing_directory(path, label)?;
    let current = identity_from_path(path)
        .with_context(|| format!("re-identify {label}: {}", path.display()))?;
    require_same_identity(current, expected, label, path)
}

fn read_stable_existing_file(path: &Path, label: &str) -> Result<Vec<u8>> {
    let path = safe_existing_file(path, label)?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("{label} has no parent directory"))?;
    let parent_identity = identity_from_path(parent)
        .with_context(|| format!("identify {label} parent: {}", parent.display()))?;
    let path_identity = identity_from_path(&path)
        .with_context(|| format!("identify {label}: {}", path.display()))?;
    let mut file = fs::OpenOptions::new()
        .read(true)
        .open(&path)
        .with_context(|| format!("open {label}: {}", path.display()))?;
    let opened_identity = identity_from_file(&file)
        .with_context(|| format!("identify opened {label}: {}", path.display()))?;
    require_same_identity(opened_identity, path_identity, label, &path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .with_context(|| format!("read {label}: {}", path.display()))?;
    require_directory_identity(parent, parent_identity, &format!("{label} parent"))?;
    ensure_safe_components(&path, label, false)?;
    let after = identity_from_path(&path)
        .with_context(|| format!("re-identify {label}: {}", path.display()))?;
    require_same_identity(after, opened_identity, label, &path)?;
    Ok(bytes)
}

/// A bounded-memory identity of a stable, regular file.  V5 publication and
/// restart authentication use this instead of `fs::read` for population-scale
/// artifacts: a real generation can have a multi-gigabyte population, while
/// an artifact's byte identity only needs a fixed-size SHA-256 state.
#[derive(Clone, Debug, Eq, PartialEq)]
struct V5FileDigest {
    byte_length: u64,
    file_sha256: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct V5StableFileIdentity {
    file_identity: FileIdentity,
    parent_identity: FileIdentity,
}

/// Hash a stable regular file through a held handle.  This follows the same
/// no-reparse/no-target-swap discipline as `read_stable_existing_file`, but
/// keeps only a fixed 1 MiB buffer.  The metadata check closes the ordinary
/// in-place write race as well as the identity checks for replacement races.
fn digest_stable_existing_file(
    path: &Path,
    label: &str,
) -> Result<(V5FileDigest, V5StableFileIdentity)> {
    let path = safe_existing_file(path, label)?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("{label} has no parent directory"))?;
    let parent_identity = identity_from_path(parent)
        .with_context(|| format!("identify {label} parent: {}", parent.display()))?;
    let path_identity = identity_from_path(&path)
        .with_context(|| format!("identify {label}: {}", path.display()))?;
    let mut file = fs::OpenOptions::new()
        .read(true)
        .open(&path)
        .with_context(|| format!("open {label}: {}", path.display()))?;
    let opened_identity = identity_from_file(&file)
        .with_context(|| format!("identify opened {label}: {}", path.display()))?;
    require_same_identity(opened_identity, path_identity, label, &path)?;
    let before = file
        .metadata()
        .with_context(|| format!("stat opened {label}: {}", path.display()))?;
    if is_link_or_reparse(&before) || !before.is_file() {
        bail!("{label} is not a stable regular file: {}", path.display());
    }
    let mut digest = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("read {label}: {}", path.display()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        total = total
            .checked_add(read as u64)
            .ok_or_else(|| anyhow!("{label} byte count overflows"))?;
    }
    let after_open = file
        .metadata()
        .with_context(|| format!("restat opened {label}: {}", path.display()))?;
    if before.len() != after_open.len() || before.modified().ok() != after_open.modified().ok() {
        bail!(
            "{label} changed while its bytes were being hashed: {}",
            path.display()
        );
    }
    if total != before.len() {
        bail!(
            "{label} byte count drifted while it was being hashed: {}",
            path.display()
        );
    }
    require_directory_identity(parent, parent_identity, &format!("{label} parent"))?;
    ensure_safe_components(&path, label, false)?;
    let after = identity_from_path(&path)
        .with_context(|| format!("re-identify {label}: {}", path.display()))?;
    require_same_identity(after, opened_identity, label, &path)?;
    Ok((
        V5FileDigest {
            byte_length: total,
            file_sha256: format!("sha256:{:x}", digest.finalize()),
        },
        V5StableFileIdentity {
            file_identity: opened_identity,
            parent_identity,
        },
    ))
}

fn require_v5_file_digest(
    path: &Path,
    expected_length: u64,
    expected_sha256: &str,
    label: &str,
) -> Result<V5StableFileIdentity> {
    let (observed, identity) = digest_stable_existing_file(path, label)?;
    if observed.byte_length != expected_length || observed.file_sha256 != expected_sha256 {
        bail!("{label} bytes drifted: {}", path.display());
    }
    Ok(identity)
}

/// Read an immutable target through a held file handle and compare identities
/// before and after the read. This rejects final-component symlink/reparse and
/// target-swap races rather than following an unknown path.
fn verify_existing(
    path: &Path,
    expected: &[u8],
    parent: FileIdentity,
) -> Result<Option<FileIdentity>> {
    let parent_path = path
        .parent()
        .ok_or_else(|| anyhow!("existing result has no parent"))?;
    require_directory_identity(parent_path, parent, "existing result parent")?;
    match fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect existing result: {}", path.display()));
        }
    }
    ensure_safe_components(path, "existing result", false)?;
    let before = identity_from_path(path)
        .with_context(|| format!("identify existing result: {}", path.display()))?;
    let mut file = fs::OpenOptions::new()
        .read(true)
        .open(path)
        .with_context(|| format!("open existing result: {}", path.display()))?;
    let opened = identity_from_file(&file)
        .with_context(|| format!("identify opened existing result: {}", path.display()))?;
    require_same_identity(opened, before, "existing result", path)?;
    let mut existing = Vec::new();
    file.read_to_end(&mut existing)
        .with_context(|| format!("read existing result: {}", path.display()))?;
    require_directory_identity(parent_path, parent, "existing result parent")?;
    ensure_safe_components(path, "existing result", false)?;
    let after = identity_from_path(path)
        .with_context(|| format!("re-identify existing result: {}", path.display()))?;
    require_same_identity(after, opened, "existing result", path)?;
    if existing != expected {
        bail!(
            "refusing to overwrite divergent immutable result: {}",
            path.display()
        )
    }
    Ok(Some(opened))
}

fn create_temporary<O: PublicationIo>(
    ops: &O,
    parent: &Path,
    filename: &str,
) -> Result<(PathBuf, fs::File)> {
    for _ in 0..TEMPORARY_CREATE_ATTEMPTS {
        let temporary = parent.join(format!(
            ".{filename}.{}-{}.tmp",
            std::process::id(),
            unique_suffix(),
        ));
        match ops.create_new(&temporary) {
            Ok(file) => return Ok((temporary, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("create result temporary: {}", temporary.display()));
            }
        }
    }
    bail!("could not allocate a unique result temporary after bounded attempts")
}

fn owned_temporary_present(
    temporary: &Path,
    temporary_identity: FileIdentity,
    parent_identity: FileIdentity,
) -> Result<bool> {
    let parent = temporary
        .parent()
        .ok_or_else(|| anyhow!("result temporary has no parent"))?;
    require_directory_identity(parent, parent_identity, "result temporary parent")?;
    let current = match fs::symlink_metadata(temporary) {
        Ok(current) => current,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("inspect owned result temporary: {}", temporary.display())
            });
        }
    };
    if is_link_or_reparse(&current) || !current.is_file() {
        bail!(
            "owned result temporary is not a regular file: {}",
            temporary.display()
        )
    }
    let current_identity = identity_from_path(temporary)
        .with_context(|| format!("identify owned result temporary: {}", temporary.display()))?;
    require_same_identity(
        current_identity,
        temporary_identity,
        "owned result temporary",
        temporary,
    )?;
    Ok(true)
}

fn require_owned_temporary(
    temporary: &Path,
    temporary_identity: FileIdentity,
    parent_identity: FileIdentity,
) -> Result<()> {
    if !owned_temporary_present(temporary, temporary_identity, parent_identity)? {
        bail!("owned result temporary vanished: {}", temporary.display())
    }
    Ok(())
}

fn remove_owned_temporary<O: PublicationIo>(
    ops: &O,
    temporary: &Path,
    temporary_identity: FileIdentity,
    parent_identity: FileIdentity,
) -> Result<()> {
    for delay_ms in SHARING_RETRY_DELAYS_MS
        .iter()
        .copied()
        .map(Some)
        .chain(std::iter::once(None))
    {
        if !owned_temporary_present(temporary, temporary_identity, parent_identity)? {
            return Ok(());
        }
        match ops.remove_file(temporary) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) if ops.is_sharing_violation(&error) && delay_ms.is_some() => {
                if let Some(delay_ms) = delay_ms {
                    ops.pause(Duration::from_millis(delay_ms));
                }
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("remove owned result temporary: {}", temporary.display())
                });
            }
        }
    }
    unreachable!("bounded temporary-removal loop always returns")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LinkOutcome {
    Published,
    Existing,
}

fn link_immutable<O: PublicationIo>(
    ops: &O,
    temporary: &Path,
    target: &Path,
    expected: &[u8],
    temporary_identity: FileIdentity,
    parent_identity: FileIdentity,
) -> Result<LinkOutcome> {
    for delay_ms in SHARING_RETRY_DELAYS_MS
        .iter()
        .copied()
        .map(Some)
        .chain(std::iter::once(None))
    {
        require_owned_temporary(temporary, temporary_identity, parent_identity)?;
        match ops.hard_link(temporary, target) {
            Ok(()) => {
                let published = verify_existing(target, expected, parent_identity)?
                    .ok_or_else(|| anyhow!("published result vanished: {}", target.display()))?;
                require_same_identity(published, temporary_identity, "published result", target)?;
                return Ok(LinkOutcome::Published);
            }
            Err(error) => {
                if verify_existing(target, expected, parent_identity)?.is_some() {
                    return Ok(LinkOutcome::Existing);
                }
                if ops.is_sharing_violation(&error)
                    && let Some(delay_ms) = delay_ms
                {
                    ops.pause(Duration::from_millis(delay_ms));
                    continue;
                }
                return Err(error).with_context(|| {
                    format!(
                        "publish immutable result without replacing an existing artifact: {}",
                        target.display()
                    )
                });
            }
        }
    }
    unreachable!("bounded immutable-link loop always returns")
}

fn finish_with_cleanup<O: PublicationIo>(
    ops: &O,
    outcome: Result<()>,
    temporary: &Path,
    temporary_identity: FileIdentity,
    parent_identity: FileIdentity,
) -> Result<()> {
    let cleanup = remove_owned_temporary(ops, temporary, temporary_identity, parent_identity);
    match (outcome, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Err(error), Err(cleanup)) => Err(error.context(format!(
            "owned result temporary could not be safely removed: {cleanup:#}"
        ))),
    }
}

fn replace_mutable_ledger(
    target: &Path,
    opened_input: &[u8],
    replacement: &[u8],
    expected_input_sha256: &str,
) -> Result<()> {
    let parent = target
        .parent()
        .ok_or_else(|| anyhow!("identity ledger path has no parent"))?;
    ensure_safe_existing_directory(parent, "identity ledger parent")?;
    let current = read_stable_existing_file(target, "caller identity ledger")?;
    if current == replacement {
        return Ok(());
    }
    if current != opened_input {
        bail!("caller identity ledger changed after opening expected {expected_input_sha256}")
    }
    let filename = target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("identity ledger filename is not UTF-8"))?;
    let mut temporary = None;
    let mut file = None;
    for _ in 0..TEMPORARY_CREATE_ATTEMPTS {
        let candidate = parent.join(format!(
            ".{filename}.{}-{}.replace.tmp",
            std::process::id(),
            unique_suffix(),
        ));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(created) => {
                temporary = Some(candidate);
                file = Some(created);
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).context("create mutable ledger replacement"),
        }
    }
    let temporary = temporary
        .ok_or_else(|| anyhow!("could not allocate mutable ledger replacement temporary"))?;
    let outcome = (|| -> Result<()> {
        let mut file = file.expect("replacement file accompanies its path");
        file.write_all(replacement)
            .context("write mutable ledger replacement")?;
        file.sync_all().context("sync mutable ledger replacement")?;
        drop(file);
        atomic_replace(&temporary, target).context("atomically replace caller identity ledger")?;
        sync_directory_platform(parent).context("synchronize identity ledger parent")?;
        let persisted = read_stable_existing_file(target, "replaced caller identity ledger")?;
        if persisted != replacement {
            bail!("caller identity ledger drifted after atomic replacement")
        }
        Ok(())
    })();
    if temporary.exists() {
        fs::remove_file(&temporary).with_context(|| {
            format!(
                "remove mutable ledger replacement temporary: {}",
                temporary.display()
            )
        })?;
    }
    outcome
}

#[cfg(not(windows))]
fn atomic_replace(source: &Path, target: &Path) -> io::Result<()> {
    fs::rename(source, target)
}

#[cfg(windows)]
fn atomic_replace(source: &Path, target: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x0000_0001;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;
    unsafe extern "system" {
        fn MoveFileExW(existing: *const u16, replacement: *const u16, flags: u32) -> i32;
    }
    let source = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let target = target
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    // SAFETY: both arguments are owned, NUL-terminated UTF-16 buffers that
    // remain alive for the duration of the Win32 call.
    if unsafe {
        MoveFileExW(
            source.as_ptr(),
            target.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    } == 0
    {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Install a fully-written result without replacing a pre-existing path. A
/// competing writer can only succeed when it produced byte-identical output.
fn publish_once(path: &Path, bytes: &[u8]) -> Result<()> {
    publish_once_with(&StdPublicationIo, path, bytes)
}

fn publish_once_with<O: PublicationIo>(ops: &O, path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("result has no parent"))?;
    ensure_safe_existing_directory(parent, "result parent")?;
    let parent_identity = identity_from_path(parent)
        .with_context(|| format!("identify result parent: {}", parent.display()))?;
    if verify_existing(path, bytes, parent_identity)?.is_some() {
        ops.sync_directory(parent)
            .with_context(|| format!("synchronize result parent: {}", parent.display()))?;
        require_directory_identity(parent, parent_identity, "result parent")?;
        verify_existing(path, bytes, parent_identity)?
            .ok_or_else(|| anyhow!("existing result vanished after directory sync"))?;
        return Ok(());
    }

    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("result filename is not Unicode"))?;
    let (temporary, mut file) = create_temporary(ops, parent, filename)?;
    let temporary_identity = identity_from_file(&file)
        .with_context(|| format!("identify result temporary: {}", temporary.display()))?;

    let write_outcome = ops
        .write_all(&mut file, bytes)
        .with_context(|| format!("write result temporary: {}", temporary.display()))
        .and_then(|()| {
            ops.sync_file(&file)
                .with_context(|| format!("synchronize result temporary: {}", temporary.display()))
        });
    drop(file);
    if let Err(error) = write_outcome {
        return finish_with_cleanup(
            ops,
            Err(error),
            &temporary,
            temporary_identity,
            parent_identity,
        );
    }

    let publication = (|| -> Result<()> {
        let link_outcome = link_immutable(
            ops,
            &temporary,
            path,
            bytes,
            temporary_identity,
            parent_identity,
        )?;
        ops.sync_directory(parent)
            .with_context(|| format!("synchronize result parent: {}", parent.display()))?;
        require_directory_identity(parent, parent_identity, "result parent")?;
        let after_sync = verify_existing(path, bytes, parent_identity)?
            .ok_or_else(|| anyhow!("published result vanished after directory sync"))?;
        if link_outcome == LinkOutcome::Published {
            require_same_identity(after_sync, temporary_identity, "published result", path)?;
        }
        Ok(())
    })();
    finish_with_cleanup(
        ops,
        publication,
        &temporary,
        temporary_identity,
        parent_identity,
    )
}

const V5_OUTPUT_INVENTORY_PATH: &str = V5_PROPOSAL_OBJECT_INVENTORY_PATH;
const V5_OUTPUT_RECEIPT_PATH: &str = "internal/v5-proposal/receipt.json";

fn parse_v5_canonical_document_bytes(bytes: &[u8], label: &str) -> Result<Value> {
    let value: Value = serde_json::from_slice(bytes)
        .with_context(|| format!("parse {label} as canonical JSON document"))?;
    if bytes != canonical_json_line(&value)? {
        bail!("{label} must be exactly one canonical JSON document followed by LF");
    }
    Ok(value)
}

/// Deliberately test-only: production v5 publication authenticates staged
/// files with bounded streaming reads and must never assemble a potentially
/// gigabyte-scale artifact as a byte vector.
#[cfg(test)]
fn require_v5_canonical_document_bytes(bytes: &[u8], label: &str) -> Result<()> {
    let _ = parse_v5_canonical_document_bytes(bytes, label)?;
    Ok(())
}

/// Publish one output-root entry through an owned temporary, file fsync,
/// write-once link, and parent-directory sync.  All output paths are sealed
/// relative paths and every created directory is walked without following a
/// symlink/reparse point.  Existing byte-identical entries are intentionally
/// accepted so a crash before the receipt can retry deterministically; any
/// divergent prior byte is refused by `publish_once_with`.
/// Small-fixture publisher retained solely for link-boundary tests.  Fresh
/// v5 construction is required to use the private file-backed staged path
/// below, never this `Vec<u8>` API.
#[cfg(test)]
fn publish_v5_relative_once_with<O: PublicationIo>(
    ops: &O,
    output_root: &Path,
    relative: &str,
    bytes: &[u8],
    label: &str,
) -> Result<()> {
    v5_safe_relative_output_path(relative, label)?;
    ensure_safe_existing_directory(output_root, "native v5 output root")?;
    let path = output_root.join(relative);
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("{label} has no parent path"))?;
    ensure_safe_directory_tree(parent, &format!("{label} parent"))?;
    publish_once_with(ops, &path, bytes)
        .with_context(|| format!("publish {label}: {}", path.display()))
}

const V5_PRIVATE_STAGING_NAMESPACE: &str = ".temporal-qd-v5-private-stage";

/// Private, manifest-bound staging directory.  It is deliberately a direct
/// output-root sibling of the public namespaces rather than a child of
/// `v5-native`, `g0-bootstrap`, `internal/v5-proposal`, or the invocation
/// root.  A process death after a public hard-link but before cleanup can
/// therefore never strand an unsealed temporary inside an adoptable namespace.
#[derive(Debug)]
struct V5PrivateStagingArea {
    root: PathBuf,
    root_identity: FileIdentity,
}

fn v5_private_staging_area(
    output_root: &Path,
    invocation_root: &Path,
    manifest_sha256: &str,
) -> Result<V5PrivateStagingArea> {
    let component = manifest_sha256
        .strip_prefix("sha256:")
        .filter(|value| {
            value.len() == 64
                && value
                    .bytes()
                    .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        })
        .ok_or_else(|| anyhow!("native v5 staging requires a canonical manifest SHA-256"))?;
    ensure_safe_existing_directory(output_root, "native v5 output root")?;
    ensure_safe_existing_directory(invocation_root, "native v5 invocation root")?;
    let root = output_root
        .join(V5_PRIVATE_STAGING_NAMESPACE)
        .join(component);
    if root.starts_with(invocation_root) || invocation_root.starts_with(&root) {
        bail!("native v5 private staging overlaps the invocation root");
    }
    ensure_safe_directory_tree(&root, "native v5 private staging root")?;
    let root_identity = identity_from_path(&root).with_context(|| {
        format!(
            "identify native v5 private staging root: {}",
            root.display()
        )
    })?;
    Ok(V5PrivateStagingArea {
        root,
        root_identity,
    })
}

/// A fully fsynced but still-private v5 artifact.  The source lives under the
/// manifest-bound private staging directory on the same volume as its target;
/// a large streaming writer never needs to retain the artifact's bytes in RAM
/// before the prepublication gate has accepted the complete bundle.
#[derive(Debug)]
struct V5StagedArtifact {
    relative_path: String,
    temporary: PathBuf,
    temporary_identity: FileIdentity,
    staging_parent_identity: FileIdentity,
    target_path: PathBuf,
    target_parent_identity: FileIdentity,
    digest: V5FileDigest,
}

/// An open, private dynamic-array fragment.  Fragments are deliberately
/// separate from staged public artifacts: the kernel may fill four of these
/// during one selected-record traversal, then splice their already-canonical
/// element streams into several public documents without materializing a
/// selected record again.
///
/// The file lives only below the manifest-bound private staging root.  It is
/// never eligible for a public hard-link and must be sealed before any core
/// assembler can consume it.
#[derive(Debug)]
struct V5OpenPrivateFragment {
    role: String,
    temporary: PathBuf,
    temporary_identity: FileIdentity,
    staging_parent_identity: FileIdentity,
    file: fs::File,
}

/// A fsynced, identity-checked private element stream.  This remains private
/// even after it is sealed; only an assembled `V5StagedArtifact` may enter
/// the receipt-last publication set.
#[derive(Debug)]
struct V5PrivateFragment {
    role: String,
    temporary: PathBuf,
    temporary_identity: FileIdentity,
    staging_parent_identity: FileIdentity,
    digest: V5FileDigest,
}

fn v5_private_fragment_role(role: &str) -> Result<()> {
    if role.is_empty()
        || !role
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        bail!("native v5 private fragment role is unsafe");
    }
    Ok(())
}

/// Allocate a single private fragment writer.  The caller may hold several
/// of these at once and hand their `file` handles to the typed core sink; the
/// sink remains the sole owner of element schema and ordering.
fn begin_v5_private_fragment<O: PublicationIo>(
    ops: &O,
    staging: &V5PrivateStagingArea,
    role: &str,
) -> Result<V5OpenPrivateFragment> {
    v5_private_fragment_role(role)?;
    require_directory_identity(
        &staging.root,
        staging.root_identity,
        "native v5 private staging root",
    )?;
    let (temporary, file) = create_temporary(ops, &staging.root, &format!("v5-fragment-{role}"))?;
    let temporary_identity = identity_from_file(&file).with_context(|| {
        format!(
            "identify native v5 private fragment: {}",
            temporary.display()
        )
    })?;
    let metadata = file
        .metadata()
        .with_context(|| format!("stat native v5 private fragment: {}", temporary.display()))?;
    if is_link_or_reparse(&metadata) || !metadata.is_file() {
        bail!(
            "native v5 private fragment is not a regular file: {}",
            temporary.display()
        );
    }
    Ok(V5OpenPrivateFragment {
        role: role.to_owned(),
        temporary,
        temporary_identity,
        staging_parent_identity: staging.root_identity,
        file,
    })
}

impl V5OpenPrivateFragment {
    fn writer(&mut self) -> &mut fs::File {
        &mut self.file
    }

    /// Fsync and close an element stream before it may be read by the core
    /// assembler.  Re-hashing at this boundary detects any in-place mutation
    /// as well as target replacement while keeping the bytes file-backed.
    fn seal<O: PublicationIo>(self, ops: &O) -> Result<V5PrivateFragment> {
        let V5OpenPrivateFragment {
            role,
            temporary,
            temporary_identity,
            staging_parent_identity,
            file,
        } = self;
        let opened_identity = identity_from_file(&file).with_context(|| {
            format!(
                "identify open native v5 private fragment: {}",
                temporary.display()
            )
        })?;
        require_same_identity(
            opened_identity,
            temporary_identity,
            "native v5 private fragment",
            &temporary,
        )?;
        let sync = ops.sync_file(&file).with_context(|| {
            format!(
                "synchronize native v5 private fragment: {}",
                temporary.display()
            )
        });
        drop(file);
        if let Err(error) = sync {
            let cleanup = remove_owned_temporary(
                ops,
                &temporary,
                temporary_identity,
                staging_parent_identity,
            );
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.context(format!(
                    "native v5 private fragment could not be safely removed after sync failure: {cleanup:#}"
                ))),
            };
        }
        require_owned_temporary(&temporary, temporary_identity, staging_parent_identity)?;
        let sealed = (|| -> Result<V5PrivateFragment> {
            let (digest, identity) =
                digest_stable_existing_file(&temporary, "native v5 private fragment")?;
            require_same_identity(
                identity.file_identity,
                temporary_identity,
                "native v5 private fragment",
                &temporary,
            )?;
            require_same_identity(
                identity.parent_identity,
                staging_parent_identity,
                "native v5 private fragment parent",
                temporary
                    .parent()
                    .ok_or_else(|| anyhow!("native v5 private fragment has no parent"))?,
            )?;
            Ok(V5PrivateFragment {
                role,
                temporary: temporary.clone(),
                temporary_identity,
                staging_parent_identity,
                digest,
            })
        })();
        match sealed {
            Ok(fragment) => Ok(fragment),
            Err(error) => {
                let cleanup = remove_owned_temporary(
                    ops,
                    &temporary,
                    temporary_identity,
                    staging_parent_identity,
                );
                match cleanup {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(error.context(format!(
                        "native v5 private fragment could not be safely removed after seal failure: {cleanup:#}"
                    ))),
                }
            }
        }
    }

    /// Discard only the exact file this process allocated.  In particular, an
    /// abandoned crash file with a similar prefix is never swept on restart.
    fn discard<O: PublicationIo>(self, ops: &O) -> Result<()> {
        let V5OpenPrivateFragment {
            temporary,
            temporary_identity,
            staging_parent_identity,
            file,
            ..
        } = self;
        drop(file);
        remove_owned_temporary(ops, &temporary, temporary_identity, staging_parent_identity)
    }
}

fn verify_v5_private_fragment(fragment: &V5PrivateFragment) -> Result<()> {
    require_owned_temporary(
        &fragment.temporary,
        fragment.temporary_identity,
        fragment.staging_parent_identity,
    )?;
    let (observed, identity) =
        digest_stable_existing_file(&fragment.temporary, "native v5 private fragment")?;
    require_same_identity(
        identity.file_identity,
        fragment.temporary_identity,
        "native v5 private fragment",
        &fragment.temporary,
    )?;
    require_same_identity(
        identity.parent_identity,
        fragment.staging_parent_identity,
        "native v5 private fragment parent",
        fragment
            .temporary
            .parent()
            .ok_or_else(|| anyhow!("native v5 private fragment has no parent"))?,
    )?;
    if observed != fragment.digest {
        bail!(
            "native v5 private fragment bytes drifted after sealing: {}",
            fragment.temporary.display()
        );
    }
    Ok(())
}

fn open_v5_private_fragment(fragment: &V5PrivateFragment) -> Result<fs::File> {
    verify_v5_private_fragment(fragment)?;
    let file = fs::OpenOptions::new()
        .read(true)
        .open(&fragment.temporary)
        .with_context(|| {
            format!(
                "open native v5 private fragment: {}",
                fragment.temporary.display()
            )
        })?;
    let opened_identity = identity_from_file(&file).with_context(|| {
        format!(
            "identify opened native v5 private fragment: {}",
            fragment.temporary.display()
        )
    })?;
    require_same_identity(
        opened_identity,
        fragment.temporary_identity,
        "native v5 private fragment",
        &fragment.temporary,
    )?;
    Ok(file)
}

fn discard_v5_private_fragment<O: PublicationIo>(
    ops: &O,
    fragment: &V5PrivateFragment,
) -> Result<()> {
    remove_owned_temporary(
        ops,
        &fragment.temporary,
        fragment.temporary_identity,
        fragment.staging_parent_identity,
    )
}

fn v5_private_fragment_role_for(kind: V5G0PublicationFragmentKind) -> &'static str {
    match kind {
        V5G0PublicationFragmentKind::PopulationCandidates => "population-candidates",
        V5G0PublicationFragmentKind::EvaluationCandidates => "evaluation-candidates",
        V5G0PublicationFragmentKind::EvaluationFunnelEntries => "funnel-entries",
        V5G0PublicationFragmentKind::GenerationJournalBindings => "journal-bindings",
    }
}

/// The evolved publication boundary deliberately has the same four
/// write-neutral fragment channels as G0.  Keep the mapping local and typed
/// to the core enum so a later schema change cannot silently route a rich
/// population row into a different private file.
fn v5_private_fragment_role_for_evolved(kind: V5EvolvedPublicationFragmentKind) -> &'static str {
    match kind {
        V5EvolvedPublicationFragmentKind::PopulationCandidates => "population-candidates",
        V5EvolvedPublicationFragmentKind::EvaluationCandidates => "evaluation-candidates",
        V5EvolvedPublicationFragmentKind::EvaluationFunnelEntries => "funnel-entries",
        V5EvolvedPublicationFragmentKind::GenerationJournalBindings => "journal-bindings",
    }
}

/// Four open file-backed dynamic-array fragments for exactly one selected
/// traversal.  The mapping is fixed by the typed core enum; batch never
/// interprets an element or reconstructs a rich candidate.
#[derive(Debug)]
struct V5OpenPrivateFragmentSet {
    population_candidates: V5OpenPrivateFragment,
    evaluation_candidates: V5OpenPrivateFragment,
    evaluation_funnel_entries: V5OpenPrivateFragment,
    generation_journal_bindings: V5OpenPrivateFragment,
}

/// Four sealed private fragments.  They can be reset/read repeatedly by the
/// core assembler, but are not public inventory entries and are always
/// removed once their staged public documents are accepted or rejected.
#[derive(Debug)]
struct V5PrivateFragmentSet {
    population_candidates: V5PrivateFragment,
    evaluation_candidates: V5PrivateFragment,
    evaluation_funnel_entries: V5PrivateFragment,
    generation_journal_bindings: V5PrivateFragment,
}

fn begin_v5_private_fragment_set<O: PublicationIo>(
    ops: &O,
    staging: &V5PrivateStagingArea,
) -> Result<V5OpenPrivateFragmentSet> {
    let population_candidates = begin_v5_private_fragment(
        ops,
        staging,
        v5_private_fragment_role_for(V5G0PublicationFragmentKind::PopulationCandidates),
    )?;
    let evaluation_candidates = match begin_v5_private_fragment(
        ops,
        staging,
        v5_private_fragment_role_for(V5G0PublicationFragmentKind::EvaluationCandidates),
    ) {
        Ok(fragment) => fragment,
        Err(error) => {
            let cleanup = population_candidates.discard(ops);
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.context(format!(
                    "native v5 population fragment could not be safely removed after allocation failure: {cleanup:#}"
                ))),
            };
        }
    };
    let evaluation_funnel_entries = match begin_v5_private_fragment(
        ops,
        staging,
        v5_private_fragment_role_for(V5G0PublicationFragmentKind::EvaluationFunnelEntries),
    ) {
        Ok(fragment) => fragment,
        Err(error) => {
            let first_cleanup = population_candidates.discard(ops);
            let second_cleanup = evaluation_candidates.discard(ops);
            return match (first_cleanup, second_cleanup) {
                (Ok(()), Ok(())) => Err(error),
                (first, second) => Err(error.context(format!(
                    "native v5 private fragments could not be safely removed after allocation failure: population={first:?}; evaluation={second:?}"
                ))),
            };
        }
    };
    let generation_journal_bindings = match begin_v5_private_fragment(
        ops,
        staging,
        v5_private_fragment_role_for(V5G0PublicationFragmentKind::GenerationJournalBindings),
    ) {
        Ok(fragment) => fragment,
        Err(error) => {
            let first_cleanup = population_candidates.discard(ops);
            let second_cleanup = evaluation_candidates.discard(ops);
            let third_cleanup = evaluation_funnel_entries.discard(ops);
            return match (first_cleanup, second_cleanup, third_cleanup) {
                (Ok(()), Ok(()), Ok(())) => Err(error),
                (first, second, third) => Err(error.context(format!(
                    "native v5 private fragments could not be safely removed after allocation failure: population={first:?}; evaluation={second:?}; funnel={third:?}"
                ))),
            };
        }
    };
    Ok(V5OpenPrivateFragmentSet {
        population_candidates,
        evaluation_candidates,
        evaluation_funnel_entries,
        generation_journal_bindings,
    })
}

struct V5PrivateFragmentSink<'a> {
    population_candidates: &'a mut fs::File,
    evaluation_candidates: &'a mut fs::File,
    evaluation_funnel_entries: &'a mut fs::File,
    generation_journal_bindings: &'a mut fs::File,
}

impl V5G0PublicationFragmentSink for V5PrivateFragmentSink<'_> {
    fn write_fragment(
        &mut self,
        kind: V5G0PublicationFragmentKind,
        canonical_bytes: &[u8],
    ) -> io::Result<()> {
        match kind {
            V5G0PublicationFragmentKind::PopulationCandidates => {
                self.population_candidates.write_all(canonical_bytes)
            }
            V5G0PublicationFragmentKind::EvaluationCandidates => {
                self.evaluation_candidates.write_all(canonical_bytes)
            }
            V5G0PublicationFragmentKind::EvaluationFunnelEntries => {
                self.evaluation_funnel_entries.write_all(canonical_bytes)
            }
            V5G0PublicationFragmentKind::GenerationJournalBindings => {
                self.generation_journal_bindings.write_all(canonical_bytes)
            }
        }
    }
}

impl V5EvolvedPublicationFragmentSink for V5PrivateFragmentSink<'_> {
    fn write_fragment(
        &mut self,
        kind: V5EvolvedPublicationFragmentKind,
        canonical_bytes: &[u8],
    ) -> io::Result<()> {
        match kind {
            V5EvolvedPublicationFragmentKind::PopulationCandidates => {
                self.population_candidates.write_all(canonical_bytes)
            }
            V5EvolvedPublicationFragmentKind::EvaluationCandidates => {
                self.evaluation_candidates.write_all(canonical_bytes)
            }
            V5EvolvedPublicationFragmentKind::EvaluationFunnelEntries => {
                self.evaluation_funnel_entries.write_all(canonical_bytes)
            }
            V5EvolvedPublicationFragmentKind::GenerationJournalBindings => {
                self.generation_journal_bindings.write_all(canonical_bytes)
            }
        }
    }
}

impl V5OpenPrivateFragmentSet {
    fn sink(&mut self) -> V5PrivateFragmentSink<'_> {
        V5PrivateFragmentSink {
            population_candidates: self.population_candidates.writer(),
            evaluation_candidates: self.evaluation_candidates.writer(),
            evaluation_funnel_entries: self.evaluation_funnel_entries.writer(),
            generation_journal_bindings: self.generation_journal_bindings.writer(),
        }
    }

    fn discard<O: PublicationIo>(self, ops: &O) -> Result<()> {
        let V5OpenPrivateFragmentSet {
            population_candidates,
            evaluation_candidates,
            evaluation_funnel_entries,
            generation_journal_bindings,
        } = self;
        let mut cleanup_error = None;
        for result in [
            population_candidates.discard(ops),
            evaluation_candidates.discard(ops),
            evaluation_funnel_entries.discard(ops),
            generation_journal_bindings.discard(ops),
        ] {
            if let Err(error) = result
                && cleanup_error.is_none()
            {
                cleanup_error = Some(error);
            }
        }
        match cleanup_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn seal<O: PublicationIo>(self, ops: &O) -> Result<V5PrivateFragmentSet> {
        let V5OpenPrivateFragmentSet {
            population_candidates,
            evaluation_candidates,
            evaluation_funnel_entries,
            generation_journal_bindings,
        } = self;
        let mut opened = vec![
            population_candidates,
            evaluation_candidates,
            evaluation_funnel_entries,
            generation_journal_bindings,
        ];
        let mut sealed = BTreeMap::<String, V5PrivateFragment>::new();
        while let Some(fragment) = opened.pop() {
            match fragment.seal(ops) {
                Ok(fragment) => {
                    // The four callers currently use fixed distinct roles,
                    // but never rely on that implementation detail for
                    // cleanup.  A future role mapping error must not leave a
                    // sealed private file behind for a later run to trip
                    // over; only our exact owned files are removed here.
                    if sealed.contains_key(&fragment.role) {
                        let duplicate_role = fragment.role.clone();
                        let mut cleanup_error = None;
                        if let Err(cleanup) = discard_v5_private_fragment(ops, &fragment) {
                            cleanup_error = Some(cleanup);
                        }
                        for fragment in opened {
                            if let Err(cleanup) = fragment.discard(ops)
                                && cleanup_error.is_none()
                            {
                                cleanup_error = Some(cleanup);
                            }
                        }
                        for fragment in sealed.values() {
                            if let Err(cleanup) = discard_v5_private_fragment(ops, fragment)
                                && cleanup_error.is_none()
                            {
                                cleanup_error = Some(cleanup);
                            }
                        }
                        let error = anyhow!(
                            "native v5 private fragment roles are not unique: {duplicate_role}"
                        );
                        return match cleanup_error {
                            Some(cleanup) => Err(error.context(format!(
                                "native v5 private fragments could not be safely removed after duplicate role: {cleanup:#}"
                            ))),
                            None => Err(error),
                        };
                    }
                    sealed.insert(fragment.role.clone(), fragment);
                }
                Err(error) => {
                    let mut cleanup_error = None;
                    for fragment in opened {
                        if let Err(cleanup) = fragment.discard(ops)
                            && cleanup_error.is_none()
                        {
                            cleanup_error = Some(cleanup);
                        }
                    }
                    for fragment in sealed.values() {
                        if let Err(cleanup) = discard_v5_private_fragment(ops, fragment)
                            && cleanup_error.is_none()
                        {
                            cleanup_error = Some(cleanup);
                        }
                    }
                    return match cleanup_error {
                        Some(cleanup) => Err(error.context(format!(
                            "native v5 private fragments could not be safely removed after seal failure: {cleanup:#}"
                        ))),
                        None => Err(error),
                    };
                }
            }
        }
        let expected_roles = [
            v5_private_fragment_role_for(V5G0PublicationFragmentKind::PopulationCandidates),
            v5_private_fragment_role_for(V5G0PublicationFragmentKind::EvaluationCandidates),
            v5_private_fragment_role_for(V5G0PublicationFragmentKind::EvaluationFunnelEntries),
            v5_private_fragment_role_for(V5G0PublicationFragmentKind::GenerationJournalBindings),
        ];
        if sealed.len() != expected_roles.len()
            || expected_roles
                .iter()
                .any(|role| !sealed.contains_key(*role))
        {
            let mut cleanup_error = None;
            for fragment in sealed.values() {
                if let Err(cleanup) = discard_v5_private_fragment(ops, fragment)
                    && cleanup_error.is_none()
                {
                    cleanup_error = Some(cleanup);
                }
            }
            let error = anyhow!("native v5 private fragment set has an incompatible role set");
            return match cleanup_error {
                Some(cleanup) => Err(error.context(format!(
                    "native v5 private fragments could not be safely removed after role-set validation: {cleanup:#}"
                ))),
                None => Err(error),
            };
        }
        let set = V5PrivateFragmentSet {
            // The role-set check above makes all four removals infallible.
            // Keeping them as explicit fixed fields prevents the typed core
            // fragment enum from being reinterpreted as a caller-selected
            // dynamic map.
            population_candidates: sealed
                .remove(v5_private_fragment_role_for(
                    V5G0PublicationFragmentKind::PopulationCandidates,
                ))
                .expect("validated native v5 population fragment role"),
            evaluation_candidates: sealed
                .remove(v5_private_fragment_role_for(
                    V5G0PublicationFragmentKind::EvaluationCandidates,
                ))
                .expect("validated native v5 evaluation fragment role"),
            evaluation_funnel_entries: sealed
                .remove(v5_private_fragment_role_for(
                    V5G0PublicationFragmentKind::EvaluationFunnelEntries,
                ))
                .expect("validated native v5 funnel fragment role"),
            generation_journal_bindings: sealed
                .remove(v5_private_fragment_role_for(
                    V5G0PublicationFragmentKind::GenerationJournalBindings,
                ))
                .expect("validated native v5 journal fragment role"),
        };
        debug_assert!(sealed.is_empty());
        Ok(set)
    }
}

impl V5PrivateFragmentSet {
    fn fragment(&self, kind: V5G0PublicationFragmentKind) -> &V5PrivateFragment {
        match kind {
            V5G0PublicationFragmentKind::PopulationCandidates => &self.population_candidates,
            V5G0PublicationFragmentKind::EvaluationCandidates => &self.evaluation_candidates,
            V5G0PublicationFragmentKind::EvaluationFunnelEntries => &self.evaluation_funnel_entries,
            V5G0PublicationFragmentKind::GenerationJournalBindings => {
                &self.generation_journal_bindings
            }
        }
    }

    fn verify_against_core(&self, fragments: &V5G0PublicationFragments) -> Result<()> {
        for kind in [
            V5G0PublicationFragmentKind::PopulationCandidates,
            V5G0PublicationFragmentKind::EvaluationCandidates,
            V5G0PublicationFragmentKind::EvaluationFunnelEntries,
            V5G0PublicationFragmentKind::GenerationJournalBindings,
        ] {
            let local = self.fragment(kind);
            if local.role != v5_private_fragment_role_for(kind) {
                bail!("native v5 private fragment role/core kind drifted");
            }
            verify_v5_private_fragment(local)?;
            let core = fragments.fragment(kind);
            if local.digest.file_sha256 != core.fragment_sha256
                || local.digest.byte_length != core.encoded_bytes
            {
                bail!(
                    "native v5 private fragment bytes differ from the typed core fragment receipt: {}",
                    kind.as_str()
                );
            }
        }
        Ok(())
    }

    /// Bind the same private, file-backed fragment set to the later-generation
    /// core receipt.  This is intentionally a separate typed entry point from
    /// G0: the public schemas diverge, while the four raw fragment channels
    /// remain byte-for-byte transport only.
    fn verify_against_evolved_core(
        &self,
        fragments: &V5EvolvedPublicationFragments,
        accepted_candidate_count: u64,
        proposal_attempt_count: u64,
    ) -> Result<()> {
        fragments
            .validate_for_counts(accepted_candidate_count, proposal_attempt_count)
            .context("validate typed native v5 evolved fragment count receipt")?;
        for kind in [
            V5EvolvedPublicationFragmentKind::PopulationCandidates,
            V5EvolvedPublicationFragmentKind::EvaluationCandidates,
            V5EvolvedPublicationFragmentKind::EvaluationFunnelEntries,
            V5EvolvedPublicationFragmentKind::GenerationJournalBindings,
        ] {
            let local = match kind {
                V5EvolvedPublicationFragmentKind::PopulationCandidates => {
                    &self.population_candidates
                }
                V5EvolvedPublicationFragmentKind::EvaluationCandidates => {
                    &self.evaluation_candidates
                }
                V5EvolvedPublicationFragmentKind::EvaluationFunnelEntries => {
                    &self.evaluation_funnel_entries
                }
                V5EvolvedPublicationFragmentKind::GenerationJournalBindings => {
                    &self.generation_journal_bindings
                }
            };
            if local.role != v5_private_fragment_role_for_evolved(kind) {
                bail!("native v5 evolved private fragment role/core kind drifted");
            }
            verify_v5_private_fragment(local)?;
            let core = fragments.fragment(kind);
            if local.digest.file_sha256 != core.fragment_sha256
                || local.digest.byte_length != core.encoded_bytes
            {
                bail!(
                    "native v5 evolved private fragment bytes differ from the typed core fragment receipt: {}",
                    kind.as_str()
                );
            }
        }
        Ok(())
    }

    fn discard<O: PublicationIo>(&self, ops: &O) -> Result<()> {
        let mut cleanup_error = None;
        for fragment in [
            &self.population_candidates,
            &self.evaluation_candidates,
            &self.evaluation_funnel_entries,
            &self.generation_journal_bindings,
        ] {
            if let Err(error) = discard_v5_private_fragment(ops, fragment)
                && cleanup_error.is_none()
            {
                cleanup_error = Some(error);
            }
        }
        match cleanup_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

struct V5PrivateFragmentSource<'a> {
    fragments: &'a V5PrivateFragmentSet,
}

impl<'a> V5PrivateFragmentSource<'a> {
    fn new(fragments: &'a V5PrivateFragmentSet) -> Self {
        Self { fragments }
    }
}

impl V5G0PublicationFragmentSource for V5PrivateFragmentSource<'_> {
    fn copy_fragment(
        &mut self,
        kind: V5G0PublicationFragmentKind,
        output: &mut dyn Write,
    ) -> io::Result<()> {
        let mut copy = || -> Result<()> {
            let fragment = self.fragments.fragment(kind);
            verify_v5_private_fragment(fragment)?;
            let mut input = open_v5_private_fragment(fragment)?;
            let copied = io::copy(&mut input, output).with_context(|| {
                format!(
                    "copy native v5 private fragment {}: {}",
                    kind.as_str(),
                    fragment.temporary.display()
                )
            })?;
            drop(input);
            if copied != fragment.digest.byte_length {
                bail!(
                    "native v5 private fragment length drifted while copying: {}",
                    kind.as_str()
                );
            }
            verify_v5_private_fragment(fragment)
        };
        copy().map_err(|error| io::Error::other(format!("{error:#}")))
    }
}

impl V5EvolvedPublicationFragmentSource for V5PrivateFragmentSource<'_> {
    fn copy_fragment(
        &mut self,
        kind: V5EvolvedPublicationFragmentKind,
        output: &mut dyn Write,
    ) -> io::Result<()> {
        let mut copy = || -> Result<()> {
            let fragment = match kind {
                V5EvolvedPublicationFragmentKind::PopulationCandidates => {
                    &self.fragments.population_candidates
                }
                V5EvolvedPublicationFragmentKind::EvaluationCandidates => {
                    &self.fragments.evaluation_candidates
                }
                V5EvolvedPublicationFragmentKind::EvaluationFunnelEntries => {
                    &self.fragments.evaluation_funnel_entries
                }
                V5EvolvedPublicationFragmentKind::GenerationJournalBindings => {
                    &self.fragments.generation_journal_bindings
                }
            };
            verify_v5_private_fragment(fragment)?;
            let mut input = open_v5_private_fragment(fragment)?;
            let copied = io::copy(&mut input, output).with_context(|| {
                format!(
                    "copy native v5 evolved private fragment {}: {}",
                    kind.as_str(),
                    fragment.temporary.display()
                )
            })?;
            drop(input);
            if copied != fragment.digest.byte_length {
                bail!(
                    "native v5 evolved private fragment length drifted while copying: {}",
                    kind.as_str()
                );
            }
            verify_v5_private_fragment(fragment)
        };
        copy().map_err(|error| io::Error::other(format!("{error:#}")))
    }
}

fn verify_existing_digest(
    path: &Path,
    expected: &V5FileDigest,
    parent: FileIdentity,
) -> Result<Option<FileIdentity>> {
    let parent_path = path
        .parent()
        .ok_or_else(|| anyhow!("existing v5 artifact has no parent"))?;
    require_directory_identity(parent_path, parent, "existing v5 artifact parent")?;
    match fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect existing v5 artifact: {}", path.display()));
        }
    }
    let (observed, identity) = digest_stable_existing_file(path, "existing v5 artifact")?;
    require_same_identity(
        identity.parent_identity,
        parent,
        "existing v5 artifact parent",
        parent_path,
    )?;
    if &observed != expected {
        bail!(
            "refusing to overwrite divergent immutable v5 artifact: {}",
            path.display()
        )
    }
    Ok(Some(identity.file_identity))
}

/// Verify a newly linked target by path, parent, identity, and expected size
/// without immediately reading its payload a second time.  The staged source
/// was hashed immediately before the hard-link and both names must resolve to
/// the same inode/file ID; the group publisher performs the full target hash
/// after the parent directory has been synchronized.
fn verify_existing_identity_and_length(
    path: &Path,
    expected_length: u64,
    parent: FileIdentity,
) -> Result<Option<FileIdentity>> {
    let parent_path = path
        .parent()
        .ok_or_else(|| anyhow!("existing v5 artifact has no parent"))?;
    require_directory_identity(parent_path, parent, "existing v5 artifact parent")?;
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect existing v5 artifact: {}", path.display()));
        }
    };
    if is_link_or_reparse(&metadata) || !metadata.is_file() || metadata.len() != expected_length {
        bail!(
            "existing v5 artifact identity/length drifted: {}",
            path.display()
        );
    }
    let identity = identity_from_path(path)
        .with_context(|| format!("identify existing v5 artifact: {}", path.display()))?;
    require_directory_identity(parent_path, parent, "existing v5 artifact parent")?;
    Ok(Some(identity))
}

/// Compare two authenticated files byte-for-byte without retaining either
/// file.  SHA-256 is the durable inventory identity, but write-once adoption
/// is deliberately stricter: a pre-existing target is accepted only when it
/// has the exact staged byte stream, not merely a matching declared digest.
fn require_staged_bytes_equal_existing(
    staged: &V5StagedArtifact,
    target: &Path,
    target_identity: FileIdentity,
) -> Result<()> {
    require_owned_temporary(
        &staged.temporary,
        staged.temporary_identity,
        staged.staging_parent_identity,
    )?;
    let mut source = fs::OpenOptions::new()
        .read(true)
        .open(&staged.temporary)
        .with_context(|| format!("open staged v5 artifact: {}", staged.temporary.display()))?;
    let source_identity = identity_from_file(&source).with_context(|| {
        format!(
            "identify staged v5 artifact: {}",
            staged.temporary.display()
        )
    })?;
    require_same_identity(
        source_identity,
        staged.temporary_identity,
        "staged v5 artifact",
        &staged.temporary,
    )?;
    let target = safe_existing_file(target, "existing v5 artifact")?;
    let mut existing = fs::OpenOptions::new()
        .read(true)
        .open(&target)
        .with_context(|| format!("open existing v5 artifact: {}", target.display()))?;
    let opened_target = identity_from_file(&existing)
        .with_context(|| format!("identify existing v5 artifact: {}", target.display()))?;
    require_same_identity(
        opened_target,
        target_identity,
        "existing v5 artifact",
        &target,
    )?;
    let mut source_buffer = vec![0_u8; 1024 * 1024];
    let mut target_buffer = vec![0_u8; 1024 * 1024];
    loop {
        let source_read = source
            .read(&mut source_buffer)
            .with_context(|| format!("read staged v5 artifact: {}", staged.temporary.display()))?;
        let target_read = existing
            .read(&mut target_buffer)
            .with_context(|| format!("read existing v5 artifact: {}", target.display()))?;
        if source_read != target_read
            || source_buffer[..source_read] != target_buffer[..target_read]
        {
            bail!(
                "refusing to overwrite divergent immutable v5 artifact: {}",
                target.display()
            );
        }
        if source_read == 0 {
            break;
        }
    }
    require_directory_identity(
        staged
            .temporary
            .parent()
            .ok_or_else(|| anyhow!("staged v5 artifact has no parent"))?,
        staged.staging_parent_identity,
        "staged v5 artifact parent",
    )?;
    require_owned_temporary(
        &staged.temporary,
        staged.temporary_identity,
        staged.staging_parent_identity,
    )?;
    let target_parent = target
        .parent()
        .ok_or_else(|| anyhow!("existing v5 artifact has no parent"))?;
    require_directory_identity(
        target_parent,
        staged.target_parent_identity,
        "existing v5 artifact parent",
    )?;
    let after_target = identity_from_path(&target)
        .with_context(|| format!("re-identify existing v5 artifact: {}", target.display()))?;
    require_same_identity(
        after_target,
        target_identity,
        "existing v5 artifact",
        &target,
    )
}

fn verify_staged_v5_artifact(staged: &V5StagedArtifact) -> Result<()> {
    require_owned_temporary(
        &staged.temporary,
        staged.temporary_identity,
        staged.staging_parent_identity,
    )?;
    let (observed, identity) =
        digest_stable_existing_file(&staged.temporary, "staged v5 artifact")?;
    require_same_identity(
        identity.parent_identity,
        staged.staging_parent_identity,
        "staged v5 artifact parent",
        staged
            .temporary
            .parent()
            .ok_or_else(|| anyhow!("staged v5 artifact has no parent"))?,
    )?;
    require_same_identity(
        identity.file_identity,
        staged.temporary_identity,
        "staged v5 artifact",
        &staged.temporary,
    )?;
    if observed != staged.digest {
        bail!(
            "staged v5 artifact bytes drifted after prepublication validation: {}",
            staged.temporary.display()
        )
    }
    Ok(())
}

/// Verify the immutable staging identities and expected length without
/// reading payload bytes.  Balanced fresh validation uses this for the full
/// closure because each source is independently re-hashed immediately before
/// publication; strict mode continues to hash here as an additional pass.
fn verify_staged_v5_artifact_identity(staged: &V5StagedArtifact) -> Result<()> {
    require_owned_temporary(
        &staged.temporary,
        staged.temporary_identity,
        staged.staging_parent_identity,
    )?;
    require_directory_identity(
        staged
            .temporary
            .parent()
            .ok_or_else(|| anyhow!("staged v5 artifact has no parent"))?,
        staged.staging_parent_identity,
        "staged v5 artifact parent",
    )?;
    require_directory_identity(
        staged
            .target_path
            .parent()
            .ok_or_else(|| anyhow!("staged v5 artifact target has no parent"))?,
        staged.target_parent_identity,
        "staged v5 artifact target parent",
    )?;
    let metadata = fs::symlink_metadata(&staged.temporary).with_context(|| {
        format!(
            "inspect staged v5 artifact identity: {}",
            staged.temporary.display()
        )
    })?;
    if is_link_or_reparse(&metadata)
        || !metadata.is_file()
        || metadata.len() != staged.digest.byte_length
    {
        bail!(
            "staged v5 artifact identity/length drifted: {}",
            staged.temporary.display()
        );
    }
    Ok(())
}

/// Open a previously sealed staged artifact for a typed streaming verifier.
/// The caller keeps the handle only for one verifier call and must re-check
/// the staged digest after closing it; no public target is touched here.
fn open_staged_v5_artifact(staged: &V5StagedArtifact, label: &str) -> Result<fs::File> {
    verify_staged_v5_artifact(staged)?;
    let file = fs::OpenOptions::new()
        .read(true)
        .open(&staged.temporary)
        .with_context(|| format!("open {label}: {}", staged.temporary.display()))?;
    let identity = identity_from_file(&file)
        .with_context(|| format!("identify open {label}: {}", staged.temporary.display()))?;
    require_same_identity(
        identity,
        staged.temporary_identity,
        label,
        &staged.temporary,
    )?;
    Ok(file)
}

fn link_staged_immutable<O: PublicationIo>(
    ops: &O,
    staged: &V5StagedArtifact,
    target: &Path,
) -> Result<LinkOutcome> {
    for delay_ms in SHARING_RETRY_DELAYS_MS
        .iter()
        .copied()
        .map(Some)
        .chain(std::iter::once(None))
    {
        verify_staged_v5_artifact(staged)?;
        match ops.hard_link(&staged.temporary, target) {
            Ok(()) => {
                let published = verify_existing_identity_and_length(
                    target,
                    staged.digest.byte_length,
                    staged.target_parent_identity,
                )?
                .ok_or_else(|| anyhow!("published v5 artifact vanished: {}", target.display()))?;
                require_same_identity(
                    published,
                    staged.temporary_identity,
                    "published v5 artifact",
                    target,
                )?;
                return Ok(LinkOutcome::Published);
            }
            Err(error) => {
                if let Some(existing) =
                    verify_existing_digest(target, &staged.digest, staged.target_parent_identity)?
                {
                    require_staged_bytes_equal_existing(staged, target, existing)?;
                    return Ok(LinkOutcome::Existing);
                }
                if ops.is_sharing_violation(&error)
                    && let Some(delay_ms) = delay_ms
                {
                    ops.pause(Duration::from_millis(delay_ms));
                    continue;
                }
                return Err(error).with_context(|| {
                    format!(
                        "publish staged immutable v5 artifact without replacing an existing artifact: {}",
                        target.display()
                    )
                });
            }
        }
    }
    unreachable!("bounded staged immutable-link loop always returns")
}

/// Stage a streaming v5 artifact under the private manifest-bound directory
/// without exposing its final name.  The writer is called exactly once; after
/// it returns the file is fsynced and re-hashed through a fixed-size buffer
/// before it can enter a sealed inventory.
fn stage_v5_relative_with_result<O, F, T>(
    ops: &O,
    output_root: &Path,
    staging: &V5PrivateStagingArea,
    relative: &str,
    label: &str,
    writer: F,
) -> Result<(V5StagedArtifact, T)>
where
    O: PublicationIo,
    F: FnOnce(&mut fs::File) -> Result<T>,
{
    v5_safe_relative_output_path(relative, label)?;
    ensure_safe_existing_directory(output_root, "native v5 output root")?;
    let target = output_root.join(relative);
    let target_parent = target
        .parent()
        .ok_or_else(|| anyhow!("{label} has no parent path"))?;
    ensure_safe_directory_tree(target_parent, &format!("{label} target parent"))?;
    let target_parent_identity = identity_from_path(target_parent).with_context(|| {
        format!(
            "identify {label} target parent: {}",
            target_parent.display()
        )
    })?;
    require_directory_identity(
        &staging.root,
        staging.root_identity,
        "native v5 private staging root",
    )?;
    let stage_filename = format!("v5-artifact-{}", &sha256_bytes(relative.as_bytes())[7..]);
    let (temporary, mut file) = create_temporary(ops, &staging.root, &stage_filename)?;
    let temporary_identity = identity_from_file(&file)
        .with_context(|| format!("identify {label} temporary: {}", temporary.display()))?;
    let write_outcome = writer(&mut file).and_then(|result| {
        ops.sync_file(&file)
            .with_context(|| format!("synchronize {label} temporary: {}", temporary.display()))
            .map(|()| result)
    });
    drop(file);
    let result = match write_outcome {
        Ok(result) => result,
        Err(error) => {
            let cleanup =
                remove_owned_temporary(ops, &temporary, temporary_identity, staging.root_identity);
            return match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.context(format!(
                    "{label} temporary could not be safely removed after write failure: {cleanup:#}"
                ))),
            };
        }
    };
    let staged = (|| -> Result<V5StagedArtifact> {
        let (digest, identity) = digest_stable_existing_file(&temporary, label)?;
        require_same_identity(
            identity.parent_identity,
            staging.root_identity,
            &format!("{label} staging parent"),
            &staging.root,
        )?;
        require_same_identity(
            identity.file_identity,
            temporary_identity,
            label,
            &temporary,
        )?;
        Ok(V5StagedArtifact {
            relative_path: relative.to_owned(),
            temporary: temporary.clone(),
            temporary_identity,
            staging_parent_identity: staging.root_identity,
            target_path: target,
            target_parent_identity,
            digest,
        })
    })();
    match staged {
        Ok(staged) => Ok((staged, result)),
        Err(error) => {
            let cleanup =
                remove_owned_temporary(ops, &temporary, temporary_identity, staging.root_identity);
            match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.context(format!(
                    "{label} temporary could not be safely removed after staging failure: {cleanup:#}"
                ))),
            }
        }
    }
}

fn stage_v5_relative_with<O, F>(
    ops: &O,
    output_root: &Path,
    staging: &V5PrivateStagingArea,
    relative: &str,
    label: &str,
    writer: F,
) -> Result<V5StagedArtifact>
where
    O: PublicationIo,
    F: FnOnce(&mut fs::File) -> Result<()>,
{
    stage_v5_relative_with_result(ops, output_root, staging, relative, label, |file| {
        writer(file)?;
        Ok(())
    })
    .map(|(staged, ())| staged)
}

fn stage_v5_relative_bytes_with<O: PublicationIo>(
    ops: &O,
    output_root: &Path,
    staging: &V5PrivateStagingArea,
    relative: &str,
    bytes: &[u8],
    label: &str,
) -> Result<V5StagedArtifact> {
    v5_safe_relative_output_path(relative, label)?;
    ensure_safe_existing_directory(output_root, "native v5 output root")?;
    let target = output_root.join(relative);
    let target_parent = target
        .parent()
        .ok_or_else(|| anyhow!("{label} has no parent path"))?;
    ensure_safe_directory_tree(target_parent, &format!("{label} target parent"))?;
    let target_parent_identity = identity_from_path(target_parent).with_context(|| {
        format!(
            "identify {label} target parent: {}",
            target_parent.display()
        )
    })?;
    require_directory_identity(
        &staging.root,
        staging.root_identity,
        "native v5 private staging root",
    )?;
    let stage_filename = format!("v5-artifact-{}", &sha256_bytes(relative.as_bytes())[7..]);
    let (temporary, mut file) = create_temporary(ops, &staging.root, &stage_filename)?;
    let temporary_identity = identity_from_file(&file)
        .with_context(|| format!("identify {label} temporary: {}", temporary.display()))?;
    let expected_digest = V5FileDigest {
        byte_length: bytes.len() as u64,
        file_sha256: sha256_bytes(bytes),
    };
    let write_outcome = ops
        .write_all(&mut file, bytes)
        .with_context(|| format!("write {label} staged bytes"))
        .and_then(|()| {
            ops.sync_file(&file)
                .with_context(|| format!("synchronize {label} temporary: {}", temporary.display()))
        })
        .and_then(|()| {
            let observed_identity = identity_from_file(&file).with_context(|| {
                format!("re-identify {label} temporary: {}", temporary.display())
            })?;
            require_same_identity(observed_identity, temporary_identity, label, &temporary)?;
            let observed_length = file
                .metadata()
                .with_context(|| format!("inspect {label} temporary: {}", temporary.display()))?
                .len();
            if observed_length != expected_digest.byte_length {
                bail!(
                    "{label} staged byte length drifted: expected {}, observed {observed_length}",
                    expected_digest.byte_length
                );
            }
            Ok(())
        });
    drop(file);
    if let Err(error) = write_outcome {
        let cleanup =
            remove_owned_temporary(ops, &temporary, temporary_identity, staging.root_identity);
        return match cleanup {
            Ok(()) => Err(error),
            Err(cleanup) => Err(error.context(format!(
                "{label} temporary could not be safely removed after byte staging failure: {cleanup:#}"
            ))),
        };
    }
    let staged = (|| -> Result<V5StagedArtifact> {
        require_owned_temporary(&temporary, temporary_identity, staging.root_identity)?;
        Ok(V5StagedArtifact {
            relative_path: relative.to_owned(),
            temporary: temporary.clone(),
            temporary_identity,
            staging_parent_identity: staging.root_identity,
            target_path: target,
            target_parent_identity,
            // The caller supplied these exact bytes and `write_all` plus the
            // length/identity checks above prove the complete buffer reached
            // the fsynced file.  Balanced publication still re-hashes every
            // source immediately before its hard-link; strict mode also
            // retains the independent prepublication pass.  Avoiding the
            // immediate read-back removes one redundant full-file pass for
            // every compact durable object without weakening the seal.
            digest: expected_digest,
        })
    })();
    match staged {
        Ok(staged) => Ok(staged),
        Err(error) => {
            let cleanup =
                remove_owned_temporary(ops, &temporary, temporary_identity, staging.root_identity);
            match cleanup {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.context(format!(
                    "{label} temporary could not be safely removed after byte staging failure: {cleanup:#}"
                ))),
            }
        }
    }
}

/// Stage the four core-owned public publication documents from a sealed
/// private fragment set.  Each writer only splices/hash-checks fragment bytes;
/// it cannot reach the selected materializer.  Before returning, the same
/// typed core stream re-verifies the staged files against the fragment
/// receipts, still without rich reconstruction.
fn stage_v5_publication_bundle_from_fragments<O: PublicationIo>(
    ops: &O,
    output_root: &Path,
    staging: &V5PrivateStagingArea,
    stream: &V5G0PublicationStream<'_>,
    fragments: &V5PrivateFragmentSet,
    core_fragments: &V5G0PublicationFragments,
) -> Result<(Vec<V5StagedArtifact>, V5G0PublicationReceipt)> {
    fragments.verify_against_core(core_fragments)?;
    let mut staged = Vec::with_capacity(4);
    let assembled = (|| -> Result<V5G0PublicationReceipt> {
        let (pair_config, _) = stage_v5_relative_with_result(
            ops,
            output_root,
            staging,
            "pair-config.json",
            "native v5 staged pair config",
            |file| stream.write_pair_config(file).map_err(Into::into),
        )?;
        staged.push(pair_config);

        let (population, _) = stage_v5_relative_with_result(
            ops,
            output_root,
            staging,
            "population.json",
            "native v5 staged population",
            |file| {
                let mut source = V5PrivateFragmentSource::new(fragments);
                stream
                    .write_population_from_fragments(core_fragments, &mut source, file)
                    .map_err(Into::into)
            },
        )?;
        staged.push(population);

        let population_receipt = {
            let mut source = V5PrivateFragmentSource::new(fragments);
            let mut input = open_staged_v5_artifact(&staged[1], "native v5 staged population")?;
            let receipt = stream
                .verify_population_from_fragments(core_fragments, &mut source, &mut input)
                .map_err(anyhow::Error::from)?;
            drop(input);
            verify_staged_v5_artifact(&staged[1])?;
            receipt
        };

        let (evaluation_population, _) = stage_v5_relative_with_result(
            ops,
            output_root,
            staging,
            "evaluation-population.json",
            "native v5 staged evaluation population",
            |file| {
                let mut source = V5PrivateFragmentSource::new(fragments);
                stream
                    .write_evaluation_population_from_fragments(
                        &population_receipt,
                        core_fragments,
                        &mut source,
                        file,
                    )
                    .map_err(Into::into)
            },
        )?;
        staged.push(evaluation_population);

        let evaluation_receipt = {
            let mut source = V5PrivateFragmentSource::new(fragments);
            let mut input =
                open_staged_v5_artifact(&staged[2], "native v5 staged evaluation population")?;
            let receipt = stream
                .verify_evaluation_population_from_fragments(
                    &population_receipt,
                    core_fragments,
                    &mut source,
                    &mut input,
                )
                .map_err(anyhow::Error::from)?;
            drop(input);
            verify_staged_v5_artifact(&staged[2])?;
            receipt
        };

        let (generation_journal, _) = stage_v5_relative_with_result(
            ops,
            output_root,
            staging,
            "generation-journal.json",
            "native v5 staged generation journal",
            |file| {
                let mut source = V5PrivateFragmentSource::new(fragments);
                stream
                    .write_generation_journal_from_fragments(
                        &population_receipt,
                        &evaluation_receipt,
                        core_fragments,
                        &mut source,
                        file,
                    )
                    .map_err(Into::into)
            },
        )?;
        staged.push(generation_journal);

        let mut source = V5PrivateFragmentSource::new(fragments);
        let mut pair = open_staged_v5_artifact(&staged[0], "native v5 staged pair config")?;
        let mut population = open_staged_v5_artifact(&staged[1], "native v5 staged population")?;
        let mut evaluation =
            open_staged_v5_artifact(&staged[2], "native v5 staged evaluation population")?;
        let mut journal =
            open_staged_v5_artifact(&staged[3], "native v5 staged generation journal")?;
        let receipt = stream
            .verify_bundle_from_fragments(
                core_fragments,
                &mut source,
                &mut pair,
                &mut population,
                &mut evaluation,
                &mut journal,
            )
            .map_err(anyhow::Error::from)?;
        drop((pair, population, evaluation, journal));
        for artifact in &staged {
            verify_staged_v5_artifact(artifact)?;
        }
        Ok(receipt)
    })();
    match assembled {
        Ok(receipt) => Ok((staged, receipt)),
        Err(error) => {
            let mut cleanup_error = None;
            for artifact in &staged {
                if let Err(cleanup) = discard_staged_v5_artifact(ops, artifact)
                    && cleanup_error.is_none()
                {
                    cleanup_error = Some(cleanup);
                }
            }
            match cleanup_error {
                Some(cleanup) => Err(error.context(format!(
                    "native v5 staged publication artifacts could not be safely removed after assembly failure: {cleanup:#}"
                ))),
                None => Err(error),
            }
        }
    }
}

/// Stage the five core-owned later-generation public documents from the
/// sealed private fragment streams.  Materialization happened exactly once
/// before this function; every dynamic document below only re-reads and
/// hash-checks those private bytes.  The separate evolved core receipt is
/// verified against all staged files before batch may delete the fragments or
/// publish a single output-root link.
fn stage_v5_evolved_publication_bundle_from_fragments<O: PublicationIo>(
    ops: &O,
    output_root: &Path,
    staging: &V5PrivateStagingArea,
    stream: &V5EvolvedPublicationStream<'_>,
    fragments: &V5PrivateFragmentSet,
    core_fragments: &V5EvolvedPublicationFragments,
) -> Result<(Vec<V5StagedArtifact>, V5EvolvedPublicationReceipt)> {
    fragments.verify_against_evolved_core(
        core_fragments,
        stream.accepted_count() as u64,
        stream.proposal_attempt_count() as u64,
    )?;
    let mut staged = Vec::with_capacity(5);
    let assembled = (|| -> Result<V5EvolvedPublicationReceipt> {
        let (pair_config, _) = stage_v5_relative_with_result(
            ops,
            output_root,
            staging,
            "pair-config.json",
            "native v5 staged evolved pair config",
            |file| stream.write_pair_config(file).map_err(Into::into),
        )?;
        staged.push(pair_config);

        let (identity_ledger, _) = stage_v5_relative_with_result(
            ops,
            output_root,
            staging,
            V5_EVOLVED_IDENTITY_LEDGER_RELATIVE_PATH,
            "native v5 staged evolved identity ledger",
            |file| stream.write_identity_ledger(file).map_err(Into::into),
        )?;
        staged.push(identity_ledger);

        let (population, _) = stage_v5_relative_with_result(
            ops,
            output_root,
            staging,
            "population.json",
            "native v5 staged evolved population",
            |file| {
                let mut source = V5PrivateFragmentSource::new(fragments);
                stream
                    .write_population_from_fragments(core_fragments, &mut source, file)
                    .map_err(Into::into)
            },
        )?;
        staged.push(population);

        let population_receipt = {
            let mut source = V5PrivateFragmentSource::new(fragments);
            let mut input =
                open_staged_v5_artifact(&staged[2], "native v5 staged evolved population")?;
            let receipt = stream
                .verify_population_from_fragments(core_fragments, &mut source, &mut input)
                .map_err(anyhow::Error::from)?;
            drop(input);
            verify_staged_v5_artifact(&staged[2])?;
            receipt
        };

        let (evaluation_population, _) = stage_v5_relative_with_result(
            ops,
            output_root,
            staging,
            "evaluation-population.json",
            "native v5 staged evolved evaluation population",
            |file| {
                let mut source = V5PrivateFragmentSource::new(fragments);
                stream
                    .write_evaluation_population_from_fragments(
                        &population_receipt,
                        core_fragments,
                        &mut source,
                        file,
                    )
                    .map_err(Into::into)
            },
        )?;
        staged.push(evaluation_population);

        let evaluation_receipt = {
            let mut source = V5PrivateFragmentSource::new(fragments);
            let mut input = open_staged_v5_artifact(
                &staged[3],
                "native v5 staged evolved evaluation population",
            )?;
            let receipt = stream
                .verify_evaluation_population_from_fragments(
                    &population_receipt,
                    core_fragments,
                    &mut source,
                    &mut input,
                )
                .map_err(anyhow::Error::from)?;
            drop(input);
            verify_staged_v5_artifact(&staged[3])?;
            receipt
        };

        let (generation_journal, _) = stage_v5_relative_with_result(
            ops,
            output_root,
            staging,
            "generation-journal.json",
            "native v5 staged evolved generation journal",
            |file| {
                let mut source = V5PrivateFragmentSource::new(fragments);
                stream
                    .write_generation_journal_from_fragments(
                        &population_receipt,
                        &evaluation_receipt,
                        core_fragments,
                        &mut source,
                        file,
                    )
                    .map_err(Into::into)
            },
        )?;
        staged.push(generation_journal);

        let mut source = V5PrivateFragmentSource::new(fragments);
        let mut pair = open_staged_v5_artifact(&staged[0], "native v5 staged evolved pair config")?;
        let mut identity_ledger =
            open_staged_v5_artifact(&staged[1], "native v5 staged evolved identity ledger")?;
        let mut population =
            open_staged_v5_artifact(&staged[2], "native v5 staged evolved population")?;
        let mut evaluation =
            open_staged_v5_artifact(&staged[3], "native v5 staged evolved evaluation population")?;
        let mut journal =
            open_staged_v5_artifact(&staged[4], "native v5 staged evolved generation journal")?;
        let receipt = stream
            .verify_bundle_from_fragments(
                core_fragments,
                &mut source,
                &mut pair,
                &mut identity_ledger,
                &mut population,
                &mut evaluation,
                &mut journal,
            )
            .map_err(anyhow::Error::from)?;
        drop((pair, identity_ledger, population, evaluation, journal));
        for artifact in &staged {
            verify_staged_v5_artifact(artifact)?;
        }
        Ok(receipt)
    })();
    match assembled {
        Ok(receipt) => Ok((staged, receipt)),
        Err(error) => {
            let mut cleanup_error = None;
            for artifact in &staged {
                if let Err(cleanup) = discard_staged_v5_artifact(ops, artifact)
                    && cleanup_error.is_none()
                {
                    cleanup_error = Some(cleanup);
                }
            }
            match cleanup_error {
                Some(cleanup) => Err(error.context(format!(
                    "native v5 staged evolved publication artifacts could not be safely removed after assembly failure: {cleanup:#}"
                ))),
                None => Err(error),
            }
        }
    }
}

/// Drive a core-owned selected-materialization callback directly into one
/// private staged file.  The batch layer deliberately receives neither a
/// population `Vec<u8>` nor a collection of rich candidate values: each
/// selection is materialized, written, and released before the next callback.
/// The core owns the selection order and every semantic byte; this helper is
/// only the bounded-memory file sink used by the receipt-last publisher.
#[allow(dead_code)]
fn stream_v5_selected_materializations_one_at_a_time<I, F>(
    file: &mut fs::File,
    selections: I,
    mut materialize_and_write: F,
) -> Result<()>
where
    I: IntoIterator,
    F: FnMut(I::Item, &mut fs::File) -> Result<()>,
{
    for selection in selections {
        materialize_and_write(selection, file)?;
    }
    Ok(())
}

fn publish_staged_v5_relative_once_with<O: PublicationIo>(
    ops: &O,
    output_root: &Path,
    staged: &V5StagedArtifact,
    label: &str,
) -> Result<()> {
    v5_safe_relative_output_path(&staged.relative_path, label)?;
    let target = output_root.join(&staged.relative_path);
    if target != staged.target_path {
        bail!(
            "native v5 staged artifact target path drifted: {}",
            staged.relative_path
        );
    }
    let target_parent = target
        .parent()
        .ok_or_else(|| anyhow!("{label} has no target parent"))?;
    ensure_safe_existing_directory(target_parent, &format!("{label} target parent"))?;
    let target_parent_identity = identity_from_path(target_parent).with_context(|| {
        format!(
            "identify {label} target parent: {}",
            target_parent.display()
        )
    })?;
    require_same_identity(
        target_parent_identity,
        staged.target_parent_identity,
        &format!("{label} target parent"),
        target_parent,
    )?;
    let publication = (|| -> Result<()> {
        let outcome = link_staged_immutable(ops, staged, &target)?;
        ops.sync_directory(target_parent)
            .with_context(|| format!("synchronize {label} parent: {}", target_parent.display()))?;
        require_directory_identity(
            target_parent,
            staged.target_parent_identity,
            &format!("{label} parent"),
        )?;
        let after = verify_existing_digest(&target, &staged.digest, staged.target_parent_identity)?
            .ok_or_else(|| anyhow!("published {label} vanished after directory sync"))?;
        if outcome == LinkOutcome::Published {
            require_same_identity(
                after,
                staged.temporary_identity,
                &format!("published {label}"),
                &target,
            )?;
        } else {
            require_staged_bytes_equal_existing(staged, &target, after)?;
        }
        Ok(())
    })();
    finish_with_cleanup(
        ops,
        publication,
        &staged.temporary,
        staged.temporary_identity,
        staged.staging_parent_identity,
    )
}

/// Publish a pre-receipt artifact group, then synchronize each distinct
/// parent directory once and perform the final target hash checks.  A G0
/// durable closure places thousands of immutable objects under one directory;
/// syncing and re-hashing that directory boundary after every individual
/// link was pure overhead because no inventory or receipt is adoptable until
/// the complete group has finished.
fn publish_staged_v5_group_with<O: PublicationIo>(
    ops: &O,
    output_root: &Path,
    artifacts: &[V5StagedArtifact],
    label: &str,
) -> Result<()> {
    let mut linked = Vec::with_capacity(artifacts.len());
    let mut parents = BTreeMap::<PathBuf, FileIdentity>::new();
    let publication = (|| -> Result<()> {
        for staged in artifacts {
            v5_safe_relative_output_path(&staged.relative_path, label)?;
            let target = output_root.join(&staged.relative_path);
            if target != staged.target_path {
                bail!(
                    "native v5 staged artifact target path drifted: {}",
                    staged.relative_path
                );
            }
            let parent = target
                .parent()
                .ok_or_else(|| anyhow!("{label} has no target parent"))?;
            ensure_safe_existing_directory(parent, &format!("{label} target parent"))?;
            let parent_identity = identity_from_path(parent)
                .with_context(|| format!("identify {label} target parent: {}", parent.display()))?;
            require_same_identity(
                parent_identity,
                staged.target_parent_identity,
                &format!("{label} target parent"),
                parent,
            )?;
            if let Some(existing) = parents.insert(parent.to_path_buf(), parent_identity)
                && existing != parent_identity
            {
                bail!("native v5 staged group parent identity drifted");
            }
            let outcome = link_staged_immutable(ops, staged, &target)?;
            linked.push((staged, target, outcome));
        }

        for (parent, identity) in &parents {
            ops.sync_directory(parent).with_context(|| {
                format!(
                    "synchronize native v5 staged group parent: {}",
                    parent.display()
                )
            })?;
            require_directory_identity(parent, *identity, "native v5 staged group parent")?;
        }

        for (staged, target, outcome) in &linked {
            let after =
                verify_existing_digest(target, &staged.digest, staged.target_parent_identity)?
                    .ok_or_else(|| {
                        anyhow!("published {label} vanished after group synchronization")
                    })?;
            if *outcome == LinkOutcome::Published {
                require_same_identity(
                    after,
                    staged.temporary_identity,
                    &format!("published {label}"),
                    target,
                )?;
            } else {
                require_staged_bytes_equal_existing(staged, target, after)?;
            }
        }
        Ok(())
    })();

    let cleanup = cleanup_staged_v5_artifacts(ops, artifacts);
    match (publication, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Err(error), Err(cleanup)) => Err(error.context(format!(
            "native v5 grouped staged artifacts could not be safely removed: {cleanup:#}"
        ))),
    }
}

fn discard_staged_v5_artifact<O: PublicationIo>(ops: &O, staged: &V5StagedArtifact) -> Result<()> {
    remove_owned_temporary(
        ops,
        &staged.temporary,
        staged.temporary_identity,
        staged.staging_parent_identity,
    )
}

/// Durable publication boundary for a typed v5 transaction.  `artifacts`
/// contains every public artifact and content-addressed object, already
/// canonicalized by the kernel/outer contract.  The ordering is intentionally
/// strict:
///
/// 1. all referenced artifacts and objects;
/// 2. the inventory that names their bytes;
/// 3. the receipt, the last output-tree seal;
/// 4. the small invocation result, the external completion marker.
///
/// A process death at any earlier boundary leaves no adoptable result.  A
/// death after the receipt but before the result is recovered by
/// `recover_v5_invocation_result_from_sealed_receipt`, without rerunning the
/// typed transaction.
#[cfg(test)]
fn publish_v5_receipt_last_with<O: PublicationIo>(
    ops: &O,
    output_root: &Path,
    artifacts: &[(String, Vec<u8>)],
    output_inventory: &[u8],
    receipt: &[u8],
    invocation_result_path: &Path,
    invocation_result: &[u8],
) -> Result<()> {
    let _ = parse_v5_canonical_jsonl(output_inventory, "native v5 object inventory")?;
    require_v5_canonical_document_bytes(receipt, "native v5 output receipt")?;
    require_v5_canonical_document_bytes(invocation_result, "native v5 invocation result")?;
    ensure_safe_existing_directory(output_root, "native v5 output root")?;

    let mut published_paths = BTreeSet::new();
    for (relative, _) in artifacts {
        v5_safe_relative_output_path(relative, "native v5 publication artifact")?;
        if relative == V5_OUTPUT_INVENTORY_PATH || relative == V5_OUTPUT_RECEIPT_PATH {
            bail!("native v5 publication artifact aliases a transaction seal: {relative}");
        }
        if !published_paths.insert(relative.clone()) {
            bail!("native v5 publication repeats an artifact path: {relative}");
        }
    }

    for (relative, bytes) in artifacts {
        publish_v5_relative_once_with(
            ops,
            output_root,
            relative,
            bytes,
            "native v5 transaction artifact",
        )?;
    }
    publish_v5_relative_once_with(
        ops,
        output_root,
        V5_OUTPUT_INVENTORY_PATH,
        output_inventory,
        "native v5 output inventory",
    )?;
    // The receipt is deliberately last among output-root entries.  Do not
    // move this before the inventory or an authenticated tree can claim a
    // receipt while one of its named facts is still only a temporary file.
    publish_v5_relative_once_with(
        ops,
        output_root,
        V5_OUTPUT_RECEIPT_PATH,
        receipt,
        "native v5 output receipt",
    )?;
    publish_once_with(ops, invocation_result_path, invocation_result)
        .context("publish native v5 invocation completion result")
}

/// Cross-check the typed transaction's fully assembled durable bundle before
/// its first artifact link.  The core owns the semantic transaction facts;
/// this outer gate owns only the sealed filesystem representation and proves
/// that it agrees byte-for-byte with the validated receipt/result contract.
///
/// `artifacts` deliberately includes both public artifacts and content-
/// addressed object-store files, but not the generated inventory, output-tree
/// receipt, or invocation result.  The latter three are checked separately so
/// their ordering cannot be smuggled through an artifact alias.
#[cfg(test)]
fn validate_v5_prepublication_bundle(
    manifest: &V5ProposalManifest,
    result: &V5ProposalResult,
    artifacts: &[(String, Vec<u8>)],
    output_inventory_bytes: &[u8],
    receipt_bytes: &[u8],
    invocation_result_bytes: &[u8],
) -> Result<()> {
    let receipt = parse_v5_canonical_document_bytes(receipt_bytes, "native v5 output receipt")?;
    let invocation_result =
        parse_v5_canonical_document_bytes(invocation_result_bytes, "native v5 invocation result")?;
    if invocation_result != result.value {
        bail!("native v5 invocation result bytes drift from the typed result");
    }
    let result_fields = result
        .value
        .as_object()
        .ok_or_else(|| anyhow!("native v5 typed result is not an object"))?;
    if result_fields.get("receipt") != Some(&receipt) {
        bail!("native v5 typed result receipt differs from the output receipt bytes");
    }
    let receipt_fields = receipt
        .as_object()
        .ok_or_else(|| anyhow!("native v5 output receipt is not an object"))?;
    let output_inventory = receipt_fields
        .get("outputInventory")
        .cloned()
        .ok_or_else(|| anyhow!("native v5 output receipt lacks compact output inventory"))?;
    let checked_result = validate_v5_proposal_result(&invocation_result, manifest)
        .context("validate native v5 prepublication result contract")?;
    if checked_result.value != result.value {
        bail!("native v5 prepublication result value drifted after validation");
    }

    let inventory_fields = output_inventory
        .as_object()
        .ok_or_else(|| anyhow!("native v5 output inventory is not an object"))?;
    let inventory_artifacts = inventory_fields
        .get("artifacts")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("native v5 output inventory lacks artifacts"))?;
    let object_entries = parse_v5_canonical_jsonl(
        output_inventory_bytes,
        "native v5 prepublication object inventory",
    )?;

    let mut supplied = BTreeMap::<String, &[u8]>::new();
    for (relative, bytes) in artifacts {
        v5_safe_relative_output_path(relative, "native v5 prepublication artifact")?;
        if relative == V5_OUTPUT_INVENTORY_PATH || relative == V5_OUTPUT_RECEIPT_PATH {
            bail!("native v5 prepublication artifact aliases a transaction seal: {relative}");
        }
        if supplied
            .insert(relative.clone(), bytes.as_slice())
            .is_some()
        {
            bail!("native v5 prepublication repeats an artifact path: {relative}");
        }
    }

    let mut public_artifacts = BTreeMap::<String, Vec<u8>>::new();
    for artifact in inventory_artifacts {
        let artifact = artifact
            .as_object()
            .ok_or_else(|| anyhow!("native v5 output inventory artifact is invalid"))?;
        let kind = artifact
            .get("kind")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 output inventory artifact lacks kind"))?;
        let relative = artifact
            .get("relativePath")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 output inventory artifact lacks path"))?;
        let expected_sha = artifact
            .get("fileSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 output inventory artifact lacks file SHA-256"))?;
        let expected_length = artifact
            .get("byteLength")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("native v5 output inventory artifact lacks byte length"))?;
        let bytes = supplied.remove(relative).ok_or_else(|| {
            anyhow!("native v5 prepublication lacks inventory artifact bytes: {relative}")
        })?;
        if bytes.len() as u64 != expected_length || sha256_bytes(bytes) != expected_sha {
            bail!("native v5 prepublication inventory artifact bytes drifted: {relative}");
        }
        if public_artifacts
            .insert(kind.to_owned(), bytes.to_vec())
            .is_some()
        {
            bail!("native v5 prepublication inventory repeats artifact kind: {kind}");
        }
    }

    let mut objects = BTreeMap::<String, Vec<u8>>::new();
    for object in &object_entries {
        let object = object
            .as_object()
            .ok_or_else(|| anyhow!("native v5 object-store entry is invalid"))?;
        let relative = object
            .get("relativePath")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 object-store entry lacks path"))?;
        let object_sha = object
            .get("objectSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 object-store entry lacks semantic identity"))?;
        let expected_sha = object
            .get("fileSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 object-store entry lacks file SHA-256"))?;
        let expected_length = object
            .get("byteLength")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("native v5 object-store entry lacks byte length"))?;
        let full_relative = format!("v5-native/objects/{relative}");
        let bytes = supplied.remove(&full_relative).ok_or_else(|| {
            anyhow!("native v5 prepublication lacks object-store bytes: {full_relative}")
        })?;
        if bytes.len() as u64 != expected_length || sha256_bytes(bytes) != expected_sha {
            bail!("native v5 prepublication object-store bytes drifted: {full_relative}");
        }
        let _ = parse_v5_canonical_document_bytes(bytes, "native v5 object-store object")?;
        if objects
            .insert(object_sha.to_owned(), bytes.to_vec())
            .is_some()
        {
            bail!("native v5 prepublication repeats object semantic identity: {object_sha}");
        }
    }
    if let Some((extra, _)) = supplied.first_key_value() {
        bail!("native v5 prepublication includes an undeclared artifact: {extra}");
    }

    let shared = public_artifacts
        .get("sharedAuthority")
        .ok_or_else(|| anyhow!("native v5 prepublication lacks shared authority artifact"))?;
    let shared_value =
        parse_v5_canonical_document_bytes(shared, "native v5 public shared authority artifact")?;
    if shared_value != manifest.frozen_authority {
        bail!("native v5 prepublication shared authority differs from the sealed manifest");
    }
    verify_v5_compact_journal_replay(manifest, result, &public_artifacts, &objects)
        .context("semantically replay native v5 compact prepublication journals")?;
    Ok(())
}

/// Keep validation adjacent to the durable publisher so a fresh caller cannot
/// accidentally link one output file before discovering an invalid
/// receipt/inventory/result bundle.
#[cfg(test)]
fn validate_and_publish_v5_receipt_last_with<O: PublicationIo>(
    ops: &O,
    output_root: &Path,
    manifest: &V5ProposalManifest,
    result: &V5ProposalResult,
    artifacts: &[(String, Vec<u8>)],
    output_inventory: &[u8],
    receipt: &[u8],
    invocation_result_path: &Path,
    invocation_result: &[u8],
) -> Result<()> {
    validate_v5_prepublication_bundle(
        manifest,
        result,
        artifacts,
        output_inventory,
        receipt,
        invocation_result,
    )?;
    publish_v5_receipt_last_with(
        ops,
        output_root,
        artifacts,
        output_inventory,
        receipt,
        invocation_result_path,
        invocation_result,
    )
}

/// File-backed counterpart to the small-fixture `Vec<u8>` bundle validator.
/// All staged files have already been written privately and fsynced; this
/// gate re-hashes them through bounded buffers, cross-binds the sealed outer
/// documents, and exposes compact replay bytes only for the typed journal
/// verifier.  It therefore remains safe when `population.json` is larger
/// than available RAM.
struct V5StagedBundle<'a> {
    artifacts: BTreeMap<String, &'a V5StagedArtifact>,
}

fn read_staged_v5_document(staged: &V5StagedArtifact, label: &str) -> Result<Value> {
    verify_staged_v5_artifact(staged)?;
    let raw = read_stable_existing_file(&staged.temporary, label)?;
    let parsed = parse_v5_canonical_document_bytes(&raw, label)?;
    verify_staged_v5_artifact(staged)?;
    Ok(parsed)
}

fn read_staged_v5_bytes(staged: &V5StagedArtifact, label: &str) -> Result<Vec<u8>> {
    verify_staged_v5_artifact(staged)?;
    let raw = read_stable_existing_file(&staged.temporary, label)?;
    verify_staged_v5_artifact(staged)?;
    Ok(raw)
}

fn stream_staged_v5_object_inventory(
    staged: &V5StagedArtifact,
    inventory: &Map<String, Value>,
) -> Result<BTreeMap<String, V5InventoryFile>> {
    let descriptor = v5_object_inventory_descriptor(inventory)?;
    if staged.relative_path != V5_OUTPUT_INVENTORY_PATH
        || descriptor.get("relativePath").and_then(Value::as_str) != Some(V5_OUTPUT_INVENTORY_PATH)
        || descriptor.get("fileSha256").and_then(Value::as_str)
            != Some(staged.digest.file_sha256.as_str())
        || descriptor.get("byteLength").and_then(Value::as_u64) != Some(staged.digest.byte_length)
    {
        bail!("native v5 staged object inventory descriptor/file binding drifted");
    }
    let expected_count = descriptor
        .get("objectCount")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("native v5 staged object inventory lacks count"))?;
    let expected_bytes = descriptor
        .get("objectByteCount")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("native v5 staged object inventory lacks object bytes"))?;
    let mut reader = BufReader::new(open_staged_v5_artifact(
        staged,
        "native v5 staged object inventory",
    )?);
    let mut objects = BTreeMap::new();
    let mut prior_sha: Option<String> = None;
    let mut ordinal = 0_u64;
    let mut object_bytes = 0_u64;
    loop {
        let mut line = Vec::new();
        let read = reader.read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        if line.len() > 16 * 1024 || line.last() != Some(&b'\n') {
            bail!("native v5 staged object inventory row is not bounded JSONL");
        }
        let value: Value = serde_json::from_slice(&line)?;
        if canonical_json_line(&value)? != line {
            bail!("native v5 staged object inventory row is not canonical");
        }
        let row = value
            .as_object()
            .ok_or_else(|| anyhow!("native v5 staged object inventory row is invalid"))?;
        let exact = [
            "schemaVersion",
            "ordinal",
            "relativePath",
            "objectSha256",
            "fileSha256",
            "byteLength",
            "rowSha256",
        ];
        if row.len() != exact.len()
            || exact.iter().any(|key| !row.contains_key(*key))
            || row.get("schemaVersion").and_then(Value::as_str)
                != Some(V5_PROPOSAL_OBJECT_INVENTORY_ROW_SCHEMA)
            || row.get("ordinal").and_then(Value::as_u64) != Some(ordinal)
        {
            bail!("native v5 staged object inventory row fields/ordinal drifted");
        }
        let object_sha = row
            .get("objectSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 staged object inventory row lacks object SHA"))?;
        let file_sha = row
            .get("fileSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 staged object inventory row lacks file SHA"))?;
        if object_sha.len() != 71
            || file_sha.len() != 71
            || !object_sha.starts_with("sha256:")
            || !file_sha.starts_with("sha256:")
            || !object_sha[7..]
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
            || !file_sha[7..]
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
            || prior_sha
                .as_deref()
                .is_some_and(|prior| object_sha <= prior)
        {
            bail!("native v5 staged object inventory rows are not strictly ordered");
        }
        let relative = format!("sha256/{}.json", &object_sha[7..]);
        if row.get("relativePath").and_then(Value::as_str) != Some(relative.as_str()) {
            bail!("native v5 staged object inventory object path drifted");
        }
        let mut material = row.clone();
        let supplied_row_sha = material
            .remove("rowSha256")
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
            .ok_or_else(|| anyhow!("native v5 staged object inventory row lacks self hash"))?;
        if canonical_sha256(&Value::Object(material))? != supplied_row_sha {
            bail!("native v5 staged object inventory row self hash drifted");
        }
        let byte_length = row
            .get("byteLength")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("native v5 staged object inventory row lacks byte length"))?;
        object_bytes = object_bytes
            .checked_add(byte_length)
            .ok_or_else(|| anyhow!("native v5 staged object inventory bytes overflow"))?;
        objects.insert(
            object_sha.to_owned(),
            V5InventoryFile {
                relative_path: format!("v5-native/objects/{relative}"),
                file_sha256: file_sha.to_owned(),
                byte_length,
            },
        );
        prior_sha = Some(object_sha.to_owned());
        ordinal += 1;
    }
    verify_staged_v5_artifact(staged)?;
    if ordinal != expected_count || object_bytes != expected_bytes {
        bail!("native v5 staged object inventory descriptor totals drifted");
    }
    Ok(objects)
}

fn staged_v5_artifact_for<'a>(
    bundle: &'a V5StagedBundle<'a>,
    relative: &str,
    label: &str,
) -> Result<&'a V5StagedArtifact> {
    bundle
        .artifacts
        .get(relative)
        .copied()
        .ok_or_else(|| anyhow!("native v5 staged bundle lacks {label}: {relative}"))
}

fn v5_staged_typed_object_values(bundle: &V5StagedBundle<'_>) -> Result<BTreeMap<String, Value>> {
    let mut objects = BTreeMap::new();
    for (relative, staged) in &bundle.artifacts {
        let Some(digest) = relative
            .strip_prefix("v5-native/objects/sha256/")
            .and_then(|path| path.strip_suffix(".json"))
        else {
            continue;
        };
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        {
            bail!("native v5 staged object path is incompatible: {relative}");
        }
        let value = read_staged_v5_document(staged, "native v5 staged durable object")?;
        if objects.insert(format!("sha256:{digest}"), value).is_some() {
            bail!("native v5 staged bundle repeats an immutable object identity");
        }
    }
    if objects.is_empty() {
        bail!("native v5 staged bundle lacks immutable objects");
    }
    Ok(objects)
}

/// Fresh balanced prepublication proof.  The current process still owns the
/// typed transaction and compact receipt objects that generated these bytes,
/// so reopening and reparsing the entire ~12k-object durable closure adds no
/// new scientific evidence.  This gate instead cross-binds every public
/// compact stream, the final scientific publication documents, and both G0
/// funnel authorities directly against the in-memory typed values.  Strict
/// mode and every restart/adoption path continue to use the full object-store
/// reconstruction below.
fn v5_verify_staged_fresh_g0_bundle(
    bundle: &V5StagedBundle<'_>,
    manifest: &V5ProposalManifest,
    result: &V5ProposalResult,
    request: &V5G0TransactionRequest,
    transaction: &V5G0TransactionResult,
    publication_receipt: &V5G0PublicationReceipt,
    g0_funnel_binding: &V5G0FunnelFragmentReceiptObjectBinding,
    g0_funnel_stream_binding: &V5G0FunnelProjectionStreamReceiptObjectBinding,
) -> Result<()> {
    let expected_documents = [
        (
            "v5-native/attempt-journal-root.json",
            transaction
                .attempt_journal
                .to_value()
                .context("encode fresh native v5 attempt journal")?,
            "attempt journal",
        ),
        (
            "v5-native/identity-ledger.json",
            transaction
                .identity_ledger
                .to_value()
                .context("encode fresh native v5 identity ledger")?,
            "identity ledger",
        ),
        (
            "v5-native/authority/shared-authority.json",
            manifest.frozen_authority.clone(),
            "shared authority",
        ),
        (
            "g0-bootstrap/accepted-pool.json",
            transaction
                .accepted_pool
                .clone()
                .ok_or_else(|| anyhow!("fresh native v5 transaction lacks accepted pool"))?,
            "G0 accepted pool",
        ),
        (
            "g0-bootstrap/campaign-construction-ledger.json",
            transaction
                .campaign_ledger
                .clone()
                .ok_or_else(|| anyhow!("fresh native v5 transaction lacks campaign ledger"))?,
            "G0 campaign ledger",
        ),
        (
            "g0-bootstrap/selection.json",
            transaction
                .g0_selection
                .clone()
                .ok_or_else(|| anyhow!("fresh native v5 transaction lacks G0 selection"))?,
            "G0 selection",
        ),
    ];
    for (relative, expected, label) in expected_documents {
        let observed = read_staged_v5_document(
            staged_v5_artifact_for(bundle, relative, label)?,
            &format!("fresh native v5 staged {label}"),
        )?;
        if observed != expected {
            bail!("fresh native v5 staged {label} differs from the typed transaction");
        }
    }

    let expected_attempts = transaction
        .attempts
        .iter()
        .map(|attempt| attempt.to_value().map_err(anyhow::Error::from))
        .collect::<Result<Vec<_>>>()?;
    let expected_records = transaction
        .accepted_records
        .iter()
        .map(|record| record.to_value().map_err(anyhow::Error::from))
        .collect::<Result<Vec<_>>>()?;
    let expected_projections = transaction
        .selected_projection_index
        .as_ref()
        .ok_or_else(|| anyhow!("fresh native v5 transaction lacks selected projections"))?
        .projections
        .iter()
        .map(|projection| projection.to_value().map_err(anyhow::Error::from))
        .collect::<Result<Vec<_>>>()?;
    for (relative, expected, label) in [
        (
            "v5-native/attempts.jsonl",
            expected_attempts,
            "attempt rows",
        ),
        (
            "v5-native/accepted-records.jsonl",
            expected_records,
            "accepted records",
        ),
        (
            "v5-native/selected-projections.jsonl",
            expected_projections,
            "selected projections",
        ),
    ] {
        let observed = parse_v5_canonical_jsonl(
            &read_staged_v5_bytes(
                staged_v5_artifact_for(bundle, relative, label)?,
                &format!("fresh native v5 staged {label}"),
            )?,
            &format!("fresh native v5 staged {label}"),
        )?;
        if observed != expected {
            bail!("fresh native v5 staged {label} differ from the typed transaction");
        }
    }

    let publication_receipt_value = publication_receipt
        .to_value()
        .context("encode fresh native v5 publication receipt")?;
    let mut pair = open_staged_v5_artifact(
        staged_v5_artifact_for(bundle, "pair-config.json", "pair config")?,
        "fresh native v5 staged pair config",
    )?;
    let mut population = open_staged_v5_artifact(
        staged_v5_artifact_for(bundle, "population.json", "population")?,
        "fresh native v5 staged population",
    )?;
    let mut evaluation = open_staged_v5_artifact(
        staged_v5_artifact_for(
            bundle,
            "evaluation-population.json",
            "evaluation population",
        )?,
        "fresh native v5 staged evaluation population",
    )?;
    let mut journal = open_staged_v5_artifact(
        staged_v5_artifact_for(bundle, "generation-journal.json", "generation journal")?,
        "fresh native v5 staged generation journal",
    )?;
    let verified_receipt = verify_v5_g0_publication_adoption(
        request,
        transaction,
        &publication_receipt_value,
        &mut pair,
        &mut population,
        &mut evaluation,
        &mut journal,
    )
    .context("verify fresh native v5 public bundle")?;
    drop((pair, population, evaluation, journal));
    if verified_receipt != *publication_receipt {
        bail!("fresh native v5 staged publication receipt differs from the typed receipt");
    }
    v5_verify_publication_receipt_inventory(result, &verified_receipt)?;

    g0_funnel_binding
        .validate()
        .context("validate fresh native v5 G0 funnel binding")?;
    let g0_funnel_binding_value = g0_funnel_binding
        .to_value()
        .context("encode fresh native v5 G0 funnel binding")?;
    let g0_funnel = verify_v5_g0_funnel_fragment_receipt(
        request,
        transaction,
        &verified_receipt,
        &g0_funnel_binding_value,
    )
    .context("verify fresh native v5 G0 funnel receipt")?;
    if g0_funnel.funnel_fragments_sha256()? != g0_funnel_binding.g0_funnel_fragments_sha256 {
        bail!("fresh native v5 G0 funnel identity drifted");
    }
    g0_funnel_stream_binding
        .validate()
        .context("validate fresh native v5 G0 funnel stream binding")?;
    let mut stream = open_staged_v5_artifact(
        staged_v5_artifact_for(
            bundle,
            V5_G0_FUNNEL_PROJECTION_STREAM_PATH,
            "G0 funnel projection stream",
        )?,
        "fresh native v5 staged G0 funnel projection stream",
    )?;
    let verified_stream = verify_v5_g0_funnel_projection_stream(
        &g0_funnel,
        &g0_funnel_stream_binding.value,
        &mut stream,
    )
    .context("verify fresh native v5 G0 funnel projection stream")?;
    if verified_stream.projection_stream_receipt_sha256()?
        != g0_funnel_stream_binding.g0_funnel_projection_stream_receipt_sha256
    {
        bail!("fresh native v5 G0 funnel projection-stream identity drifted");
    }
    Ok(())
}

fn v5_verify_staged_typed_bundle(
    bundle: &V5StagedBundle<'_>,
    manifest: &V5ProposalManifest,
    result: &V5ProposalResult,
    request: &V5G0TransactionRequest,
) -> Result<()> {
    let mut public_documents = BTreeMap::new();
    for (kind, relative) in [
        ("attemptJournal", "v5-native/attempt-journal-root.json"),
        ("identityLedger", "v5-native/identity-ledger.json"),
        (
            "sharedAuthority",
            "v5-native/authority/shared-authority.json",
        ),
        ("g0AcceptedPool", "g0-bootstrap/accepted-pool.json"),
        (
            "g0CampaignConstructionLedger",
            "g0-bootstrap/campaign-construction-ledger.json",
        ),
        ("g0Selection", "g0-bootstrap/selection.json"),
    ] {
        public_documents.insert(
            kind.to_owned(),
            read_staged_v5_document(
                staged_v5_artifact_for(bundle, relative, kind)?,
                "native v5 staged compact public document",
            )?,
        );
    }
    if v5_public_document(&public_documents, "sharedAuthority")? != &manifest.frozen_authority {
        bail!("native v5 staged shared authority differs from the sealed manifest");
    }
    let attempt_rows = parse_v5_canonical_jsonl(
        &read_staged_v5_bytes(
            staged_v5_artifact_for(bundle, "v5-native/attempts.jsonl", "attempt rows")?,
            "native v5 staged attempt rows",
        )?,
        "native v5 staged attempt rows",
    )?;
    let accepted_records = parse_v5_canonical_jsonl(
        &read_staged_v5_bytes(
            staged_v5_artifact_for(
                bundle,
                "v5-native/accepted-records.jsonl",
                "compact accepted records",
            )?,
            "native v5 staged compact accepted records",
        )?,
        "native v5 staged compact accepted records",
    )?;
    let selected_projections = parse_v5_canonical_jsonl(
        &read_staged_v5_bytes(
            staged_v5_artifact_for(
                bundle,
                "v5-native/selected-projections.jsonl",
                "selected projections",
            )?,
            "native v5 staged selected projections",
        )?,
        "native v5 staged selected projections",
    )?;
    let object_values = v5_staged_typed_object_values(bundle)?;
    let replay = v5_reconstruct_typed_transaction(
        manifest,
        result,
        request,
        &public_documents,
        &attempt_rows,
        &accepted_records,
        &selected_projections,
        &object_values,
    )?;
    let mut pair = open_staged_v5_artifact(
        staged_v5_artifact_for(bundle, "pair-config.json", "pair config")?,
        "native v5 staged pair config",
    )?;
    let mut population = open_staged_v5_artifact(
        staged_v5_artifact_for(bundle, "population.json", "population")?,
        "native v5 staged population",
    )?;
    let mut evaluation = open_staged_v5_artifact(
        staged_v5_artifact_for(
            bundle,
            "evaluation-population.json",
            "evaluation population",
        )?,
        "native v5 staged evaluation population",
    )?;
    let mut journal = open_staged_v5_artifact(
        staged_v5_artifact_for(bundle, "generation-journal.json", "generation journal")?,
        "native v5 staged generation journal",
    )?;
    let receipt = verify_v5_g0_publication_adoption(
        request,
        &replay.transaction,
        &replay.publication_receipt_value,
        &mut pair,
        &mut population,
        &mut evaluation,
        &mut journal,
    )
    .context("verify native v5 staged publication bundle without rich reconstruction")?;
    drop((pair, population, evaluation, journal));
    if receipt.to_value()? != replay.publication_receipt_value {
        bail!("native v5 staged publication receipt object differs from typed verifier");
    }
    v5_verify_publication_receipt_inventory(result, &receipt)?;
    let result_fields = result
        .value
        .as_object()
        .ok_or_else(|| anyhow!("native v5 staged result is invalid"))?;
    let g0_funnel_fragments_sha256 = result_fields
        .get("g0FunnelFragmentsSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native v5 staged result lacks G0 funnel receipt identity"))?;
    let g0_funnel_binding_value =
        object_values
            .get(g0_funnel_fragments_sha256)
            .ok_or_else(|| {
                anyhow!("native v5 staged object inventory lacks G0 funnel receipt binding")
            })?;
    let g0_funnel = verify_v5_g0_funnel_fragment_receipt(
        request,
        &replay.transaction,
        &receipt,
        g0_funnel_binding_value,
    )
    .context("verify native v5 staged G0 funnel receipt without private fragments")?;
    if g0_funnel
        .funnel_fragments_sha256()
        .context("identify native v5 staged G0 funnel receipt")?
        != g0_funnel_fragments_sha256
    {
        bail!("native v5 staged G0 funnel receipt/result root binding drifted");
    }
    let g0_funnel_stream_receipt_sha256 = result_fields
        .get("g0FunnelProjectionStreamReceiptSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            anyhow!("native v5 staged result lacks G0 funnel projection-stream identity")
        })?;
    let g0_funnel_stream_binding_value = object_values
        .get(g0_funnel_stream_receipt_sha256)
        .ok_or_else(|| {
            anyhow!(
                "native v5 staged object inventory lacks G0 funnel projection-stream receipt binding"
            )
        })?;
    let g0_funnel_stream_binding =
        V5G0FunnelProjectionStreamReceiptObjectBinding::from_value(g0_funnel_stream_binding_value)
            .context("parse native v5 staged G0 funnel projection-stream receipt binding")?;
    if g0_funnel_stream_binding.g0_funnel_projection_stream_receipt_sha256
        != g0_funnel_stream_receipt_sha256
    {
        bail!("native v5 staged G0 funnel projection-stream result root drifted");
    }
    let mut g0_funnel_stream = open_staged_v5_artifact(
        staged_v5_artifact_for(
            bundle,
            V5_G0_FUNNEL_PROJECTION_STREAM_PATH,
            "G0 funnel projection stream",
        )?,
        "native v5 staged G0 funnel projection stream",
    )?;
    let verified_stream_receipt = verify_v5_g0_funnel_projection_stream(
        &g0_funnel,
        &g0_funnel_stream_binding.value,
        &mut g0_funnel_stream,
    )
    .context("verify native v5 staged G0 funnel projection stream")?;
    if verified_stream_receipt
        .projection_stream_receipt_sha256()
        .context("identify native v5 staged G0 funnel projection-stream receipt")?
        != g0_funnel_stream_receipt_sha256
    {
        bail!("native v5 staged G0 funnel projection-stream receipt/result binding drifted");
    }
    for relative in [
        "pair-config.json",
        "population.json",
        "evaluation-population.json",
        "generation-journal.json",
        V5_G0_FUNNEL_PROJECTION_STREAM_PATH,
    ] {
        verify_staged_v5_artifact(staged_v5_artifact_for(
            bundle,
            relative,
            "publication artifact",
        )?)?;
    }
    Ok(())
}

/// Cross-bind the staged evolved durable closure, publication plan/receipt
/// objects, and five public artifact streams before the outer receipt is
/// allowed to publish.  This is intentionally a fresh-only deep gate: normal
/// sealed adoption uses the receipt-only verifier below and never reopens an
/// input archive or reconstructs structural parents.
#[allow(clippy::too_many_arguments)]
fn v5_verify_staged_evolved_typed_bundle(
    bundle: &V5StagedBundle<'_>,
    manifest: &V5ProposalManifest,
    result: &V5EvolvedProposalResult,
    request: &V5EvolvedTransactionRequest,
    transaction_sha256: &str,
    expected_plan_value: &Value,
    expected_publication_receipt_value: &Value,
    expected_publication_fragments_value: &Value,
) -> Result<()> {
    let result_fields = result
        .value
        .as_object()
        .ok_or_else(|| anyhow!("native v5 staged evolved result is not an object"))?;
    if result_fields
        .get("transactionSha256")
        .and_then(Value::as_str)
        != Some(transaction_sha256)
    {
        bail!("native v5 staged evolved result transaction root binding drifted");
    }
    let transaction_relative = format!(
        "v5-native/objects/sha256/{}.json",
        transaction_sha256
            .strip_prefix("sha256:")
            .ok_or_else(|| anyhow!("native v5 evolved transaction root lacks SHA-256 prefix"))?
    );
    let transaction_value = read_staged_v5_document(
        staged_v5_artifact_for(
            bundle,
            &transaction_relative,
            "native v5 staged evolved transaction root",
        )?,
        "native v5 staged evolved transaction root",
    )?;
    let transaction = V5EvolvedTransactionResult::from_value(&transaction_value)
        .context("parse native v5 staged evolved transaction root")?;
    if transaction
        .transaction_sha256()
        .context("identify native v5 staged evolved transaction root")?
        != transaction_sha256
    {
        bail!("native v5 staged evolved transaction root identity drifted");
    }
    transaction
        .verify_replay()
        .context("validate native v5 staged evolved transaction root")?;
    let durable_bindings = transaction
        .durable_object_bindings()
        .context("enumerate native v5 staged evolved durable closure")?;
    let mut expected_object_paths = BTreeSet::new();
    for binding in &durable_bindings {
        binding
            .validate()
            .context("validate expected native v5 evolved durable object binding")?;
        if !expected_object_paths.insert(binding.relative_path.clone()) {
            bail!("native v5 staged evolved durable closure repeats an object path");
        }
        let staged = staged_v5_artifact_for(
            bundle,
            &binding.relative_path,
            "native v5 staged evolved durable object",
        )?;
        let staged_value =
            read_staged_v5_document(staged, "native v5 staged evolved durable object")?;
        if staged_value != binding.value {
            bail!("native v5 staged evolved durable object bytes drift from its typed binding");
        }
    }
    let replayed =
        reconstruct_v5_evolved_transaction_from_durable_objects(request, &durable_bindings)
            .context("offline-replay native v5 staged evolved durable closure")?;
    if replayed
        .to_value()
        .context("encode replayed native v5 staged evolved transaction")?
        != transaction_value
    {
        bail!("native v5 staged evolved durable replay differs from its transaction root");
    }

    let plan_sha256 = result_fields
        .get("publicationPlanSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            anyhow!("native v5 staged evolved result lacks publication plan identity")
        })?;
    let receipt_sha256 = result_fields
        .get("publicationReceiptSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            anyhow!("native v5 staged evolved result lacks publication receipt identity")
        })?;
    let expected_fragments =
        V5EvolvedPublicationFragments::from_value(expected_publication_fragments_value)
            .context("parse expected native v5 evolved publication-fragments receipt")?;
    let expected_fragments_binding = expected_fragments
        .object_binding()
        .context("bind expected native v5 evolved publication-fragments receipt")?;
    expected_fragments_binding
        .validate()
        .context("validate expected native v5 evolved publication-fragments receipt binding")?;
    let fragments_sha256 = result_fields
        .get("publicationFragmentsSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            anyhow!("native v5 staged evolved result lacks publication-fragments identity")
        })?;
    if fragments_sha256 != expected_fragments_binding.fragment_bundle_sha256
        || &expected_fragments_binding.value != expected_publication_fragments_value
        || expected_fragments.accepted_candidate_count != transaction.accepted_records.len() as u64
        || expected_fragments.proposal_attempt_count != transaction.attempts.len() as u64
    {
        bail!(
            "native v5 staged evolved publication-fragments receipt differs from typed transaction"
        );
    }
    if !expected_object_paths.insert(expected_fragments_binding.relative_path.clone()) {
        bail!("native v5 staged evolved publication-fragments object aliases a durable object");
    }
    for sha in [plan_sha256, receipt_sha256] {
        let relative = format!(
            "v5-native/objects/sha256/{}.json",
            sha.strip_prefix("sha256:").ok_or_else(|| anyhow!(
                "native v5 evolved publication object lacks SHA-256 prefix"
            ))?
        );
        if !expected_object_paths.insert(relative) {
            bail!("native v5 staged evolved object closure aliases a durable object");
        }
    }
    let observed_object_paths = bundle
        .artifacts
        .keys()
        .filter(|relative| relative.starts_with("v5-native/objects/sha256/"))
        .cloned()
        .collect::<BTreeSet<_>>();
    if observed_object_paths != expected_object_paths {
        bail!("native v5 staged evolved object inventory is not the exact typed closure");
    }
    let fragments_value = read_staged_v5_document(
        staged_v5_artifact_for(
            bundle,
            &expected_fragments_binding.relative_path,
            "evolved publication-fragments receipt object",
        )?,
        "native v5 staged evolved publication-fragments receipt object",
    )?;
    let fragments = V5EvolvedPublicationFragments::from_value(&fragments_value)
        .context("parse native v5 staged evolved publication-fragments receipt")?;
    let fragments_binding = fragments
        .object_binding()
        .context("bind native v5 staged evolved publication-fragments receipt")?;
    if fragments_binding != expected_fragments_binding
        || fragments_value != *expected_publication_fragments_value
        || fragments.accepted_candidate_count != transaction.accepted_records.len() as u64
        || fragments.proposal_attempt_count != transaction.attempts.len() as u64
    {
        bail!(
            "native v5 staged evolved publication-fragments object differs from the typed receipt"
        );
    }
    let plan_relative = format!(
        "v5-native/objects/sha256/{}.json",
        plan_sha256
            .strip_prefix("sha256:")
            .ok_or_else(|| anyhow!("native v5 evolved publication plan lacks SHA-256 prefix"))?
    );
    let plan_value = read_staged_v5_document(
        staged_v5_artifact_for(bundle, &plan_relative, "evolved publication plan object")?,
        "native v5 staged evolved publication plan object",
    )?;
    let plan = V5EvolvedPublicationPlan::from_value(&plan_value)
        .context("parse native v5 staged evolved publication plan")?;
    if plan
        .publication_plan_sha256()
        .context("identify native v5 staged evolved publication plan")?
        != plan_sha256
        || &plan_value != expected_plan_value
    {
        bail!("native v5 staged evolved publication plan differs from the typed plan");
    }
    let receipt_relative = format!(
        "v5-native/objects/sha256/{}.json",
        receipt_sha256
            .strip_prefix("sha256:")
            .ok_or_else(|| anyhow!("native v5 evolved publication receipt lacks SHA-256 prefix"))?
    );
    let publication_receipt_value = read_staged_v5_document(
        staged_v5_artifact_for(
            bundle,
            &receipt_relative,
            "evolved publication receipt object",
        )?,
        "native v5 staged evolved publication receipt object",
    )?;
    let publication_receipt = V5EvolvedPublicationReceipt::from_value(&publication_receipt_value)
        .context("parse native v5 staged evolved publication receipt")?;
    if publication_receipt
        .publication_receipt_sha256()
        .context("identify native v5 staged evolved publication receipt")?
        != receipt_sha256
        || &publication_receipt_value != expected_publication_receipt_value
    {
        bail!("native v5 staged evolved publication receipt differs from the typed receipt");
    }

    let mut pair = open_staged_v5_artifact(
        staged_v5_artifact_for(bundle, "pair-config.json", "evolved pair config")?,
        "native v5 staged evolved pair config",
    )?;
    let mut identity_ledger = open_staged_v5_artifact(
        staged_v5_artifact_for(
            bundle,
            V5_EVOLVED_IDENTITY_LEDGER_RELATIVE_PATH,
            "evolved public identity ledger",
        )?,
        "native v5 staged evolved public identity ledger",
    )?;
    let mut population = open_staged_v5_artifact(
        staged_v5_artifact_for(bundle, "population.json", "evolved population")?,
        "native v5 staged evolved population",
    )?;
    let mut evaluation = open_staged_v5_artifact(
        staged_v5_artifact_for(
            bundle,
            "evaluation-population.json",
            "evolved evaluation population",
        )?,
        "native v5 staged evolved evaluation population",
    )?;
    let mut journal = open_staged_v5_artifact(
        staged_v5_artifact_for(
            bundle,
            "generation-journal.json",
            "evolved generation journal",
        )?,
        "native v5 staged evolved generation journal",
    )?;
    let verified_receipt = verify_v5_evolved_publication_adoption(
        request,
        &replayed,
        &plan_value,
        &publication_receipt_value,
        &mut pair,
        &mut identity_ledger,
        &mut population,
        &mut evaluation,
        &mut journal,
    )
    .context("verify native v5 staged evolved publication bundle without rich reconstruction")?;
    drop((pair, identity_ledger, population, evaluation, journal));
    if verified_receipt != publication_receipt {
        bail!("native v5 staged evolved publication receipt differs from typed verifier");
    }
    v5_verify_evolved_publication_receipt_inventory(result, &verified_receipt)?;
    if verified_receipt.identity_ledger.semantic_sha256
        != result_fields
            .get("identityLedgerSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 evolved result lacks public ledger identity"))?
    {
        bail!("native v5 staged evolved public ledger root differs from outer result");
    }
    for relative in [
        "pair-config.json",
        V5_EVOLVED_IDENTITY_LEDGER_RELATIVE_PATH,
        "population.json",
        "evaluation-population.json",
        "generation-journal.json",
    ] {
        verify_staged_v5_artifact(staged_v5_artifact_for(
            bundle,
            relative,
            "evolved publication artifact",
        )?)?;
    }
    let _ = manifest;
    Ok(())
}

fn v5_verify_evolved_publication_receipt_inventory(
    result: &V5EvolvedProposalResult,
    receipt: &V5EvolvedPublicationReceipt,
) -> Result<()> {
    let result_fields = result
        .value
        .as_object()
        .ok_or_else(|| anyhow!("native v5 evolved immutable result is invalid"))?;
    if result_fields
        .get("publicationPlanSha256")
        .and_then(Value::as_str)
        != Some(receipt.publication_plan_sha256.as_str())
        || result_fields
            .get("publicationRequestSha256")
            .and_then(Value::as_str)
            != Some(receipt.publication_request_sha256.as_str())
        || result_fields
            .get("publicationReceiptSha256")
            .and_then(Value::as_str)
            != Some(receipt.publication_receipt_sha256()?.as_str())
        || result_fields
            .get("identityLedgerSha256")
            .and_then(Value::as_str)
            != Some(receipt.identity_ledger.semantic_sha256.as_str())
    {
        bail!("native v5 evolved publication receipt/result root binding drifted");
    }
    let inventory = result_fields
        .get("receipt")
        .and_then(Value::as_object)
        .and_then(|receipt| receipt.get("outputInventory"))
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("native v5 evolved immutable result lacks output inventory"))?;
    let artifacts = inventory
        .get("artifacts")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("native v5 evolved immutable output inventory lacks artifacts"))?;
    let expected = [
        ("pairConfig", "pair-config.json", &receipt.pair_config),
        (
            "identityLedger",
            V5_EVOLVED_IDENTITY_LEDGER_RELATIVE_PATH,
            &receipt.identity_ledger,
        ),
        ("population", "population.json", &receipt.population),
        (
            "evaluationPopulation",
            "evaluation-population.json",
            &receipt.evaluation_population,
        ),
        (
            "generationJournal",
            "generation-journal.json",
            &receipt.generation_journal,
        ),
    ];
    for (kind, path, streamed) in expected {
        let artifact = artifacts
            .iter()
            .find(|value| {
                value.as_object().is_some_and(|artifact| {
                    artifact.get("kind").and_then(Value::as_str) == Some(kind)
                })
            })
            .and_then(Value::as_object)
            .ok_or_else(|| anyhow!("native v5 evolved output inventory lacks {kind} artifact"))?;
        if artifact.get("relativePath").and_then(Value::as_str) != Some(path)
            || artifact.get("semanticSha256").and_then(Value::as_str)
                != Some(streamed.semantic_sha256.as_str())
            || artifact.get("fileSha256").and_then(Value::as_str)
                != Some(streamed.file_sha256.as_str())
            || artifact.get("byteLength").and_then(Value::as_u64) != Some(streamed.encoded_bytes)
        {
            bail!(
                "native v5 evolved {kind} inventory identity differs from the publication receipt"
            );
        }
    }
    Ok(())
}

/// Later-generation counterpart to the G0 typed prepublication gate.  It
/// authenticates exact staged inventory/receipt/result bytes and every declared
/// file identity before the mandatory core semantic callback is allowed to
/// approve a first output-root link.
fn validate_v5_evolved_staged_prepublication_bundle_with<F>(
    manifest: &V5ProposalManifest,
    result: &V5EvolvedProposalResult,
    artifacts: &[V5StagedArtifact],
    output_inventory: &V5StagedArtifact,
    receipt: &V5StagedArtifact,
    invocation_result: &[u8],
    validation_mode: V5ValidationMode,
    semantic_replay: F,
) -> Result<()>
where
    F: FnOnce(&V5StagedBundle<'_>) -> Result<()>,
{
    if output_inventory.relative_path != V5_OUTPUT_INVENTORY_PATH
        || receipt.relative_path != V5_OUTPUT_RECEIPT_PATH
    {
        bail!("native v5 staged evolved transaction seals use incompatible output paths");
    }
    let mut supplied = BTreeMap::<String, &V5StagedArtifact>::new();
    for staged in artifacts {
        v5_safe_relative_output_path(&staged.relative_path, "native v5 staged evolved artifact")?;
        if staged.relative_path == V5_OUTPUT_INVENTORY_PATH
            || staged.relative_path == V5_OUTPUT_RECEIPT_PATH
        {
            bail!(
                "native v5 staged evolved artifact aliases a transaction seal: {}",
                staged.relative_path
            );
        }
        verify_staged_v5_artifact(staged)?;
        if supplied
            .insert(staged.relative_path.clone(), staged)
            .is_some()
        {
            bail!(
                "native v5 staged evolved bundle repeats an artifact path: {}",
                staged.relative_path
            );
        }
    }
    verify_staged_v5_artifact(output_inventory)?;
    verify_staged_v5_artifact(receipt)?;
    let bundle = V5StagedBundle {
        artifacts: supplied,
    };
    let receipt_value =
        read_staged_v5_document(receipt, "native v5 staged evolved output receipt")?;
    let invocation_result = parse_v5_canonical_document_bytes(
        invocation_result,
        "native v5 staged evolved invocation result",
    )?;
    if invocation_result != result.value {
        bail!("native v5 staged evolved invocation result bytes drift from the typed result");
    }
    let result_fields = result
        .value
        .as_object()
        .ok_or_else(|| anyhow!("native v5 staged evolved typed result is not an object"))?;
    if result_fields.get("receipt") != Some(&receipt_value) {
        bail!("native v5 staged evolved result receipt differs from the receipt bytes");
    }
    let receipt_fields = receipt_value
        .as_object()
        .ok_or_else(|| anyhow!("native v5 staged evolved output receipt is not an object"))?;
    let inventory = receipt_fields
        .get("outputInventory")
        .cloned()
        .ok_or_else(|| anyhow!("native v5 staged evolved receipt lacks output inventory"))?;
    let checked_result = validate_v5_evolved_proposal_result(&invocation_result, manifest)
        .context("validate native v5 staged evolved prepublication result contract")?;
    if checked_result.value != result.value {
        bail!("native v5 staged evolved result value drifted after validation");
    }
    let inventory_fields = inventory
        .as_object()
        .ok_or_else(|| anyhow!("native v5 staged evolved output inventory is not an object"))?;
    let inventory_artifacts = inventory_fields
        .get("artifacts")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("native v5 staged evolved output inventory lacks artifacts"))?;
    let object_entries = stream_staged_v5_object_inventory(output_inventory, inventory_fields)?;
    let mut declared_paths = BTreeSet::new();
    let mut artifact_kinds = BTreeSet::new();
    for artifact in inventory_artifacts {
        let fields = artifact
            .as_object()
            .ok_or_else(|| anyhow!("native v5 staged evolved inventory artifact is invalid"))?;
        let kind = fields
            .get("kind")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 staged evolved inventory artifact lacks kind"))?;
        let relative = fields
            .get("relativePath")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 staged evolved inventory artifact lacks path"))?;
        let staged = staged_v5_artifact_for(&bundle, relative, "evolved inventory artifact")?;
        if !artifact_kinds.insert(kind.to_owned())
            || !declared_paths.insert(relative.to_owned())
            || staged.digest.file_sha256
                != fields
                    .get("fileSha256")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        anyhow!("native v5 staged evolved inventory artifact lacks file SHA-256")
                    })?
            || staged.digest.byte_length
                != fields
                    .get("byteLength")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| {
                        anyhow!("native v5 staged evolved inventory artifact lacks byte length")
                    })?
        {
            bail!("native v5 staged evolved inventory artifact identity/path drifted: {relative}");
        }
    }
    let mut object_sha256s = BTreeSet::new();
    for (object_sha256, identity) in &object_entries {
        let full_relative = identity.relative_path.clone();
        let staged =
            staged_v5_artifact_for(&bundle, &full_relative, "evolved object-store artifact")?;
        if !object_sha256s.insert(object_sha256.to_owned())
            || !declared_paths.insert(full_relative.clone())
            || staged.digest.file_sha256 != identity.file_sha256
            || staged.digest.byte_length != identity.byte_length
        {
            bail!("native v5 staged evolved object-store identity/path drifted: {full_relative}");
        }
    }
    if declared_paths.len() != bundle.artifacts.len() {
        bail!("native v5 staged evolved bundle includes an undeclared artifact path");
    }
    semantic_replay(&bundle)?;
    // Publication re-hashes each staged source immediately before the hard
    // link and verifies the linked target. Balanced mode avoids this duplicate
    // whole-closure pass; strict mode preserves the historical paranoia gate.
    if validation_mode.is_strict() {
        for staged in artifacts {
            verify_staged_v5_artifact(staged)?;
        }
        verify_staged_v5_artifact(output_inventory)?;
        verify_staged_v5_artifact(receipt)?;
    }
    Ok(())
}

fn validate_v5_staged_prepublication_bundle_with<F>(
    manifest: &V5ProposalManifest,
    result: &V5ProposalResult,
    artifacts: &[V5StagedArtifact],
    output_inventory: &V5StagedArtifact,
    receipt: &V5StagedArtifact,
    invocation_result: &[u8],
    validation_mode: V5ValidationMode,
    semantic_replay: F,
) -> Result<()>
where
    F: FnOnce(&V5StagedBundle<'_>) -> Result<()>,
{
    if output_inventory.relative_path != V5_OUTPUT_INVENTORY_PATH
        || receipt.relative_path != V5_OUTPUT_RECEIPT_PATH
    {
        bail!("native v5 staged transaction seals use incompatible output paths");
    }
    let mut supplied = BTreeMap::<String, &V5StagedArtifact>::new();
    for staged in artifacts {
        v5_safe_relative_output_path(&staged.relative_path, "native v5 staged artifact")?;
        if staged.relative_path == V5_OUTPUT_INVENTORY_PATH
            || staged.relative_path == V5_OUTPUT_RECEIPT_PATH
        {
            bail!(
                "native v5 staged artifact aliases a transaction seal: {}",
                staged.relative_path
            );
        }
        if validation_mode.is_strict() {
            verify_staged_v5_artifact(staged)?;
        } else {
            verify_staged_v5_artifact_identity(staged)?;
        }
        if supplied
            .insert(staged.relative_path.clone(), staged)
            .is_some()
        {
            bail!(
                "native v5 staged bundle repeats an artifact path: {}",
                staged.relative_path
            );
        }
    }
    verify_staged_v5_artifact(output_inventory)?;
    verify_staged_v5_artifact(receipt)?;
    let bundle = V5StagedBundle {
        artifacts: supplied,
    };
    let receipt_value = read_staged_v5_document(receipt, "native v5 staged output receipt")?;
    let invocation_result =
        parse_v5_canonical_document_bytes(invocation_result, "native v5 staged invocation result")?;
    if invocation_result != result.value {
        bail!("native v5 staged invocation result bytes drift from the typed result");
    }
    let result_fields = result
        .value
        .as_object()
        .ok_or_else(|| anyhow!("native v5 staged typed result is not an object"))?;
    if result_fields.get("receipt") != Some(&receipt_value) {
        bail!("native v5 staged result receipt differs from the receipt bytes");
    }
    let receipt_fields = receipt_value
        .as_object()
        .ok_or_else(|| anyhow!("native v5 staged output receipt is not an object"))?;
    let inventory = receipt_fields
        .get("outputInventory")
        .cloned()
        .ok_or_else(|| anyhow!("native v5 staged receipt lacks output inventory"))?;
    let checked_result = validate_v5_proposal_result(&invocation_result, manifest)
        .context("validate native v5 staged prepublication result contract")?;
    if checked_result.value != result.value {
        bail!("native v5 staged result value drifted after validation");
    }

    let inventory_fields = inventory
        .as_object()
        .ok_or_else(|| anyhow!("native v5 staged output inventory is not an object"))?;
    let inventory_artifacts = inventory_fields
        .get("artifacts")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("native v5 staged output inventory lacks artifacts"))?;
    let object_entries = stream_staged_v5_object_inventory(output_inventory, inventory_fields)?;

    let mut public_paths = BTreeMap::<String, String>::new();
    let mut compact_artifacts = BTreeMap::<String, Vec<u8>>::new();
    for artifact in inventory_artifacts {
        let artifact = artifact
            .as_object()
            .ok_or_else(|| anyhow!("native v5 staged inventory artifact is invalid"))?;
        let kind = artifact
            .get("kind")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 staged inventory artifact lacks kind"))?;
        let relative = artifact
            .get("relativePath")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 staged inventory artifact lacks path"))?;
        let expected_sha = artifact
            .get("fileSha256")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("native v5 staged inventory artifact lacks file SHA-256"))?;
        let expected_length = artifact
            .get("byteLength")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("native v5 staged inventory artifact lacks byte length"))?;
        let staged = staged_v5_artifact_for(&bundle, relative, "inventory artifact")?;
        if staged.digest.byte_length != expected_length || staged.digest.file_sha256 != expected_sha
        {
            bail!("native v5 staged inventory artifact bytes drifted: {relative}");
        }
        if public_paths
            .insert(kind.to_owned(), relative.to_owned())
            .is_some()
        {
            bail!("native v5 staged inventory repeats artifact kind: {kind}");
        }
        if validation_mode.is_strict()
            && matches!(
                kind,
                "attemptJournal"
                    | "attemptRows"
                    | "compactJournal"
                    | "identityLedger"
                    | "selectedProjectionIndex"
            )
        {
            compact_artifacts.insert(
                kind.to_owned(),
                read_staged_v5_bytes(staged, "native v5 staged compact replay artifact")?,
            );
        }
    }

    let shared_path = public_paths
        .get("sharedAuthority")
        .ok_or_else(|| anyhow!("native v5 staged bundle lacks shared authority artifact"))?;
    let shared = read_staged_v5_document(
        staged_v5_artifact_for(&bundle, shared_path, "shared authority artifact")?,
        "native v5 staged shared authority artifact",
    )?;
    if shared != manifest.frozen_authority {
        bail!("native v5 staged shared authority differs from the sealed manifest");
    }

    let mut object_paths = BTreeMap::<String, String>::new();
    for (object_sha, identity) in &object_entries {
        let full_relative = identity.relative_path.clone();
        let staged = staged_v5_artifact_for(&bundle, &full_relative, "object-store artifact")?;
        if staged.digest.byte_length != identity.byte_length
            || staged.digest.file_sha256 != identity.file_sha256
        {
            bail!("native v5 staged object-store bytes drifted: {full_relative}");
        }
        if object_paths
            .insert(object_sha.to_owned(), full_relative)
            .is_some()
        {
            bail!("native v5 staged object store repeats semantic identity: {object_sha}");
        }
    }
    let supplied_count = bundle.artifacts.len();
    let declared_count = inventory_artifacts.len() + object_entries.len();
    if supplied_count != declared_count {
        bail!("native v5 staged bundle includes an undeclared artifact path");
    }

    if validation_mode.is_strict() {
        // Strict mode fetches the compact audit subset named by the canonical
        // attempt journal and independently replays it.  Balanced fresh mode
        // already owns the typed in-memory transaction that produced these
        // exact content-addressed bytes and therefore avoids reopening 4,000
        // audit objects merely to prove the same fact again.
        let attempt_rows = compact_artifacts
            .get("attemptRows")
            .ok_or_else(|| anyhow!("native v5 staged compact replay lacks attempt rows"))?;
        let attempt_rows = parse_v5_canonical_jsonl(attempt_rows, "native v5 staged attempt rows")?;
        let journal = V5AttemptJournal::from_rows(
            manifest.generation_index,
            &manifest.generation_config_sha256,
            &manifest.expected_authority_sha256,
            &attempt_rows,
        )
        .context("replay native v5 staged attempt object references")?;
        let mut audit_objects = BTreeMap::<String, Vec<u8>>::new();
        for object_sha in journal
            .attempts
            .iter()
            .map(|attempt| &attempt.outcome_audit_sha256)
        {
            let relative = object_paths.get(object_sha).ok_or_else(|| {
                anyhow!("native v5 staged object store lacks outcome audit {object_sha}")
            })?;
            let staged = staged_v5_artifact_for(&bundle, relative, "outcome-audit object")?;
            if audit_objects
                .insert(
                    object_sha.clone(),
                    read_staged_v5_bytes(staged, "native v5 staged outcome audit")?,
                )
                .is_some()
            {
                bail!("native v5 staged attempt journal repeats outcome-audit object identity");
            }
        }
        verify_v5_compact_journal_replay(manifest, result, &compact_artifacts, &audit_objects)
            .context("semantically replay native v5 staged compact journals")?;
    }
    // The caller is the typed core integration seam.  It must cross-bind the
    // remaining ledger/G0/evaluation/generation/population facts before any
    // file is linked; the generic batch layer intentionally cannot fabricate
    // a parallel rich reconstruction schema.
    semantic_replay(&bundle)?;
    // Publication re-hashes each staged source immediately before the hard
    // link and verifies the linked target. Balanced mode avoids this duplicate
    // whole-closure pass; strict mode preserves the historical paranoia gate.
    if validation_mode.is_strict() {
        for staged in artifacts {
            verify_staged_v5_artifact(staged)?;
        }
        verify_staged_v5_artifact(output_inventory)?;
        verify_staged_v5_artifact(receipt)?;
    }
    Ok(())
}

fn cleanup_staged_v5_bundle<O: PublicationIo>(
    ops: &O,
    artifacts: &[V5StagedArtifact],
    output_inventory: &V5StagedArtifact,
    receipt: &V5StagedArtifact,
) -> Result<()> {
    let mut cleanup_error = None;
    for staged in artifacts
        .iter()
        .chain(std::iter::once(output_inventory))
        .chain(std::iter::once(receipt))
    {
        if let Err(error) = discard_staged_v5_artifact(ops, staged)
            && cleanup_error.is_none()
        {
            cleanup_error = Some(error);
        }
    }
    match cleanup_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Remove only this invocation's exact private staged files.  This is used
/// before the inventory/receipt seals exist, so it must not enumerate or
/// sweep the manifest-bound staging root: an interrupted sibling invocation
/// or an unknown stale file is never ours to delete.
fn cleanup_staged_v5_artifacts<O: PublicationIo>(
    ops: &O,
    artifacts: &[V5StagedArtifact],
) -> Result<()> {
    let mut cleanup_error = None;
    for staged in artifacts {
        if let Err(error) = discard_staged_v5_artifact(ops, staged)
            && cleanup_error.is_none()
        {
            cleanup_error = Some(error);
        }
    }
    match cleanup_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn publish_v5_staged_receipt_last_with<O: PublicationIo>(
    ops: &O,
    output_root: &Path,
    artifacts: &[V5StagedArtifact],
    output_inventory: &V5StagedArtifact,
    receipt: &V5StagedArtifact,
    invocation_result_path: &Path,
    invocation_result: &[u8],
) -> Result<()> {
    let publication = (|| -> Result<()> {
        publish_staged_v5_group_with(
            ops,
            output_root,
            artifacts,
            "native v5 staged transaction artifact",
        )?;
        publish_staged_v5_relative_once_with(
            ops,
            output_root,
            output_inventory,
            "native v5 staged output inventory",
        )?;
        // The immutable receipt remains last among output-root artifacts.
        // The invocation result is outside that inventory and is deliberately
        // linked only after the receipt directory entry has been synchronized.
        publish_staged_v5_relative_once_with(
            ops,
            output_root,
            receipt,
            "native v5 staged output receipt",
        )?;
        publish_once_with(ops, invocation_result_path, invocation_result)
            .context("publish native v5 staged invocation completion result")
    })();
    let cleanup = cleanup_staged_v5_bundle(ops, artifacts, output_inventory, receipt);
    match (publication, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Err(error), Err(cleanup)) => Err(error.context(format!(
            "native v5 private staged artifacts could not be safely removed: {cleanup:#}"
        ))),
    }
}

/// File-backed receipt-last integration boundary.  Calling this is the only
/// production path allowed to link a fresh typed v5 output: validation runs
/// first, then staged artifact links, then inventory, receipt, and finally
/// the small invocation result.  The callback is intentionally mandatory so
/// the core-owned typed semantic replay cannot be bypassed by a filesystem
/// assembly bug.
fn validate_and_publish_v5_staged_receipt_last_with<O, F>(
    ops: &O,
    output_root: &Path,
    manifest: &V5ProposalManifest,
    result: &V5ProposalResult,
    artifacts: &[V5StagedArtifact],
    output_inventory: &V5StagedArtifact,
    receipt: &V5StagedArtifact,
    invocation_result_path: &Path,
    invocation_result: &[u8],
    validation_mode: V5ValidationMode,
    semantic_replay: F,
) -> Result<V5PublishDurations>
where
    O: PublicationIo,
    F: FnOnce(&V5StagedBundle<'_>) -> Result<()>,
{
    let validation_started = Instant::now();
    let validation = validate_v5_staged_prepublication_bundle_with(
        manifest,
        result,
        artifacts,
        output_inventory,
        receipt,
        invocation_result,
        validation_mode,
        semantic_replay,
    );
    let validation_elapsed = validation_started.elapsed();
    if let Err(error) = validation {
        let cleanup = cleanup_staged_v5_bundle(ops, artifacts, output_inventory, receipt);
        return match cleanup {
            Ok(()) => Err(error),
            Err(cleanup) => Err(error.context(format!(
                "invalid native v5 staged bundle could not be safely removed: {cleanup:#}"
            ))),
        };
    }
    let publication_started = Instant::now();
    publish_v5_staged_receipt_last_with(
        ops,
        output_root,
        artifacts,
        output_inventory,
        receipt,
        invocation_result_path,
        invocation_result,
    )?;
    Ok(V5PublishDurations {
        validation: validation_elapsed,
        publication: publication_started.elapsed(),
    })
}

/// Receipt-last publisher for the distinct evolved outer contract.  Keeping
/// this sibling explicit prevents an evolved transaction from being accepted
/// through G0's compact-journal replay assumptions.
fn validate_and_publish_v5_evolved_staged_receipt_last_with<O, F>(
    ops: &O,
    output_root: &Path,
    manifest: &V5ProposalManifest,
    result: &V5EvolvedProposalResult,
    artifacts: &[V5StagedArtifact],
    output_inventory: &V5StagedArtifact,
    receipt: &V5StagedArtifact,
    invocation_result_path: &Path,
    invocation_result: &[u8],
    validation_mode: V5ValidationMode,
    semantic_replay: F,
) -> Result<V5PublishDurations>
where
    O: PublicationIo,
    F: FnOnce(&V5StagedBundle<'_>) -> Result<()>,
{
    let validation_started = Instant::now();
    let validation = validate_v5_evolved_staged_prepublication_bundle_with(
        manifest,
        result,
        artifacts,
        output_inventory,
        receipt,
        invocation_result,
        validation_mode,
        semantic_replay,
    );
    let validation_elapsed = validation_started.elapsed();
    if let Err(error) = validation {
        let cleanup = cleanup_staged_v5_bundle(ops, artifacts, output_inventory, receipt);
        return match cleanup {
            Ok(()) => Err(error),
            Err(cleanup) => Err(error.context(format!(
                "invalid native v5 staged evolved bundle could not be safely removed: {cleanup:#}"
            ))),
        };
    }
    let publication_started = Instant::now();
    publish_v5_staged_receipt_last_with(
        ops,
        output_root,
        artifacts,
        output_inventory,
        receipt,
        invocation_result_path,
        invocation_result,
    )?;
    Ok(V5PublishDurations {
        validation: validation_elapsed,
        publication: publication_started.elapsed(),
    })
}

fn unique_suffix() -> u128 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed) as u128;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    now ^ counter
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};

    use temporal_qd_kernel::v5_publication::V5G0PublicationFragment;

    use super::*;

    const EXACT: &[u8] = b"exact\n";

    #[test]
    fn v5_unsupported_parent_share_is_rebound_to_the_verified_archive() {
        let zero = RotatingParentSchedule::from_counts(128, 0).unwrap();
        let zero_value = serde_json::json!({
            "schemaVersion": "temporal_qd_rotating_parent_schedule_v2",
            "breederWidth": zero.breeder_width,
            "breederParentCount": zero.breeder_parent_count,
            "minimumImmigrantNumerator": 1,
            "minimumImmigrantDenominator": 5,
            "parentSampling": "with_replacement_supported_parents_v1",
            "unsupportedParentPolicy": "immigrant_only_authority_bound_v1",
            "schedulingMethod": "accepted_quota_prefix_balance_v1",
            "scheduleSha256": zero.schedule_sha256(),
        });
        let configured = v5_evolved_configured_parent_schedule(Some(&zero_value)).unwrap();
        assert_eq!(
            v5_evolved_effective_parent_schedule(configured, 0).unwrap(),
            None
        );

        let populated = RotatingParentSchedule::from_counts(128, 3).unwrap();
        let populated_value = serde_json::json!({
            "schemaVersion": "temporal_qd_rotating_parent_schedule_v2",
            "breederWidth": populated.breeder_width,
            "breederParentCount": populated.breeder_parent_count,
            "minimumImmigrantNumerator": 1,
            "minimumImmigrantDenominator": 5,
            "parentSampling": "with_replacement_supported_parents_v1",
            "unsupportedParentPolicy": "immigrant_only_authority_bound_v1",
            "schedulingMethod": "accepted_quota_prefix_balance_v1",
            "scheduleSha256": populated.schedule_sha256(),
        });
        let configured = v5_evolved_configured_parent_schedule(Some(&populated_value)).unwrap();
        assert_eq!(
            v5_evolved_effective_parent_schedule(configured, 0).unwrap(),
            None
        );
        assert_eq!(
            v5_evolved_effective_parent_schedule(configured, 2).unwrap(),
            Some(RotatingParentSchedule::from_counts(128, 2).unwrap())
        );
        let effective = RotatingParentSchedule::from_counts(128, 2).unwrap();
        assert_eq!(
            v5_evolved_adopted_parent_schedule(configured, Some(&effective.schedule_sha256()))
                .unwrap(),
            Some(effective)
        );
        assert_eq!(
            v5_evolved_adopted_parent_schedule(configured, None).unwrap(),
            None
        );
        assert!(v5_evolved_effective_parent_schedule(configured, 129).is_err());
        assert!(
            v5_evolved_adopted_parent_schedule(
                configured,
                Some("sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
            )
            .is_err()
        );
    }

    #[test]
    fn windows_public_crlf_policy_still_requires_exact_pretty_lf_caller_ledger() {
        let value = serde_json::json!({
            "nested": [true, {"tiny": 1e-7}],
            "text": "\u{007f} 😀",
        });
        let public_newline = generation_final_newline("crlf");
        assert_eq!(public_newline.bytes(), b"\r\n");
        let lf_ledger = python_pretty_json_line(&value, JsonNewline::Lf).unwrap();
        assert!(
            parse_python_pretty_json_document(&lf_ledger, JsonNewline::Lf, "test ledger").is_ok()
        );

        let compact = canonical_json_line(&value).unwrap();
        let error = parse_python_pretty_json_document(&compact, JsonNewline::Lf, "test ledger")
            .unwrap_err();
        assert!(format!("{error:#}").contains("exact Python pretty JSON"));

        let crlf_ledger = python_pretty_json_line(&value, JsonNewline::Crlf).unwrap();
        assert!(
            parse_python_pretty_json_document(&crlf_ledger, JsonNewline::Lf, "test ledger")
                .is_err()
        );
    }

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let path = env::temp_dir().join(format!(
                "temporal-qd-batch-{label}-{}-{}",
                std::process::id(),
                unique_suffix(),
            ));
            fs::create_dir(&path).expect("create isolated test directory");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn test_object_inventory_rows(count: usize) -> Vec<Value> {
        (0..count)
            .map(|ordinal| {
                let object_sha = format!("sha256:{ordinal:064x}");
                let file_sha = format!("sha256:{:064x}", ordinal + count + 1);
                let mut row = serde_json::json!({
                    "schemaVersion": V5_PROPOSAL_OBJECT_INVENTORY_ROW_SCHEMA,
                    "ordinal": ordinal as u64,
                    "relativePath": format!("sha256/{:064x}.json", ordinal),
                    "objectSha256": object_sha,
                    "fileSha256": file_sha,
                    "byteLength": (ordinal + 1) as u64,
                });
                let row_sha = canonical_sha256(&row).unwrap();
                row.as_object_mut()
                    .unwrap()
                    .insert("rowSha256".to_owned(), Value::String(row_sha));
                row
            })
            .collect()
    }

    fn test_object_inventory_bytes(rows: &[Value]) -> Vec<u8> {
        rows.iter()
            .flat_map(|row| canonical_json_line(row).unwrap())
            .collect()
    }

    fn test_object_inventory_envelope(
        bytes: &[u8],
        count: u64,
        object_bytes: u64,
    ) -> Map<String, Value> {
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
                Value::String(V5_OUTPUT_INVENTORY_PATH.to_owned()),
            ),
            ("fileSha256".to_owned(), Value::String(sha256_bytes(bytes))),
            ("byteLength".to_owned(), Value::from(bytes.len() as u64)),
            ("objectCount".to_owned(), Value::from(count)),
            ("objectByteCount".to_owned(), Value::from(object_bytes)),
        ]);
        let descriptor_sha = canonical_sha256(&Value::Object(descriptor.clone())).unwrap();
        descriptor.insert("descriptorSha256".to_owned(), Value::String(descriptor_sha));
        Map::from_iter([(
            "objectStore".to_owned(),
            serde_json::json!({
                "inventory": Value::Object(descriptor)
            }),
        )])
    }

    fn stage_test_object_inventory(bytes: &[u8], label: &str) -> (TestDirectory, V5StagedArtifact) {
        let directory = TestDirectory::new(label);
        let output_root = directory.path().join("output");
        let invocation_root = directory.path().join("invocation");
        fs::create_dir_all(&output_root).unwrap();
        fs::create_dir_all(&invocation_root).unwrap();
        let staging = test_v5_private_staging(&output_root, &invocation_root);
        let staged = stage_v5_relative_bytes_with(
            &StdPublicationIo,
            &output_root,
            &staging,
            V5_OUTPUT_INVENTORY_PATH,
            bytes,
            "test object inventory",
        )
        .unwrap();
        (directory, staged)
    }

    #[test]
    fn v5_object_inventory_stream_rejects_missing_reordered_duplicate_tampered_and_traversal_rows()
    {
        let base = test_object_inventory_rows(4);
        let object_bytes = 10_u64;
        let valid_bytes = test_object_inventory_bytes(&base);
        let (valid_dir, valid) =
            stage_test_object_inventory(&valid_bytes, "object-inventory-valid");
        let envelope = test_object_inventory_envelope(&valid_bytes, 4, object_bytes);
        assert_eq!(
            stream_staged_v5_object_inventory(&valid, &envelope)
                .unwrap()
                .len(),
            4
        );
        drop(valid_dir);

        let mut cases = Vec::<(&str, Vec<Value>, u64, u64)>::new();
        let mut missing = base.clone();
        missing.remove(2);
        cases.push(("missing", missing, 4, object_bytes));
        let mut reordered = base.clone();
        reordered.swap(1, 2);
        cases.push(("reordered", reordered, 4, object_bytes));
        let mut duplicate = base.clone();
        duplicate[2] = duplicate[1].clone();
        cases.push(("duplicate", duplicate, 4, object_bytes));
        let mut tampered = base.clone();
        tampered[1]["byteLength"] = Value::from(999_u64);
        cases.push(("tampered", tampered, 4, object_bytes));
        let mut traversal = base;
        traversal[1]["relativePath"] = Value::String("../escape.json".to_owned());
        let mut material = traversal[1].as_object().unwrap().clone();
        material.remove("rowSha256");
        traversal[1]["rowSha256"] =
            Value::String(canonical_sha256(&Value::Object(material)).unwrap());
        cases.push(("traversal", traversal, 4, object_bytes));

        for (label, rows, expected_count, expected_bytes) in cases {
            let bytes = test_object_inventory_bytes(&rows);
            let (_directory, staged) = stage_test_object_inventory(&bytes, label);
            let envelope = test_object_inventory_envelope(&bytes, expected_count, expected_bytes);
            assert!(
                stream_staged_v5_object_inventory(&staged, &envelope).is_err(),
                "{label}"
            );
        }
    }

    fn test_v5_private_staging(output_root: &Path, invocation_root: &Path) -> V5PrivateStagingArea {
        v5_private_staging_area(
            output_root,
            invocation_root,
            "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        )
        .expect("open isolated v5 private staging area")
    }

    #[test]
    fn v5_private_staging_avoids_nested_invocation_and_owned_public_namespaces() {
        let directory = TestDirectory::new("v5-private-stage-nested-invocation");
        let output_root = directory.path().join("output-root");
        let invocation_root = output_root
            .join("native-batch")
            .join("v5-proposal")
            .join("sealed-invocation");
        fs::create_dir_all(&invocation_root).unwrap();
        let staging = test_v5_private_staging(&output_root, &invocation_root);

        assert!(staging.root.starts_with(&output_root));
        assert!(!staging.root.starts_with(&invocation_root));
        assert!(!invocation_root.starts_with(&staging.root));
        for owned_namespace in ["v5-native", "g0-bootstrap", "internal"] {
            assert!(!staging.root.starts_with(output_root.join(owned_namespace)));
        }
    }

    #[test]
    fn v5_owned_public_namespaces_reject_extras_but_tolerate_supervisor_root_state() {
        let directory = TestDirectory::new("v5-owned-namespaces");
        let root = directory.path();
        fs::create_dir_all(root.join("v5-native")).unwrap();
        fs::write(root.join("v5-native/attempts.jsonl"), b"{}\n").unwrap();
        // This root-level file belongs to the supervisor, not the native
        // transaction.  It must not force a Python copy or make adoption
        // reject an otherwise sealed public tree.
        fs::write(root.join("supervisor-state.json"), b"{}\n").unwrap();
        let allowed = BTreeSet::from(["v5-native/attempts.jsonl".to_owned()]);
        require_v5_owned_namespace_file_set(root, &allowed).unwrap();

        fs::write(root.join("v5-native/unsealed-extra.json"), b"{}\n").unwrap();
        assert!(require_v5_owned_namespace_file_set(root, &allowed).is_err());
    }

    #[test]
    fn v5_inventory_file_rejects_unsafe_path_and_same_length_replacement() {
        let directory = TestDirectory::new("v5-inventory-replacement");
        let root = directory.path();
        fs::write(root.join("artifact.json"), b"first").unwrap();
        assert!(v5_inventory_file(root, "../artifact.json", "test artifact").is_err());

        let artifact = serde_json::json!({
            "relativePath": "artifact.json",
            "fileSha256": sha256_bytes(b"first"),
            "byteLength": 5_u64,
            "semanticSha256": sha256_bytes(b"semantic"),
        });
        let mut observed = V5AdoptionBytes::default();
        verify_v5_inventory_artifact(root, artifact.as_object().unwrap(), &mut observed, false)
            .unwrap();
        assert_eq!(observed.public_artifact_bytes, 5);
        assert_eq!(observed.authenticated_file_count, 1);

        // The raw SHA is checked in addition to byte length, so replacement
        // with a same-size file cannot reuse a stale result/receipt chain.
        fs::write(root.join("artifact.json"), b"other").unwrap();
        assert!(
            verify_v5_inventory_artifact(
                root,
                artifact.as_object().unwrap(),
                &mut V5AdoptionBytes::default(),
                false,
            )
            .is_err()
        );

        fs::remove_file(root.join("artifact.json")).unwrap();
        assert!(
            verify_v5_inventory_artifact(
                root,
                artifact.as_object().unwrap(),
                &mut V5AdoptionBytes::default(),
                false,
            )
            .is_err()
        );
    }

    #[cfg(windows)]
    fn create_test_file_link(target: &Path, link: &Path) -> io::Result<()> {
        std::os::windows::fs::symlink_file(target, link)
    }

    #[cfg(not(windows))]
    fn create_test_file_link(target: &Path, link: &Path) -> io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn create_test_directory_link(target: &Path, link: &Path) -> io::Result<()> {
        std::os::windows::fs::symlink_dir(target, link)
    }

    #[cfg(not(windows))]
    fn create_test_directory_link(target: &Path, link: &Path) -> io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[test]
    fn v5_paths_reject_final_links_and_root_aliases() {
        let directory = TestDirectory::new("v5-link-rejection");
        let root = directory.path();
        assert_eq!(
            resolved_safe_existing_directory(root, "ordinary output root").unwrap(),
            root
        );
        let target = root.join("target.json");
        fs::write(&target, b"sealed").unwrap();
        let final_link = root.join("artifact.json");
        if let Err(error) = create_test_file_link(&target, &final_link) {
            // Windows machines without Developer Mode may not permit symbolic
            // links. The production predicate covers both symlinks and
            // reparse points, so make that platform capability skip explicit.
            if error.kind() == io::ErrorKind::PermissionDenied {
                return;
            }
            panic!("create final-component link: {error}");
        }
        assert!(v5_inventory_file(root, "artifact.json", "linked artifact").is_err());

        let target_directory = root.join("target-directory");
        fs::create_dir(&target_directory).unwrap();
        let root_alias = root.join("root-alias");
        if let Err(error) = create_test_directory_link(&target_directory, &root_alias) {
            if error.kind() == io::ErrorKind::PermissionDenied {
                return;
            }
            panic!("create output-root alias: {error}");
        }
        assert!(resolved_safe_existing_directory(&root_alias, "linked output root").is_err());
    }

    #[cfg(windows)]
    #[test]
    fn v5_existing_file_accepts_exact_windows_verbatim_drive_path() {
        let directory = TestDirectory::new("v5-verbatim-input");
        let target = directory.path().join("archive.json");
        fs::write(&target, b"sealed").unwrap();
        let verbatim = fs::canonicalize(&target).unwrap();
        assert!(verbatim.to_string_lossy().starts_with(r"\\?\"));
        assert_eq!(
            safe_existing_file(&verbatim, "verbatim archive").unwrap(),
            verbatim
        );
    }

    #[test]
    fn v5_compact_jsonl_requires_exact_canonical_lf_rows() {
        let parsed = parse_v5_canonical_jsonl(
            b"{\"attempt\":0}\n{\"attempt\":1}\n",
            "test compact journal",
        )
        .expect("canonical compact JSONL");
        assert_eq!(parsed.len(), 2);

        for raw in [
            b"{\"attempt\":0}\r\n".as_slice(),
            b"{\"attempt\":0}".as_slice(),
            b"{ \"attempt\":0}\n".as_slice(),
            b"{\"attempt\":0}\n\n".as_slice(),
        ] {
            assert!(parse_v5_canonical_jsonl(raw, "test compact journal").is_err());
        }
    }

    #[test]
    fn v5_outcome_audit_replay_requires_every_canonical_object() {
        use temporal_qd_kernel::v5::{
            V5AttemptLineageRefs, V5ProposalAttemptRecord, v5_proposal_seed,
        };

        let config = canonical_sha256(&serde_json::json!({"config": "v5-test"})).unwrap();
        let authority = canonical_sha256(&serde_json::json!({"authority": "v5-test"})).unwrap();
        let lineage_refs = V5AttemptLineageRefs {
            parent: None,
            mate: None,
            parent_selection_receipt_sha256: None,
            operator_plan_sha256: None,
            operator_application_sha256: None,
            operator_trace_sha256: None,
            step_index: None,
        };
        let proposal_seed = v5_proposal_seed(&config, 0).unwrap();
        let audit = V5AttemptOutcomeAudit {
            generation_index: 1,
            proposal_ordinal: 0,
            generation_config_sha256: config.clone(),
            shared_authority_sha256: authority.clone(),
            proposal_seed: proposal_seed.clone(),
            origin_kind: "random_immigrant".to_owned(),
            disposition: "rejected".to_owned(),
            reason_code: "pre_plan_rejected".to_owned(),
            stage: "pre_plan".to_owned(),
            proposal_delta_sha256: None,
            lineage_refs_sha256: canonical_sha256(&lineage_refs.to_value().unwrap()).unwrap(),
            identity_ledger_effect: "not_applicable".to_owned(),
            accepted_record_sha256: None,
        };
        let audit_sha = audit.audit_sha256().unwrap();
        let attempt = V5ProposalAttemptRecord {
            generation_index: 1,
            proposal_ordinal: 0,
            generation_config_sha256: config.clone(),
            shared_authority_sha256: authority.clone(),
            proposal_seed,
            origin_kind: "random_immigrant".to_owned(),
            proposal_delta_sha256: None,
            disposition: "rejected".to_owned(),
            reason_code: "pre_plan_rejected".to_owned(),
            lineage_refs,
            identity_ledger_effect: "not_applicable".to_owned(),
            outcome_audit_sha256: audit_sha.clone(),
            accepted_record_sha256: None,
        };
        let journal = V5AttemptJournal {
            generation_index: 1,
            generation_config_sha256: config,
            shared_authority_sha256: authority,
            attempts: vec![attempt],
        };
        let canonical_audit = canonical_json_line(&audit.to_value().unwrap()).unwrap();
        let objects = BTreeMap::from([(audit_sha.clone(), canonical_audit.clone())]);
        verify_v5_attempt_outcome_audit_replay(&journal, &objects).unwrap();

        assert!(verify_v5_attempt_outcome_audit_replay(&journal, &BTreeMap::new()).is_err());

        let mut noncanonical = objects;
        let mut whitespace_prefixed = b" ".to_vec();
        whitespace_prefixed.extend(canonical_audit);
        noncanonical.insert(audit_sha, whitespace_prefixed);
        assert!(verify_v5_attempt_outcome_audit_replay(&journal, &noncanonical).is_err());
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum FailurePoint {
        Write,
        FileSync,
        Link,
        DirectorySync,
    }

    #[derive(Default)]
    struct InjectedOps {
        failure: Option<FailurePoint>,
        sharing_failures: Cell<usize>,
        classify_sharing: bool,
        create_collisions: Cell<usize>,
        fail_file_sync_at: Cell<Option<usize>>,
        file_sync_attempts: Cell<usize>,
        fail_link_at: Cell<Option<usize>>,
        link_attempts: Cell<usize>,
        linked_targets: RefCell<Vec<PathBuf>>,
        swap_target_during_link: bool,
        swap_target_during_directory_sync: Option<PathBuf>,
        swap_temporary_during_link: bool,
        events: RefCell<Vec<&'static str>>,
        pauses: Cell<usize>,
    }

    impl PublicationIo for InjectedOps {
        fn create_new(&self, path: &Path) -> io::Result<fs::File> {
            let collisions = self.create_collisions.get();
            if collisions > 0 {
                self.create_collisions.set(collisions - 1);
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "injected stale temporary collision",
                ));
            }
            StdPublicationIo.create_new(path)
        }

        fn write_all(&self, file: &mut fs::File, bytes: &[u8]) -> io::Result<()> {
            self.events.borrow_mut().push("payload-write");
            if self.failure == Some(FailurePoint::Write) {
                return Err(io::Error::other("injected write failure"));
            }
            StdPublicationIo.write_all(file, bytes)
        }

        fn sync_file(&self, file: &fs::File) -> io::Result<()> {
            self.events.borrow_mut().push("payload-sync");
            let attempt = self.file_sync_attempts.get() + 1;
            self.file_sync_attempts.set(attempt);
            if self.failure == Some(FailurePoint::FileSync) {
                return Err(io::Error::other("injected file sync failure"));
            }
            if self.fail_file_sync_at.get() == Some(attempt) {
                return Err(io::Error::other("injected file sync boundary failure"));
            }
            StdPublicationIo.sync_file(file)
        }

        fn hard_link(&self, source: &Path, target: &Path) -> io::Result<()> {
            self.events.borrow_mut().push("publish-link");
            let link_attempt = self.link_attempts.get() + 1;
            self.link_attempts.set(link_attempt);
            if self.fail_link_at.get() == Some(link_attempt) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected publication-boundary crash",
                ));
            }
            if self.swap_temporary_during_link {
                // Materialize the attacker's file while the owned temporary
                // still exists, then rename it into place.  Creating the
                // replacement only after unlinking the temporary lets Unix
                // filesystems immediately reuse the same inode and makes the
                // injected swap indistinguishable from the owned file.
                let replacement = source.with_extension("unknown-replacement");
                fs::write(&replacement, b"unknown temporary")?;
                fs::remove_file(source)?;
                fs::rename(replacement, source)?;
                return Err(io::Error::other("injected temporary swap"));
            }
            if self.swap_target_during_link {
                fs::write(target, b"attacker\n")?;
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "injected target swap",
                ));
            }
            let failures = self.sharing_failures.get();
            if failures > 0 {
                self.sharing_failures.set(failures - 1);
                return Err(io::Error::other("injected sharing violation"));
            }
            if self.failure == Some(FailurePoint::Link) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected link failure",
                ));
            }
            StdPublicationIo.hard_link(source, target)?;
            self.linked_targets.borrow_mut().push(target.to_path_buf());
            Ok(())
        }

        fn remove_file(&self, path: &Path) -> io::Result<()> {
            self.events.borrow_mut().push("temporary-remove");
            StdPublicationIo.remove_file(path)
        }

        fn sync_directory(&self, path: &Path) -> io::Result<()> {
            self.events.borrow_mut().push("directory-sync");
            if let Some(target) = &self.swap_target_during_directory_sync {
                fs::remove_file(target)?;
                fs::write(target, b"attacker\n")?;
                return Ok(());
            }
            if self.failure == Some(FailurePoint::DirectorySync) {
                return Err(io::Error::other("injected directory sync failure"));
            }
            StdPublicationIo.sync_directory(path)
        }

        fn is_sharing_violation(&self, _error: &io::Error) -> bool {
            self.classify_sharing
        }

        fn pause(&self, _duration: Duration) {
            self.pauses.set(self.pauses.get() + 1);
        }
    }

    fn owned_temporaries(parent: &Path) -> Vec<PathBuf> {
        fs::read_dir(parent)
            .expect("read test directory")
            .map(|entry| entry.expect("read test entry").path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(".result.json.") && name.ends_with(".tmp"))
            })
            .collect()
    }

    #[test]
    fn publication_orders_payload_link_directory_sync_and_cleanup() {
        let directory = TestDirectory::new("ordering");
        let target = directory.path().join("result.json");
        let ops = InjectedOps::default();

        publish_once_with(&ops, &target, EXACT).unwrap();

        assert_eq!(fs::read(&target).unwrap(), EXACT);
        assert_eq!(
            *ops.events.borrow(),
            [
                "payload-write",
                "payload-sync",
                "publish-link",
                "directory-sync",
                "temporary-remove",
            ]
        );
        assert!(owned_temporaries(directory.path()).is_empty());
    }

    #[test]
    fn v5_receipt_last_publication_survives_a_crash_at_every_link_boundary() {
        let artifacts = vec![
            ("v5-native/attempts.jsonl".to_owned(), b"{\"attempt\":0}\n".to_vec()),
            (
                "v5-native/objects/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json"
                    .to_owned(),
                b"{\"object\":0}\n".to_vec(),
            ),
        ];
        let inventory = canonical_json_line(&serde_json::json!({"inventory": "sealed"})).unwrap();
        let receipt = canonical_json_line(&serde_json::json!({"receipt": "sealed"})).unwrap();
        let result = canonical_json_line(&serde_json::json!({"result": "sealed"})).unwrap();
        // Two artifacts, then inventory, receipt, and the outer result.
        let total_link_boundaries = artifacts.len() + 3;

        for fail_at in 1..=total_link_boundaries {
            let directory = TestDirectory::new("v5-receipt-last-crash");
            let output_root = directory.path().join("output-root");
            let invocation_root = directory.path().join("invocation");
            fs::create_dir(&output_root).unwrap();
            fs::create_dir(&invocation_root).unwrap();
            let invocation_result = invocation_root.join(V5_PROPOSAL_RESULT_PATH);
            let ops = InjectedOps {
                fail_link_at: Cell::new(Some(fail_at)),
                ..InjectedOps::default()
            };

            assert!(
                publish_v5_receipt_last_with(
                    &ops,
                    &output_root,
                    &artifacts,
                    &inventory,
                    &receipt,
                    &invocation_result,
                    &result,
                )
                .is_err()
            );
            assert_eq!(ops.link_attempts.get(), fail_at);
            assert!(!invocation_result.exists());
            let receipt_path = output_root.join(V5_OUTPUT_RECEIPT_PATH);
            if fail_at < total_link_boundaries {
                assert!(!receipt_path.exists());
            } else {
                // A crash at the external result boundary leaves the output
                // tree sealed but deliberately lacks an adoptable invocation
                // marker. The recovery branch must publish this exact result,
                // never reconstruct candidates.
                assert_eq!(fs::read(&receipt_path).unwrap(), receipt);
            }

            // A retry merely verifies the exact already-linked bytes and
            // completes the remaining boundary; it cannot overwrite an
            // attacker substitution.
            publish_v5_receipt_last_with(
                &StdPublicationIo,
                &output_root,
                &artifacts,
                &inventory,
                &receipt,
                &invocation_result,
                &result,
            )
            .unwrap();
            assert_eq!(fs::read(&invocation_result).unwrap(), result);
            assert_eq!(
                fs::read(output_root.join(V5_OUTPUT_RECEIPT_PATH)).unwrap(),
                receipt
            );
        }
    }

    #[test]
    fn v5_receipt_last_publication_refuses_divergent_existing_bytes_at_every_boundary() {
        let artifacts = vec![
            ("v5-native/attempts.jsonl".to_owned(), b"{\"attempt\":0}\n".to_vec()),
            (
                "v5-native/objects/sha256/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json"
                    .to_owned(),
                b"{\"object\":0}\n".to_vec(),
            ),
        ];
        let inventory = canonical_json_line(&serde_json::json!({"inventory": "sealed"})).unwrap();
        let receipt = canonical_json_line(&serde_json::json!({"receipt": "sealed"})).unwrap();
        let result = canonical_json_line(&serde_json::json!({"result": "sealed"})).unwrap();

        for relative in artifacts
            .iter()
            .map(|(relative, _)| relative.clone())
            .chain(std::iter::once(V5_OUTPUT_INVENTORY_PATH.to_owned()))
            .chain(std::iter::once(V5_OUTPUT_RECEIPT_PATH.to_owned()))
            .chain(std::iter::once("__invocation_result__".to_owned()))
        {
            let directory = TestDirectory::new("v5-divergent-boundary");
            let output_root = directory.path().join("output-root");
            let invocation_root = directory.path().join("invocation");
            fs::create_dir(&output_root).unwrap();
            fs::create_dir(&invocation_root).unwrap();
            let invocation_result = invocation_root.join(V5_PROPOSAL_RESULT_PATH);
            let target = if relative == "__invocation_result__" {
                invocation_result.clone()
            } else {
                output_root.join(&relative)
            };
            fs::create_dir_all(target.parent().unwrap()).unwrap();
            fs::write(&target, b"attacker replacement\n").unwrap();

            let error = publish_v5_receipt_last_with(
                &StdPublicationIo,
                &output_root,
                &artifacts,
                &inventory,
                &receipt,
                &invocation_result,
                &result,
            )
            .unwrap_err();
            assert!(format!("{error:#}").contains("divergent immutable result"));
            assert_eq!(fs::read(&target).unwrap(), b"attacker replacement\n");
        }
    }

    #[test]
    fn v5_prepublication_rejects_a_cross_bound_bundle_before_any_link() {
        let directory = TestDirectory::new("v5-prepublication-cross-binding");
        let output_root = directory.path().join("output-root");
        let invocation_root = directory.path().join("invocation");
        fs::create_dir(&output_root).unwrap();
        fs::create_dir(&invocation_root).unwrap();
        let result_value = serde_json::json!({
            "receipt": {"outputInventory": {"which": "result-receipt"}},
        });
        let typed_result = V5ProposalResult {
            value: result_value.clone(),
        };
        let invocation_result = canonical_json_line(&result_value).unwrap();
        let receipt = canonical_json_line(&serde_json::json!({
            "outputInventory": {"which": "receipt"},
        }))
        .unwrap();
        let inventory = canonical_json_line(&serde_json::json!({"which": "inventory"})).unwrap();
        let manifest = V5ProposalManifest {
            authority_sha256:
                "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_owned(),
            execution_authority: serde_json::json!({}),
            frozen_authority: serde_json::json!({}),
            expected_authority_sha256:
                "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_owned(),
            output_root: output_root.to_string_lossy().into_owned(),
            final_newline: "lf".to_owned(),
            generation_config: serde_json::json!({}),
            generation_config_sha256:
                "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_owned(),
            generation_index: 1,
            generation_kind: "g0".to_owned(),
            requested_count: 1,
            evaluation_population_size: 1,
            max_proposal_attempts: 1,
            thread_cap: 1,
            inputs: serde_json::json!({}),
            result_path: V5_PROPOSAL_RESULT_PATH.to_owned(),
            manifest_sha256:
                "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_owned(),
        };
        let ops = InjectedOps::default();
        let invocation_result_path = invocation_root.join(V5_PROPOSAL_RESULT_PATH);

        let error = validate_and_publish_v5_receipt_last_with(
            &ops,
            &output_root,
            &manifest,
            &typed_result,
            &[("v5-native/attempts.jsonl".to_owned(), b"{}\n".to_vec())],
            &inventory,
            &receipt,
            &invocation_result_path,
            &invocation_result,
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("result receipt differs"));
        assert_eq!(ops.link_attempts.get(), 0);
        assert!(ops.linked_targets.borrow().is_empty());
        assert!(!output_root.join("v5-native").exists());
        assert!(!invocation_result_path.exists());
    }

    #[test]
    fn v5_file_backed_staging_streams_a_population_without_a_payload_vector() {
        let directory = TestDirectory::new("v5-file-backed-population");
        let output_root = directory.path().join("output-root");
        let invocation_root = directory.path().join("invocation");
        fs::create_dir(&output_root).unwrap();
        fs::create_dir(&invocation_root).unwrap();
        let staging = test_v5_private_staging(&output_root, &invocation_root);
        let chunk = vec![0x5a_u8; 64 * 1024];
        let chunk_count = 257_u64;
        let mut expected = Sha256::new();
        for _ in 0..chunk_count {
            expected.update(&chunk);
        }
        let expected_sha = format!("sha256:{:x}", expected.finalize());
        let staged = stage_v5_relative_with(
            &StdPublicationIo,
            &output_root,
            &staging,
            "population.json",
            "native v5 streamed population",
            |file| {
                for _ in 0..chunk_count {
                    file.write_all(&chunk)
                        .context("write bounded population staging chunk")?;
                }
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(staged.digest.byte_length, chunk.len() as u64 * chunk_count);
        assert_eq!(staged.digest.file_sha256, expected_sha);
        publish_staged_v5_relative_once_with(
            &StdPublicationIo,
            &output_root,
            &staged,
            "native v5 streamed population",
        )
        .unwrap();
        let population = output_root.join("population.json");
        assert_eq!(
            fs::metadata(&population).unwrap().len(),
            chunk.len() as u64 * chunk_count
        );
        assert_eq!(
            digest_stable_existing_file(&population, "published streamed population")
                .unwrap()
                .0
                .file_sha256,
            expected_sha
        );
    }

    #[test]
    fn v5_selected_materialization_stream_keeps_only_one_rich_value_live() {
        struct LiveRichMaterialization<'a> {
            bytes: Vec<u8>,
            live: &'a Cell<u64>,
        }

        impl<'a> LiveRichMaterialization<'a> {
            fn new(ordinal: u64, live: &'a Cell<u64>, peak: &'a Cell<u64>) -> Self {
                let now_live = live.get() + 1;
                live.set(now_live);
                peak.set(peak.get().max(now_live));
                let mut bytes = vec![0xa5_u8; 4096];
                bytes[..8].copy_from_slice(&ordinal.to_le_bytes());
                Self { bytes, live }
            }
        }

        impl Drop for LiveRichMaterialization<'_> {
            fn drop(&mut self) {
                self.live.set(self.live.get() - 1);
            }
        }

        let directory = TestDirectory::new("v5-selected-materialization-stream");
        let output_root = directory.path().join("output-root");
        let invocation_root = directory.path().join("invocation");
        fs::create_dir(&output_root).unwrap();
        fs::create_dir(&invocation_root).unwrap();
        let staging = test_v5_private_staging(&output_root, &invocation_root);
        let selected_count = 1024_u64;
        let live = Cell::new(0_u64);
        let peak = Cell::new(0_u64);
        let materializations = Cell::new(0_u64);

        let staged = stage_v5_relative_with(
            &StdPublicationIo,
            &output_root,
            &staging,
            "population.json",
            "native v5 selected materialization stream",
            |file| {
                stream_v5_selected_materializations_one_at_a_time(
                    file,
                    0..selected_count,
                    |ordinal, file| {
                        let rich = LiveRichMaterialization::new(ordinal, &live, &peak);
                        materializations.set(materializations.get() + 1);
                        file.write_all(&rich.bytes).with_context(|| {
                            format!("write selected rich materialization {ordinal}")
                        })?;
                        Ok(())
                    },
                )
            },
        )
        .unwrap();

        assert_eq!(materializations.get(), selected_count);
        assert_eq!(peak.get(), 1);
        assert_eq!(live.get(), 0);
        assert_eq!(staged.digest.byte_length, selected_count * 4096);
        verify_staged_v5_artifact(&staged).unwrap();
    }

    #[test]
    fn v5_private_fragment_set_keeps_one_selected_materialization_across_all_outputs() {
        struct LiveRichMaterialization<'a> {
            bytes: Vec<u8>,
            live: &'a Cell<u64>,
        }

        impl<'a> LiveRichMaterialization<'a> {
            fn new(ordinal: u64, live: &'a Cell<u64>, peak: &'a Cell<u64>) -> Self {
                let now_live = live.get() + 1;
                live.set(now_live);
                peak.set(peak.get().max(now_live));
                let mut bytes = vec![0x5c_u8; 4096];
                bytes[..8].copy_from_slice(&ordinal.to_le_bytes());
                Self { bytes, live }
            }
        }

        impl Drop for LiveRichMaterialization<'_> {
            fn drop(&mut self) {
                self.live.set(self.live.get() - 1);
            }
        }

        let directory = TestDirectory::new("v5-private-fragment-set");
        let output_root = directory.path().join("output-root");
        let invocation_root = directory.path().join("invocation");
        fs::create_dir(&output_root).unwrap();
        fs::create_dir(&invocation_root).unwrap();
        let staging = test_v5_private_staging(&output_root, &invocation_root);
        let mut opened = begin_v5_private_fragment_set(&StdPublicationIo, &staging).unwrap();
        let selected_count = 1024_u64;
        let live = Cell::new(0_u64);
        let peak = Cell::new(0_u64);
        let materializations = Cell::new(0_u64);

        // This mirrors the typed core fragment sink boundary: a single rich
        // materialization fan-outs its canonical element bytes to all four
        // private streams before the next selected record is opened.
        {
            let mut sink = opened.sink();
            for ordinal in 0..selected_count {
                let rich = LiveRichMaterialization::new(ordinal, &live, &peak);
                materializations.set(materializations.get() + 1);
                V5G0PublicationFragmentSink::write_fragment(
                    &mut sink,
                    V5G0PublicationFragmentKind::PopulationCandidates,
                    &rich.bytes,
                )
                .unwrap();
                V5G0PublicationFragmentSink::write_fragment(
                    &mut sink,
                    V5G0PublicationFragmentKind::EvaluationCandidates,
                    &rich.bytes[..32],
                )
                .unwrap();
                V5G0PublicationFragmentSink::write_fragment(
                    &mut sink,
                    V5G0PublicationFragmentKind::EvaluationFunnelEntries,
                    &ordinal.to_le_bytes(),
                )
                .unwrap();
                V5G0PublicationFragmentSink::write_fragment(
                    &mut sink,
                    V5G0PublicationFragmentKind::GenerationJournalBindings,
                    &ordinal.to_be_bytes(),
                )
                .unwrap();
            }
        }

        let fragments = opened.seal(&StdPublicationIo).unwrap();
        assert_eq!(materializations.get(), selected_count);
        assert_eq!(peak.get(), 1);
        assert_eq!(live.get(), 0);
        assert_eq!(
            fragments.population_candidates.role,
            "population-candidates"
        );
        assert_eq!(
            fragments.evaluation_candidates.role,
            "evaluation-candidates"
        );
        assert_eq!(fragments.evaluation_funnel_entries.role, "funnel-entries");
        assert_eq!(
            fragments.generation_journal_bindings.role,
            "journal-bindings"
        );
        assert_eq!(
            fragments.population_candidates.digest.byte_length,
            selected_count * 4096
        );
        assert_eq!(
            fragments.evaluation_candidates.digest.byte_length,
            selected_count * 32
        );
        assert_eq!(
            fragments.evaluation_funnel_entries.digest.byte_length,
            selected_count * 8
        );
        assert_eq!(
            fragments.generation_journal_bindings.digest.byte_length,
            selected_count * 8
        );
        let core_fragment = |kind, fragment: &V5PrivateFragment| V5G0PublicationFragment {
            kind,
            fragment_sha256: fragment.digest.file_sha256.clone(),
            encoded_bytes: fragment.digest.byte_length,
            row_count: selected_count,
        };
        let core_receipt = V5G0PublicationFragments {
            population_candidates: core_fragment(
                V5G0PublicationFragmentKind::PopulationCandidates,
                &fragments.population_candidates,
            ),
            evaluation_candidates: core_fragment(
                V5G0PublicationFragmentKind::EvaluationCandidates,
                &fragments.evaluation_candidates,
            ),
            evaluation_funnel_entries: core_fragment(
                V5G0PublicationFragmentKind::EvaluationFunnelEntries,
                &fragments.evaluation_funnel_entries,
            ),
            generation_journal_bindings: core_fragment(
                V5G0PublicationFragmentKind::GenerationJournalBindings,
                &fragments.generation_journal_bindings,
            ),
        };
        fragments.verify_against_core(&core_receipt).unwrap();
        let mut first_population = [0_u8; 8];
        open_v5_private_fragment(&fragments.population_candidates)
            .unwrap()
            .read_exact(&mut first_population)
            .unwrap();
        assert_eq!(first_population, 0_u64.to_le_bytes());
        let mut source = V5PrivateFragmentSource::new(&fragments);
        let mut copied = Vec::new();
        V5G0PublicationFragmentSource::copy_fragment(
            &mut source,
            V5G0PublicationFragmentKind::EvaluationFunnelEntries,
            &mut copied,
        )
        .unwrap();
        assert_eq!(copied.len() as u64, selected_count * 8);
        copied.clear();
        V5G0PublicationFragmentSource::copy_fragment(
            &mut source,
            V5G0PublicationFragmentKind::EvaluationFunnelEntries,
            &mut copied,
        )
        .unwrap();
        assert_eq!(copied.len() as u64, selected_count * 8);

        fragments.discard(&StdPublicationIo).unwrap();
        assert!(fs::read_dir(&staging.root).unwrap().next().is_none());
    }

    #[test]
    fn v5_evolved_fragment_receipt_binds_all_attempt_funnel_count() {
        use temporal_qd_kernel::v5_evolved_publication::V5EvolvedPublicationFragment;

        let directory = TestDirectory::new("v5-evolved-private-fragment-counts");
        let output_root = directory.path().join("output-root");
        let invocation_root = directory.path().join("invocation");
        fs::create_dir(&output_root).unwrap();
        fs::create_dir(&invocation_root).unwrap();
        let staging = test_v5_private_staging(&output_root, &invocation_root);
        let mut opened = begin_v5_private_fragment_set(&StdPublicationIo, &staging).unwrap();
        {
            let mut sink = opened.sink();
            for kind in [
                V5EvolvedPublicationFragmentKind::PopulationCandidates,
                V5EvolvedPublicationFragmentKind::EvaluationCandidates,
                V5EvolvedPublicationFragmentKind::EvaluationFunnelEntries,
                V5EvolvedPublicationFragmentKind::GenerationJournalBindings,
            ] {
                V5EvolvedPublicationFragmentSink::write_fragment(&mut sink, kind, b"{}").unwrap();
            }
        }
        let fragments = opened.seal(&StdPublicationIo).unwrap();
        let receipt_fragment =
            |kind, fragment: &V5PrivateFragment, row_count| V5EvolvedPublicationFragment {
                kind,
                fragment_sha256: fragment.digest.file_sha256.clone(),
                encoded_bytes: fragment.digest.byte_length,
                row_count,
            };
        let receipt = V5EvolvedPublicationFragments {
            accepted_candidate_count: 2,
            proposal_attempt_count: 5,
            population_candidates: receipt_fragment(
                V5EvolvedPublicationFragmentKind::PopulationCandidates,
                &fragments.population_candidates,
                2,
            ),
            evaluation_candidates: receipt_fragment(
                V5EvolvedPublicationFragmentKind::EvaluationCandidates,
                &fragments.evaluation_candidates,
                2,
            ),
            evaluation_funnel_entries: receipt_fragment(
                V5EvolvedPublicationFragmentKind::EvaluationFunnelEntries,
                &fragments.evaluation_funnel_entries,
                5,
            ),
            generation_journal_bindings: receipt_fragment(
                V5EvolvedPublicationFragmentKind::GenerationJournalBindings,
                &fragments.generation_journal_bindings,
                2,
            ),
        };
        fragments
            .verify_against_evolved_core(&receipt, 2, 5)
            .unwrap();
        assert!(
            fragments
                .verify_against_evolved_core(&receipt, 2, 4)
                .is_err()
        );
        fragments.discard(&StdPublicationIo).unwrap();
    }

    #[test]
    fn v5_private_fragment_sync_failure_removes_every_owned_fragment() {
        let directory = TestDirectory::new("v5-private-fragment-sync-cleanup");
        let output_root = directory.path().join("output-root");
        let invocation_root = directory.path().join("invocation");
        fs::create_dir(&output_root).unwrap();
        fs::create_dir(&invocation_root).unwrap();
        let staging = test_v5_private_staging(&output_root, &invocation_root);
        // The first fragment seals successfully.  The second sync failure
        // exercises the cleanup branch that must discard both the already
        // sealed exact file and every still-open exact file, without sweeping
        // unrelated staging entries.
        let ops = InjectedOps {
            fail_file_sync_at: Cell::new(Some(2)),
            ..InjectedOps::default()
        };
        let mut opened = begin_v5_private_fragment_set(&ops, &staging).unwrap();
        {
            let mut sink = opened.sink();
            for kind in [
                V5G0PublicationFragmentKind::PopulationCandidates,
                V5G0PublicationFragmentKind::EvaluationCandidates,
                V5G0PublicationFragmentKind::EvaluationFunnelEntries,
                V5G0PublicationFragmentKind::GenerationJournalBindings,
            ] {
                V5G0PublicationFragmentSink::write_fragment(&mut sink, kind, b"{}\n").unwrap();
            }
        }
        let error = opened.seal(&ops).unwrap_err();
        assert!(format!("{error:#}").contains("file sync"));
        assert_eq!(ops.file_sync_attempts.get(), 2);
        assert!(fs::read_dir(&staging.root).unwrap().next().is_none());
    }

    #[test]
    fn v5_file_backed_receipt_last_publication_survives_each_link_boundary() {
        let artifact_bytes = [
            ("v5-native/attempts.jsonl", b"{\"attempt\":0}\n".as_slice()),
            (
                "v5-native/objects/sha256/cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc.json",
                b"{\"object\":0}\n".as_slice(),
            ),
        ];
        let inventory = canonical_json_line(&serde_json::json!({"inventory": "sealed"})).unwrap();
        let receipt = canonical_json_line(&serde_json::json!({"receipt": "sealed"})).unwrap();
        let result = canonical_json_line(&serde_json::json!({"result": "sealed"})).unwrap();
        let total_link_boundaries = artifact_bytes.len() + 3;

        for fail_at in 1..=total_link_boundaries {
            let directory = TestDirectory::new("v5-file-backed-receipt-last-crash");
            let output_root = directory.path().join("output-root");
            let invocation_root = directory.path().join("invocation");
            fs::create_dir(&output_root).unwrap();
            fs::create_dir(&invocation_root).unwrap();
            let staging = test_v5_private_staging(&output_root, &invocation_root);
            let invocation_result = invocation_root.join(V5_PROPOSAL_RESULT_PATH);
            let ops = InjectedOps {
                fail_link_at: Cell::new(Some(fail_at)),
                ..InjectedOps::default()
            };
            let artifacts = artifact_bytes
                .iter()
                .map(|(relative, bytes)| {
                    stage_v5_relative_bytes_with(
                        &ops,
                        &output_root,
                        &staging,
                        relative,
                        bytes,
                        "native v5 file-backed transaction artifact",
                    )
                })
                .collect::<Result<Vec<_>>>()
                .unwrap();
            let staged_inventory = stage_v5_relative_bytes_with(
                &ops,
                &output_root,
                &staging,
                V5_OUTPUT_INVENTORY_PATH,
                &inventory,
                "native v5 file-backed output inventory",
            )
            .unwrap();
            let staged_receipt = stage_v5_relative_bytes_with(
                &ops,
                &output_root,
                &staging,
                V5_OUTPUT_RECEIPT_PATH,
                &receipt,
                "native v5 file-backed output receipt",
            )
            .unwrap();

            assert!(
                publish_v5_staged_receipt_last_with(
                    &ops,
                    &output_root,
                    &artifacts,
                    &staged_inventory,
                    &staged_receipt,
                    &invocation_result,
                    &result,
                )
                .is_err()
            );
            assert_eq!(ops.link_attempts.get(), fail_at);
            assert!(!invocation_result.exists());
            let receipt_path = output_root.join(V5_OUTPUT_RECEIPT_PATH);
            if fail_at < total_link_boundaries {
                assert!(!receipt_path.exists());
            } else {
                assert_eq!(fs::read(&receipt_path).unwrap(), receipt);
            }

            let retry_artifacts = artifact_bytes
                .iter()
                .map(|(relative, bytes)| {
                    stage_v5_relative_bytes_with(
                        &StdPublicationIo,
                        &output_root,
                        &staging,
                        relative,
                        bytes,
                        "native v5 file-backed retry artifact",
                    )
                })
                .collect::<Result<Vec<_>>>()
                .unwrap();
            let retry_inventory = stage_v5_relative_bytes_with(
                &StdPublicationIo,
                &output_root,
                &staging,
                V5_OUTPUT_INVENTORY_PATH,
                &inventory,
                "native v5 file-backed retry inventory",
            )
            .unwrap();
            let retry_receipt = stage_v5_relative_bytes_with(
                &StdPublicationIo,
                &output_root,
                &staging,
                V5_OUTPUT_RECEIPT_PATH,
                &receipt,
                "native v5 file-backed retry receipt",
            )
            .unwrap();
            publish_v5_staged_receipt_last_with(
                &StdPublicationIo,
                &output_root,
                &retry_artifacts,
                &retry_inventory,
                &retry_receipt,
                &invocation_result,
                &result,
            )
            .unwrap();
            assert_eq!(fs::read(&invocation_result).unwrap(), result);
        }
    }

    #[test]
    fn v5_crash_after_receipt_link_with_private_stage_keeps_adoption_namespace_clean() {
        let directory = TestDirectory::new("v5-receipt-private-stage-crash");
        let output_root = directory.path().join("output-root");
        let invocation_root = directory.path().join("invocation");
        fs::create_dir(&output_root).unwrap();
        fs::create_dir(&invocation_root).unwrap();
        let staging = test_v5_private_staging(&output_root, &invocation_root);
        let artifact = stage_v5_relative_bytes_with(
            &StdPublicationIo,
            &output_root,
            &staging,
            "v5-native/attempts.jsonl",
            b"{\"attempt\":0}\n",
            "native v5 crash fixture artifact",
        )
        .unwrap();
        let receipt = stage_v5_relative_bytes_with(
            &StdPublicationIo,
            &output_root,
            &staging,
            V5_OUTPUT_RECEIPT_PATH,
            &canonical_json_line(&serde_json::json!({"receipt": "sealed"})).unwrap(),
            "native v5 crash fixture receipt",
        )
        .unwrap();

        // Model process death immediately after the receipt's hard-link and
        // parent sync: neither owned staging source is cleaned up here.
        assert_eq!(
            link_staged_immutable(&StdPublicationIo, &artifact, &artifact.target_path).unwrap(),
            LinkOutcome::Published
        );
        StdPublicationIo
            .sync_directory(artifact.target_path.parent().unwrap())
            .unwrap();
        assert_eq!(
            link_staged_immutable(&StdPublicationIo, &receipt, &receipt.target_path).unwrap(),
            LinkOutcome::Published
        );
        StdPublicationIo
            .sync_directory(receipt.target_path.parent().unwrap())
            .unwrap();
        assert!(artifact.temporary.exists());
        assert!(receipt.temporary.exists());
        assert!(artifact.temporary.starts_with(&staging.root));
        assert!(receipt.temporary.starts_with(&staging.root));
        assert!(
            !artifact
                .temporary
                .starts_with(output_root.join("v5-native"))
        );
        assert!(
            !receipt
                .temporary
                .starts_with(output_root.join("internal/v5-proposal"))
        );

        // This is the exact namespace audit that receipt-present/result-absent
        // recovery performs before it may recreate the small invocation
        // result.  Private left-behind sources must not make the sealed tree
        // appear to contain unsealed public files.
        let allowed = BTreeSet::from([
            "v5-native/attempts.jsonl".to_owned(),
            V5_OUTPUT_RECEIPT_PATH.to_owned(),
        ]);
        require_v5_owned_namespace_file_set(&output_root, &allowed).unwrap();

        // Test cleanup is explicitly by held identity, never a prefix sweep.
        discard_staged_v5_artifact(&StdPublicationIo, &artifact).unwrap();
        discard_staged_v5_artifact(&StdPublicationIo, &receipt).unwrap();
    }

    #[test]
    fn v5_private_staging_never_sweeps_an_unknown_stale_file() {
        let directory = TestDirectory::new("v5-private-stage-stale");
        let output_root = directory.path().join("output-root");
        let invocation_root = directory.path().join("invocation");
        fs::create_dir(&output_root).unwrap();
        fs::create_dir(&invocation_root).unwrap();
        let staging = test_v5_private_staging(&output_root, &invocation_root);
        let stale = staging.root.join(".v5-artifact-unowned-stale.tmp");
        fs::write(&stale, b"do not sweep\n").unwrap();
        let staged = stage_v5_relative_bytes_with(
            &StdPublicationIo,
            &output_root,
            &staging,
            "v5-native/attempts.jsonl",
            b"{\"attempt\":0}\n",
            "native v5 private-stage stale fixture",
        )
        .unwrap();
        publish_staged_v5_relative_once_with(
            &StdPublicationIo,
            &output_root,
            &staged,
            "native v5 private-stage stale fixture",
        )
        .unwrap();
        assert_eq!(fs::read(stale).unwrap(), b"do not sweep\n");
    }

    #[test]
    fn v5_file_backed_prepublication_rejects_before_any_link() {
        let directory = TestDirectory::new("v5-file-backed-prepublication");
        let output_root = directory.path().join("output-root");
        let invocation_root = directory.path().join("invocation");
        fs::create_dir(&output_root).unwrap();
        fs::create_dir(&invocation_root).unwrap();
        let staging = test_v5_private_staging(&output_root, &invocation_root);
        let result_value = serde_json::json!({
            "receipt": {"outputInventory": {"which": "result-receipt"}},
        });
        let typed_result = V5ProposalResult {
            value: result_value.clone(),
        };
        let invocation_result = canonical_json_line(&result_value).unwrap();
        let receipt = canonical_json_line(&serde_json::json!({
            "outputInventory": {"which": "receipt"},
        }))
        .unwrap();
        let inventory = canonical_json_line(&serde_json::json!({"which": "inventory"})).unwrap();
        let manifest = V5ProposalManifest {
            authority_sha256:
                "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_owned(),
            execution_authority: serde_json::json!({}),
            frozen_authority: serde_json::json!({}),
            expected_authority_sha256:
                "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_owned(),
            output_root: output_root.to_string_lossy().into_owned(),
            final_newline: "lf".to_owned(),
            generation_config: serde_json::json!({}),
            generation_config_sha256:
                "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_owned(),
            generation_index: 1,
            generation_kind: "g0".to_owned(),
            requested_count: 1,
            evaluation_population_size: 1,
            max_proposal_attempts: 1,
            thread_cap: 1,
            inputs: serde_json::json!({}),
            result_path: V5_PROPOSAL_RESULT_PATH.to_owned(),
            manifest_sha256:
                "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_owned(),
        };
        let ops = InjectedOps::default();
        let artifacts = vec![
            stage_v5_relative_bytes_with(
                &ops,
                &output_root,
                &staging,
                "v5-native/attempts.jsonl",
                b"{}\n",
                "native v5 invalid staged artifact",
            )
            .unwrap(),
        ];
        let staged_inventory = stage_v5_relative_bytes_with(
            &ops,
            &output_root,
            &staging,
            V5_OUTPUT_INVENTORY_PATH,
            &inventory,
            "native v5 invalid staged inventory",
        )
        .unwrap();
        let staged_receipt = stage_v5_relative_bytes_with(
            &ops,
            &output_root,
            &staging,
            V5_OUTPUT_RECEIPT_PATH,
            &receipt,
            "native v5 invalid staged receipt",
        )
        .unwrap();
        let invocation_result_path = invocation_root.join(V5_PROPOSAL_RESULT_PATH);

        let error = validate_and_publish_v5_staged_receipt_last_with(
            &ops,
            &output_root,
            &manifest,
            &typed_result,
            &artifacts,
            &staged_inventory,
            &staged_receipt,
            &invocation_result_path,
            &invocation_result,
            V5ValidationMode::Strict,
            |_| Ok(()),
        )
        .unwrap_err();
        assert!(format!("{error:#}").contains("result receipt differs"));
        assert_eq!(ops.link_attempts.get(), 0);
        assert!(ops.linked_targets.borrow().is_empty());
        assert!(!output_root.join("v5-native/attempts.jsonl").exists());
        assert!(!output_root.join(V5_OUTPUT_INVENTORY_PATH).exists());
        assert!(!output_root.join(V5_OUTPUT_RECEIPT_PATH).exists());
        assert!(!invocation_result_path.exists());
    }

    #[test]
    fn injected_write_sync_and_link_failures_leave_no_partial_target() {
        for failure in [
            FailurePoint::Write,
            FailurePoint::FileSync,
            FailurePoint::Link,
        ] {
            let directory = TestDirectory::new("prepublication-failure");
            let target = directory.path().join("result.json");
            let ops = InjectedOps {
                failure: Some(failure),
                ..InjectedOps::default()
            };

            assert!(publish_once_with(&ops, &target, EXACT).is_err());
            assert!(!target.exists());
            assert!(owned_temporaries(directory.path()).is_empty());
        }
    }

    #[test]
    fn directory_sync_failure_reports_after_safe_link_and_retry_recovers() {
        let directory = TestDirectory::new("directory-sync-failure");
        let target = directory.path().join("result.json");
        let failing = InjectedOps {
            failure: Some(FailurePoint::DirectorySync),
            ..InjectedOps::default()
        };

        let error = publish_once_with(&failing, &target, EXACT).unwrap_err();
        assert!(format!("{error:#}").contains("synchronize result parent"));
        assert_eq!(fs::read(&target).unwrap(), EXACT);
        assert!(owned_temporaries(directory.path()).is_empty());

        publish_once_with(&InjectedOps::default(), &target, EXACT).unwrap();
        assert_eq!(fs::read(&target).unwrap(), EXACT);
    }

    #[test]
    fn sharing_violation_retries_are_bounded() {
        let directory = TestDirectory::new("sharing-bounded");
        let target = directory.path().join("result.json");
        let ops = InjectedOps {
            sharing_failures: Cell::new(usize::MAX),
            classify_sharing: true,
            ..InjectedOps::default()
        };

        assert!(publish_once_with(&ops, &target, EXACT).is_err());
        let links = ops
            .events
            .borrow()
            .iter()
            .filter(|event| **event == "publish-link")
            .count();
        assert_eq!(links, SHARING_RETRY_DELAYS_MS.len() + 1);
        assert_eq!(ops.pauses.get(), SHARING_RETRY_DELAYS_MS.len());
        assert!(!target.exists());
        assert!(owned_temporaries(directory.path()).is_empty());
    }

    #[test]
    fn transient_sharing_violation_converges_without_replacement() {
        let directory = TestDirectory::new("sharing-transient");
        let target = directory.path().join("result.json");
        let ops = InjectedOps {
            sharing_failures: Cell::new(2),
            classify_sharing: true,
            ..InjectedOps::default()
        };

        publish_once_with(&ops, &target, EXACT).unwrap();
        assert_eq!(fs::read(&target).unwrap(), EXACT);
        assert_eq!(ops.pauses.get(), 2);
    }

    #[test]
    fn divergent_existing_result_is_refused() {
        let directory = TestDirectory::new("divergent-existing");
        let target = directory.path().join("result.json");
        fs::write(&target, b"attacker\n").unwrap();

        let error = publish_once_with(&InjectedOps::default(), &target, EXACT).unwrap_err();
        assert!(format!("{error:#}").contains("divergent immutable result"));
        assert_eq!(fs::read(&target).unwrap(), b"attacker\n");
    }

    #[test]
    fn stale_temporary_is_ignored_and_never_removed() {
        let directory = TestDirectory::new("stale-temp");
        let target = directory.path().join("result.json");
        let stale = directory.path().join(".result.json.stale.tmp");
        fs::write(&stale, b"unowned stale").unwrap();
        let ops = InjectedOps {
            create_collisions: Cell::new(1),
            ..InjectedOps::default()
        };

        publish_once_with(&ops, &target, EXACT).unwrap();

        assert_eq!(fs::read(&target).unwrap(), EXACT);
        assert_eq!(fs::read(&stale).unwrap(), b"unowned stale");
        assert_eq!(owned_temporaries(directory.path()), [stale]);
    }

    #[test]
    fn target_swap_during_link_is_refused_without_replacement() {
        let directory = TestDirectory::new("target-swap-link");
        let target = directory.path().join("result.json");
        let ops = InjectedOps {
            swap_target_during_link: true,
            ..InjectedOps::default()
        };

        let error = publish_once_with(&ops, &target, EXACT).unwrap_err();
        assert!(format!("{error:#}").contains("divergent immutable result"));
        assert_eq!(fs::read(&target).unwrap(), b"attacker\n");
        assert!(owned_temporaries(directory.path()).is_empty());
    }

    #[test]
    fn target_swap_after_link_is_detected_after_directory_sync() {
        let directory = TestDirectory::new("target-swap-sync");
        let target = directory.path().join("result.json");
        let ops = InjectedOps {
            swap_target_during_directory_sync: Some(target.clone()),
            ..InjectedOps::default()
        };

        let error = publish_once_with(&ops, &target, EXACT).unwrap_err();
        assert!(format!("{error:#}").contains("divergent immutable result"));
        assert_eq!(fs::read(&target).unwrap(), b"attacker\n");
        assert!(owned_temporaries(directory.path()).is_empty());
    }

    #[test]
    fn cleanup_refuses_to_remove_a_swapped_unknown_temporary() {
        let directory = TestDirectory::new("temporary-swap");
        let target = directory.path().join("result.json");
        let ops = InjectedOps {
            swap_temporary_during_link: true,
            ..InjectedOps::default()
        };

        let error = publish_once_with(&ops, &target, EXACT).unwrap_err();
        assert!(format!("{error:#}").contains("could not be safely removed"));
        let temporaries = owned_temporaries(directory.path());
        assert_eq!(temporaries.len(), 1);
        assert_eq!(fs::read(&temporaries[0]).unwrap(), b"unknown temporary");
        assert!(!target.exists());
    }

    #[test]
    fn mutable_ledger_replace_is_atomic_idempotent_and_rejects_substitution() {
        let directory = TestDirectory::new("mutable-ledger-replace");
        let target = directory.path().join("identity-ledger.json");
        let old = b"{\"ledgerSha256\":\"old\"}\n";
        let new = b"{\"ledgerSha256\":\"new\"}\n";
        fs::write(&target, old).unwrap();

        replace_mutable_ledger(&target, old, new, "sha256:old").unwrap();
        assert_eq!(fs::read(&target).unwrap(), new);
        // Simulates a crash after the atomic replace but before outer-result
        // publication: repair is recognized without another replacement.
        replace_mutable_ledger(&target, old, new, "sha256:old").unwrap();
        assert_eq!(fs::read(&target).unwrap(), new);

        fs::write(&target, b"attacker\n").unwrap();
        let error = replace_mutable_ledger(&target, old, new, "sha256:old").unwrap_err();
        assert!(format!("{error:#}").contains("changed after opening"));
        assert_eq!(fs::read(&target).unwrap(), b"attacker\n");
    }

    #[cfg(unix)]
    #[test]
    fn symlink_target_and_parent_are_rejected() {
        use std::os::unix::fs::symlink;

        let directory = TestDirectory::new("symlink");
        let victim = directory.path().join("victim.json");
        fs::write(&victim, b"victim\n").unwrap();
        let target = directory.path().join("result.json");
        symlink(&victim, &target).unwrap();
        assert!(publish_once_with(&InjectedOps::default(), &target, EXACT).is_err());
        assert_eq!(fs::read(&victim).unwrap(), b"victim\n");

        let real_parent = directory.path().join("real-parent");
        fs::create_dir(&real_parent).unwrap();
        let linked_parent = directory.path().join("linked-parent");
        symlink(&real_parent, &linked_parent).unwrap();
        assert!(
            publish_once_with(
                &InjectedOps::default(),
                &linked_parent.join("result.json"),
                EXACT,
            )
            .is_err()
        );
        assert!(fs::read_dir(real_parent).unwrap().next().is_none());
    }

    #[cfg(windows)]
    #[test]
    fn symlink_reparse_target_and_parent_are_rejected_when_available() {
        use std::os::windows::fs::{symlink_dir, symlink_file};

        let directory = TestDirectory::new("symlink-reparse");
        let victim = directory.path().join("victim.json");
        fs::write(&victim, b"victim\n").unwrap();
        let target = directory.path().join("result.json");
        if symlink_file(&victim, &target).is_err() {
            return;
        }
        assert!(publish_once_with(&InjectedOps::default(), &target, EXACT).is_err());
        assert_eq!(fs::read(&victim).unwrap(), b"victim\n");

        let real_parent = directory.path().join("real-parent");
        fs::create_dir(&real_parent).unwrap();
        let linked_parent = directory.path().join("linked-parent");
        if symlink_dir(&real_parent, &linked_parent).is_err() {
            return;
        }
        assert!(
            publish_once_with(
                &InjectedOps::default(),
                &linked_parent.join("result.json"),
                EXACT,
            )
            .is_err()
        );
        assert!(fs::read_dir(real_parent).unwrap().next().is_none());
    }
}
