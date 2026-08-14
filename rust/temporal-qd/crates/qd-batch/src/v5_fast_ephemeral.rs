//! Deliberately non-resumable current-v5 proposal publication.
//!
//! This is not a relaxed durable transaction. It is a separate execution
//! contract for disposable research runs: write the scientifically required
//! selected evaluation population directly, write a compact identity ledger,
//! retain only the selected G0 records/deltas needed to authenticate G2
//! parents, then publish one completion marker. A crash leaves an incomplete
//! directory which must be discarded; this module never adopts or repairs it.

use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use temporal_qd_contract::{
    Map, NativeProgress, NativeProgressHandle, NativeProgressSection, Value, canonical_json_line,
    canonical_sha256,
};
use temporal_qd_kernel::v5_evolved_publication::{
    V5EvolvedPublicationFragmentKind, V5EvolvedPublicationFragmentSink,
    V5EvolvedPublicationFragmentSource, V5EvolvedPublicationFragments, V5EvolvedPublicationInputs,
    V5EvolvedPublicationPlan, V5EvolvedStreamedArtifact, prepare_v5_evolved_publication_stream,
};
use temporal_qd_kernel::v5_evolved_transaction::execute_v5_evolved_transaction_with_progress;
use temporal_qd_kernel::v5_publication::{
    V5G0PublicationFragmentKind, V5G0PublicationFragmentSink, V5G0PublicationFragmentSource,
    V5G0PublicationFragments, V5G0StreamedArtifact,
    prepare_v5_g0_publication_stream_from_fresh_transaction,
};
use temporal_qd_kernel::v5_transaction::{
    V5G0TransactionResult, compact_delta_object_relative_path,
    compact_record_object_relative_path, execute_v5_g0_transaction_with_progress,
};

use crate::v5_proposal_contract::V5ProposalManifest;

const EXECUTION_MODE: &str = "fast-ephemeral-v1";
const STATUS_SCHEMA: &str = "temporal_qd_v5_fast_ephemeral_status_v1";
const RESULT_SCHEMA: &str = "temporal_qd_v5_fast_ephemeral_result_v1";
const COMPLETE_SCHEMA: &str = "temporal_qd_v5_fast_ephemeral_complete_v1";
const STATUS_PATH: &str = "STATUS.json";
const COMPLETE_PATH: &str = "COMPLETE.json";
const EVALUATION_POPULATION_PATH: &str = "evaluation-population.json";
const IDENTITY_LEDGER_PATH: &str = "identity-ledger.json";

#[derive(Default)]
struct MemoryFragments {
    population_candidates: Vec<u8>,
    evaluation_candidates: Vec<u8>,
    evaluation_funnel_entries: Vec<u8>,
    generation_journal_bindings: Vec<u8>,
}

impl MemoryFragments {
    fn evolved_bytes(&self, kind: V5EvolvedPublicationFragmentKind) -> &[u8] {
        match kind {
            V5EvolvedPublicationFragmentKind::PopulationCandidates => &self.population_candidates,
            V5EvolvedPublicationFragmentKind::EvaluationCandidates => &self.evaluation_candidates,
            V5EvolvedPublicationFragmentKind::EvaluationFunnelEntries => {
                &self.evaluation_funnel_entries
            }
            V5EvolvedPublicationFragmentKind::GenerationJournalBindings => {
                &self.generation_journal_bindings
            }
        }
    }
}

impl V5EvolvedPublicationFragmentSink for MemoryFragments {
    fn write_fragment(
        &mut self,
        kind: V5EvolvedPublicationFragmentKind,
        canonical_bytes: &[u8],
    ) -> io::Result<()> {
        match kind {
            V5EvolvedPublicationFragmentKind::PopulationCandidates => self
                .population_candidates
                .extend_from_slice(canonical_bytes),
            V5EvolvedPublicationFragmentKind::EvaluationCandidates => self
                .evaluation_candidates
                .extend_from_slice(canonical_bytes),
            V5EvolvedPublicationFragmentKind::EvaluationFunnelEntries => self
                .evaluation_funnel_entries
                .extend_from_slice(canonical_bytes),
            V5EvolvedPublicationFragmentKind::GenerationJournalBindings => self
                .generation_journal_bindings
                .extend_from_slice(canonical_bytes),
        }
        Ok(())
    }
}

impl V5EvolvedPublicationFragmentSource for MemoryFragments {
    fn copy_fragment(
        &mut self,
        kind: V5EvolvedPublicationFragmentKind,
        output: &mut dyn Write,
    ) -> io::Result<()> {
        output.write_all(self.evolved_bytes(kind))
    }
}

impl MemoryFragments {
    fn bytes(&self, kind: V5G0PublicationFragmentKind) -> &[u8] {
        match kind {
            V5G0PublicationFragmentKind::PopulationCandidates => &self.population_candidates,
            V5G0PublicationFragmentKind::EvaluationCandidates => &self.evaluation_candidates,
            V5G0PublicationFragmentKind::EvaluationFunnelEntries => &self.evaluation_funnel_entries,
            V5G0PublicationFragmentKind::GenerationJournalBindings => {
                &self.generation_journal_bindings
            }
        }
    }
}

impl V5G0PublicationFragmentSink for MemoryFragments {
    fn write_fragment(
        &mut self,
        kind: V5G0PublicationFragmentKind,
        canonical_bytes: &[u8],
    ) -> io::Result<()> {
        match kind {
            V5G0PublicationFragmentKind::PopulationCandidates => self
                .population_candidates
                .extend_from_slice(canonical_bytes),
            V5G0PublicationFragmentKind::EvaluationCandidates => self
                .evaluation_candidates
                .extend_from_slice(canonical_bytes),
            V5G0PublicationFragmentKind::EvaluationFunnelEntries => self
                .evaluation_funnel_entries
                .extend_from_slice(canonical_bytes),
            V5G0PublicationFragmentKind::GenerationJournalBindings => self
                .generation_journal_bindings
                .extend_from_slice(canonical_bytes),
        }
        Ok(())
    }
}

impl V5G0PublicationFragmentSource for MemoryFragments {
    fn copy_fragment(
        &mut self,
        kind: V5G0PublicationFragmentKind,
        output: &mut dyn Write,
    ) -> io::Result<()> {
        output.write_all(self.bytes(kind))
    }
}

fn write_new(path: &Path, bytes: &[u8], label: &str) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("create {label}: {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("write {label}: {}", path.display()))?;
    file.flush()
        .with_context(|| format!("flush {label}: {}", path.display()))?;
    Ok(())
}

fn create_new_writer(path: &Path, label: &str) -> Result<BufWriter<fs::File>> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("create {label}: {}", path.display()))?;
    Ok(BufWriter::new(file))
}

fn artifact_value(relative_path: &str, artifact: &V5G0StreamedArtifact) -> Value {
    Value::Object(Map::from_iter([
        (
            "relativePath".to_owned(),
            Value::String(relative_path.to_owned()),
        ),
        (
            "semanticSha256".to_owned(),
            Value::String(artifact.semantic_sha256.clone()),
        ),
        (
            "fileSha256".to_owned(),
            Value::String(artifact.file_sha256.clone()),
        ),
        ("byteLength".to_owned(), Value::from(artifact.encoded_bytes)),
    ]))
}

fn evolved_artifact_value(relative_path: &str, artifact: &V5EvolvedStreamedArtifact) -> Value {
    Value::Object(Map::from_iter([
        (
            "relativePath".to_owned(),
            Value::String(relative_path.to_owned()),
        ),
        (
            "semanticSha256".to_owned(),
            Value::String(artifact.semantic_sha256.clone()),
        ),
        (
            "fileSha256".to_owned(),
            Value::String(artifact.file_sha256.clone()),
        ),
        ("byteLength".to_owned(), Value::from(artifact.encoded_bytes)),
    ]))
}

fn inline_artifact_value(
    relative_path: &str,
    semantic_sha256: String,
    file_sha256: String,
    byte_length: usize,
) -> Value {
    Value::Object(Map::from_iter([
        (
            "relativePath".to_owned(),
            Value::String(relative_path.to_owned()),
        ),
        ("semanticSha256".to_owned(), Value::String(semantic_sha256)),
        ("fileSha256".to_owned(), Value::String(file_sha256)),
        ("byteLength".to_owned(), Value::from(byte_length as u64)),
    ]))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("sha256:{:x}", digest.finalize())
}

fn duration_ms(duration: Duration) -> Result<u64> {
    u64::try_from(duration.as_millis()).context("convert fast-ephemeral duration")
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct SelectedG0ParentPublication {
    encoded_bytes: u64,
    object_count: u64,
}

fn insert_selected_g0_parent_object(
    objects: &mut BTreeMap<String, Vec<u8>>,
    relative_path: String,
    bytes: Vec<u8>,
) -> Result<()> {
    match objects.entry(relative_path) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(bytes);
        }
        std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &bytes => {}
        std::collections::btree_map::Entry::Occupied(entry) => {
            bail!(
                "fast-ephemeral selected G0 parent object path collision: {}",
                entry.key()
            );
        }
    }
    Ok(())
}

/// Retain only the evaluation-width compact closure needed by the G2 parent
/// selector. The archive later names each record by `proposalEntrySha256`; the
/// record names its exact proposal delta. No attempt journal, accepted-pool
/// staging, rich candidate, or unselected construction object is persisted.
fn publish_selected_g0_parent_objects(
    output_root: &Path,
    transaction: &V5G0TransactionResult,
) -> Result<SelectedG0ParentPublication> {
    let selected = transaction
        .selected_projection_index
        .as_ref()
        .ok_or_else(|| anyhow!("fast-ephemeral G0 transaction lacks selected projections"))?;
    selected
        .to_value()
        .context("validate fast-ephemeral selected projection index")?;

    if transaction.accepted_records.len() != transaction.accepted_proposal_deltas.len() {
        bail!("fast-ephemeral accepted G0 records/deltas have different counts");
    }
    let mut accepted = BTreeMap::new();
    for (record, delta) in transaction
        .accepted_records
        .iter()
        .zip(transaction.accepted_proposal_deltas.iter())
    {
        let record_sha256 = record
            .record_sha256()
            .context("identify fast-ephemeral compact G0 parent record")?;
        if accepted
            .insert(record_sha256.clone(), (record, delta))
            .is_some()
        {
            bail!("fast-ephemeral G0 transaction repeats compact record {record_sha256}");
        }
    }

    let mut objects = BTreeMap::new();
    for projection in &selected.projections {
        let (record, delta) = accepted
            .get(&projection.record_sha256)
            .copied()
            .ok_or_else(|| {
                anyhow!(
                    "fast-ephemeral selected projection lacks compact record {}",
                    projection.record_sha256
                )
            })?;
        projection
            .verify_against_record(record)
            .context("bind fast-ephemeral selected projection to compact record")?;
        if delta.get("deltaSha256").and_then(Value::as_str)
            != Some(record.proposal_delta_sha256.as_str())
        {
            bail!(
                "fast-ephemeral compact G0 parent record/delta identity drifted for {}",
                record.candidate_id
            );
        }

        let record_value = record
            .to_value()
            .context("encode fast-ephemeral compact G0 parent record")?;
        let record_bytes = canonical_json_line(&record_value)
            .context("encode fast-ephemeral compact G0 parent record bytes")?;
        insert_selected_g0_parent_object(
            &mut objects,
            compact_record_object_relative_path(&projection.record_sha256)
                .context("derive fast-ephemeral compact G0 parent record path")?,
            record_bytes,
        )?;

        let delta_bytes = canonical_json_line(delta)
            .context("encode fast-ephemeral compact G0 parent delta bytes")?;
        insert_selected_g0_parent_object(
            &mut objects,
            compact_delta_object_relative_path(&record.proposal_delta_sha256)
                .context("derive fast-ephemeral compact G0 parent delta path")?,
            delta_bytes,
        )?;
    }

    let mut encoded_bytes = 0_u64;
    for (relative_path, bytes) in &objects {
        let path = output_root.join(relative_path);
        let parent = path
            .parent()
            .ok_or_else(|| anyhow!("fast-ephemeral selected parent object lacks a parent path"))?;
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "create fast-ephemeral selected parent object directory: {}",
                parent.display()
            )
        })?;
        write_new(&path, bytes, "fast-ephemeral selected G0 parent object")?;
        encoded_bytes = encoded_bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| anyhow!("fast-ephemeral selected parent byte count overflowed"))?;
    }

    Ok(SelectedG0ParentPublication {
        encoded_bytes,
        object_count: objects.len() as u64,
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_g0(
    output_root: &Path,
    _invocation_root: &Path,
    invocation_result_path: &Path,
    manifest: &V5ProposalManifest,
    progress_handle: &NativeProgressHandle,
    progress: NativeProgress,
    started: Instant,
    static_authority_elapsed: Duration,
) -> Result<()> {
    for relative in [
        STATUS_PATH,
        COMPLETE_PATH,
        EVALUATION_POPULATION_PATH,
        IDENTITY_LEDGER_PATH,
        "v5-native",
        "internal",
    ] {
        if output_root.join(relative).exists() {
            bail!("fast-ephemeral-v1 refuses pre-existing output or durable state: {relative}");
        }
    }
    if invocation_result_path.exists() {
        bail!("fast-ephemeral-v1 is non-resumable; discard the incomplete run directory");
    }

    let status = Value::Object(Map::from_iter([
        (
            "schemaVersion".to_owned(),
            Value::String(STATUS_SCHEMA.to_owned()),
        ),
        (
            "executionMode".to_owned(),
            Value::String(EXECUTION_MODE.to_owned()),
        ),
        ("status".to_owned(), Value::String("running".to_owned())),
        (
            "generationIndex".to_owned(),
            Value::from(manifest.generation_index),
        ),
        (
            "manifestSha256".to_owned(),
            Value::String(manifest.manifest_sha256.clone()),
        ),
    ]));
    write_new(
        &output_root.join(STATUS_PATH),
        &canonical_json_line(&status).context("encode fast-ephemeral status")?,
        "fast-ephemeral status",
    )?;

    progress_handle.begin_phase(
        "construction",
        "construct_and_admit_g0",
        "accepted_candidate",
        Some(manifest.requested_count),
        true,
        Some(manifest.thread_cap),
        None,
    );
    let construction_started = Instant::now();
    let request = super::v5_g0_transaction_request(manifest)?;
    let transaction =
        execute_v5_g0_transaction_with_progress(request.clone(), Some(progress_handle))
            .context("execute fast-ephemeral native v5 G0 transaction")?;
    let construction_elapsed = construction_started.elapsed();
    if !transaction.target_reached
        || transaction.accepted_records.len() as u64 != manifest.requested_count
        || transaction.selected_projection_index.is_none()
    {
        bail!("fast-ephemeral native v5 G0 construction did not reach its exact dimensions");
    }

    progress_handle.begin_phase(
        "ephemeral_publication",
        "write_selected_evaluation_population",
        "selected_candidate",
        Some(manifest.evaluation_population_size),
        true,
        Some(manifest.thread_cap),
        None,
    );
    let publication_started = Instant::now();
    let stream = prepare_v5_g0_publication_stream_from_fresh_transaction(&request, &transaction)
        .context("prepare fast-ephemeral G0 publication stream")?;
    if stream.selected_count() as u64 != manifest.evaluation_population_size {
        bail!("fast-ephemeral selected evaluation width drifted");
    }
    let mut memory = MemoryFragments::default();
    let fragments: V5G0PublicationFragments = stream
        .materialize_selected_fragments_parallel(
            manifest.thread_cap,
            Some(progress_handle),
            &mut memory,
        )
        .context("materialize fast-ephemeral selected candidates")?;
    let selected_parent_publication =
        publish_selected_g0_parent_objects(output_root, &transaction)
            .context("publish fast-ephemeral selected G0 parent closure")?;
    if selected_parent_publication.object_count == 0 {
        bail!("fast-ephemeral selected G0 parent closure is empty");
    }

    // Population identity is required by the evaluation-population schema,
    // but the duplicate population document is not scientifically consumed
    // on the ephemeral path. Compute it into a sink and persist only the
    // selected evaluation population.
    let population_receipt = stream
        .write_population_from_fragments(&fragments, &mut memory, &mut io::sink())
        .context("derive fast-ephemeral population identity")?;
    let evaluation_path = output_root.join(EVALUATION_POPULATION_PATH);
    let mut evaluation_writer =
        create_new_writer(&evaluation_path, "fast-ephemeral evaluation population")?;
    let evaluation_receipt = stream
        .write_evaluation_population_from_fragments(
            &population_receipt,
            &fragments,
            &mut memory,
            &mut evaluation_writer,
        )
        .context("write fast-ephemeral evaluation population")?;
    evaluation_writer
        .flush()
        .context("flush fast-ephemeral evaluation population")?;
    drop(evaluation_writer);

    let ledger_value = transaction
        .identity_ledger
        .to_value()
        .context("encode fast-ephemeral identity ledger")?;
    let ledger_semantic_sha256 = transaction
        .identity_ledger
        .identity_ledger_sha256()
        .context("identify fast-ephemeral identity ledger")?;
    let ledger_bytes = canonical_json_line(&ledger_value)
        .context("encode fast-ephemeral identity-ledger bytes")?;
    let ledger_file_sha256 = sha256_bytes(&ledger_bytes);
    write_new(
        &output_root.join(IDENTITY_LEDGER_PATH),
        &ledger_bytes,
        "fast-ephemeral identity ledger",
    )?;
    let publication_elapsed = publication_started.elapsed();

    let artifacts = Value::Object(Map::from_iter([
        (
            "evaluationPopulation".to_owned(),
            artifact_value(EVALUATION_POPULATION_PATH, &evaluation_receipt),
        ),
        (
            "identityLedger".to_owned(),
            inline_artifact_value(
                IDENTITY_LEDGER_PATH,
                ledger_semantic_sha256,
                ledger_file_sha256,
                ledger_bytes.len(),
            ),
        ),
    ]));
    let mut result_fields = Map::from_iter([
        (
            "schemaVersion".to_owned(),
            Value::String(RESULT_SCHEMA.to_owned()),
        ),
        (
            "executionMode".to_owned(),
            Value::String(EXECUTION_MODE.to_owned()),
        ),
        ("status".to_owned(), Value::String("completed".to_owned())),
        ("generationKind".to_owned(), Value::String("g0".to_owned())),
        (
            "generationIndex".to_owned(),
            Value::from(manifest.generation_index),
        ),
        (
            "manifestSha256".to_owned(),
            Value::String(manifest.manifest_sha256.clone()),
        ),
        (
            "generationConfigSha256".to_owned(),
            Value::String(manifest.generation_config_sha256.clone()),
        ),
        (
            "attemptCount".to_owned(),
            Value::from(transaction.attempts.len() as u64),
        ),
        (
            "acceptedCandidateCount".to_owned(),
            Value::from(transaction.accepted_records.len() as u64),
        ),
        (
            "selectedEvaluationCandidateCount".to_owned(),
            Value::from(stream.selected_count() as u64),
        ),
        ("artifacts".to_owned(), artifacts.clone()),
        (
            "timings".to_owned(),
            Value::Object(Map::from_iter([
                (
                    "staticAuthorityMilliseconds".to_owned(),
                    Value::from(duration_ms(static_authority_elapsed)?),
                ),
                (
                    "constructionMilliseconds".to_owned(),
                    Value::from(duration_ms(construction_elapsed)?),
                ),
                (
                    "ephemeralPublicationMilliseconds".to_owned(),
                    Value::from(duration_ms(publication_elapsed)?),
                ),
                (
                    "totalMilliseconds".to_owned(),
                    Value::from(duration_ms(started.elapsed())?),
                ),
            ])),
        ),
    ]);
    let result_sha256 = canonical_sha256(&Value::Object(result_fields.clone()))
        .context("identify fast-ephemeral result")?;
    result_fields.insert(
        "resultSha256".to_owned(),
        Value::String(result_sha256.clone()),
    );
    let result = Value::Object(result_fields);
    let result_bytes = canonical_json_line(&result).context("encode fast-ephemeral result")?;
    write_new(
        invocation_result_path,
        &result_bytes,
        "fast-ephemeral invocation result",
    )?;

    let mut complete_fields = Map::from_iter([
        (
            "schemaVersion".to_owned(),
            Value::String(COMPLETE_SCHEMA.to_owned()),
        ),
        (
            "executionMode".to_owned(),
            Value::String(EXECUTION_MODE.to_owned()),
        ),
        (
            "generationIndex".to_owned(),
            Value::from(manifest.generation_index),
        ),
        ("resultSha256".to_owned(), Value::String(result_sha256)),
        ("artifacts".to_owned(), artifacts),
    ]);
    let complete_sha256 = canonical_sha256(&Value::Object(complete_fields.clone()))
        .context("identify fast-ephemeral completion marker")?;
    complete_fields.insert("completeSha256".to_owned(), Value::String(complete_sha256));
    write_new(
        &output_root.join(COMPLETE_PATH),
        &canonical_json_line(&Value::Object(complete_fields))
            .context("encode fast-ephemeral completion marker")?,
        "fast-ephemeral completion marker",
    )?;

    progress_handle.record_section(NativeProgressSection {
        name: "static_authority".to_owned(),
        wall: static_authority_elapsed,
        parallel_workers: Some(1),
        ..NativeProgressSection::default()
    });
    progress_handle.record_section(NativeProgressSection {
        name: "construction".to_owned(),
        wall: construction_elapsed,
        completed_work_units: Some(manifest.requested_count),
        ..NativeProgressSection::default()
    });
    progress_handle.record_section(NativeProgressSection {
        name: "ephemeral_publication".to_owned(),
        wall: publication_elapsed,
        completed_work_units: Some(manifest.evaluation_population_size),
        bytes_processed: Some(
            evaluation_receipt
                .encoded_bytes
                .checked_add(ledger_bytes.len() as u64)
                .and_then(|bytes| bytes.checked_add(selected_parent_publication.encoded_bytes))
                .ok_or_else(|| anyhow!("fast-ephemeral byte telemetry overflow"))?,
        ),
        parallel_workers: Some(manifest.thread_cap),
        ..NativeProgressSection::default()
    });
    progress.finish(None);
    super::write_stdout_json(&result)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_evolved(
    output_root: &Path,
    invocation_result_path: &Path,
    manifest: &V5ProposalManifest,
    progress_handle: &NativeProgressHandle,
    progress: NativeProgress,
    started: Instant,
    static_authority_elapsed: Duration,
) -> Result<()> {
    for relative in [
        STATUS_PATH,
        COMPLETE_PATH,
        EVALUATION_POPULATION_PATH,
        IDENTITY_LEDGER_PATH,
        "v5-native",
        "internal",
    ] {
        if output_root.join(relative).exists() {
            bail!("fast-ephemeral-v1 refuses pre-existing output or durable state: {relative}");
        }
    }
    if invocation_result_path.exists() {
        bail!("fast-ephemeral-v1 is non-resumable; discard the incomplete run directory");
    }

    let status = Value::Object(Map::from_iter([
        (
            "schemaVersion".to_owned(),
            Value::String(STATUS_SCHEMA.to_owned()),
        ),
        (
            "executionMode".to_owned(),
            Value::String(EXECUTION_MODE.to_owned()),
        ),
        ("status".to_owned(), Value::String("running".to_owned())),
        (
            "generationIndex".to_owned(),
            Value::from(manifest.generation_index),
        ),
        (
            "manifestSha256".to_owned(),
            Value::String(manifest.manifest_sha256.clone()),
        ),
    ]));
    write_new(
        &output_root.join(STATUS_PATH),
        &canonical_json_line(&status).context("encode fast-ephemeral status")?,
        "fast-ephemeral status",
    )?;

    progress_handle.begin_phase(
        "construction",
        "construct_and_admit_evolved",
        "accepted_candidate",
        Some(manifest.requested_count),
        true,
        Some(manifest.thread_cap),
        None,
    );
    let construction_started = Instant::now();
    let (request, mut parents, mut identity_ledger) =
        super::v5_evolved_transaction_request(manifest)?;
    let transaction = execute_v5_evolved_transaction_with_progress(
        request.clone(),
        &mut parents,
        &mut identity_ledger,
        Some(progress_handle),
    )
    .context("execute fast-ephemeral native v5 evolved transaction")?;
    let construction_elapsed = construction_started.elapsed();
    if !transaction.target_reached
        || transaction.accepted_records.len() as u64 != manifest.requested_count
        || transaction.attempts.len() as u64 > manifest.max_proposal_attempts
    {
        bail!("fast-ephemeral native v5 evolved construction did not reach its exact dimensions");
    }

    progress_handle.begin_phase(
        "ephemeral_publication",
        "write_evolved_evaluation_population",
        "accepted_candidate",
        Some(manifest.requested_count),
        true,
        Some(manifest.thread_cap),
        None,
    );
    let publication_started = Instant::now();
    let final_newline = Value::String(manifest.final_newline.clone());
    let publication_inputs = V5EvolvedPublicationInputs::from_manifest_values(
        &manifest.generation_config,
        &final_newline,
        &manifest.execution_authority,
        &manifest.inputs,
    )
    .context("parse fast-ephemeral evolved publication inputs")?;
    let publication_plan = V5EvolvedPublicationPlan::derive(&request, &publication_inputs)
        .context("derive fast-ephemeral evolved publication plan")?;
    let stream = prepare_v5_evolved_publication_stream(&request, &transaction, &publication_plan)
        .context("prepare fast-ephemeral evolved publication stream")?;
    if stream.accepted_count() as u64 != manifest.requested_count {
        bail!("fast-ephemeral evolved accepted width drifted");
    }
    let mut memory = MemoryFragments::default();
    let fragments: V5EvolvedPublicationFragments = stream
        .materialize_accepted_fragments(&mut memory)
        .context("materialize fast-ephemeral evolved accepted candidates")?;

    let population_receipt = stream
        .write_population_from_fragments(&fragments, &mut memory, &mut io::sink())
        .context("derive fast-ephemeral evolved population identity")?;
    let evaluation_path = output_root.join(EVALUATION_POPULATION_PATH);
    let mut evaluation_writer = create_new_writer(
        &evaluation_path,
        "fast-ephemeral evolved evaluation population",
    )?;
    let evaluation_receipt = stream
        .write_evaluation_population_from_fragments(
            &population_receipt,
            &fragments,
            &mut memory,
            &mut evaluation_writer,
        )
        .context("write fast-ephemeral evolved evaluation population")?;
    evaluation_writer
        .flush()
        .context("flush fast-ephemeral evolved evaluation population")?;
    drop(evaluation_writer);

    let ledger_path = output_root.join(IDENTITY_LEDGER_PATH);
    let mut ledger_writer =
        create_new_writer(&ledger_path, "fast-ephemeral evolved identity ledger")?;
    let ledger_receipt = stream
        .write_identity_ledger(&mut ledger_writer)
        .context("write fast-ephemeral evolved identity ledger")?;
    ledger_writer
        .flush()
        .context("flush fast-ephemeral evolved identity ledger")?;
    drop(ledger_writer);
    let publication_elapsed = publication_started.elapsed();

    let artifacts = Value::Object(Map::from_iter([
        (
            "evaluationPopulation".to_owned(),
            evolved_artifact_value(EVALUATION_POPULATION_PATH, &evaluation_receipt),
        ),
        (
            "identityLedger".to_owned(),
            evolved_artifact_value(IDENTITY_LEDGER_PATH, &ledger_receipt),
        ),
    ]));
    let mut result_fields = Map::from_iter([
        (
            "schemaVersion".to_owned(),
            Value::String(RESULT_SCHEMA.to_owned()),
        ),
        (
            "executionMode".to_owned(),
            Value::String(EXECUTION_MODE.to_owned()),
        ),
        ("status".to_owned(), Value::String("completed".to_owned())),
        (
            "generationKind".to_owned(),
            Value::String("evolved".to_owned()),
        ),
        (
            "generationIndex".to_owned(),
            Value::from(manifest.generation_index),
        ),
        (
            "manifestSha256".to_owned(),
            Value::String(manifest.manifest_sha256.clone()),
        ),
        (
            "generationConfigSha256".to_owned(),
            Value::String(manifest.generation_config_sha256.clone()),
        ),
        (
            "attemptCount".to_owned(),
            Value::from(transaction.attempts.len() as u64),
        ),
        (
            "acceptedCandidateCount".to_owned(),
            Value::from(transaction.accepted_records.len() as u64),
        ),
        (
            "selectedEvaluationCandidateCount".to_owned(),
            Value::from(stream.accepted_count() as u64),
        ),
        ("artifacts".to_owned(), artifacts.clone()),
        (
            "timings".to_owned(),
            Value::Object(Map::from_iter([
                (
                    "staticAuthorityMilliseconds".to_owned(),
                    Value::from(duration_ms(static_authority_elapsed)?),
                ),
                (
                    "constructionMilliseconds".to_owned(),
                    Value::from(duration_ms(construction_elapsed)?),
                ),
                (
                    "ephemeralPublicationMilliseconds".to_owned(),
                    Value::from(duration_ms(publication_elapsed)?),
                ),
                (
                    "totalMilliseconds".to_owned(),
                    Value::from(duration_ms(started.elapsed())?),
                ),
            ])),
        ),
    ]);
    let result_sha256 = canonical_sha256(&Value::Object(result_fields.clone()))
        .context("identify fast-ephemeral evolved result")?;
    result_fields.insert(
        "resultSha256".to_owned(),
        Value::String(result_sha256.clone()),
    );
    let result = Value::Object(result_fields);
    write_new(
        invocation_result_path,
        &canonical_json_line(&result).context("encode fast-ephemeral evolved result")?,
        "fast-ephemeral evolved invocation result",
    )?;

    let mut complete_fields = Map::from_iter([
        (
            "schemaVersion".to_owned(),
            Value::String(COMPLETE_SCHEMA.to_owned()),
        ),
        (
            "executionMode".to_owned(),
            Value::String(EXECUTION_MODE.to_owned()),
        ),
        (
            "generationIndex".to_owned(),
            Value::from(manifest.generation_index),
        ),
        ("resultSha256".to_owned(), Value::String(result_sha256)),
        ("artifacts".to_owned(), artifacts),
    ]);
    let complete_sha256 = canonical_sha256(&Value::Object(complete_fields.clone()))
        .context("identify fast-ephemeral evolved completion marker")?;
    complete_fields.insert("completeSha256".to_owned(), Value::String(complete_sha256));
    write_new(
        &output_root.join(COMPLETE_PATH),
        &canonical_json_line(&Value::Object(complete_fields))
            .context("encode fast-ephemeral evolved completion marker")?,
        "fast-ephemeral evolved completion marker",
    )?;

    progress_handle.record_section(NativeProgressSection {
        name: "static_authority".to_owned(),
        wall: static_authority_elapsed,
        parallel_workers: Some(1),
        ..NativeProgressSection::default()
    });
    progress_handle.record_section(NativeProgressSection {
        name: "construction".to_owned(),
        wall: construction_elapsed,
        completed_work_units: Some(manifest.requested_count),
        ..NativeProgressSection::default()
    });
    progress_handle.record_section(NativeProgressSection {
        name: "ephemeral_publication".to_owned(),
        wall: publication_elapsed,
        completed_work_units: Some(manifest.requested_count),
        bytes_processed: Some(
            evaluation_receipt
                .encoded_bytes
                .checked_add(ledger_receipt.encoded_bytes)
                .ok_or_else(|| anyhow!("fast-ephemeral evolved byte telemetry overflow"))?,
        ),
        parallel_workers: Some(manifest.thread_cap),
        ..NativeProgressSection::default()
    });
    progress.finish(None);
    super::write_stdout_json(&result)
}


#[cfg(test)]
mod tests {
    include!("v5_fast_ephemeral_tests.rs");
}
