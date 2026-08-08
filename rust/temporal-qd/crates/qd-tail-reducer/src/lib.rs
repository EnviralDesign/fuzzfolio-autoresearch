//! Identity-bound native reducer for Temporal QD post-evaluation evidence.
//!
//! The boundary deliberately starts after Python has built its exact compact
//! evaluation-population and tail-result-index projections. It does not read
//! replay blobs or reinterpret worker output.

#![recursion_limit = "256"]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, ensure};
use base64::Engine as _;
use flate2::read::GzDecoder;
use serde_json::{Map, Number, Value, json};
use sha2::{Digest, Sha256};
use temporal_qd_contract::{
    CONTRACT_VERSION, CanonicalSha256Writer, canonical_json_bytes, canonical_json_line,
    canonical_sha256, canonical_sha256_without_object_field, sha256_prefixed, write_canonical_json,
};

pub const MANIFEST_SCHEMA: &str = "temporal_qd_native_tail_reduction_manifest_v1";
pub const RESULT_SCHEMA: &str = "temporal_qd_native_tail_reduction_result_v1";
pub const OPERATION: &str = "reduce_evaluated_members_and_provisional";
pub const RESULT_PATH: &str = "tail-reduction-result.json";
pub const MEMBERS_PATH: &str = "evaluated-members.jsonl";

const EVALUATION_POPULATION_SCHEMA: &str = "temporal_qd_evaluation_population_v1";
const INDEX_SCHEMA: &str = "temporal_qd_tail_result_index_v3";
const ENTRY_SCHEMA: &str = "temporal_qd_tail_result_index_entry_v3";
const PROJECTION_SCHEMA: &str = "temporal_qd_tail_stage_projection_v1";
const EVALUATED_SCHEMA: &str = "temporal_qd_evaluated_members_v1";
const PROVISIONAL_SCHEMA: &str = "temporal_qd_native_provisional_survivors_v1";

#[derive(Clone, Debug)]
pub struct TailReductionManifest {
    pub runtime_authority_sha256: Option<String>,
    pub evaluation_population_path: PathBuf,
    pub evaluation_population_sha256: String,
    pub tail_result_index_path: PathBuf,
    pub tail_result_index_sha256: String,
    pub generation_index: u64,
    pub minimum_total_trades: i64,
    pub minimum_trades_per_window: i64,
    pub cap_trades: i64,
    pub provisional_limit: usize,
    pub manifest_sha256: String,
}

#[derive(Debug)]
struct CandidateWindows {
    windows: Vec<Value>,
}

#[derive(Debug)]
struct ReductionState {
    population_sha256: String,
    evaluation_population_sha256: String,
    tail_result_index_sha256: String,
    task_matrix_sha256: String,
    result_set_sha256: String,
    member_count: usize,
    rejected: Vec<Value>,
    provisional: Vec<Value>,
    members_sha256: String,
    members_bytes: u64,
}

#[derive(Clone, Debug)]
struct ProvisionalBase {
    candidate_id: String,
    candidate_identity_sha256: Value,
    program_sha256: Value,
    profile_snapshot_sha256: Value,
    cell_id: String,
    rank: f64,
}

#[derive(Debug)]
struct CandidateWork {
    candidate_id: String,
    candidate: Value,
    windows: Vec<Value>,
}

#[derive(Debug)]
struct CandidateReduction {
    result_set_entry: Vec<u8>,
    rejection: Option<Value>,
    member_line: Option<Vec<u8>>,
    provisional: Option<ProvisionalBase>,
    result_set_nanos: u128,
    member_compute_nanos: u128,
    member_encode_nanos: u128,
}

struct StageProfile {
    enabled: bool,
    started: Instant,
    previous: Instant,
}

impl StageProfile {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            enabled: std::env::var_os("TEMPORAL_QD_TAIL_PROFILE").is_some(),
            started: now,
            previous: now,
        }
    }

    fn mark(&mut self, stage: &str) {
        let now = Instant::now();
        if self.enabled {
            eprintln!(
                "{{\"stage\":\"{stage}\",\"elapsedMilliseconds\":{:.3}}}",
                now.duration_since(self.previous).as_secs_f64() * 1000.0
            );
        }
        self.previous = now;
    }

    fn finish(&self) {
        if self.enabled {
            eprintln!(
                "{{\"stage\":\"total\",\"elapsedMilliseconds\":{:.3}}}",
                self.started.elapsed().as_secs_f64() * 1000.0
            );
        }
    }

    fn accumulated(&self, stage: &str, nanoseconds: u128) {
        if self.enabled {
            eprintln!(
                "{{\"stage\":\"{stage}\",\"elapsedMilliseconds\":{:.3},\"accumulated\":true}}",
                nanoseconds as f64 / 1_000_000.0
            );
        }
    }
}

pub fn execute_manifest(manifest_path: &Path) -> Result<Value> {
    let mut profile = StageProfile::new();
    let manifest_path = existing_regular_file(manifest_path, "tail reduction manifest")?;
    let raw = fs::read(&manifest_path)
        .with_context(|| format!("read tail reduction manifest: {}", manifest_path.display()))?;
    let manifest = parse_manifest(&raw)?;
    profile.mark("manifest_read_validate");
    let output_dir = manifest_path
        .parent()
        .ok_or_else(|| anyhow!("tail reduction manifest has no parent directory"))?;
    ensure_real_directory(output_dir, "tail reduction output directory")?;
    let result_path = output_dir.join(RESULT_PATH);
    let members_path = output_dir.join(MEMBERS_PATH);

    if result_path.exists() {
        let result = reopen_result(&result_path, &members_path, &manifest)?;
        profile.mark("restart_reopen_validate");
        profile.finish();
        return Ok(result);
    }

    let temporary_members = temporary_path(output_dir, MEMBERS_PATH);
    let state = match reduce_to_members_file(&manifest, &temporary_members, &mut profile) {
        Ok(state) => state,
        Err(error) => {
            let _ = fs::remove_file(&temporary_members);
            return Err(error);
        }
    };
    publish_once(
        &temporary_members,
        &members_path,
        Some(&state.members_sha256),
    )?;
    profile.mark("members_publish");

    let result = build_result(&manifest, &state);
    validate_result_value(&result, &manifest)?;
    let bytes = canonical_json_line(&result).context("encode tail reduction result")?;
    let temporary_result = temporary_path(output_dir, RESULT_PATH);
    write_new_synced(&temporary_result, &bytes)?;
    publish_once(&temporary_result, &result_path, None)?;
    sync_directory(output_dir)?;
    profile.mark("result_build_publish");
    // The exact bytes and member digest were validated before publication.
    // Reopening both freshly-created artifacts here repeated canonical parsing
    // and hashing without adding evidence. Restart still takes the full
    // `reopen_result` validation path above.
    profile.mark("fresh_commit_complete");
    profile.finish();
    Ok(result)
}

pub fn parse_manifest(raw: &[u8]) -> Result<TailReductionManifest> {
    let value: Value = serde_json::from_slice(raw).context("parse tail reduction manifest")?;
    ensure!(
        canonical_json_line(&value)? == raw,
        "tail reduction manifest must be canonical JSON followed by one LF"
    );
    let fields = object(&value, "tail reduction manifest")?;
    let mut manifest_keys = vec![
        "schemaVersion",
        "contractVersion",
        "operation",
        "runtimeAuthoritySha256",
        "evaluationPopulationPath",
        "evaluationPopulationSha256",
        "tailResultIndexPath",
        "tailResultIndexSha256",
        "generationIndex",
        "minimumTotalTrades",
        "minimumTradesPerWindow",
        "capTrades",
        "provisionalLimit",
        "resultPath",
        "manifestSha256",
    ];
    let runtime_authority_sha256 = match fields.get("runtimeAuthoritySha256") {
        Some(_) => Some(sha_field(
            fields,
            "runtimeAuthoritySha256",
            "tail reduction manifest",
        )?),
        None => {
            manifest_keys.retain(|key| *key != "runtimeAuthoritySha256");
            None
        }
    };
    exact_keys(fields, &manifest_keys, "tail reduction manifest")?;
    ensure!(
        string(fields, "schemaVersion", "tail reduction manifest")? == MANIFEST_SCHEMA,
        "tail reduction manifest schema is incompatible"
    );
    ensure!(
        string(fields, "contractVersion", "tail reduction manifest")? == CONTRACT_VERSION,
        "tail reduction contract version is incompatible"
    );
    ensure!(
        string(fields, "operation", "tail reduction manifest")? == OPERATION,
        "tail reduction operation is incompatible"
    );
    ensure!(
        string(fields, "resultPath", "tail reduction manifest")? == RESULT_PATH,
        "tail reduction result path is not fixed"
    );
    let manifest_sha256 = sha_field(fields, "manifestSha256", "tail reduction manifest")?;
    ensure!(
        canonical_sha256_without_object_field(&value, "manifestSha256")? == manifest_sha256,
        "tail reduction manifest identity mismatch"
    );
    let generation_index = integer(fields, "generationIndex", "tail reduction manifest")?;
    let minimum_total_trades =
        nonnegative_i64(fields, "minimumTotalTrades", "tail reduction manifest")?;
    let minimum_trades_per_window =
        nonnegative_i64(fields, "minimumTradesPerWindow", "tail reduction manifest")?;
    let cap_trades = nonnegative_i64(fields, "capTrades", "tail reduction manifest")?;
    let provisional_limit = integer(fields, "provisionalLimit", "tail reduction manifest")?;
    ensure!(
        provisional_limit > 0,
        "tail reduction provisional limit must be positive"
    );
    Ok(TailReductionManifest {
        runtime_authority_sha256,
        evaluation_population_path: PathBuf::from(string(
            fields,
            "evaluationPopulationPath",
            "tail reduction manifest",
        )?),
        evaluation_population_sha256: sha_field(
            fields,
            "evaluationPopulationSha256",
            "tail reduction manifest",
        )?,
        tail_result_index_path: PathBuf::from(string(
            fields,
            "tailResultIndexPath",
            "tail reduction manifest",
        )?),
        tail_result_index_sha256: sha_field(
            fields,
            "tailResultIndexSha256",
            "tail reduction manifest",
        )?,
        generation_index,
        minimum_total_trades,
        minimum_trades_per_window,
        cap_trades,
        provisional_limit: usize::try_from(provisional_limit)
            .context("provisional limit exceeds platform size")?,
        manifest_sha256,
    })
}

fn reduce_to_members_file(
    manifest: &TailReductionManifest,
    output: &Path,
    profile: &mut StageProfile,
) -> Result<ReductionState> {
    let evaluation_path = existing_regular_file(
        &manifest.evaluation_population_path,
        "evaluation population",
    )?;
    let index_path = existing_regular_file(&manifest.tail_result_index_path, "tail result index")?;
    let mut evaluation: Value =
        serde_json::from_reader(BufReader::new(File::open(&evaluation_path)?))
            .context("parse evaluation population")?;
    profile.mark("evaluation_read_parse");
    let evaluation_fields = object(&evaluation, "evaluation population")?;
    exact_keys(
        evaluation_fields,
        &[
            "schemaVersion",
            "generationIndex",
            "populationSha256",
            "populationFileSha256",
            "pairGenerationConfigSha256",
            "policyName",
            "policySha256",
            "pairPolicySha256",
            "bidirectionalPairPolicy",
            "operatorImplementationSha256",
            "predeclaredEvidenceContextSha256",
            "g0Bootstrap",
            "proposalAttempts",
            "funnelEntries",
            "candidateCount",
            "candidates",
            "evaluationPopulationSha256",
        ],
        "evaluation population",
    )?;
    ensure!(
        evaluation_fields
            .get("schemaVersion")
            .and_then(Value::as_str)
            == Some(EVALUATION_POPULATION_SCHEMA),
        "evaluation population schema is incompatible"
    );
    let supplied_evaluation = sha_field(
        evaluation_fields,
        "evaluationPopulationSha256",
        "evaluation population",
    )?;
    ensure!(
        supplied_evaluation == manifest.evaluation_population_sha256,
        "evaluation population manifest binding mismatch"
    );
    ensure!(
        canonical_sha256_without_object_field(&evaluation, "evaluationPopulationSha256")?
            == supplied_evaluation,
        "evaluation population identity mismatch"
    );
    let population_sha256 = sha_field(
        evaluation_fields,
        "populationSha256",
        "evaluation population",
    )?;
    ensure!(
        integer(
            evaluation_fields,
            "generationIndex",
            "evaluation population"
        )? == manifest.generation_index,
        "evaluation population generation binding mismatch"
    );
    for field_name in [
        "populationFileSha256",
        "pairGenerationConfigSha256",
        "policySha256",
        "pairPolicySha256",
        "operatorImplementationSha256",
    ] {
        sha_field(evaluation_fields, field_name, "evaluation population")?;
    }
    ensure!(
        canonical_sha256(field(
            evaluation_fields,
            "bidirectionalPairPolicy",
            "evaluation population",
        )?)? == string(
            evaluation_fields,
            "pairPolicySha256",
            "evaluation population",
        )?,
        "evaluation population pair policy identity mismatch"
    );
    let proposal_attempts = usize::try_from(integer(
        evaluation_fields,
        "proposalAttempts",
        "evaluation population",
    )?)?;
    ensure!(
        evaluation_fields
            .get("funnelEntries")
            .and_then(Value::as_array)
            .is_some_and(|rows| rows.len() == proposal_attempts),
        "evaluation population proposal accounting mismatch"
    );
    profile.mark("evaluation_root_validate");
    let expected_candidates = usize::try_from(integer(
        evaluation_fields,
        "candidateCount",
        "evaluation population",
    )?)?;
    let candidates_value = evaluation
        .as_object_mut()
        .expect("object checked")
        .remove("candidates")
        .ok_or_else(|| anyhow!("evaluation population lacks candidates"))?;
    let candidates = candidates_value
        .as_array()
        .ok_or_else(|| anyhow!("evaluation population candidates must be an array"))?;
    ensure!(
        candidates.len() == expected_candidates,
        "evaluation population candidate count mismatch"
    );
    let mut candidate_map = BTreeMap::new();
    for candidate in candidates.iter().cloned() {
        validate_candidate(&candidate)?;
        let candidate_id = candidate
            .get("candidateId")
            .and_then(Value::as_str)
            .expect("validated")
            .to_owned();
        ensure!(
            candidate_map.insert(candidate_id, candidate).is_none(),
            "evaluation population repeats a candidate identity"
        );
    }
    drop(evaluation);
    profile.mark("candidate_extract_validate");

    let mut index: Value = serde_json::from_reader(BufReader::new(File::open(&index_path)?))
        .context("parse tail result index")?;
    profile.mark("index_read_parse");
    let index_fields = object(&index, "tail result index")?;
    exact_keys(
        index_fields,
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
        "tail result index",
    )?;
    ensure!(
        index_fields.get("schemaVersion").and_then(Value::as_str) == Some(INDEX_SCHEMA),
        "tail result index schema is incompatible"
    );
    let supplied_index = sha_field(index_fields, "tailResultIndexSha256", "tail result index")?;
    ensure!(
        supplied_index == manifest.tail_result_index_sha256,
        "tail result index manifest binding mismatch"
    );
    ensure!(
        canonical_sha256_without_object_field(&index, "tailResultIndexSha256")? == supplied_index,
        "tail result index identity mismatch"
    );
    let task_matrix_sha256 = sha_field(index_fields, "taskMatrixSha256", "tail result index")?;
    for field_name in ["authoritySha256", "taskManifestSha256", "checkpointSha256"] {
        sha_field(index_fields, field_name, "tail result index")?;
    }
    let include_funnel = index_fields
        .get("funnelProjectionIncluded")
        .and_then(Value::as_bool)
        .ok_or_else(|| anyhow!("tail result index funnel feature flag must be boolean"))?;
    let expected_source_bytes =
        integer(index_fields, "sourceResultBlobBytes", "tail result index")?;
    profile.mark("index_root_validate");
    let expected_entries =
        usize::try_from(integer(index_fields, "taskCount", "tail result index")?)?;
    let entries_value = index
        .as_object_mut()
        .expect("object checked")
        .remove("entries")
        .ok_or_else(|| anyhow!("tail result index lacks entries"))?;
    let entries = entries_value
        .as_array()
        .ok_or_else(|| anyhow!("tail result index entries must be an array"))?;
    ensure!(
        entries.len() == expected_entries,
        "tail result index task count mismatch"
    );
    let decoded_entries = decode_entries_parallel(entries, include_funnel)?;
    let mut last_task_id: Option<String> = None;
    let mut source_bytes = 0u64;
    let mut windows_by_candidate: HashMap<String, CandidateWindows> = HashMap::new();
    for (candidate_id, task_id, blob_bytes, window) in decoded_entries {
        source_bytes = source_bytes
            .checked_add(blob_bytes)
            .ok_or_else(|| anyhow!("tail result source byte count overflow"))?;
        if let Some(last) = &last_task_id {
            ensure!(
                task_id > *last,
                "tail result index entries are not unique canonical task order"
            );
        }
        last_task_id = Some(task_id);
        windows_by_candidate
            .entry(candidate_id)
            .or_insert_with(|| CandidateWindows {
                windows: Vec::new(),
            })
            .windows
            .push(window);
    }
    ensure!(
        source_bytes == expected_source_bytes,
        "tail result index source blob byte count drifted"
    );
    drop(index);
    profile.mark("entry_decode_group");
    ensure!(
        windows_by_candidate.len() == candidate_map.len()
            && windows_by_candidate
                .keys()
                .all(|key| candidate_map.contains_key(key)),
        "tail result index does not exactly cover the evaluation population"
    );
    for rows in windows_by_candidate.values_mut() {
        rows.windows.sort_by(|left, right| {
            let left_key = (
                text_at(left, "analysisWindowStart"),
                text_at(left, "analysisWindowEnd"),
            );
            let right_key = (
                text_at(right, "analysisWindowStart"),
                text_at(right, "analysisWindowEnd"),
            );
            left_key.cmp(&right_key)
        });
    }

    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output)
        .with_context(|| format!("create temporary member artifact: {}", output.display()))?;
    let mut member_writer = RawHashWriter::new(BufWriter::new(file));
    let mut result_set_writer = CanonicalSha256Writer::default();
    result_set_writer.write_all(b"[")?;
    let mut rejected = Vec::new();
    let mut provisional_base = Vec::new();
    let mut cell_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut member_count = 0usize;
    let mut result_set_nanos = 0u128;
    let mut member_compute_nanos = 0u128;
    let mut member_encode_write_nanos = 0u128;
    let candidate_work = candidate_map
        .into_iter()
        .map(|(candidate_id, candidate)| {
            let windows = windows_by_candidate
                .remove(&candidate_id)
                .expect("coverage checked")
                .windows;
            CandidateWork {
                candidate_id,
                candidate,
                windows,
            }
        })
        .collect();
    let reductions = reduce_candidates_parallel(candidate_work, manifest)?;
    for (ordinal, reduction) in reductions.into_iter().enumerate() {
        if ordinal > 0 {
            result_set_writer.write_all(b",")?;
        }
        result_set_writer.write_all(&reduction.result_set_entry)?;
        result_set_nanos += reduction.result_set_nanos;
        member_compute_nanos += reduction.member_compute_nanos;
        member_encode_write_nanos += reduction.member_encode_nanos;
        if let Some(rejection) = reduction.rejection {
            rejected.push(rejection);
            continue;
        }
        let line = reduction
            .member_line
            .expect("accepted candidate has encoded member");
        member_writer.write_all(&line)?;
        member_count += 1;
        let provisional = reduction
            .provisional
            .expect("accepted candidate has provisional projection");
        *cell_counts.entry(provisional.cell_id.clone()).or_default() += 1;
        provisional_base.push(provisional);
    }
    result_set_writer.write_all(b"]")?;
    let result_set_sha256 = result_set_writer.finish();
    member_writer.flush()?;
    member_writer.inner.get_ref().sync_all()?;
    let (members_sha256, members_bytes) = member_writer.finish();
    let provisional =
        provisional_reduce(provisional_base, &cell_counts, manifest.provisional_limit)?;
    profile.accumulated("result_set_hash", result_set_nanos);
    profile.accumulated("member_compute", member_compute_nanos);
    profile.accumulated("member_encode_write", member_encode_write_nanos);
    profile.mark("aggregate_serialize_members");
    Ok(ReductionState {
        population_sha256,
        evaluation_population_sha256: supplied_evaluation,
        tail_result_index_sha256: supplied_index,
        task_matrix_sha256,
        result_set_sha256,
        member_count,
        rejected,
        provisional,
        members_sha256,
        members_bytes,
    })
}

fn reduce_candidate(
    work: CandidateWork,
    manifest: &TailReductionManifest,
) -> Result<CandidateReduction> {
    let CandidateWork {
        candidate_id,
        candidate,
        windows,
    } = work;
    let stage_started = Instant::now();
    let mut result_set_entry = Vec::new();
    write_result_set_entry(&mut result_set_entry, 0, &candidate_id, &windows)?;
    let result_set_nanos = stage_started.elapsed().as_nanos();
    let stage_started = Instant::now();
    if let Some(rejection) = candidate_rejection(&candidate, &candidate_id, &windows)? {
        return Ok(CandidateReduction {
            result_set_entry,
            rejection: Some(rejection),
            member_line: None,
            provisional: None,
            result_set_nanos,
            member_compute_nanos: stage_started.elapsed().as_nanos(),
            member_encode_nanos: 0,
        });
    }
    let aggregate = aggregate_candidate(&candidate, &windows)?;
    let descriptor = behavior_descriptor(&candidate, &aggregate)?;
    let objectives = objective_row(&candidate, &aggregate)?;
    let validity = finite_data_validity(&aggregate, manifest)?;
    let total_trades = i64_at(&aggregate, "totalTrades")?;
    let member = json!({
        "candidateId": candidate_id,
        "generationIndex": manifest.generation_index,
        "candidate": candidate,
        "aggregate": aggregate,
        "descriptor": descriptor,
        "objectives": objectives,
        "finiteDataValidity": validity,
        "cappedTradeSupport": (total_trades.max(0).min(manifest.cap_trades)) as f64,
    });
    let member_compute_nanos = stage_started.elapsed().as_nanos();
    let stage_started = Instant::now();
    let member_line = canonical_json_line(&member)?;
    let provisional = ProvisionalBase {
        candidate_id: text_at(&member, "candidateId").to_owned(),
        candidate_identity_sha256: member["candidate"]["candidateIdentitySha256"].clone(),
        program_sha256: member["candidate"]["programSha256"].clone(),
        profile_snapshot_sha256: member["candidate"]["profileSnapshotSha256"].clone(),
        cell_id: text_at(&member["descriptor"], "cellId").to_owned(),
        rank: f64_at(&member["aggregate"], "totalConservativeNetR")?,
    };
    Ok(CandidateReduction {
        result_set_entry,
        rejection: None,
        member_line: Some(member_line),
        provisional: Some(provisional),
        result_set_nanos,
        member_compute_nanos,
        member_encode_nanos: stage_started.elapsed().as_nanos(),
    })
}

fn reduce_candidates_parallel(
    work: Vec<CandidateWork>,
    manifest: &TailReductionManifest,
) -> Result<Vec<CandidateReduction>> {
    let worker_count = runtime_threads(work.len())?;
    if worker_count == 1 || work.len() < 2 {
        return work
            .into_iter()
            .map(|candidate| reduce_candidate(candidate, manifest))
            .collect();
    }

    let work: Vec<Mutex<Option<CandidateWork>>> = work
        .into_iter()
        .map(|item| Mutex::new(Some(item)))
        .collect();
    let outputs: Vec<Mutex<Option<Result<CandidateReduction>>>> =
        (0..work.len()).map(|_| Mutex::new(None)).collect();
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let work = &work;
            let outputs = &outputs;
            let next = &next;
            let handle = std::thread::Builder::new()
                .name("temporal-qd-candidate-reducer".to_owned())
                .stack_size(16 * 1024 * 1024)
                .spawn_scoped(scope, move || {
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        if index >= work.len() {
                            break;
                        }
                        let candidate = work[index]
                            .lock()
                            .expect("tail candidate work mutex poisoned")
                            .take()
                            .expect("tail candidate work already consumed");
                        let reduction = reduce_candidate(candidate, manifest);
                        *outputs[index]
                            .lock()
                            .expect("tail candidate output mutex poisoned") = Some(reduction);
                    }
                })
                .context("spawn tail candidate reducer")?;
            handles.push(handle);
        }
        for handle in handles {
            ensure!(
                handle.join().is_ok(),
                "parallel tail candidate reducer panicked"
            );
        }
        Ok(())
    })?;

    outputs
        .into_iter()
        .enumerate()
        .map(|(index, output)| {
            output
                .into_inner()
                .map_err(|_| anyhow!("tail candidate output mutex poisoned"))?
                .ok_or_else(|| anyhow!("tail candidate reducer did not produce entry {index}"))?
        })
        .collect()
}

fn write_result_set_entry<W: Write>(
    writer: &mut W,
    ordinal: usize,
    candidate_id: &str,
    windows: &[Value],
) -> Result<()> {
    if ordinal > 0 {
        writer.write_all(b",")?;
    }
    writer.write_all(b"{\"candidateId\":")?;
    write_canonical_json(&Value::String(candidate_id.to_owned()), writer)?;
    writer.write_all(b",\"windows\":[")?;
    for (index, window) in windows.iter().enumerate() {
        if index > 0 {
            writer.write_all(b",")?;
        }
        write_canonical_json(window, writer)?;
    }
    writer.write_all(b"]}")?;
    Ok(())
}

type DecodedEntry = (String, String, u64, Value);

fn runtime_threads(work_items: usize) -> Result<usize> {
    let requested = match std::env::var("TEMPORAL_QD_TAIL_THREADS") {
        Ok(raw) => raw
            .parse::<usize>()
            .context("TEMPORAL_QD_TAIL_THREADS must be an integer")?,
        Err(std::env::VarError::NotPresent) => 2,
        Err(error) => return Err(error).context("read TEMPORAL_QD_TAIL_THREADS"),
    };
    ensure!(
        (1..=2).contains(&requested),
        "TEMPORAL_QD_TAIL_THREADS must be 1 or 2"
    );
    Ok(requested.min(work_items.max(1)))
}

fn decode_entries_parallel(entries: &[Value], include_funnel: bool) -> Result<Vec<DecodedEntry>> {
    let worker_count = runtime_threads(entries.len())?;
    if worker_count == 1 || entries.len() < 2 {
        return entries
            .iter()
            .map(|entry| decode_entry(entry, include_funnel))
            .collect();
    }

    let next = AtomicUsize::new(0);
    let outputs: Vec<Mutex<Option<Result<DecodedEntry>>>> =
        (0..entries.len()).map(|_| Mutex::new(None)).collect();
    std::thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let next = &next;
            let outputs = &outputs;
            let handle = std::thread::Builder::new()
                .name("temporal-qd-entry-decoder".to_owned())
                .stack_size(8 * 1024 * 1024)
                .spawn_scoped(scope, move || {
                    loop {
                        let index = next.fetch_add(1, Ordering::Relaxed);
                        if index >= entries.len() {
                            break;
                        }
                        let decoded = decode_entry(&entries[index], include_funnel);
                        *outputs[index]
                            .lock()
                            .expect("tail entry decoder output mutex poisoned") = Some(decoded);
                    }
                })
                .context("spawn tail entry decoder")?;
            handles.push(handle);
        }
        for handle in handles {
            ensure!(
                handle.join().is_ok(),
                "parallel tail entry decoder panicked"
            );
        }
        Ok(())
    })?;

    outputs
        .into_iter()
        .enumerate()
        .map(|(index, output)| {
            output
                .into_inner()
                .map_err(|_| anyhow!("tail entry decoder output mutex poisoned"))?
                .ok_or_else(|| anyhow!("tail entry decoder did not produce entry {index}"))?
        })
        .collect()
}

fn decode_entry(entry: &Value, include_funnel: bool) -> Result<(String, String, u64, Value)> {
    let fields = object(entry, "tail result index entry")?;
    ensure!(
        fields.get("schemaVersion").and_then(Value::as_str) == Some(ENTRY_SCHEMA),
        "tail result index entry schema is incompatible"
    );
    let supplied = sha_field(fields, "entrySha256", "tail result index entry")?;
    ensure!(
        canonical_sha256_without_object_field(entry, "entrySha256")? == supplied,
        "tail result index entry identity mismatch"
    );
    let task = object(
        field(fields, "task", "tail result index entry")?,
        "tail result index task",
    )?;
    exact_keys(
        task,
        &[
            "taskId",
            "candidateId",
            "analysisWindowStart",
            "analysisWindowEnd",
            "evidencePlanSemanticSha256",
            "taskPayloadSha256",
        ],
        "tail result index task",
    )?;
    let task_id = string(task, "taskId", "tail result index task")?;
    let candidate_id = string(task, "candidateId", "tail result index task")?;
    let start = string(task, "analysisWindowStart", "tail result index task")?;
    let end = string(task, "analysisWindowEnd", "tail result index task")?;
    sha_field(task, "evidencePlanSemanticSha256", "tail result index task")?;
    sha_field(task, "taskPayloadSha256", "tail result index task")?;
    let raw_ref = object(
        field(fields, "rawResultRef", "tail result index entry")?,
        "tail raw result reference",
    )?;
    exact_keys(
        raw_ref,
        &[
            "schemaVersion",
            "relativePath",
            "codec",
            "resultSha256",
            "semanticSizeBytes",
            "uncompressedSha256",
            "uncompressedSizeBytes",
            "blobSha256",
            "blobSizeBytes",
        ],
        "tail raw result reference",
    )?;
    ensure!(
        raw_ref.get("schemaVersion").and_then(Value::as_str)
            == Some("temporal_qd_tail_raw_result_ref_v1"),
        "tail raw result reference schema is incompatible"
    );
    for field_name in ["resultSha256", "uncompressedSha256", "blobSha256"] {
        sha_field(raw_ref, field_name, "tail raw result reference")?;
    }
    for field_name in [
        "semanticSizeBytes",
        "uncompressedSizeBytes",
        "blobSizeBytes",
    ] {
        integer(raw_ref, field_name, "tail raw result reference")?;
    }
    let provenance = object(
        field(fields, "rawTaskProvenance", "tail result index entry")?,
        "tail raw task provenance",
    )?;
    exact_keys(
        provenance,
        &["taskId", "resultSha256"],
        "tail raw task provenance",
    )?;
    ensure!(
        provenance.get("taskId").and_then(Value::as_str) == Some(task_id.as_str())
            && provenance.get("resultSha256") == raw_ref.get("resultSha256"),
        "tail raw task provenance binding drifted"
    );
    let rejected = fields.contains_key("rejection");
    let mut expected_fields = if rejected {
        vec![
            "schemaVersion",
            "task",
            "rawResultRef",
            "rawTaskProvenance",
            "rejection",
            "entrySha256",
        ]
    } else {
        vec![
            "schemaVersion",
            "task",
            "rawResultRef",
            "rawTaskProvenance",
            "stageProjection",
            "rotatingEvidenceMetrics",
            "entrySha256",
        ]
    };
    if include_funnel && !rejected {
        expected_fields.push("funnelProjection");
    }
    exact_keys(fields, &expected_fields, "tail result index entry")?;
    let window = if let Some(rejection) = fields.get("rejection") {
        let rejection_fields = object(rejection, "tail evaluation rejection")?;
        ensure!(
            rejection_fields.get("disposition").and_then(Value::as_str) == Some("rejected")
                && rejection_fields
                    .get("reason_code")
                    .and_then(Value::as_str)
                    .is_some(),
            "tail evaluation rejection is invalid"
        );
        json!({
            "economicsBasis": format!("not_evaluated_{}", rejection.get("reason_code").and_then(Value::as_str).unwrap_or("")),
            "v3Admissible": false,
            "evaluationRejected": true,
            "rejection": rejection,
            "candidateId": candidate_id,
            "windowId": format!("{start}/{end}"),
            "analysisWindowStart": start,
            "analysisWindowEnd": end,
        })
    } else {
        let projection = object(
            field(fields, "stageProjection", "tail result index entry")?,
            "tail stage projection",
        )?;
        exact_keys(
            projection,
            &[
                "schemaVersion",
                "codec",
                "semanticSha256",
                "semanticSizeBytes",
                "blobBase64",
            ],
            "tail stage projection",
        )?;
        ensure!(
            projection.get("schemaVersion").and_then(Value::as_str) == Some(PROJECTION_SCHEMA),
            "tail stage projection schema is incompatible"
        );
        ensure!(
            projection.get("codec").and_then(Value::as_str) == Some("gzip-canonical-json-v1"),
            "tail stage projection codec is incompatible"
        );
        let semantic_sha = sha_field(projection, "semanticSha256", "tail stage projection")?;
        let semantic_size = integer(projection, "semanticSizeBytes", "tail stage projection")?;
        let encoded = string(projection, "blobBase64", "tail stage projection")?;
        let compressed = base64::engine::general_purpose::STANDARD
            .decode(encoded.as_bytes())
            .context("decode tail stage projection base64")?;
        let mut decoder = GzDecoder::new(compressed.as_slice());
        let mut semantic = Vec::new();
        decoder
            .read_to_end(&mut semantic)
            .context("decompress tail stage projection")?;
        ensure!(
            semantic.len() as u64 == semantic_size,
            "tail stage projection semantic size drifted"
        );
        ensure!(
            sha256_prefixed(&semantic) == semantic_sha,
            "tail stage projection semantic identity drifted"
        );
        let record: Value = serde_json::from_slice(&semantic).context("parse tail stage record")?;
        ensure!(
            record.is_object() && canonical_json_bytes(&record)? == semantic,
            "tail stage record is not canonical JSON"
        );
        let metrics = object(
            field(fields, "rotatingEvidenceMetrics", "tail result index entry")?,
            "tail rotating metrics",
        )?;
        exact_keys(
            metrics,
            &[
                "conservativeNetR",
                "noCostNetR",
                "maxDrawdownR",
                "closedTrades",
                "observations",
                "v3Admissible",
                "resolvedProgramSha256",
                "resolvedProfileSnapshotSha256",
                "sourceProfileSnapshotSha256",
            ],
            "tail rotating metrics",
        )?;
        ensure!(
            record.get("v3Admissible") == Some(&Value::Bool(true)),
            "tail stage record is not v3-admissible"
        );
        for (metric, record_key) in [
            ("conservativeNetR", "conservativeNetR"),
            ("noCostNetR", "noCostNetR"),
            ("maxDrawdownR", "maxDrawdownR"),
            ("closedTrades", "trades"),
            ("observations", "observations"),
            ("v3Admissible", "v3Admissible"),
            ("resolvedProgramSha256", "resolvedProgramSha256"),
            (
                "resolvedProfileSnapshotSha256",
                "resolvedProfileSnapshotSha256",
            ),
            ("sourceProfileSnapshotSha256", "sourceProfileSnapshotSha256"),
        ] {
            ensure!(
                metrics.get(metric) == record.get(record_key),
                "tail stage projection drifted from rotating metrics for {metric}"
            );
        }
        record
    };
    ensure!(
        window.get("candidateId").and_then(Value::as_str) == Some(candidate_id.as_str())
            && window.get("analysisWindowStart").and_then(Value::as_str) == Some(start.as_str())
            && window.get("analysisWindowEnd").and_then(Value::as_str) == Some(end.as_str()),
        "tail stage record task binding drifted"
    );
    validate_candidate_id(&candidate_id)?;
    Ok((
        candidate_id,
        task_id,
        integer(raw_ref, "blobSizeBytes", "tail raw result reference")?,
        window,
    ))
}

fn candidate_rejection(
    candidate: &Value,
    candidate_id: &str,
    windows: &[Value],
) -> Result<Option<Value>> {
    let violations = duplicate_break_even_modules(candidate)?;
    if !violations.is_empty() {
        return Ok(Some(json!({
            "candidateId": candidate_id,
            "disposition": "rejected",
            "reasonCode": "duplicate_break_even_execution_invariant",
            "structuralProvenance": {"modules": violations},
        })));
    }
    let rejected: Vec<&Value> = windows
        .iter()
        .filter(|window| window.get("evaluationRejected") == Some(&Value::Bool(true)))
        .collect();
    if rejected.is_empty() {
        return Ok(None);
    }
    let reason = rejected[0]
        .get("rejection")
        .and_then(Value::as_object)
        .and_then(|value| value.get("reason_code"))
        .map(value_to_python_str)
        .unwrap_or_else(|| "None".to_owned());
    let rows = rejected
        .iter()
        .map(|window| {
            json!({
                "windowId": window["windowId"],
                "rejection": window["rejection"],
            })
        })
        .collect::<Vec<_>>();
    Ok(Some(json!({
        "candidateId": candidate_id,
        "disposition": "rejected",
        "reasonCode": reason,
        "windowRejections": rows,
    })))
}

fn aggregate_candidate(candidate: &Value, windows: &[Value]) -> Result<Value> {
    ensure!(
        !windows.is_empty(),
        "candidate aggregate requires at least one window"
    );
    ensure!(
        windows
            .iter()
            .all(|window| window.get("v3Admissible") == Some(&Value::Bool(true))),
        "rotating QD evidence requires terminal-adjusted v3 results"
    );
    let binding = execution_binding(candidate, windows)?;
    let mut action_counts = BTreeMap::new();
    let mut close_counts = BTreeMap::new();
    let mut state_counts = BTreeMap::new();
    let mut transition_counts = BTreeMap::new();
    let mut entry_hour_counts = BTreeMap::new();
    let mut equity_shapes = Vec::new();
    let mut holding_bars = Vec::new();
    let mut holds = Vec::new();
    let mut win_rates = Vec::new();
    for window in windows {
        for (target, key) in [
            (&mut action_counts, "actionCounts"),
            (&mut close_counts, "closeReasonCounts"),
            (&mut state_counts, "stateOccupancy"),
            (&mut transition_counts, "transitionCounts"),
            (&mut entry_hour_counts, "entryHourCounts"),
        ] {
            for (name, value) in object(field(object(window, "window")?, key, "window")?, key)? {
                let increment = value
                    .as_i64()
                    .ok_or_else(|| anyhow!("window {key}.{name} must be an integer"))?;
                let slot = target.entry(name.clone()).or_insert(0i64);
                *slot = slot
                    .checked_add(increment)
                    .ok_or_else(|| anyhow!("window {key}.{name} count overflow"))?;
            }
        }
        equity_shapes.push(equity_shape(
            window
                .get("equityCurveR")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or(&[]),
        )?);
        if let Some(value) = window
            .get("averageHoldingBars")
            .filter(|value| !value.is_null())
        {
            holds.push(f64_value(value, "averageHoldingBars")?);
        }
        if let Some(value) = window.get("winRate").filter(|value| !value.is_null()) {
            win_rates.push(f64_value(value, "winRate")?);
        }
        if let Some(values) = window.get("holdingBars").and_then(Value::as_array) {
            for value in values {
                holding_bars.push(
                    value
                        .as_i64()
                        .ok_or_else(|| anyhow!("holdingBars must contain integers"))?,
                );
            }
        }
    }
    holding_bars.sort_unstable();
    let trades = sum_i64_field(windows, "trades")?;
    let observations = sum_i64_field(windows, "observations")?;
    let terminal_evidence = windows.iter().map(|window| json!({
        "windowId": window["windowId"],
        "terminalPolicy": nested_or_null(window, &["conservativeTerminal", "terminalPolicy"]),
        "terminalPolicySchemaVersion": nested_or_null(window, &["conservativeTerminal", "terminalPolicySchemaVersion"]),
        "lastCompletedBarId": nested_or_null(window, &["conservativeTerminal", "terminalLastCompletedBarId"]),
        "lastCompletedBarStart": nested_or_null(window, &["conservativeTerminal", "terminalLastCompletedBarStart"]),
        "lastCompletedBarClose": nested_or_null(window, &["conservativeTerminal", "terminalLastCompletedBarClose"]),
        "positionStatus": nested_or_null(window, &["conservativeTerminal", "terminalPositionStatus"]),
        "pendingEffectStatus": nested_or_null(window, &["conservativeTerminal", "terminalPendingEffectStatus"]),
        "markPrice": nested_or_null(window, &["conservativeTerminal", "terminalMarkPrice"]),
        "terminalGrossR": nested_or_null(window, &["conservativeTerminal", "terminalGrossR"]),
        "terminalNetR": nested_or_null(window, &["conservativeTerminal", "terminalNetR"]),
        "terminalExitCostPercent": nested_or_null(window, &["conservativeTerminal", "terminalExitCostPercent"]),
        "terminalAdjustedMaxDrawdownR": nested_or_null(window, &["conservativeTerminal", "terminalAdjustedMaxDrawdownR"]),
        "noCostTerminalNetR": nested_or_null(window, &["noCostTerminal", "terminalNetR"]),
        "costViewTerminalDeltaR": window.get("terminalAdjustedCostViewDeltaR").cloned().unwrap_or(Value::Null),
        "evidenceEndpoints": window.get("evidenceContractEndpoints").cloned().unwrap_or(Value::Null),
    })).collect::<Vec<_>>();
    let source_profile = object(
        field(
            object(candidate, "candidate")?,
            "sourceProfile",
            "candidate",
        )?,
        "candidate sourceProfile",
    )?;
    let graph = source_profile.get("graph").and_then(Value::as_object);
    let execution_config = source_profile
        .get("executionConfig")
        .and_then(Value::as_object);
    let complexity = json!({
        "stateCount": graph.and_then(|g| g.get("states")).and_then(Value::as_array).map_or(0, Vec::len),
        "transitionCount": graph.and_then(|g| g.get("transitions")).and_then(Value::as_array).map_or(0, Vec::len),
        "indicatorCount": source_profile.get("indicators").and_then(Value::as_array).map_or(0, Vec::len),
        "managementPlanCount": execution_config.and_then(|e| e.get("managementLibrary")).and_then(Value::as_object).and_then(|m| m.get("plans")).and_then(Value::as_array).map_or(0, Vec::len),
    });
    let total_conservative = sum_field(windows, "conservativeNetR")?;
    let total_no_cost = sum_field(windows, "noCostNetR")?;
    let raw_conservative = sum_field(windows, "rawClosedConservativeNetR")?;
    let raw_no_cost = sum_field(windows, "rawClosedNoCostNetR")?;
    let terminal_conservative = sum_field(windows, "terminalAdjustedConservativeNetR")?;
    let terminal_no_cost = sum_field(windows, "terminalAdjustedNoCostNetR")?;
    let mut aggregate = json!({
        "candidateId": candidate["candidateId"], "sourceMode": candidate["sourceMode"], "seedId": candidate["seedId"],
        "sourceProfileSha256": candidate["sourceProfileSha256"],
        "authoredProgramSha256": binding["authoredProgramSha256"],
        "sourceProfileSnapshotSha256": binding["sourceProfileSnapshotSha256"],
        "resolvedProfileSnapshotSha256": binding["resolvedProfileSnapshotSha256"],
        "resolvedProgramSha256": binding["resolvedProgramSha256"], "programSha256": binding["resolvedProgramSha256"],
        "windowCount": windows.len(), "economicsBasis": "stage5e7_v3_terminal_adjusted", "v3Admissible": true,
        "tradeCountsByWindow": windows.iter().map(|row| row["trades"].clone()).collect::<Vec<_>>(),
        "totalTrades": trades, "totalObservations": observations,
        "totalConservativeNetR": total_conservative, "totalNoCostNetR": total_no_cost,
        "worstWindowConservativeNetR": min_field(windows, "conservativeNetR")?,
        "profitableWindowCount": windows.iter().filter(|row| f64_at(row, "conservativeNetR").is_ok_and(|value| value > 0.0)).count(),
        "maxWindowDrawdownR": max_field(windows, "maxDrawdownR")?,
        "costDragR": sum_difference_field(windows, "noCostNetR", "conservativeNetR")?,
        "totalRawClosedConservativeNetR": raw_conservative, "totalRawClosedNoCostNetR": raw_no_cost,
        "worstWindowRawClosedConservativeNetR": min_field(windows, "rawClosedConservativeNetR")?,
        "totalTerminalAdjustedConservativeNetR": terminal_conservative,
        "totalTerminalAdjustedNoCostNetR": terminal_no_cost,
        "worstWindowTerminalAdjustedConservativeNetR": min_field(windows, "terminalAdjustedConservativeNetR")?,
        "maxWindowRawClosedDrawdownR": max_field(windows, "rawClosedMaxDrawdownR")?,
        "totalRawClosedCostDragR": sum_difference_field(windows, "rawClosedNoCostNetR", "rawClosedConservativeNetR")?,
        "totalTerminalAdjustedCostDragR": sum_difference_field(windows, "terminalAdjustedNoCostNetR", "terminalAdjustedConservativeNetR")?,
        "entryFrequencyPerThousand": if observations != 0 { trades as f64 / observations as f64 * 1000.0 } else { 0.0 },
        "averageExposureRatio": average_field(windows, "exposureRatio")?,
        "averageHoldingBars": average(&holds), "medianHoldingBars": median_i64(&holding_bars),
        "averageWinRate": average(&win_rates), "averageTransitionEntropy": average_field(windows, "transitionEntropy")?,
        "averageMfeR": average_field(windows, "averageMfeR")?, "averageMaeR": average_field(windows, "averageMaeR")?,
        "equityShape": transpose_average(&equity_shapes),
        "entryHourDistribution": distribution(&entry_hour_counts), "actionDistribution": distribution(&action_counts),
        "closeReasonDistribution": distribution(&close_counts), "stateOccupancyDistribution": distribution(&state_counts),
        "transitionDistribution": distribution(&transition_counts), "complexity": complexity,
        "terminalEvidence": terminal_evidence, "windowRecords": windows,
    });
    let fingerprint_keys = [
        "entryFrequencyPerThousand",
        "averageExposureRatio",
        "averageHoldingBars",
        "averageWinRate",
        "averageTransitionEntropy",
        "averageMfeR",
        "averageMaeR",
        "equityShape",
        "entryHourDistribution",
        "actionDistribution",
        "closeReasonDistribution",
        "stateOccupancyDistribution",
        "complexity",
        "economicsBasis",
        "v3Admissible",
        "terminalEvidence",
    ];
    let mut fingerprint = Map::new();
    for key in fingerprint_keys {
        fingerprint.insert(key.to_owned(), aggregate[key].clone());
    }
    aggregate.as_object_mut().expect("object").insert(
        "fingerprintSha256".to_owned(),
        Value::String(canonical_sha256(&Value::Object(fingerprint))?),
    );
    Ok(aggregate)
}

fn execution_binding(candidate: &Value, windows: &[Value]) -> Result<Value> {
    let authored = sha_at(candidate, "programSha256", "candidate authored program")?;
    let source = sha_at(
        candidate,
        "profileSnapshotSha256",
        "candidate profile snapshot",
    )?;
    let mut resolved_profile: Option<String> = None;
    let mut resolved_program: Option<String> = None;
    for window in windows {
        ensure!(
            sha_at(
                window,
                "sourceProfileSnapshotSha256",
                "window source profile"
            )? == source,
            "result window source profile snapshot identity does not match candidate"
        );
        let profile = sha_at(
            window,
            "resolvedProfileSnapshotSha256",
            "window resolved profile",
        )?;
        let program = sha_at(window, "resolvedProgramSha256", "window resolved program")?;
        ensure!(
            resolved_profile.as_ref().is_none_or(|old| old == &profile),
            "resolved profile identity changed across windows"
        );
        ensure!(
            resolved_program.as_ref().is_none_or(|old| old == &program),
            "resolved program identity changed across windows"
        );
        resolved_profile = Some(profile);
        resolved_program = Some(program);
    }
    Ok(json!({
        "authoredProgramSha256": authored, "sourceProfileSnapshotSha256": source,
        "resolvedProfileSnapshotSha256": resolved_profile.ok_or_else(|| anyhow!("candidate lacks windows"))?,
        "resolvedProgramSha256": resolved_program.ok_or_else(|| anyhow!("candidate lacks windows"))?,
    }))
}

fn behavior_descriptor(candidate: &Value, aggregate: &Value) -> Result<Value> {
    let structure = graph_structure(candidate)?;
    let trades = i64_at(aggregate, "totalTrades")?;
    let operator = bucket(
        f64_at(&structure, "operatorFamilyCount")?,
        &[1.0, 2.0, 3.0, f64::INFINITY],
        &["none", "one", "two", "three_plus"],
    );
    let mutation = bucket(
        f64_at(&structure, "mutationDepth")?,
        &[1.0, 2.0, 3.0, f64::INFINITY],
        &["root", "one", "two", "three_plus"],
    );
    let entry = bucket(
        f64_at(&structure, "entryEventCount")?,
        &[1.0, 2.0, f64::INFINITY],
        &["none", "one", "two_plus"],
    );
    let management = bucket(
        f64_at(&structure, "managementActionCount")?,
        &[1.0, 2.0, 3.0, f64::INFINITY],
        &["none", "one", "two", "three_plus"],
    );
    let nodes = bucket(
        f64_at(&structure, "graphNodeCount")?,
        &[9.0, 13.0, 19.0, f64::INFINITY],
        &["small", "medium", "large", "very_large"],
    );
    let frequency = if trades == 0 {
        "dormant"
    } else {
        bucket(
            f64_at(aggregate, "entryFrequencyPerThousand")?,
            &[1.0, 4.0, 12.0, f64::INFINITY],
            &["very_sparse", "sparse", "moderate", "active"],
        )
    };
    let holding = if trades == 0 {
        "none"
    } else {
        bucket(
            f64_at(aggregate, "medianHoldingBars")?,
            &[24.0, 96.0, 384.0, 1536.0, f64::INFINITY],
            &["short", "medium", "long", "very_long", "extreme"],
        )
    };
    let cell = [
        operator, mutation, entry, management, nodes, frequency, holding,
    ]
    .join("|");
    Ok(json!({
        "operatorFamilies": operator, "mutationDepth": mutation, "entryEvents": entry,
        "managementActions": management, "graphNodes": nodes, "tradeFrequency": frequency,
        "medianHolding": holding, "structuralMeasurements": structure, "cellId": cell,
    }))
}

fn graph_structure(candidate: &Value) -> Result<Value> {
    let profile = candidate
        .get("sourceProfile")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("candidate sourceProfile must be an object"))?;
    let graph = profile.get("graph").and_then(Value::as_object);
    let transitions = graph
        .and_then(|value| value.get("transitions"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let history = candidate
        .get("structuralOperatorHistory")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let mut operators = BTreeSet::new();
    for row in history {
        if let Some(operator) = row
            .get("operatorId")
            .filter(|value| python_truthy(value))
            .map(value_to_python_str)
        {
            operators.insert(operator);
        }
    }
    let mut entry_events = BTreeSet::new();
    let mut management_count = 0usize;
    let mut guard_count = 0usize;
    for transition in transitions {
        let actions = transition
            .get("actions")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let is_entry = actions
            .iter()
            .any(|action| action.get("kind").and_then(Value::as_str) == Some("enter_next_open"));
        management_count += actions
            .iter()
            .filter(|action| {
                matches!(
                    action.get("kind").and_then(Value::as_str),
                    Some(
                        "exit_next_open"
                            | "move_stop_to_break_even_next_open"
                            | "move_stop_next_open"
                    )
                )
            })
            .count();
        walk_guards(transition.get("guard"), &mut |guard| {
            guard_count += 1;
            if is_entry
                && matches!(
                    guard.get("kind").and_then(Value::as_str),
                    Some("fresh_event" | "event_age_at_most" | "event_age_window")
                )
            {
                if let Some(event) = guard
                    .get("eventId")
                    .filter(|value| python_truthy(value))
                    .map(value_to_python_str)
                {
                    entry_events.insert(event);
                }
            }
        });
    }
    let state_count = graph
        .and_then(|value| value.get("states"))
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let transition_count = transitions.len();
    let indicator_count = profile
        .get("indicators")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    Ok(json!({
        "operatorFamilyIds": operators.into_iter().collect::<Vec<_>>(), "operatorFamilyCount": history_operator_count(history),
        "mutationDepth": history.len(), "entryEventCount": entry_events.len(), "managementActionCount": management_count,
        "stateCount": state_count, "transitionCount": transition_count, "graphNodeCount": state_count + transition_count,
        "guardCount": guard_count, "indicatorCount": indicator_count,
        "structuralComplexity": state_count as f64 + transition_count as f64 + 0.25 * guard_count as f64 + 0.5 * indicator_count as f64,
    }))
}

fn history_operator_count(history: &[Value]) -> usize {
    history
        .iter()
        .filter_map(|row| row.get("operatorId"))
        .filter(|value| python_truthy(value))
        .map(value_to_python_str)
        .collect::<BTreeSet<_>>()
        .len()
}

fn walk_guards<F: FnMut(&Map<String, Value>)>(value: Option<&Value>, visit: &mut F) {
    let Some(map) = value.and_then(Value::as_object) else {
        return;
    };
    visit(map);
    walk_guards(map.get("guard"), visit);
    if let Some(guards) = map.get("guards").and_then(Value::as_array) {
        for guard in guards {
            walk_guards(Some(guard), visit);
        }
    }
}

fn duplicate_break_even_modules(candidate: &Value) -> Result<Vec<Value>> {
    let profile = candidate
        .get("sourceProfile")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("candidate sourceProfile must be an object"))?;
    let graph = profile.get("graph").and_then(Value::as_object);
    let transitions = graph
        .and_then(|value| value.get("transitions"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let mut owners: Vec<(String, BTreeSet<String>)> = Vec::new();
    if let Some(modules) = graph
        .and_then(|value| value.get("entryArbitration"))
        .and_then(Value::as_object)
        .and_then(|value| value.get("modules"))
        .and_then(Value::as_array)
    {
        for module in modules {
            let Some(direction @ ("long" | "short")) =
                module.get("direction").and_then(Value::as_str)
            else {
                continue;
            };
            let Some(states) = module.get("stateIds").and_then(Value::as_array) else {
                continue;
            };
            owners.push((
                direction.to_owned(),
                states.iter().map(value_to_python_str).collect(),
            ));
        }
    }
    if owners.is_empty() {
        if let Some(direction @ ("long" | "short")) =
            profile.get("directionMode").and_then(Value::as_str)
        {
            owners.push((direction.to_owned(), BTreeSet::new()));
        }
    }
    let mut violations = Vec::new();
    for (direction, states) in owners {
        let count = transitions
            .iter()
            .filter(|transition| {
                states.is_empty()
                    || states.contains(&value_to_python_str(
                        transition.get("sourceStateId").unwrap_or(&Value::Null),
                    ))
            })
            .flat_map(|transition| {
                transition
                    .get("actions")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .filter(|action| {
                action.get("kind").and_then(Value::as_str)
                    == Some("move_stop_to_break_even_next_open")
            })
            .count();
        if count > 1 {
            violations.push(json!({"direction": direction, "breakEvenActionCount": count}));
        }
    }
    Ok(violations)
}

fn objective_row(candidate: &Value, aggregate: &Value) -> Result<Value> {
    let structure = graph_structure(candidate)?;
    Ok(json!({
        "worstWindowConservativeNetR": finite_or_neutral(aggregate.get("worstWindowConservativeNetR")),
        "maximumDrawdownR": finite_or_neutral(aggregate.get("maxWindowDrawdownR")).max(0.0),
        "structuralComplexity": f64_at(&structure, "structuralComplexity")?,
    }))
}

fn finite_data_validity(aggregate: &Value, manifest: &TailReductionManifest) -> Result<Value> {
    let counts = aggregate
        .get("tradeCountsByWindow")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("aggregate tradeCountsByWindow must be an array"))?
        .iter()
        .map(|value| {
            value
                .as_i64()
                .ok_or_else(|| anyhow!("trade count must be an integer"))
        })
        .collect::<Result<Vec<_>>>()?;
    let total = i64_at(aggregate, "totalTrades")?;
    let observations = i64_at(aggregate, "totalObservations")?;
    let finite = ["worstWindowConservativeNetR", "maxWindowDrawdownR"]
        .iter()
        .all(|key| {
            aggregate
                .get(*key)
                .and_then(Value::as_f64)
                .is_some_and(f64::is_finite)
        });
    let min_total = total >= manifest.minimum_total_trades;
    let min_windows = !counts.is_empty()
        && counts
            .iter()
            .all(|value| *value >= manifest.minimum_trades_per_window);
    let positive = observations > 0;
    let support = min_total && min_windows && positive;
    Ok(json!({
        "minimumTotalTrades": manifest.minimum_total_trades, "minimumTradesPerWindow": manifest.minimum_trades_per_window,
        "capTrades": manifest.cap_trades, "tradeCountsByWindow": counts, "totalTrades": total,
        "checks": {"minimumTotalTrades": min_total, "minimumTradesEveryWindow": min_windows, "positiveObservationSupport": positive, "finiteEconomicMetrics": finite},
        "isFiniteData": finite, "passesSupportGate": support, "validForQuality": finite && support,
    }))
}

fn provisional_reduce(
    rows: Vec<ProvisionalBase>,
    counts: &BTreeMap<String, usize>,
    limit: usize,
) -> Result<Vec<Value>> {
    let mut groups: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for row in rows {
        let count = *counts
            .get(&row.cell_id)
            .ok_or_else(|| anyhow!("provisional cell count is missing"))?;
        let value = json!({
            "candidateId": row.candidate_id, "candidateIdentitySha256": row.candidate_identity_sha256,
            "programSha256": row.program_sha256, "profileSnapshotSha256": row.profile_snapshot_sha256,
            "cellId": row.cell_id, "costView": "research_conservative", "currentPanelRank": row.rank,
            "novelty": 1.0 / count as f64,
        });
        groups
            .entry(value["cellId"].as_str().expect("string").to_owned())
            .or_default()
            .push(value);
    }
    for rows in groups.values_mut() {
        rows.sort_by(|left, right| {
            let lrank = left["currentPanelRank"].as_f64().expect("number");
            let rrank = right["currentPanelRank"].as_f64().expect("number");
            rrank
                .partial_cmp(&lrank)
                .expect("validated finite rank")
                .then_with(|| {
                    left["candidateId"]
                        .as_str()
                        .cmp(&right["candidateId"].as_str())
                })
        });
    }
    let mut selected = Vec::new();
    while selected.len() < limit {
        let mut added = false;
        for rows in groups.values_mut() {
            if !rows.is_empty() && selected.len() < limit {
                selected.push(rows.remove(0));
                added = true;
            }
        }
        if !added {
            break;
        }
    }
    Ok(selected)
}

fn build_result(manifest: &TailReductionManifest, state: &ReductionState) -> Value {
    let mut result = json!({
        "schemaVersion": RESULT_SCHEMA, "contractVersion": CONTRACT_VERSION, "operation": OPERATION,
        "status": "completed", "manifestSha256": manifest.manifest_sha256,
        "generationIndex": manifest.generation_index,
        "evaluationPopulationSha256": state.evaluation_population_sha256,
        "populationSha256": state.population_sha256, "tailResultIndexSha256": state.tail_result_index_sha256,
        "taskMatrixSha256": state.task_matrix_sha256, "resultSetSha256": state.result_set_sha256,
        "policy": {"minimumTotalTrades": manifest.minimum_total_trades, "minimumTradesPerWindow": manifest.minimum_trades_per_window, "capTrades": manifest.cap_trades},
        "evaluatedMembers": {
            "schemaVersion": EVALUATED_SCHEMA, "memberCount": state.member_count,
            "evaluationRejectionCount": state.rejected.len(), "evaluationRejectedCandidates": state.rejected,
            "membersFile": {"path": MEMBERS_PATH, "rawSha256": state.members_sha256, "sizeBytes": state.members_bytes, "recordCount": state.member_count},
        },
        "provisional": {"schemaVersion": PROVISIONAL_SCHEMA, "limit": manifest.provisional_limit, "candidateCount": state.provisional.len(), "candidates": state.provisional},
    });
    if let Some(runtime_authority_sha256) = &manifest.runtime_authority_sha256 {
        result
            .as_object_mut()
            .expect("tail reduction result object")
            .insert(
                "runtimeAuthoritySha256".into(),
                json!(runtime_authority_sha256),
            );
    }
    let identity = canonical_sha256(&result).expect("finite reducer result");
    result
        .as_object_mut()
        .expect("object")
        .insert("resultSha256".to_owned(), Value::String(identity));
    result
}

fn reopen_result(
    result_path: &Path,
    members_path: &Path,
    manifest: &TailReductionManifest,
) -> Result<Value> {
    let raw = fs::read(result_path).with_context(|| {
        format!(
            "read existing tail reduction result: {}",
            result_path.display()
        )
    })?;
    let value: Value =
        serde_json::from_slice(&raw).context("parse existing tail reduction result")?;
    ensure!(
        canonical_json_line(&value)? == raw,
        "existing tail reduction result is not canonical JSON"
    );
    validate_result_value(&value, manifest)?;
    let members = object(
        field(
            object(&value, "tail reduction result")?,
            "evaluatedMembers",
            "tail reduction result",
        )?,
        "evaluated members descriptor",
    )?;
    let file = object(
        field(members, "membersFile", "evaluated members descriptor")?,
        "members file descriptor",
    )?;
    ensure!(
        string(file, "path", "members file descriptor")? == MEMBERS_PATH,
        "members path drifted"
    );
    let expected_hash = sha_field(file, "rawSha256", "members file descriptor")?;
    let expected_size = integer(file, "sizeBytes", "members file descriptor")?;
    let expected_records = integer(file, "recordCount", "members file descriptor")?;
    let (actual_hash, actual_size, actual_records) = validate_members_file(members_path)?;
    ensure!(
        actual_hash == expected_hash
            && actual_size == expected_size
            && actual_records == expected_records,
        "durable evaluated member file is corrupt, truncated, or replaced"
    );
    Ok(value)
}

fn validate_result_value(value: &Value, manifest: &TailReductionManifest) -> Result<()> {
    let fields = object(value, "tail reduction result")?;
    let mut result_keys = vec![
        "schemaVersion",
        "contractVersion",
        "operation",
        "status",
        "manifestSha256",
        "runtimeAuthoritySha256",
        "generationIndex",
        "evaluationPopulationSha256",
        "populationSha256",
        "tailResultIndexSha256",
        "taskMatrixSha256",
        "resultSetSha256",
        "policy",
        "evaluatedMembers",
        "provisional",
        "resultSha256",
    ];
    if manifest.runtime_authority_sha256.is_none() {
        result_keys.retain(|key| *key != "runtimeAuthoritySha256");
    }
    exact_keys(fields, &result_keys, "tail reduction result")?;
    ensure!(
        fields.get("schemaVersion").and_then(Value::as_str) == Some(RESULT_SCHEMA)
            && fields.get("contractVersion").and_then(Value::as_str) == Some(CONTRACT_VERSION)
            && fields.get("operation").and_then(Value::as_str) == Some(OPERATION)
            && fields.get("status").and_then(Value::as_str) == Some("completed"),
        "tail reduction result is incompatible"
    );
    ensure!(
        fields.get("manifestSha256").and_then(Value::as_str)
            == Some(manifest.manifest_sha256.as_str())
            && fields
                .get("evaluationPopulationSha256")
                .and_then(Value::as_str)
                == Some(manifest.evaluation_population_sha256.as_str())
            && fields.get("tailResultIndexSha256").and_then(Value::as_str)
                == Some(manifest.tail_result_index_sha256.as_str())
            && fields.get("generationIndex").and_then(Value::as_u64)
                == Some(manifest.generation_index),
        "tail reduction result input binding drifted"
    );
    if let Some(runtime_authority_sha256) = &manifest.runtime_authority_sha256 {
        ensure!(
            fields.get("runtimeAuthoritySha256").and_then(Value::as_str)
                == Some(runtime_authority_sha256.as_str()),
            "tail reduction runtime authority binding drifted"
        );
    }
    for field_name in [
        "manifestSha256",
        "evaluationPopulationSha256",
        "populationSha256",
        "tailResultIndexSha256",
        "taskMatrixSha256",
        "resultSetSha256",
    ] {
        sha_field(fields, field_name, "tail reduction result")?;
    }
    let policy = object(
        field(fields, "policy", "tail reduction result")?,
        "tail reduction policy",
    )?;
    exact_keys(
        policy,
        &["minimumTotalTrades", "minimumTradesPerWindow", "capTrades"],
        "tail reduction policy",
    )?;
    ensure!(
        policy.get("minimumTotalTrades").and_then(Value::as_i64)
            == Some(manifest.minimum_total_trades)
            && policy.get("minimumTradesPerWindow").and_then(Value::as_i64)
                == Some(manifest.minimum_trades_per_window)
            && policy.get("capTrades").and_then(Value::as_i64) == Some(manifest.cap_trades),
        "tail reduction result policy binding drifted"
    );
    let evaluated = object(
        field(fields, "evaluatedMembers", "tail reduction result")?,
        "evaluated members descriptor",
    )?;
    exact_keys(
        evaluated,
        &[
            "schemaVersion",
            "memberCount",
            "evaluationRejectionCount",
            "evaluationRejectedCandidates",
            "membersFile",
        ],
        "evaluated members descriptor",
    )?;
    ensure!(
        evaluated.get("schemaVersion").and_then(Value::as_str) == Some(EVALUATED_SCHEMA)
            && evaluated
                .get("evaluationRejectedCandidates")
                .and_then(Value::as_array)
                .zip(
                    evaluated
                        .get("evaluationRejectionCount")
                        .and_then(Value::as_u64)
                )
                .is_some_and(|(rows, count)| rows.len() as u64 == count),
        "evaluated members result accounting drifted"
    );
    let provisional = object(
        field(fields, "provisional", "tail reduction result")?,
        "provisional result",
    )?;
    exact_keys(
        provisional,
        &["schemaVersion", "limit", "candidateCount", "candidates"],
        "provisional result",
    )?;
    ensure!(
        provisional.get("schemaVersion").and_then(Value::as_str) == Some(PROVISIONAL_SCHEMA)
            && provisional.get("limit").and_then(Value::as_u64)
                == Some(manifest.provisional_limit as u64)
            && provisional
                .get("candidates")
                .and_then(Value::as_array)
                .zip(provisional.get("candidateCount").and_then(Value::as_u64))
                .is_some_and(|(rows, count)| rows.len() as u64 == count),
        "provisional result accounting drifted"
    );
    let supplied = sha_field(fields, "resultSha256", "tail reduction result")?;
    ensure!(
        canonical_sha256_without_object_field(value, "resultSha256")? == supplied,
        "tail reduction result identity mismatch"
    );
    Ok(())
}

fn validate_members_file(path: &Path) -> Result<(String, u64, u64)> {
    let path = existing_regular_file(path, "evaluated members file")?;
    let mut reader = BufReader::new(File::open(path)?);
    let mut digest = Sha256::new();
    let mut bytes = 0u64;
    let mut records = 0u64;
    let mut pending = Vec::new();
    let mut buffer = [0u8; 1024 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        bytes += read as u64;
        pending.extend_from_slice(&buffer[..read]);
        while let Some(position) = pending.iter().position(|byte| *byte == b'\n') {
            let line: Vec<u8> = pending.drain(..=position).collect();
            let value: Value = serde_json::from_slice(&line[..line.len() - 1])
                .context("parse evaluated member JSONL record")?;
            ensure!(
                canonical_json_line(&value)? == line,
                "evaluated member JSONL record is not canonical"
            );
            records += 1;
        }
    }
    ensure!(pending.is_empty(), "evaluated member JSONL is truncated");
    Ok((digest_prefixed(digest.finalize()), bytes, records))
}

fn validate_candidate(candidate: &Value) -> Result<()> {
    let fields = object(candidate, "evaluation candidate")?;
    let id = string(fields, "candidateId", "evaluation candidate")?;
    validate_candidate_id(&id)?;
    for field_name in [
        "candidateIdentitySha256",
        "programSha256",
        "sourceProfileSha256",
        "profileSnapshotSha256",
    ] {
        sha_field(fields, field_name, "evaluation candidate")?;
    }
    ensure!(
        !string(fields, "sourceMode", "evaluation candidate")?
            .trim()
            .is_empty()
            && !string(fields, "seedId", "evaluation candidate")?
                .trim()
                .is_empty(),
        "evaluation candidate source identity is invalid"
    );
    let profile = field(fields, "sourceProfile", "evaluation candidate")?;
    object(profile, "evaluation candidate sourceProfile")?;
    ensure!(
        canonical_sha256(profile)?
            == fields
                .get("sourceProfileSha256")
                .and_then(Value::as_str)
                .unwrap_or(""),
        "evaluation candidate source profile identity mismatch"
    );
    Ok(())
}

fn validate_candidate_id(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 240
            && value.as_bytes()[0].is_ascii_lowercase()
            && value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
        "candidate identity is invalid"
    );
    Ok(())
}

fn equity_shape(curve: &[Value]) -> Result<Vec<f64>> {
    const POINTS: usize = 12;
    if curve.is_empty() {
        return Ok(vec![0.0; POINTS]);
    }
    let values = curve
        .iter()
        .map(|value| f64_value(value, "equityCurveR"))
        .collect::<Result<Vec<_>>>()?;
    let scale = values
        .iter()
        .fold(1.0f64, |acc, value| acc.max(value.abs()));
    if values.len() == 1 {
        return Ok(vec![values[0] / scale; POINTS]);
    }
    let mut output = Vec::with_capacity(POINTS);
    for index in 0..POINTS {
        let position = index as f64 * (values.len() - 1) as f64 / (POINTS - 1) as f64;
        let left = position.floor() as usize;
        let right = (left + 1).min(values.len() - 1);
        let fraction = position - left as f64;
        output.push((values[left] * (1.0 - fraction) + values[right] * fraction) / scale);
    }
    Ok(output)
}

fn distribution(counts: &BTreeMap<String, i64>) -> Value {
    let total: f64 = counts.values().map(|value| (*value).max(0) as f64).sum();
    if total <= 0.0 {
        return json!({});
    }
    Value::Object(
        counts
            .iter()
            .map(|(key, value)| (key.clone(), number_value((*value).max(0) as f64 / total)))
            .collect(),
    )
}

fn average(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}
fn median_i64(values: &[i64]) -> f64 {
    if values.is_empty() {
        0.0
    } else if values.len() % 2 == 1 {
        values[values.len() / 2] as f64
    } else {
        (values[values.len() / 2 - 1] as f64 + values[values.len() / 2] as f64) / 2.0
    }
}
fn transpose_average(values: &[Vec<f64>]) -> Vec<f64> {
    (0..values[0].len())
        .map(|index| values.iter().map(|row| row[index]).sum::<f64>() / values.len() as f64)
        .collect()
}
fn sum_field(rows: &[Value], key: &str) -> Result<f64> {
    rows.iter().map(|row| f64_at(row, key)).sum()
}
fn sum_i64_field(rows: &[Value], key: &str) -> Result<i64> {
    rows.iter().try_fold(0i64, |total, row| {
        total
            .checked_add(i64_at(row, key)?)
            .ok_or_else(|| anyhow!("{key} total overflow"))
    })
}
fn sum_difference_field(rows: &[Value], left: &str, right: &str) -> Result<f64> {
    rows.iter()
        .map(|row| Ok(f64_at(row, left)? - f64_at(row, right)?))
        .sum()
}
fn average_field(rows: &[Value], key: &str) -> Result<f64> {
    Ok(sum_field(rows, key)? / rows.len() as f64)
}
fn min_field(rows: &[Value], key: &str) -> Result<f64> {
    let mut values = rows.iter().map(|row| f64_at(row, key));
    let mut selected = values
        .next()
        .ok_or_else(|| anyhow!("minimum requires at least one row"))??;
    for value in values {
        let value = value?;
        if value < selected {
            selected = value;
        }
    }
    Ok(selected)
}
fn max_field(rows: &[Value], key: &str) -> Result<f64> {
    let mut values = rows.iter().map(|row| f64_at(row, key));
    let mut selected = values
        .next()
        .ok_or_else(|| anyhow!("maximum requires at least one row"))??;
    for value in values {
        let value = value?;
        if value > selected {
            selected = value;
        }
    }
    Ok(selected)
}
fn finite_or_neutral(value: Option<&Value>) -> f64 {
    value
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite())
        .unwrap_or(0.0)
}
fn bucket<'a>(value: f64, bounds: &[f64], labels: &[&'a str]) -> &'a str {
    bounds
        .iter()
        .zip(labels)
        .find(|(bound, _)| value < **bound)
        .map_or_else(|| labels[labels.len() - 1], |(_, label)| *label)
}

fn nested_or_null(value: &Value, keys: &[&str]) -> Value {
    let mut current = value;
    for key in keys {
        let Some(next) = current.as_object().and_then(|map| map.get(*key)) else {
            return Value::Null;
        };
        current = next;
    }
    if current.is_null() {
        Value::Null
    } else {
        current.clone()
    }
}

fn python_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(v) => v.as_f64() != Some(0.0),
        Value::String(v) => !v.is_empty(),
        Value::Array(v) => !v.is_empty(),
        Value::Object(v) => !v.is_empty(),
    }
}
fn value_to_python_str(value: &Value) -> String {
    match value {
        Value::Null => "None".to_owned(),
        Value::Bool(true) => "True".to_owned(),
        Value::Bool(false) => "False".to_owned(),
        Value::String(value) => value.clone(),
        _ => value.to_string(),
    }
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
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("{label} {key} must be a nonempty string"))
}
fn integer(map: &Map<String, Value>, key: &str, label: &str) -> Result<u64> {
    field(map, key, label)?
        .as_u64()
        .ok_or_else(|| anyhow!("{label} {key} must be a nonnegative integer"))
}
fn nonnegative_i64(map: &Map<String, Value>, key: &str, label: &str) -> Result<i64> {
    field(map, key, label)?
        .as_i64()
        .filter(|value| *value >= 0)
        .ok_or_else(|| anyhow!("{label} {key} must be a nonnegative integer"))
}
fn sha_field(map: &Map<String, Value>, key: &str, label: &str) -> Result<String> {
    let value = string(map, key, label)?;
    require_sha(&value, &format!("{label} {key}"))?;
    Ok(value)
}
fn sha_at(value: &Value, key: &str, label: &str) -> Result<String> {
    let value = value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("{label} must be a SHA-256 string"))?
        .to_owned();
    require_sha(&value, label)?;
    Ok(value)
}
fn require_sha(value: &str, label: &str) -> Result<()> {
    ensure!(
        value.len() == 71
            && value.starts_with("sha256:")
            && value.as_bytes()[7..]
                .iter()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
        "{label} must be a lowercase SHA-256 identity"
    );
    Ok(())
}
fn exact_keys(map: &Map<String, Value>, keys: &[&str], label: &str) -> Result<()> {
    ensure!(
        map.len() == keys.len() && keys.iter().all(|key| map.contains_key(*key)),
        "{label} fields are not exact"
    );
    Ok(())
}
fn f64_value(value: &Value, label: &str) -> Result<f64> {
    let value = value
        .as_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| anyhow!("{label} must be finite numeric"))?;
    Ok(value)
}
fn f64_at(value: &Value, key: &str) -> Result<f64> {
    f64_value(
        value.get(key).ok_or_else(|| anyhow!("value lacks {key}"))?,
        key,
    )
}
fn i64_at(value: &Value, key: &str) -> Result<i64> {
    value
        .get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow!("{key} must be an integer"))
}
fn text_at<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or("")
}
fn number_value(value: f64) -> Value {
    Value::Number(Number::from_f64(value).expect("finite number"))
}

fn existing_regular_file(path: &Path, label: &str) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("{label} is unavailable: {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
        "{label} must be a real regular file"
    );
    path.canonicalize()
        .with_context(|| format!("resolve {label}: {}", path.display()))
}

fn ensure_real_directory(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("{label} is unavailable: {}", path.display()))?;
    ensure!(
        metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
        "{label} must be a real directory"
    );
    Ok(())
}

fn temporary_path(parent: &Path, base: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    parent.join(format!(".{base}.{}.{}.tmp", std::process::id(), nanos))
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn publish_once(
    temporary: &Path,
    destination: &Path,
    expected_raw_sha: Option<&str>,
) -> Result<()> {
    match fs::hard_link(temporary, destination) {
        Ok(()) => {
            fs::remove_file(temporary)?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if let Some(expected) = expected_raw_sha {
                let (actual, _, _) = validate_members_file(destination)?;
                ensure!(
                    actual == expected,
                    "refusing divergent existing immutable artifact: {}",
                    destination.display()
                );
            } else {
                let existing = fs::read(destination)?;
                let pending = fs::read(temporary)?;
                ensure!(
                    existing == pending,
                    "refusing divergent existing immutable artifact: {}",
                    destination.display()
                );
            }
            fs::remove_file(temporary)?;
            Ok(())
        }
        Err(error) => Err(error)
            .with_context(|| format!("publish immutable artifact: {}", destination.display())),
    }
}

#[cfg(not(windows))]
fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(windows)]
fn sync_directory(path: &Path) -> Result<()> {
    use std::os::windows::fs::OpenOptionsExt;

    const GENERIC_READ: u32 = 0x8000_0000;
    const GENERIC_WRITE: u32 = 0x4000_0000;
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_SHARE_DELETE: u32 = 0x0000_0004;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;

    let mut options = OpenOptions::new();
    options
        .access_mode(GENERIC_READ | GENERIC_WRITE)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
    let directory = match options.open(path) {
        Ok(directory) => directory,
        Err(error) if matches!(error.raw_os_error(), Some(1 | 5 | 50 | 87)) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    match directory.sync_all() {
        Err(error) if matches!(error.raw_os_error(), Some(1 | 5 | 50 | 87)) => Ok(()),
        outcome => outcome.map_err(Into::into),
    }
}
fn digest_prefixed(digest: impl AsRef<[u8]>) -> String {
    let mut output = String::from("sha256:");
    for byte in digest.as_ref() {
        use std::fmt::Write as _;
        write!(output, "{byte:02x}").expect("String write");
    }
    output
}

struct RawHashWriter<W> {
    inner: W,
    digest: Sha256,
    bytes: u64,
}
impl<W> RawHashWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
            bytes: 0,
        }
    }
    fn finish(self) -> (String, u64) {
        (digest_prefixed(self.digest.finalize()), self.bytes)
    }
}
impl<W: Write> Write for RawHashWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buffer)?;
        self.digest.update(&buffer[..written]);
        self.bytes += written as u64;
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
