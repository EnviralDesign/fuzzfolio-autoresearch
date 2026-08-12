//! Streaming, restart-safe archive reduction for the v5 evidence ladder.
//!
//! This boundary deliberately consumes the authenticated output of
//! `temporal-qd-tail-reducer`; it neither reads raw tasks/results nor invokes
//! Python.  The member spool holds opaque canonical JSON records on disk while
//! the reducer retains only ordering keys in memory.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail, ensure};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use temporal_qd_contract::{
    CONTRACT_VERSION, canonical_json_line, canonical_sha256, canonical_sha256_without_object_field,
    sha256_prefixed,
};

pub const MANIFEST_SCHEMA: &str = "temporal_qd_native_archive_reduction_manifest_v2";
pub const RESULT_SCHEMA: &str = "temporal_qd_native_archive_reduction_result_v1";
pub const ARCHIVE_TRANSPORT_DESCRIPTOR_SCHEMA: &str = "temporal_qd_archive_transport_descriptor_v1";
pub const ARCHIVE_SCHEMA: &str = "temporal_qd_archive_v3";
pub const OPERATION: &str = "reduce_evidence_ladder_archive";
pub const ARCHIVE_PATH: &str = "archive.json";
pub const RESULT_PATH: &str = "archive-reduction-result.json";

const TAIL_RESULT_SCHEMA: &str = "temporal_qd_native_tail_reduction_result_v1";
const EVALUATED_SCHEMA: &str = "temporal_qd_evaluated_members_v1";
const TAIL_AUTHORITY_SCHEMA: &str = "temporal_qd_tail_authority_receipt_v1";

#[derive(Clone)]
struct Manifest {
    tail_authority_path: PathBuf,
    tail_authority_sha256: String,
    cell_capacity: usize,
    policy_name: String,
    policy_sha256: String,
    frozen_policy: Value,
    direction_aware: bool,
    previous_archive_path: Option<PathBuf>,
    generation_proposal_accounting: Value,
    bidirectional_pair_policy: Option<Value>,
    manifest_sha256: String,
}

struct TailInput {
    runtime_authority_sha256: String,
    members_path: PathBuf,
    members_sha256: String,
    population_sha256: String,
    result_set_sha256: String,
    generation_index: u64,
    result: Value,
}

#[derive(Clone, Debug)]
struct MemberRef {
    candidate_id: String,
    resolved_program: String,
    authored_program: String,
    cell_id: String,
    worst: f64,
    drawdown: f64,
    complexity: f64,
    support: f64,
    finite: bool,
    support_gate: bool,
    valid_quality: bool,
    direction_lane: Option<String>,
    offset: u64,
    length: u64,
}

type SelectedMember = (MemberRef, String, Option<usize>, Option<f64>, String);

/// Certify an already-committed archive for transport into a later native
/// finalizer boundary.  This is intentionally read-only: callers receive an
/// immutable descriptor and never need to parse or hash archive bytes in
/// Python just to freeze an input binding.
pub fn certify_archive_transport(path: &Path) -> Result<Value> {
    ensure!(
        !path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir)),
        "archive transport path must not traverse parent directories"
    );
    let canonical_path = regular_file(path, "archive transport source")?;
    ensure!(
        canonical_path.is_absolute(),
        "archive transport path must resolve to an absolute path"
    );
    let raw = fs::read(&canonical_path)?;
    let archive: Value = serde_json::from_slice(&raw).context("parse archive transport source")?;
    ensure!(
        canonical_json_line(&archive)? == raw,
        "archive transport source is not canonical JSON"
    );
    let map = obj(&archive, "archive transport source")?;
    ensure!(
        text(map, "schemaVersion")? == ARCHIVE_SCHEMA,
        "archive transport source schema is incompatible"
    );
    let archive_sha256 = sha(map, "archiveSha256")?;
    ensure!(
        archive_sha256 == canonical_sha256_without_object_field(&archive, "archiveSha256")?,
        "archive transport source identity mismatch"
    );
    let mut descriptor = json!({
        "schemaVersion": ARCHIVE_TRANSPORT_DESCRIPTOR_SCHEMA,
        "absolutePath": canonical_path.to_string_lossy(),
        "documentSchemaVersion": ARCHIVE_SCHEMA,
        "archiveSha256": archive_sha256,
        "fileSha256": sha256_prefixed(&raw),
        "sizeBytes": raw.len(),
    });
    let descriptor_sha256 = canonical_sha256(&descriptor)?;
    descriptor
        .as_object_mut()
        .expect("archive transport descriptor object")
        .insert(
            "descriptorSha256".to_owned(),
            Value::String(descriptor_sha256),
        );
    Ok(descriptor)
}

impl MemberRef {
    fn quality(&self) -> bool {
        self.finite && self.support_gate && self.valid_quality && self.worst >= 0.0
    }
    fn negative(&self) -> bool {
        self.finite && self.support_gate && self.valid_quality && self.worst < 0.0
    }
    fn representative_cmp(&self, other: &Self) -> Ordering {
        // Python's `min` representative key: valid quality desc, worst desc,
        // support desc, drawdown asc, complexity asc, candidate id asc.
        other
            .valid_quality
            .cmp(&self.valid_quality)
            .then_with(|| desc_f64(self.worst, other.worst))
            .then_with(|| desc_f64(self.support, other.support))
            .then_with(|| asc_f64(self.drawdown, other.drawdown))
            .then_with(|| asc_f64(self.complexity, other.complexity))
            .then_with(|| self.candidate_id.cmp(&other.candidate_id))
    }
    fn observation_cmp(&self, other: &Self) -> Ordering {
        other
            .finite
            .cmp(&self.finite)
            .then_with(|| other.support_gate.cmp(&self.support_gate))
            .then_with(|| desc_f64(self.worst, other.worst))
            .then_with(|| desc_f64(self.support, other.support))
            .then_with(|| asc_f64(self.complexity, other.complexity))
            .then_with(|| self.candidate_id.cmp(&other.candidate_id))
    }
}

pub fn execute_manifest(manifest_path: &Path) -> Result<Value> {
    let manifest_path = regular_file(manifest_path, "archive reduction manifest")?;
    let output_dir = manifest_path
        .parent()
        .context("archive manifest has no parent")?;
    let manifest_raw = fs::read(&manifest_path)?;
    let manifest = parse_manifest(&manifest_raw)?;
    let result_path = output_dir.join(RESULT_PATH);
    let archive_path = output_dir.join(ARCHIVE_PATH);
    if result_path.exists() {
        return reopen(&result_path, &archive_path, &manifest);
    }

    let tail = load_tail_authority(&manifest)?;
    let previous = load_previous(manifest.previous_archive_path.as_deref(), &manifest)?;
    let spool_path = temporary_path(output_dir, "archive-member-spool.jsonl");
    let run = reduce(&manifest, &tail, previous.as_ref(), &spool_path);
    let _ = fs::remove_file(&spool_path);
    let archive = run?;
    validate_archive(&archive, &manifest)?;
    publish_once(&archive_path, &canonical_json_line(&archive)?)?;
    let result = build_result(&archive, &archive_path, &manifest, &tail)?;
    publish_once(&result_path, &canonical_json_line(&result)?)?;
    Ok(result)
}

fn parse_manifest(raw: &[u8]) -> Result<Manifest> {
    let value: Value = serde_json::from_slice(raw).context("parse archive reduction manifest")?;
    ensure!(
        canonical_json_line(&value)? == raw,
        "archive reduction manifest must be canonical JSON followed by LF"
    );
    let map = obj(&value, "archive reduction manifest")?;
    let mut allowed = BTreeSet::from([
        "schemaVersion",
        "contractVersion",
        "operation",
        "tailAuthority",
        "cellCapacity",
        "archivePolicy",
        "directionAware",
        "previousArchivePath",
        "generationProposalAccounting",
        "bidirectionalPairPolicy",
        "manifestSha256",
    ]);
    ensure!(
        map.keys().all(|key| allowed.remove(key.as_str())),
        "archive reduction manifest has unknown fields"
    );
    ensure!(
        text(map, "schemaVersion")? == MANIFEST_SCHEMA
            && text(map, "contractVersion")? == CONTRACT_VERSION
            && text(map, "operation")? == OPERATION,
        "archive reduction manifest is incompatible"
    );
    let manifest_sha256 = sha(map, "manifestSha256")?;
    ensure!(
        canonical_sha256_without_object_field(&value, "manifestSha256")? == manifest_sha256,
        "archive reduction manifest identity mismatch"
    );
    let policy = obj(field(map, "archivePolicy")?, "archive policy")?;
    require_exact(policy, &["policyName", "policySha256", "frozenPolicy"])?;
    let policy_name = text(policy, "policyName")?.to_owned();
    let policy_sha256 = sha(policy, "policySha256")?;
    let frozen_policy = field(policy, "frozenPolicy")?.clone();
    for key in [
        "archive",
        "parentSelection",
        "resolvedExecutionDeduplication",
    ] {
        ensure!(
            obj(
                field(obj(&frozen_policy, "frozen archive policy")?, key)?,
                "frozen archive policy field"
            )
            .is_ok(),
            "frozen archive policy lacks {key}"
        );
    }
    let capacity = usize::try_from(integer(map, "cellCapacity")?)?;
    ensure!(
        (1..=32).contains(&capacity),
        "archive cell capacity must be 1..32"
    );
    let direction_aware = bool_field(map, "directionAware")?;
    if direction_aware {
        ensure!(
            obj(&frozen_policy, "frozen archive policy")?.contains_key("directionSelection"),
            "direction-aware archive policy lacks directionSelection"
        );
    }
    let authority = obj(field(map, "tailAuthority")?, "tail authority reference")?;
    require_exact(authority, &["receiptPath", "receiptSha256"])?;
    let tail_authority_path = PathBuf::from(text(authority, "receiptPath")?);
    ensure!(
        !tail_authority_path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir)),
        "tail authority receipt path must not traverse parent directories"
    );
    Ok(Manifest {
        tail_authority_path,
        tail_authority_sha256: sha(authority, "receiptSha256")?,
        cell_capacity: capacity,
        policy_name,
        policy_sha256,
        frozen_policy,
        direction_aware,
        previous_archive_path: map
            .get("previousArchivePath")
            .map(|v| text_value(v, "previous archive path").map(PathBuf::from))
            .transpose()?,
        generation_proposal_accounting: map
            .get("generationProposalAccounting")
            .cloned()
            .unwrap_or_else(|| json!({})),
        bidirectional_pair_policy: map.get("bidirectionalPairPolicy").cloned(),
        manifest_sha256,
    })
}

fn load_tail_authority(manifest: &Manifest) -> Result<TailInput> {
    let receipt_path = regular_file(&manifest.tail_authority_path, "tail authority receipt")?;
    let receipt_root = receipt_path
        .parent()
        .context("tail authority receipt has no parent")?;
    let receipt: Value = serde_json::from_slice(&fs::read(&receipt_path)?)?;
    let receipt_map = obj(&receipt, "tail authority receipt")?;
    require_exact(
        receipt_map,
        &[
            "schemaVersion",
            "generationIndex",
            "tailReductionManifestSha256",
            "evaluationPopulationSha256",
            "populationSha256",
            "tailResultIndexSha256",
            "taskMatrixSha256",
            "resultSetSha256",
            "runtimeAuthoritySha256",
            "tailReductionResult",
            "evaluatedMembers",
            "tailAuthoritySha256",
        ],
    )?;
    ensure!(
        text(receipt_map, "schemaVersion")? == TAIL_AUTHORITY_SCHEMA,
        "tail authority receipt schema is incompatible"
    );
    ensure!(
        sha(receipt_map, "tailAuthoritySha256")? == manifest.tail_authority_sha256
            && canonical_sha256_without_object_field(&receipt, "tailAuthoritySha256")?
                == manifest.tail_authority_sha256,
        "tail authority receipt identity mismatch"
    );
    let runtime_authority_sha256 = sha(receipt_map, "runtimeAuthoritySha256")?;
    let tail_descriptor = obj(
        field(receipt_map, "tailReductionResult")?,
        "tail authority tail result descriptor",
    )?;
    require_exact(
        tail_descriptor,
        &["path", "rawSha256", "sizeBytes", "resultSha256"],
    )?;
    ensure!(
        text(tail_descriptor, "path")? == temporal_qd_tail_reducer::RESULT_PATH,
        "tail authority tail result path is not fixed"
    );
    let members_descriptor = obj(
        field(receipt_map, "evaluatedMembers")?,
        "tail authority members descriptor",
    )?;
    require_exact(
        members_descriptor,
        &["path", "rawSha256", "sizeBytes", "recordCount"],
    )?;
    ensure!(
        text(members_descriptor, "path")? == temporal_qd_tail_reducer::MEMBERS_PATH,
        "tail authority members path is not fixed"
    );
    let path = regular_file(
        &receipt_root.join(temporal_qd_tail_reducer::RESULT_PATH),
        "tail reduction result",
    )?;
    let raw = fs::read(path)?;
    ensure!(
        sha256_prefixed(&raw) == sha(tail_descriptor, "rawSha256")?
            && raw.len() as u64 == integer(tail_descriptor, "sizeBytes")? as u64,
        "tail reduction result file identity mismatch"
    );
    let value: Value = serde_json::from_slice(&raw)?;
    ensure!(
        canonical_json_line(&value)? == raw,
        "tail reduction result is not canonical"
    );
    let m = obj(&value, "tail reduction result")?;
    ensure!(
        text(m, "schemaVersion")? == TAIL_RESULT_SCHEMA,
        "tail reduction result schema is incompatible"
    );
    ensure!(
        sha(m, "resultSha256")? == canonical_sha256_without_object_field(&value, "resultSha256")?,
        "tail reduction result identity mismatch"
    );
    for (key, expected) in [
        ("generationIndex", &receipt_map["generationIndex"]),
        (
            "evaluationPopulationSha256",
            &receipt_map["evaluationPopulationSha256"],
        ),
        ("populationSha256", &receipt_map["populationSha256"]),
        (
            "tailResultIndexSha256",
            &receipt_map["tailResultIndexSha256"],
        ),
        ("taskMatrixSha256", &receipt_map["taskMatrixSha256"]),
        ("resultSetSha256", &receipt_map["resultSetSha256"]),
        (
            "runtimeAuthoritySha256",
            &receipt_map["runtimeAuthoritySha256"],
        ),
        (
            "manifestSha256",
            &receipt_map["tailReductionManifestSha256"],
        ),
    ] {
        ensure!(
            m.get(key) == Some(expected),
            "tail reduction {key} binding drifted"
        );
    }
    let evaluated = obj(field(m, "evaluatedMembers")?, "tail evaluated members")?;
    ensure!(
        text(evaluated, "schemaVersion")? == EVALUATED_SCHEMA,
        "evaluated member descriptor schema is incompatible"
    );
    let members = obj(
        field(evaluated, "membersFile")?,
        "tail evaluated member file",
    )?;
    ensure!(
        sha(members, "rawSha256")? == sha(members_descriptor, "rawSha256")?
            && integer(members, "sizeBytes")? == integer(members_descriptor, "sizeBytes")?
            && integer(members, "recordCount")? == integer(members_descriptor, "recordCount")?,
        "evaluated member digest binding drifted"
    );
    let members_path = regular_file(
        &receipt_root.join(temporal_qd_tail_reducer::MEMBERS_PATH),
        "evaluated members",
    )?;
    Ok(TailInput {
        runtime_authority_sha256,
        members_path,
        members_sha256: sha(members_descriptor, "rawSha256")?,
        population_sha256: sha(receipt_map, "populationSha256")?,
        result_set_sha256: sha(receipt_map, "resultSetSha256")?,
        generation_index: u64::try_from(integer(receipt_map, "generationIndex")?)?,
        result: value,
    })
}

fn load_previous(path: Option<&Path>, manifest: &Manifest) -> Result<Option<Value>> {
    let Some(path) = path else { return Ok(None) };
    let raw = fs::read(regular_file(path, "previous archive")?)?;
    let value: Value = serde_json::from_slice(&raw).context("parse previous archive")?;
    let map = obj(&value, "previous archive")?;
    ensure!(
        text(map, "schemaVersion")? == ARCHIVE_SCHEMA,
        "previous archive schema is incompatible"
    );
    ensure!(
        sha(map, "archiveSha256")?
            == canonical_sha256_without_object_field(&value, "archiveSha256")?,
        "previous archive identity mismatch"
    );
    ensure!(
        text(map, "policyName")? == manifest.policy_name
            && sha(map, "policySha256")? == manifest.policy_sha256,
        "previous archive policy differs from reducer policy"
    );
    ensure!(
        integer(map, "cellCapacity")? == manifest.cell_capacity as i64,
        "previous archive capacity differs from reducer capacity"
    );
    Ok(Some(value))
}

fn reduce(
    manifest: &Manifest,
    tail: &TailInput,
    previous: Option<&Value>,
    spool: &Path,
) -> Result<Value> {
    let mut writer = BufWriter::new(
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(spool)?,
    );
    let mut by_candidate: BTreeMap<String, MemberRef> = BTreeMap::new();
    let mut previous_cell_ids = BTreeSet::new();
    let mut prior_counts = BTreeMap::new();
    let mut previous_member_ids = BTreeSet::new();
    if let Some(previous) = previous {
        let m = obj(previous, "previous archive")?;
        for cell in array(m, "cells")? {
            let cell = obj(cell, "previous archive cell")?;
            let cell_id = text(cell, "cellId")?.to_owned();
            previous_cell_ids.insert(cell_id.clone());
            prior_counts.insert(
                cell_id,
                (
                    integer_default(cell, "selectionVisitCount")?,
                    integer_default(cell, "offspringAttemptCount")?,
                ),
            );
            for value in array(cell, "members")? {
                let r = spool_member(&mut writer, value, tail.generation_index)?;
                previous_member_ids.insert(r.candidate_id.clone());
                by_candidate.insert(r.candidate_id.clone(), r);
            }
        }
    }
    let tail_map = obj(&tail.result, "tail reduction result")?;
    let evaluated = obj(
        field(tail_map, "evaluatedMembers")?,
        "tail evaluated members",
    )?;
    let descriptor = obj(field(evaluated, "membersFile")?, "tail member descriptor")?;
    let expected_count = integer(descriptor, "recordCount")? as usize;
    let input = File::open(&tail.members_path)?;
    let mut digest = Sha256::new();
    let mut count = 0usize;
    let mut offset = writer.stream_position()?;
    for line in BufReader::new(input).split(b'\n') {
        let mut line = line?;
        if line.is_empty() {
            continue;
        }
        line.push(b'\n');
        digest.update(&line);
        let value: Value = serde_json::from_slice(&line[..line.len() - 1])
            .context("parse evaluated member JSONL")?;
        ensure!(
            canonical_json_line(&value)? == line,
            "evaluated member JSONL is not canonical"
        );
        let r = member_ref(&value, offset, line.len() as u64, tail.generation_index)?;
        writer.write_all(&line)?;
        offset += line.len() as u64;
        by_candidate.insert(r.candidate_id.clone(), r);
        count += 1;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    let actual_members_sha = digest_prefixed(digest.finalize());
    ensure!(
        actual_members_sha == tail.members_sha256 && count == expected_count,
        "evaluated members artifact is corrupt, truncated, or replaced (sha={actual_members_sha}, expected={}, count={count}, expectedCount={expected_count})",
        tail.members_sha256
    );

    let mut resolved: BTreeMap<String, MemberRef> = BTreeMap::new();
    let mut discarded: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in by_candidate.values() {
        match resolved.get(&row.resolved_program) {
            Some(current) if row.representative_cmp(current).is_lt() => {
                discarded
                    .entry(row.resolved_program.clone())
                    .or_default()
                    .push(current.candidate_id.clone());
                resolved.insert(row.resolved_program.clone(), row.clone());
            }
            Some(_) => discarded
                .entry(row.resolved_program.clone())
                .or_default()
                .push(row.candidate_id.clone()),
            None => {
                resolved.insert(row.resolved_program.clone(), row.clone());
            }
        }
    }
    let all_rows: Vec<MemberRef> = by_candidate.values().cloned().collect();
    let mut groups: BTreeMap<String, Vec<MemberRef>> = BTreeMap::new();
    for row in resolved.values() {
        groups
            .entry(row.cell_id.clone())
            .or_default()
            .push(row.clone());
    }
    let mut reader = File::open(spool)?;
    let mut cells = Vec::new();
    for (cell_id, rows) in groups {
        cells.push(reduce_cell(
            &cell_id,
            &rows,
            manifest,
            &mut reader,
            prior_counts.get(&cell_id).copied(),
            generation_cell_counts(&manifest.generation_proposal_accounting, &cell_id),
        )?);
    }
    let selected: BTreeSet<String> = cells
        .iter()
        .flat_map(|cell| {
            array(obj(cell, "archive cell").expect("cell"), "members")
                .expect("members")
                .iter()
        })
        .filter_map(|m| m.get("candidateId").and_then(Value::as_str))
        .map(str::to_owned)
        .collect();
    let admitted: BTreeSet<String> = selected.difference(&previous_member_ids).cloned().collect();
    let evicted = previous_member_ids.difference(&selected).count();
    let policy = obj(&manifest.frozen_policy, "frozen archive policy")?;
    let tail_rejections = array(evaluated, "evaluationRejectedCandidates")?.clone();
    let new_candidate_count = count + tail_rejections.len();
    let previous_seen = previous
        .and_then(|p| p.get("candidateCountSeen"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let resolved_duplicate_count: usize = discarded.values().map(Vec::len).sum();
    let duplicates = discarded.into_iter().filter(|(_, ids)| !ids.is_empty()).map(|(program, mut ids)| { ids.sort(); let retained = resolved.get(&program).expect("winner"); json!({"resolvedProgramSha256":program,"retainedCandidateId":retained.candidate_id,"discardedCandidateIds":ids}) }).collect::<Vec<_>>();
    let mut family_survivors: BTreeMap<String, usize> = BTreeMap::new();
    for cell in &cells {
        for member in array(obj(cell, "archive cell")?, "members")? {
            if let Some(id) = member.get("candidateId").and_then(Value::as_str) {
                if admitted.contains(id) {
                    for family in lineage_families(member)? {
                        *family_survivors.entry(family).or_default() += 1;
                    }
                }
            }
        }
    }
    let quality_count = count_lane(&cells, "quality")?;
    let observational_count = count_lane(&cells, "observational")?;
    let negative_count = count_lane(&cells, "negative_novelty")?;
    let mut archive = json!({
        "schemaVersion": ARCHIVE_SCHEMA, "qdVersion": "temporal_qd_evolution_v3",
        "policyName": manifest.policy_name, "policySha256": manifest.policy_sha256,
        "frozenPolicy": manifest.frozen_policy, "generationIndex": tail.generation_index,
        "populationSha256": tail.population_sha256, "resultSetSha256": tail.result_set_sha256,
        "tailAuthoritySha256": manifest.tail_authority_sha256,
        "previousArchiveSha256": previous.map(|p| p["archiveSha256"].clone()).unwrap_or(Value::Null),
        "cellCapacity": manifest.cell_capacity,
        "qualityEligibilityPolicy": {"minimumTotalTrades":8,"minimumTradesPerWindow":4,"capTrades":20,"qualityRequiresFiniteData":true,"qualityRequiresNonnegativeRobustReturn":true,"undersupportedRetainedOnlyInObservationalLane":true},
        "archiveRetentionPolicy": field(policy, "archive")?, "parentSelectionPolicy": field(policy, "parentSelection")?,
        "objectives": [{"name":"worstWindowConservativeNetR","direction":"max"},{"name":"maximumDrawdownR","direction":"min"},{"name":"structuralComplexity","direction":"min"}],
        "candidateCountSeen": previous_seen + new_candidate_count as i64,
        "evaluationRejectionCount": tail_rejections.len(), "evaluationRejectedCandidates": tail_rejections,
        "candidateCountReducedThisGeneration": new_candidate_count,
        "authoredCandidateCountBeforeResolvedDeduplication": all_rows.len(),
        "authoredProgramCountBeforeResolvedDeduplication": all_rows.iter().map(|r| r.authored_program.clone()).collect::<BTreeSet<_>>().len(),
        "resolvedProgramCountBeforeReduction": resolved.len(), "resolvedProgramDuplicateCount": resolved_duplicate_count,
        "resolvedExecutionDeduplication": {"schemaVersion":"temporal_qd_resolved_execution_deduplication_v1","stage":"before_archive_reduction","frozenPolicy":field(policy,"resolvedExecutionDeduplication")?,"inputMemberCount":all_rows.len(),"uniqueResolvedProgramCount":resolved.len(),"duplicates":duplicates},
        "occupiedCellCount": cells.len(), "newCellCount": cells.iter().filter(|cell| !previous_cell_ids.contains(cell["cellId"].as_str().unwrap_or(""))).count(),
        "memberCount": cells.iter().map(|c| array(obj(c,"archive cell").expect("cell"),"members").expect("members").len()).sum::<usize>(),
        "qualityMemberCount":quality_count,"observationalMemberCount":observational_count,"negativeNoveltyMemberCount":negative_count,
        "paretoAdmissionCount": admitted.len(), "paretoEvictionCount": evicted,
        "survivingDescendantsByOperatorFamily":family_survivors, "generationProposalAccounting":manifest.generation_proposal_accounting,
        "cells":cells,
    });
    if manifest.direction_aware {
        archive.as_object_mut().expect("archive").insert(
            "directionSelectionPolicy".to_owned(),
            field(policy, "directionSelection")?.clone(),
        );
    }
    if let Some(pair) = &manifest.bidirectional_pair_policy {
        let mut public_pair = obj(pair, "bidirectional pair policy")?.clone();
        public_pair.remove("policySha256");
        archive.as_object_mut().expect("archive").insert(
            "bidirectionalPairPolicy".to_owned(),
            Value::Object(public_pair),
        );
    }
    archive.as_object_mut().expect("archive").insert(
        "runtimeAuthoritySha256".to_owned(),
        json!(tail.runtime_authority_sha256),
    );
    let hash = canonical_sha256(&archive)?;
    archive
        .as_object_mut()
        .expect("archive object")
        .insert("archiveSha256".to_owned(), Value::String(hash));
    Ok(archive)
}

fn reduce_cell(
    cell_id: &str,
    rows: &[MemberRef],
    manifest: &Manifest,
    reader: &mut File,
    prior: Option<(i64, i64)>,
    current: (i64, i64),
) -> Result<Value> {
    let mut quality: Vec<MemberRef> = rows.iter().filter(|row| row.quality()).cloned().collect();
    let negative: Vec<MemberRef> = rows.iter().filter(|row| row.negative()).cloned().collect();
    let mut observational: Vec<MemberRef> = rows
        .iter()
        .filter(|row| !row.quality() && !row.negative())
        .cloned()
        .collect();
    let mut selected: Vec<SelectedMember> = Vec::new();
    let mut directional_eligible = Vec::new();
    let mut directional_ineligible = Vec::new();
    if manifest.direction_aware {
        for row in quality.drain(..) {
            if row.direction_lane.is_some() {
                directional_eligible.push(row)
            } else {
                directional_ineligible.push(row)
            }
        }
        observational.extend(directional_ineligible.iter().cloned());
        quality = directional_eligible.clone();
        let quotas = direction_quotas(&manifest.frozen_policy)?;
        let mut consumed = BTreeSet::new();
        for lane in [
            "balanced_bidirectional",
            "long_specialist",
            "short_specialist",
        ] {
            let lane_rows: Vec<MemberRef> = quality
                .iter()
                .filter(|r| r.direction_lane.as_deref() == Some(lane))
                .cloned()
                .collect();
            for (row, front, crowd) in rank_quality(&lane_rows)?.into_iter().take(
                quotas
                    .get(lane)
                    .copied()
                    .unwrap_or(0)
                    .min(manifest.cell_capacity.saturating_sub(selected.len())),
            ) {
                consumed.insert(row.candidate_id.clone());
                selected.push((
                    row,
                    "quality".to_owned(),
                    Some(front),
                    crowd,
                    format!("direction_{lane}_quota"),
                ));
            }
        }
        for (row, front, crowd) in rank_quality(&quality)? {
            if selected.len() == manifest.cell_capacity {
                break;
            }
            if consumed.insert(row.candidate_id.clone()) {
                selected.push((
                    row,
                    "quality".to_owned(),
                    Some(front),
                    crowd,
                    "direction_eligible_quality_fallback".to_owned(),
                ));
            }
        }
    } else {
        for (row, front, crowd) in rank_quality(&quality)?
            .into_iter()
            .take(manifest.cell_capacity)
        {
            selected.push((
                row,
                "quality".to_owned(),
                Some(front),
                crowd,
                "quality_pareto".to_owned(),
            ));
        }
    }
    if selected.len() < manifest.cell_capacity {
        if let Some(row) = negative.iter().min_by(|a, b| a.observation_cmp(b)) {
            selected.push((
                row.clone(),
                "negative_novelty".to_owned(),
                None,
                None,
                "negative_novelty_exploration".to_owned(),
            ));
        }
    }
    let observational_count_before_capacity = observational.len();
    observational.sort_by(|a, b| a.observation_cmp(b));
    for row in observational
        .into_iter()
        .take(manifest.cell_capacity.saturating_sub(selected.len()))
    {
        selected.push((
            row,
            "observational".to_owned(),
            None,
            None,
            "observational_retention".to_owned(),
        ));
    }
    ensure!(!selected.is_empty(), "archive cell selection became empty");
    let descriptor = load_value(reader, &selected[0].0)?
        .get("descriptor")
        .cloned()
        .context("member lacks descriptor")?;
    let mut output_members = Vec::new();
    for (reference, lane, front, crowd, reason) in selected {
        let mut row = load_value(reader, &reference)?;
        let m = obj_mut(&mut row, "selected archive member")?;
        m.insert("archiveLane".to_owned(), Value::String(lane));
        m.insert(
            "paretoFront".to_owned(),
            front.map(|v| json!(v)).unwrap_or(Value::Null),
        );
        m.insert(
            "crowdingDistance".to_owned(),
            crowd.map(number).unwrap_or(Value::Null),
        );
        m.insert("retentionReason".to_owned(), Value::String(reason));
        if manifest.direction_aware {
            if let Some(lane) = &reference.direction_lane {
                m.insert(
                    "directionBreedingLane".to_owned(),
                    Value::String(lane.clone()),
                );
            }
        }
        output_members.push(row);
    }
    output_members.sort_by(|a, b| a["candidateId"].as_str().cmp(&b["candidateId"].as_str()));
    let mut cell = json!({"cellId":cell_id,"descriptor":descriptor,"candidateCountBeforeCapacity":rows.len(),"qualityEligibleCountBeforeCapacity":quality.len(),"negativeNoveltyEligibleCountBeforeCapacity":negative.len(),"observationalCountBeforeCapacity":observational_count_before_capacity,"breedingEligibleMemberCount":output_members.iter().filter(|m| m["archiveLane"] == "quality").count(),"negativeNoveltyMemberCount":output_members.iter().filter(|m| m["archiveLane"] == "negative_novelty").count(),"selectionVisitCount":prior.map(|v|v.0).unwrap_or(0)+current.0,"offspringAttemptCount":prior.map(|v|v.1).unwrap_or(0)+current.1,"members":output_members});
    if manifest.direction_aware {
        let c = cell.as_object_mut().expect("cell");
        c.insert(
            "directionBreedingEligibleCountBeforeCapacity".to_owned(),
            json!(directional_eligible.len()),
        );
        c.insert(
            "directionIneligibleQualityCountBeforeCapacity".to_owned(),
            json!(directional_ineligible.len()),
        );
    }
    Ok(cell)
}

fn member_ref(value: &Value, offset: u64, length: u64, generation_index: u64) -> Result<MemberRef> {
    let m = obj(value, "evaluated member")?;
    ensure!(
        u64::try_from(integer(m, "generationIndex")?)? == generation_index,
        "evaluated member generation binding drifted"
    );
    let candidate_id = text(m, "candidateId")?.to_owned();
    let aggregate = obj(field(m, "aggregate")?, "member aggregate")?;
    let objectives = obj(field(m, "objectives")?, "member objectives")?;
    let descriptor = obj(field(m, "descriptor")?, "member descriptor")?;
    let validity = obj(field(m, "finiteDataValidity")?, "member validity")?;
    let resolved_program = sha(aggregate, "resolvedProgramSha256")?;
    let authored_program = aggregate
        .get("authoredProgramSha256")
        .and_then(Value::as_str)
        .or_else(|| {
            m.get("candidate")
                .and_then(|v| v.get("programSha256"))
                .and_then(Value::as_str)
        })
        .context("member lacks authored program SHA")?
        .to_owned();
    let f = |name: &str| finite(field(objectives, name)?, name);
    Ok(MemberRef {
        candidate_id,
        resolved_program,
        authored_program,
        cell_id: text(descriptor, "cellId")?.to_owned(),
        worst: f("worstWindowConservativeNetR")?,
        drawdown: f("maximumDrawdownR")?,
        complexity: f("structuralComplexity")?,
        support: finite(field(m, "cappedTradeSupport")?, "cappedTradeSupport")?,
        finite: bool_field(validity, "isFiniteData")?,
        support_gate: bool_field(validity, "passesSupportGate")?,
        valid_quality: bool_field(validity, "validForQuality")?,
        direction_lane: m
            .get("directionBreedingLane")
            .and_then(Value::as_str)
            .map(str::to_owned),
        offset,
        length,
    })
}

fn spool_member(
    writer: &mut BufWriter<File>,
    value: &Value,
    generation_index: u64,
) -> Result<MemberRef> {
    let offset = writer.stream_position()?;
    let line = canonical_json_line(value)?;
    writer.write_all(&line)?;
    member_ref(value, offset, line.len() as u64, generation_index)
}
fn load_value(reader: &mut File, reference: &MemberRef) -> Result<Value> {
    reader.seek(SeekFrom::Start(reference.offset))?;
    let mut bytes = vec![0; reference.length as usize];
    reader.read_exact(&mut bytes)?;
    serde_json::from_slice(&bytes).context("parse spooled archive member")
}

fn rank_quality(rows: &[MemberRef]) -> Result<Vec<(MemberRef, usize, Option<f64>)>> {
    let mut remaining = rows.to_vec();
    remaining.sort_by(|a, b| a.candidate_id.cmp(&b.candidate_id));
    let mut out = Vec::new();
    let mut front = 0;
    while !remaining.is_empty() {
        let current: Vec<_> = remaining
            .iter()
            .filter(|row| {
                !remaining
                    .iter()
                    .any(|other| other.candidate_id != row.candidate_id && dominates(other, row))
            })
            .cloned()
            .collect::<Vec<_>>();
        let ordered = crowding(current)?;
        let ids: BTreeSet<_> = ordered
            .iter()
            .map(|(r, _)| r.candidate_id.clone())
            .collect();
        out.extend(ordered.into_iter().map(|(r, c)| (r, front, c)));
        remaining.retain(|r| !ids.contains(&r.candidate_id));
        front += 1;
    }
    Ok(out)
}
fn dominates(a: &MemberRef, b: &MemberRef) -> bool {
    (a.worst >= b.worst && a.drawdown <= b.drawdown && a.complexity <= b.complexity)
        && (a.worst > b.worst || a.drawdown < b.drawdown || a.complexity < b.complexity)
}
fn crowding(mut rows: Vec<MemberRef>) -> Result<Vec<(MemberRef, Option<f64>)>> {
    if rows.len() <= 2 {
        rows.sort_by(|a, b| {
            desc_f64(a.worst, b.worst)
                .then_with(|| desc_f64(a.support, b.support))
                .then_with(|| asc_f64(a.complexity, b.complexity))
                .then_with(|| a.candidate_id.cmp(&b.candidate_id))
        });
        return Ok(rows.into_iter().map(|r| (r, None)).collect());
    };
    let mut d: HashMap<String, f64> = rows.iter().map(|r| (r.candidate_id.clone(), 0.)).collect();
    for value in [0, 1, 2] {
        rows.sort_by(|a, b| {
            metric(a, value)
                .total_cmp(&metric(b, value))
                .then_with(|| a.candidate_id.cmp(&b.candidate_id))
        });
        let first = rows.first().context("crowding first")?.candidate_id.clone();
        let last = rows.last().context("crowding last")?.candidate_id.clone();
        d.insert(first, f64::INFINITY);
        d.insert(last, f64::INFINITY);
        let scale = metric(rows.last().unwrap(), value) - metric(rows.first().unwrap(), value);
        if scale > 0. {
            for i in 1..rows.len() - 1 {
                let id = &rows[i].candidate_id;
                if d[id].is_finite() {
                    *d.get_mut(id).unwrap() +=
                        (metric(&rows[i + 1], value) - metric(&rows[i - 1], value)).abs() / scale;
                }
            }
        }
    }
    rows.sort_by(|a, b| {
        d[&b.candidate_id]
            .total_cmp(&d[&a.candidate_id])
            .then_with(|| desc_f64(a.worst, b.worst))
            .then_with(|| desc_f64(a.support, b.support))
            .then_with(|| asc_f64(a.complexity, b.complexity))
            .then_with(|| a.candidate_id.cmp(&b.candidate_id))
    });
    Ok(rows
        .into_iter()
        .map(|r| {
            let x = d[&r.candidate_id];
            (r, if x.is_infinite() { None } else { Some(x) })
        })
        .collect())
}
fn metric(r: &MemberRef, index: usize) -> f64 {
    match index {
        0 => r.worst,
        1 => r.drawdown,
        _ => r.complexity,
    }
}
fn desc_f64(a: f64, b: f64) -> Ordering {
    b.total_cmp(&a)
}
fn asc_f64(a: f64, b: f64) -> Ordering {
    a.total_cmp(&b)
}
fn direction_quotas(policy: &Value) -> Result<BTreeMap<String, usize>> {
    let selection = obj(
        field(obj(policy, "frozen archive policy")?, "directionSelection")?,
        "direction policy",
    )?;
    let quotas = obj(
        field(selection, "perCellBreedingQuotas")?,
        "direction quota",
    )?;
    quotas
        .iter()
        .map(|(k, v)| {
            Ok((
                k.clone(),
                usize::try_from(v.as_i64().context("direction quota is not integer")?)?,
            ))
        })
        .collect()
}
fn count_lane(cells: &[Value], lane: &str) -> Result<usize> {
    let mut count = 0;
    for cell in cells {
        for member in array(obj(cell, "archive cell")?, "members")? {
            if member.get("archiveLane").and_then(Value::as_str) == Some(lane) {
                count += 1
            }
        }
    }
    Ok(count)
}
fn generation_cell_counts(accounting: &Value, cell_id: &str) -> (i64, i64) {
    let selection = accounting
        .get("parentCellSelectionCounts")
        .and_then(Value::as_object)
        .and_then(|m| m.get(cell_id))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let attempts = accounting
        .get("parentCellOffspringAttemptCounts")
        .and_then(Value::as_object)
        .and_then(|m| m.get(cell_id))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    (selection, attempts)
}
fn lineage_families(member: &Value) -> Result<Vec<String>> {
    let history = member
        .get("candidate")
        .and_then(|c| c.get("structuralOperatorHistory"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut families = BTreeSet::new();
    for entry in history {
        let Some(m) = entry.as_object() else { continue };
        if matches!(
            m.get("operation").and_then(Value::as_str),
            Some("rich_immigrant_construction" | "typed_seed")
        ) {
            continue;
        }
        for key in ["operatorId", "operation", "kind"] {
            if let Some(value) = m.get(key).and_then(Value::as_str).filter(|v| !v.is_empty()) {
                families.insert(value.to_owned());
                break;
            }
        }
    }
    Ok(families.into_iter().collect())
}

fn build_result(
    archive: &Value,
    archive_path: &Path,
    manifest: &Manifest,
    tail: &TailInput,
) -> Result<Value> {
    let mut r = json!({"schemaVersion":RESULT_SCHEMA,"contractVersion":CONTRACT_VERSION,"operation":OPERATION,"status":"completed","manifestSha256":manifest.manifest_sha256,"tailAuthoritySha256":manifest.tail_authority_sha256,"archiveSha256":archive["archiveSha256"],"archiveRawSha256":digest_file(archive_path)?,"archiveSizeBytes":fs::metadata(regular_file(archive_path, "archive")?)?.len(),"populationSha256":tail.population_sha256,"resultSetSha256":tail.result_set_sha256,"generationIndex":tail.generation_index,"candidateCountSeen":archive["candidateCountSeen"],"occupiedCellCount":archive["occupiedCellCount"],"memberCount":archive["memberCount"],"qualityMemberCount":archive["qualityMemberCount"],"observationalMemberCount":archive["observationalMemberCount"],"negativeNoveltyMemberCount":archive["negativeNoveltyMemberCount"],"archivePath":ARCHIVE_PATH,"runtimeAuthoritySha256":tail.runtime_authority_sha256});
    let h = canonical_sha256(&r)?;
    r.as_object_mut()
        .unwrap()
        .insert("resultSha256".to_owned(), json!(h));
    Ok(r)
}
fn reopen(result_path: &Path, archive_path: &Path, manifest: &Manifest) -> Result<Value> {
    let r: Value = serde_json::from_slice(&fs::read(regular_file(
        result_path,
        "archive reduction result",
    )?)?)?;
    let m = obj(&r, "archive reduction result")?;
    ensure!(
        text(m, "schemaVersion")? == RESULT_SCHEMA
            && sha(m, "resultSha256")?
                == canonical_sha256_without_object_field(&r, "resultSha256")?,
        "existing archive reduction result is invalid"
    );
    ensure!(
        sha(m, "manifestSha256")? == manifest.manifest_sha256,
        "existing result manifest binding drifted"
    );
    let a: Value = serde_json::from_slice(&fs::read(regular_file(archive_path, "archive")?)?)?;
    validate_archive(&a, manifest)?;
    ensure!(
        sha(m, "archiveSha256")? == sha(obj(&a, "archive")?, "archiveSha256")?,
        "existing result archive binding drifted"
    );
    ensure!(
        sha(m, "archiveRawSha256")? == digest_file(archive_path)?
            && m.get("archiveSizeBytes").and_then(Value::as_u64)
                == Some(fs::metadata(regular_file(archive_path, "archive")?)?.len()),
        "existing result archive byte binding drifted"
    );
    Ok(r)
}
fn validate_archive(archive: &Value, manifest: &Manifest) -> Result<()> {
    let m = obj(archive, "archive")?;
    ensure!(
        text(m, "schemaVersion")? == ARCHIVE_SCHEMA
            && text(m, "policyName")? == manifest.policy_name
            && sha(m, "policySha256")? == manifest.policy_sha256,
        "archive policy/schema mismatch"
    );
    ensure!(
        sha(m, "archiveSha256")?
            == canonical_sha256_without_object_field(archive, "archiveSha256")?,
        "archive identity mismatch"
    );
    ensure!(
        integer(m, "cellCapacity")? == manifest.cell_capacity as i64,
        "archive capacity mismatch"
    );
    ensure!(
        sha(m, "tailAuthoritySha256")? == manifest.tail_authority_sha256,
        "archive tail authority binding mismatch"
    );
    Ok(())
}

fn obj<'a>(v: &'a Value, label: &str) -> Result<&'a Map<String, Value>> {
    v.as_object()
        .ok_or_else(|| anyhow!("{label} must be an object"))
}
fn obj_mut<'a>(v: &'a mut Value, label: &str) -> Result<&'a mut Map<String, Value>> {
    v.as_object_mut()
        .ok_or_else(|| anyhow!("{label} must be an object"))
}
fn array<'a>(m: &'a Map<String, Value>, key: &str) -> Result<&'a Vec<Value>> {
    field(m, key)?
        .as_array()
        .ok_or_else(|| anyhow!("{key} must be an array"))
}
fn field<'a>(m: &'a Map<String, Value>, key: &str) -> Result<&'a Value> {
    m.get(key).ok_or_else(|| anyhow!("missing {key}"))
}
fn text<'a>(m: &'a Map<String, Value>, key: &str) -> Result<&'a str> {
    text_value(field(m, key)?, key)
}
fn text_value<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v.as_str()
        .filter(|x| !x.is_empty())
        .ok_or_else(|| anyhow!("{key} must be a nonempty string"))
}
fn integer(m: &Map<String, Value>, key: &str) -> Result<i64> {
    field(m, key)?
        .as_i64()
        .ok_or_else(|| anyhow!("{key} must be an integer"))
}
fn integer_default(m: &Map<String, Value>, key: &str) -> Result<i64> {
    Ok(m.get(key).and_then(Value::as_i64).unwrap_or(0))
}
fn bool_field(m: &Map<String, Value>, key: &str) -> Result<bool> {
    field(m, key)?
        .as_bool()
        .ok_or_else(|| anyhow!("{key} must be boolean"))
}
fn sha(m: &Map<String, Value>, key: &str) -> Result<String> {
    let s = text(m, key)?;
    ensure!(
        s.starts_with("sha256:") && s.len() == 71,
        "{key} must be SHA-256"
    );
    Ok(s.to_owned())
}
fn finite(v: &Value, key: &str) -> Result<f64> {
    let n = v.as_f64().ok_or_else(|| anyhow!("{key} must be numeric"))?;
    ensure!(n.is_finite(), "{key} must be finite");
    Ok(n)
}
fn number(v: f64) -> Value {
    serde_json::Number::from_f64(v)
        .map(Value::Number)
        .expect("finite number")
}
fn require_exact(m: &Map<String, Value>, keys: &[&str]) -> Result<()> {
    let got: BTreeSet<_> = m.keys().map(String::as_str).collect();
    let expected: BTreeSet<_> = keys.iter().copied().collect();
    ensure!(got == expected, "object fields differ from contract");
    Ok(())
}
fn regular_file(path: &Path, label: &str) -> Result<PathBuf> {
    let link =
        fs::symlink_metadata(path).with_context(|| format!("read {label}: {}", path.display()))?;
    ensure!(
        !link.file_type().is_symlink(),
        "{label} path must not be a symlink or alias"
    );
    let meta = fs::metadata(path)?;
    ensure!(meta.is_file(), "{label} must be a regular file");
    fs::canonicalize(path).context("canonicalize regular file")
}
fn digest_prefixed(digest: impl std::fmt::LowerHex) -> String {
    format!("sha256:{digest:x}")
}
fn digest_file(path: &Path) -> Result<String> {
    let mut f = BufReader::new(File::open(regular_file(path, "bound input")?)?);
    let mut h = Sha256::new();
    let mut b = [0u8; 65536];
    loop {
        let n = f.read(&mut b)?;
        if n == 0 {
            break;
        }
        h.update(&b[..n]);
    }
    Ok(digest_prefixed(h.finalize()))
}
fn temporary_path(dir: &Path, name: &str) -> PathBuf {
    let n = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    dir.join(format!(".{name}.{n}.tmp"))
}
fn publish_once(path: &Path, bytes: &[u8]) -> Result<()> {
    let temp = temporary_path(
        path.parent().context("output path has no parent")?,
        path.file_name()
            .and_then(|v| v.to_str())
            .unwrap_or("output"),
    );
    {
        let mut f = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    match fs::hard_link(&temp, path) {
        Ok(_) => {
            fs::remove_file(&temp)?;
            Ok(())
        }
        Err(_e) if path.exists() => {
            fs::remove_file(&temp)?;
            bail!("refusing to replace existing output {}", path.display())
        }
        Err(e) => {
            let _ = fs::remove_file(&temp);
            Err(e.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use tempfile::TempDir;

    fn sha_value(value: &Value) -> String {
        canonical_sha256(value).unwrap()
    }
    fn write_value(path: &Path, value: &Value) {
        fs::write(path, canonical_json_line(value).unwrap()).unwrap();
    }
    fn policy() -> Value {
        json!({"schemaVersion":"temporal_qd_policy_v4","archive":{"defaultCellCapacity":4,"lanes":{},"negativeNoveltyMaxMembersPerCell":1},"parentSelection":{},"resolvedExecutionDeduplication":{"identity":"aggregate.resolvedProgramSha256","representativeOrdering":[],"required":true,"stage":"before_archive_reduction"},"tradeSupport":{"capTrades":20}})
    }
    fn member(
        id: &str,
        program: &str,
        worst: f64,
        drawdown: f64,
        complexity: f64,
        supported: bool,
    ) -> Value {
        json!({"candidateId":id,"generationIndex":7,"candidate":{"programSha256":program},"aggregate":{"resolvedProgramSha256":program,"authoredProgramSha256":program},"descriptor":{"cellId":"one|root|one|none|small|moderate|medium"},"objectives":{"worstWindowConservativeNetR":worst,"maximumDrawdownR":drawdown,"structuralComplexity":complexity},"finiteDataValidity":{"isFiniteData":true,"passesSupportGate":supported,"validForQuality":supported},"cappedTradeSupport":20.0})
    }
    fn fixture(rows: Vec<Value>) -> (TempDir, PathBuf) {
        let root = TempDir::new().unwrap();
        let members = root.path().join("evaluated-members.jsonl");
        let bytes: Vec<u8> = rows
            .iter()
            .flat_map(|v| canonical_json_line(v).unwrap())
            .collect();
        fs::write(&members, &bytes).unwrap();
        let population = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let result_set = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let rejected = json!([{"candidateId":"structural","disposition":"rejected","reasonCode":"duplicate_break_even_execution_invariant","structuralProvenance":{"modules":[]}},{"candidateId":"warmup","disposition":"rejected","reasonCode":"insufficient_aligned_history","windowRejections":[]}]);
        let runtime = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let mut tail = json!({"schemaVersion":TAIL_RESULT_SCHEMA,"manifestSha256":"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","generationIndex":7,"evaluationPopulationSha256":population,"populationSha256":population,"tailResultIndexSha256":"sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","taskMatrixSha256":"sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff","resultSetSha256":result_set,"runtimeAuthoritySha256":runtime,"evaluatedMembers":{"schemaVersion":EVALUATED_SCHEMA,"evaluationRejectedCandidates":rejected,"membersFile":{"rawSha256":sha256_prefixed(&bytes),"sizeBytes":bytes.len(),"recordCount":rows.len()}}});
        let tail_sha = sha_value(&tail);
        tail.as_object_mut()
            .unwrap()
            .insert("resultSha256".into(), json!(tail_sha));
        let tail_path = root.path().join("tail-reduction-result.json");
        write_value(&tail_path, &tail);
        let tail_bytes = canonical_json_line(&tail).unwrap();
        let mut authority = json!({"schemaVersion":TAIL_AUTHORITY_SCHEMA,"generationIndex":7,"tailReductionManifestSha256":tail["manifestSha256"],"evaluationPopulationSha256":population,"populationSha256":population,"tailResultIndexSha256":tail["tailResultIndexSha256"],"taskMatrixSha256":tail["taskMatrixSha256"],"resultSetSha256":result_set,"runtimeAuthoritySha256":runtime,"tailReductionResult":{"path":temporal_qd_tail_reducer::RESULT_PATH,"rawSha256":sha256_prefixed(&tail_bytes),"sizeBytes":tail_bytes.len(),"resultSha256":tail["resultSha256"]},"evaluatedMembers":{"path":temporal_qd_tail_reducer::MEMBERS_PATH,"rawSha256":sha256_prefixed(&bytes),"sizeBytes":bytes.len(),"recordCount":rows.len()}});
        let authority_sha = sha_value(&authority);
        authority["tailAuthoritySha256"] = json!(authority_sha);
        let authority_path = root
            .path()
            .join(temporal_qd_tail_reducer::TAIL_AUTHORITY_PATH);
        write_value(&authority_path, &authority);
        let frozen = policy();
        let policy_sha = sha_value(&frozen);
        let mut manifest = json!({"schemaVersion":MANIFEST_SCHEMA,"contractVersion":CONTRACT_VERSION,"operation":OPERATION,"tailAuthority":{"receiptPath":authority_path,"receiptSha256":authority["tailAuthoritySha256"]},"cellCapacity":4,"archivePolicy":{"policyName":"test-policy","policySha256":policy_sha,"frozenPolicy":frozen},"directionAware":false});
        let manifest_sha = sha_value(&manifest);
        manifest
            .as_object_mut()
            .unwrap()
            .insert("manifestSha256".into(), json!(manifest_sha));
        let manifest_path = root.path().join("manifest.json");
        write_value(&manifest_path, &manifest);
        (root, manifest_path)
    }

    fn upgrade_python_case_to_tail_authority(case: &Path) {
        let members_path = case.join(temporal_qd_tail_reducer::MEMBERS_PATH);
        let members = fs::read(&members_path).unwrap();
        let tail_path = case.join(temporal_qd_tail_reducer::RESULT_PATH);
        let mut tail: Value = serde_json::from_slice(&fs::read(&tail_path).unwrap()).unwrap();
        let runtime = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let population = tail["populationSha256"].clone();
        tail["manifestSha256"] =
            json!("sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd");
        tail["generationIndex"] = json!(7);
        tail["evaluationPopulationSha256"] = population.clone();
        tail["tailResultIndexSha256"] =
            json!("sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        tail["taskMatrixSha256"] =
            json!("sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
        tail["runtimeAuthoritySha256"] = json!(runtime);
        tail["evaluatedMembers"]["membersFile"]["sizeBytes"] = json!(members.len());
        tail.as_object_mut().unwrap().remove("resultSha256");
        tail["resultSha256"] = json!(sha_value(&tail));
        write_value(&tail_path, &tail);
        let tail_bytes = canonical_json_line(&tail).unwrap();
        let mut authority = json!({"schemaVersion":TAIL_AUTHORITY_SCHEMA,"generationIndex":tail["generationIndex"],"tailReductionManifestSha256":tail["manifestSha256"],"evaluationPopulationSha256":tail["evaluationPopulationSha256"],"populationSha256":tail["populationSha256"],"tailResultIndexSha256":tail["tailResultIndexSha256"],"taskMatrixSha256":tail["taskMatrixSha256"],"resultSetSha256":tail["resultSetSha256"],"runtimeAuthoritySha256":runtime,"tailReductionResult":{"path":temporal_qd_tail_reducer::RESULT_PATH,"rawSha256":sha256_prefixed(&tail_bytes),"sizeBytes":tail_bytes.len(),"resultSha256":tail["resultSha256"]},"evaluatedMembers":{"path":temporal_qd_tail_reducer::MEMBERS_PATH,"rawSha256":sha256_prefixed(&members),"sizeBytes":members.len(),"recordCount":tail["evaluatedMembers"]["membersFile"]["recordCount"]}});
        authority["tailAuthoritySha256"] = json!(sha_value(&authority));
        let authority_path = case.join(temporal_qd_tail_reducer::TAIL_AUTHORITY_PATH);
        write_value(&authority_path, &authority);
        let old: Value =
            serde_json::from_slice(&fs::read(case.join("manifest.json")).unwrap()).unwrap();
        let mut manifest = json!({"schemaVersion":MANIFEST_SCHEMA,"contractVersion":CONTRACT_VERSION,"operation":OPERATION,"tailAuthority":{"receiptPath":authority_path,"receiptSha256":authority["tailAuthoritySha256"]},"cellCapacity":old["cellCapacity"],"archivePolicy":old["archivePolicy"],"directionAware":old["directionAware"]});
        manifest["manifestSha256"] = json!(sha_value(&manifest));
        write_value(&case.join("manifest.json"), &manifest);
    }
    #[test]
    fn oracle_shape_lanes_dedup_rejections_and_restart() {
        let rows = vec![
            member(
                "alpha",
                "sha256:1111111111111111111111111111111111111111111111111111111111111111",
                1.,
                2.,
                3.,
                true,
            ),
            member(
                "bravo",
                "sha256:1111111111111111111111111111111111111111111111111111111111111111",
                2.,
                2.,
                3.,
                true,
            ),
            member(
                "charlie",
                "sha256:2222222222222222222222222222222222222222222222222222222222222222",
                1.5,
                1.,
                4.,
                true,
            ),
            member(
                "delta",
                "sha256:3333333333333333333333333333333333333333333333333333333333333333",
                -1.,
                1.,
                2.,
                true,
            ),
            member(
                "echo",
                "sha256:4444444444444444444444444444444444444444444444444444444444444444",
                0.2,
                1.,
                9.,
                false,
            ),
        ];
        let (root, manifest) = fixture(rows);
        let result = execute_manifest(&manifest).unwrap();
        let archive: Value =
            serde_json::from_slice(&fs::read(root.path().join(ARCHIVE_PATH)).unwrap()).unwrap();
        assert_eq!(result["memberCount"], 4);
        assert_eq!(archive["evaluationRejectionCount"], 2);
        assert_eq!(archive["resolvedProgramDuplicateCount"], 1);
        let lanes: Vec<_> = archive["cells"][0]["members"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["archiveLane"].as_str().unwrap())
            .collect();
        assert!(
            lanes.contains(&"quality")
                && lanes.contains(&"negative_novelty")
                && lanes.contains(&"observational")
        );
        assert_eq!(execute_manifest(&manifest).unwrap(), result);
    }
    #[test]
    fn one_hundred_twenty_eight_members_and_tamper_rejected() {
        let rows: Vec<_> = (0..128)
            .map(|i| {
                member(
                    &format!("candidate{i:03}"),
                    &format!("sha256:{i:064x}"),
                    i as f64 / 100.,
                    (127 - i) as f64,
                    i as f64,
                    true,
                )
            })
            .collect();
        let (before_commit, before_manifest) = fixture(rows.clone());
        fs::remove_file(before_commit.path().join("evaluated-members.jsonl")).unwrap();
        assert!(
            execute_manifest(&before_manifest).is_err(),
            "source deletion before archive publication must fail closed"
        );
        let (root, manifest) = fixture(rows);
        let r = execute_manifest(&manifest).unwrap();
        assert_eq!(r["memberCount"], 4);
        fs::remove_file(root.path().join("evaluated-members.jsonl")).unwrap();
        fs::remove_file(root.path().join("tail-reduction-result.json")).unwrap();
        fs::remove_file(root.path().join("tail-authority.json")).unwrap();
        assert_eq!(
            execute_manifest(&manifest).unwrap(),
            r,
            "receipt-last restart must validate only committed archive artifacts"
        );
    }

    #[test]
    fn authority_rehashed_substitution_traversal_and_raw_manifest_fail_closed() {
        let (root, manifest_path) = fixture(vec![member(
            "alpha",
            "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            1.0,
            1.0,
            1.0,
            true,
        )]);
        let authority_path = root
            .path()
            .join(temporal_qd_tail_reducer::TAIL_AUTHORITY_PATH);
        let mut authority: Value =
            serde_json::from_slice(&fs::read(&authority_path).unwrap()).unwrap();
        authority["resultSetSha256"] =
            json!("sha256:9999999999999999999999999999999999999999999999999999999999999999");
        authority
            .as_object_mut()
            .unwrap()
            .remove("tailAuthoritySha256");
        authority["tailAuthoritySha256"] = json!(sha_value(&authority));
        write_value(&authority_path, &authority);
        let mut manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        manifest["tailAuthority"]["receiptSha256"] = authority["tailAuthoritySha256"].clone();
        manifest.as_object_mut().unwrap().remove("manifestSha256");
        manifest["manifestSha256"] = json!(sha_value(&manifest));
        write_value(&manifest_path, &manifest);
        let error =
            execute_manifest(&manifest_path).expect_err("rehashed receipt substitution must fail");
        assert!(format!("{error:#}").contains("resultSetSha256 binding drifted"));

        let (root, manifest_path) = fixture(vec![member(
            "bravo",
            "sha256:2222222222222222222222222222222222222222222222222222222222222222",
            1.0,
            1.0,
            1.0,
            true,
        )]);
        let mut manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        manifest["tailAuthority"]["receiptPath"] =
            json!(root.path().join("../tail-authority.json"));
        manifest.as_object_mut().unwrap().remove("manifestSha256");
        manifest["manifestSha256"] = json!(sha_value(&manifest));
        write_value(&manifest_path, &manifest);
        let error = execute_manifest(&manifest_path).expect_err("traversal must fail");
        assert!(format!("{error:#}").contains("must not traverse"));

        let (_root, manifest_path) = fixture(vec![member(
            "charlie",
            "sha256:3333333333333333333333333333333333333333333333333333333333333333",
            1.0,
            1.0,
            1.0,
            true,
        )]);
        let mut manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        manifest["schemaVersion"] = json!("temporal_qd_native_archive_reduction_manifest_v1");
        manifest.as_object_mut().unwrap().remove("manifestSha256");
        manifest["manifestSha256"] = json!(sha_value(&manifest));
        write_value(&manifest_path, &manifest);
        assert!(
            execute_manifest(&manifest_path).is_err(),
            "raw v1 tail-input manifest must not be accepted by the v2 authority path"
        );
    }

    #[test]
    fn precommit_result_member_and_authority_tamper_fail_closed() {
        let row = member(
            "alpha",
            "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            1.0,
            1.0,
            1.0,
            true,
        );
        let (root, manifest) = fixture(vec![row.clone()]);
        fs::write(
            root.path().join(temporal_qd_tail_reducer::RESULT_PATH),
            b"tampered\n",
        )
        .unwrap();
        assert!(
            execute_manifest(&manifest).is_err(),
            "tail result replacement before archive commit must fail"
        );

        let (root, manifest) = fixture(vec![row.clone()]);
        fs::write(
            root.path().join(temporal_qd_tail_reducer::MEMBERS_PATH),
            b"tampered\n",
        )
        .unwrap();
        assert!(
            execute_manifest(&manifest).is_err(),
            "member replacement before archive commit must fail"
        );

        let (root, manifest) = fixture(vec![row]);
        fs::remove_file(
            root.path()
                .join(temporal_qd_tail_reducer::TAIL_AUTHORITY_PATH),
        )
        .unwrap();
        assert!(
            execute_manifest(&manifest).is_err(),
            "missing authority receipt must fail"
        );
    }

    #[test]
    fn archive_transport_descriptor_is_self_hashed_and_rejects_bad_sources() {
        let (root, manifest) = fixture(vec![member(
            "alpha",
            "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            1.0,
            1.0,
            1.0,
            true,
        )]);
        execute_manifest(&manifest).unwrap();
        let archive_path = root.path().join(ARCHIVE_PATH);
        let descriptor = certify_archive_transport(&archive_path).unwrap();
        require_exact(
            obj(&descriptor, "archive transport descriptor").unwrap(),
            &[
                "schemaVersion",
                "absolutePath",
                "documentSchemaVersion",
                "archiveSha256",
                "fileSha256",
                "sizeBytes",
                "descriptorSha256",
            ],
        )
        .unwrap();
        assert_eq!(
            descriptor["schemaVersion"],
            json!(ARCHIVE_TRANSPORT_DESCRIPTOR_SCHEMA)
        );
        assert_eq!(
            descriptor["absolutePath"],
            json!(fs::canonicalize(&archive_path).unwrap())
        );
        assert_eq!(
            descriptor["descriptorSha256"],
            json!(canonical_sha256_without_object_field(&descriptor, "descriptorSha256").unwrap())
        );

        let mut tampered: Value =
            serde_json::from_slice(&fs::read(&archive_path).unwrap()).unwrap();
        tampered["memberCount"] = json!(999);
        write_value(&archive_path, &tampered);
        assert!(
            certify_archive_transport(&archive_path).is_err(),
            "self-hash-invalid archive replacement must fail"
        );

        let (root, manifest) = fixture(vec![member(
            "bravo",
            "sha256:2222222222222222222222222222222222222222222222222222222222222222",
            1.0,
            1.0,
            1.0,
            true,
        )]);
        execute_manifest(&manifest).unwrap();
        let archive_path = root.path().join(ARCHIVE_PATH);
        let mut bad_schema: Value =
            serde_json::from_slice(&fs::read(&archive_path).unwrap()).unwrap();
        bad_schema["schemaVersion"] = json!("wrong");
        bad_schema.as_object_mut().unwrap().remove("archiveSha256");
        bad_schema["archiveSha256"] = json!(sha_value(&bad_schema));
        write_value(&archive_path, &bad_schema);
        assert!(
            certify_archive_transport(&archive_path).is_err(),
            "wrong archive schema must fail"
        );

        let (root, manifest) = fixture(vec![member(
            "charlie",
            "sha256:3333333333333333333333333333333333333333333333333333333333333333",
            1.0,
            1.0,
            1.0,
            true,
        )]);
        execute_manifest(&manifest).unwrap();
        let archive_path = root.path().join(ARCHIVE_PATH);
        let mut noncanonical = fs::read(&archive_path).unwrap();
        noncanonical.push(b'\n');
        fs::write(&archive_path, noncanonical).unwrap();
        assert!(
            certify_archive_transport(&archive_path).is_err(),
            "noncanonical archive bytes must fail"
        );
        assert!(
            certify_archive_transport(&root.path().join("..\\archive.json")).is_err(),
            "traversal path must fail"
        );
    }

    #[cfg(windows)]
    #[test]
    fn authority_symlink_is_rejected() {
        let (root, manifest_path) = fixture(vec![member(
            "alpha",
            "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            1.0,
            1.0,
            1.0,
            true,
        )]);
        let actual = root
            .path()
            .join(temporal_qd_tail_reducer::TAIL_AUTHORITY_PATH);
        let linked = root.path().join("authority-link.json");
        if std::os::windows::fs::symlink_file(&actual, &linked).is_err() {
            // Windows hosts without Developer Mode may deny symlink creation;
            // the production guard is still covered by regular_file itself.
            return;
        }
        let mut manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        manifest["tailAuthority"]["receiptPath"] = json!(linked);
        manifest.as_object_mut().unwrap().remove("manifestSha256");
        manifest["manifestSha256"] = json!(sha_value(&manifest));
        write_value(&manifest_path, &manifest);
        let error = execute_manifest(&manifest_path).expect_err("authority symlink must fail");
        assert!(format!("{error:#}").contains("must not be a symlink"));
    }

    #[cfg(windows)]
    #[test]
    fn archive_transport_symlink_is_rejected() {
        let (root, manifest) = fixture(vec![member(
            "alpha",
            "sha256:1111111111111111111111111111111111111111111111111111111111111111",
            1.0,
            1.0,
            1.0,
            true,
        )]);
        execute_manifest(&manifest).unwrap();
        let archive_path = root.path().join(ARCHIVE_PATH);
        let linked = root.path().join("archive-link.json");
        if std::os::windows::fs::symlink_file(&archive_path, &linked).is_err() {
            return;
        }
        assert!(
            certify_archive_transport(&linked).is_err(),
            "archive transport must reject symlink sources"
        );
    }

    #[test]
    fn directional_quotas_preserve_each_breeding_lane() {
        let lanes = [
            "balanced_bidirectional",
            "balanced_bidirectional",
            "balanced_bidirectional",
            "long_specialist",
            "short_specialist",
        ];
        let mut rows = Vec::new();
        for (index, lane) in lanes.into_iter().enumerate() {
            let mut row = member(
                &format!("lane{index}"),
                &format!("sha256:{:064x}", index + 100),
                1. + index as f64,
                1.,
                index as f64,
                true,
            );
            row.as_object_mut().unwrap().insert(
                "directionBreedingLane".to_owned(),
                Value::String(lane.to_owned()),
            );
            rows.push(row);
        }
        let (root, manifest_path) = fixture(rows);
        let mut manifest: Value =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        let frozen = manifest["archivePolicy"]["frozenPolicy"]
            .as_object_mut()
            .unwrap();
        frozen.insert(
            "directionSelection".to_owned(),
            json!({"perCellBreedingQuotas":{"balanced_bidirectional":2,"long_specialist":1,"short_specialist":1}}),
        );
        let policy_sha = sha_value(&manifest["archivePolicy"]["frozenPolicy"]);
        manifest["archivePolicy"]["policySha256"] = Value::String(policy_sha);
        manifest["directionAware"] = Value::Bool(true);
        manifest.as_object_mut().unwrap().remove("manifestSha256");
        let manifest_sha = sha_value(&manifest);
        manifest
            .as_object_mut()
            .unwrap()
            .insert("manifestSha256".to_owned(), Value::String(manifest_sha));
        write_value(&manifest_path, &manifest);
        execute_manifest(&manifest_path).unwrap();
        let archive: Value =
            serde_json::from_slice(&fs::read(root.path().join(ARCHIVE_PATH)).unwrap()).unwrap();
        let retained: BTreeSet<_> = archive["cells"][0]["members"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|member| member["directionBreedingLane"].as_str())
            .collect();
        assert_eq!(
            retained,
            BTreeSet::from([
                "balanced_bidirectional",
                "long_specialist",
                "short_specialist"
            ])
        );
    }

    #[test]
    fn python_select_oracle_parity_small_and_one_hundred_twenty_eight() {
        let root = TempDir::new().unwrap();
        let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(4)
            .unwrap();
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/python_oracle_fixture.py");
        let python = if cfg!(windows) {
            let local = repo.join(".venv/Scripts/python.exe");
            if local.is_file() {
                local
            } else {
                PathBuf::from("python")
            }
        } else {
            PathBuf::from("python3")
        };
        let mut python_paths = vec![repo.to_path_buf()];
        if let Some(existing) = std::env::var_os("PYTHONPATH") {
            python_paths.extend(std::env::split_paths(&existing));
        }
        let python_path = std::env::join_paths(python_paths).unwrap();
        let status = Command::new(python)
            .arg(script)
            .arg(root.path())
            .current_dir(repo)
            .env("PYTHONPATH", python_path)
            .status()
            .unwrap();
        assert!(status.success());
        for case in ["small", "large"] {
            let path = root.path().join(case);
            upgrade_python_case_to_tail_authority(&path);
            execute_manifest(&path.join("manifest.json")).unwrap();
            let archive: Value =
                serde_json::from_slice(&fs::read(path.join(ARCHIVE_PATH)).unwrap()).unwrap();
            let expected: Value = serde_json::from_slice(
                &fs::read(path.join("python-select-expected.json")).unwrap(),
            )
            .unwrap();
            assert_eq!(
                archive["cells"], expected,
                "Python select oracle parity failed for {case}"
            );
        }
    }
}
