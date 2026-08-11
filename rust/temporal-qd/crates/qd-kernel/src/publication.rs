//! Byte-stable public generation artifact assembly.
//!
//! Rich candidate bytes are reopened one entry at a time from the compatibility
//! journal. G0 descriptor projections were derived and sealed by the kernel at
//! admission time, then travel only as compact accepted-reference material.

use std::{
    collections::BTreeMap,
    io::{SeekFrom, Write},
    time::Instant,
};

use temporal_qd_contract::{
    CanonicalSha256Writer, ContractError, Map, Value, canonical_json, canonical_sha256,
    canonical_sha256_streaming, write_canonical_json,
};

use crate::{
    g0::{
        build_accepted_pool, materialize_campaign_ledger,
        project_accepted_pair_entry_with_descriptor, select_g0_bootstrap,
    },
    journal::{
        AcceptedReference, JournalError, ProposalJournal, RewritableTemporaryWrite, WrittenArtifact,
    },
};

pub const POPULATION_SCHEMA: &str = "temporal_qd_generation_population_v3";
pub const EVALUATION_POPULATION_SCHEMA: &str = "temporal_qd_evaluation_population_v1";
pub const GENERATION_JOURNAL_SCHEMA: &str = "temporal_qd_generation_journal_v3";
pub const FRONT_GENERATION_RESULT_SCHEMA: &str = "temporal_qd_front_generation_result_v1";
const POPULATION_SHA256_PLACEHOLDER: &str =
    "sha256:0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Debug, thiserror::Error)]
pub enum PublicationError {
    #[error("canonical contract failure: {0}")]
    Canonical(#[from] ContractError),
    #[error("journal publication failure: {0}")]
    Journal(#[from] JournalError),
    #[error("G0 publication failure: {0}")]
    G0(#[from] crate::g0::G0Error),
    #[error("generation publication contract failure: {0}")]
    Contract(String),
}

pub type Result<T> = std::result::Result<T, PublicationError>;

fn contract(message: impl Into<String>) -> PublicationError {
    PublicationError::Contract(message.into())
}

fn publication_error_as_journal(error: PublicationError) -> JournalError {
    match error {
        PublicationError::Journal(error) => error,
        PublicationError::Canonical(error) => JournalError::Canonical(error),
        PublicationError::Contract(message) => JournalError::Contract(message),
        PublicationError::G0(error) => JournalError::Contract(error.to_string()),
    }
}

fn object(entries: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    let mut map = Map::new();
    for (key, value) in entries {
        map.insert(key.to_owned(), value);
    }
    Value::Object(map)
}

fn sha(value: &str, label: &str) -> Result<()> {
    if value.len() != 71
        || !value.starts_with("sha256:")
        || !value.as_bytes()[7..]
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(contract(format!(
            "{label} must be a lowercase SHA-256 identity"
        )));
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct PublicationPolicy {
    pub qd_version: String,
    pub policy_name: String,
    pub policy_sha256: String,
    pub pair_policy: Value,
    pub operator_implementation_identity: Value,
    pub predeclared_evidence_context_sha256: Option<String>,
    /// Direction-aware v5 generations bind the complete archive-policy
    /// authority into every public artifact.  Earlier authorities deliberately
    /// omit it, so this remains an explicit optional compatibility field.
    pub archive_policy_authority: Option<Value>,
}

impl PublicationPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.qd_version.trim().is_empty() || self.policy_name.trim().is_empty() {
            return Err(contract("publication policy labels must be nonempty"));
        }
        sha(&self.policy_sha256, "publication policy SHA-256")?;
        if !self.pair_policy.is_object() || !self.operator_implementation_identity.is_object() {
            return Err(contract(
                "pair policy and operator implementation identity must be objects",
            ));
        }
        if let Some(evidence) = &self.predeclared_evidence_context_sha256 {
            sha(evidence, "predeclared evidence context SHA-256")?;
        }
        if let Some(authority) = &self.archive_policy_authority {
            if !authority.is_object() {
                return Err(contract("archive policy authority must be an object"));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct PublicationRequest {
    pub request_sha256: String,
    pub config_sha256: String,
    pub generation_index: u64,
    pub target_unique_candidates: u64,
    pub max_proposal_attempts: u64,
    pub proposal_count: u64,
    pub origin_proposal_counts: BTreeMap<String, u64>,
    pub origin_accepted_counts: BTreeMap<String, u64>,
    pub disposition_counts: BTreeMap<String, u64>,
    pub entry_sha256s: Vec<String>,
    pub entry_ordinals: Vec<u64>,
    pub construction_references: Vec<AcceptedReference>,
    pub g0_evaluation_width: Option<u64>,
    /// Compact binding to a separately persisted Python-compatible global
    /// identity ledger.  The full ledger remains its own artifact; this exact
    /// summary is what the Python generation journal publishes.
    pub global_identity_ledger: Option<Value>,
    /// These two values were added by the v5/evolvable Python oracle after
    /// the original native front-half admission.  They are optional only for
    /// historical v4 compatibility; a v5 G0 funnel must provide both.
    pub reproduction_allocation: Option<Value>,
    pub reproduction_allocation_accounting: Option<Value>,
    /// The Python oracle's pair-genome count is the number of accepted
    /// executable pair semantics, not merely the number of construction
    /// references.  A native post-construction admission computes it while
    /// streaming the rich journal and supplies it here.
    pub unique_pair_genome_count: Option<u64>,
    pub policy: PublicationPolicy,
}

impl PublicationRequest {
    pub fn validate(&self) -> Result<()> {
        sha(&self.request_sha256, "generation request SHA-256")?;
        sha(&self.config_sha256, "generation config SHA-256")?;
        self.policy.validate()?;
        if self.proposal_count != self.entry_sha256s.len() as u64
            || self.proposal_count != self.entry_ordinals.len() as u64
        {
            return Err(contract(
                "proposal count does not bind rich entry ordinal/hash collections",
            ));
        }
        for identity in &self.entry_sha256s {
            sha(identity, "proposal entry SHA-256")?;
        }
        if self.construction_references.len() as u64 > self.proposal_count {
            return Err(contract(
                "accepted construction count exceeds proposal count",
            ));
        }
        if let Some(width) = self.g0_evaluation_width {
            if self.generation_index != 1
                || width == 0
                || width > self.construction_references.len() as u64
            {
                return Err(contract("G0 construction/evaluation widths are invalid"));
            }
        }
        if let Some(ledger) = &self.global_identity_ledger {
            let fields = ledger
                .as_object()
                .ok_or_else(|| contract("global identity ledger binding must be an object"))?;
            sha(
                fields
                    .get("identityLedgerSha256")
                    .and_then(Value::as_str)
                    .ok_or_else(|| contract("global identity ledger binding lacks identity"))?,
                "global identity ledger SHA-256",
            )?;
            for field in [
                "pairExecutableSemanticCount",
                "pairExecutableSemanticDuplicateRejections",
            ] {
                if fields.get(field).and_then(Value::as_u64).is_none() {
                    return Err(contract(format!(
                        "global identity ledger binding lacks integer {field}"
                    )));
                }
            }
        }
        match (
            &self.reproduction_allocation,
            &self.reproduction_allocation_accounting,
        ) {
            (None, None) => {}
            (Some(allocation), Some(accounting))
                if allocation.is_object() && accounting.is_object() => {}
            _ => {
                return Err(contract(
                    "reproduction allocation and accounting must be paired objects",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct PublishedGeneration {
    pub population_sha256: String,
    pub population_file_sha256: String,
    pub evaluation_population_sha256: String,
    pub generation_journal_sha256: String,
    pub selected_references: Vec<AcceptedReference>,
    pub g0_binding: Option<BTreeMap<String, String>>,
    pub population_artifact: WrittenArtifact,
    pub evaluation_artifact: WrittenArtifact,
    pub journal_artifact: WrittenArtifact,
    /// Untrusted G0 telemetry: source journal bytes reopened to stream the
    /// selected rich population after admission.  It is not public state.
    pub population_source_journal_bytes_read: u64,
    /// Untrusted wall-clock diagnostic for the rich population stream only.
    pub population_stream_seconds: f64,
}

/// Compact values produced while a G0 construction journal is admitted in a
/// single streaming pass.  Keeping this separate from the public request lets
/// the normal native front-half retain its historical behavior while the v5
/// post-construction funnel avoids reopening every rich entry to derive the
/// same descriptor and constructor distribution a second time.
#[derive(Clone, Debug)]
pub struct PrecomputedG0Admission {
    pub accepted_references: Vec<Value>,
    pub immigrant_construction_distribution: Option<Value>,
    /// Compact projections derived while each sealed journal row is already
    /// authenticated by the funnel.  Keeping these avoids reopening every
    /// selected rich entry merely to build the small evaluation artifact.
    /// They are keyed by immutable proposal ordinal and re-bound to the
    /// selected reference before publication.
    pub evaluation_candidates: BTreeMap<u64, Value>,
    pub funnel_entries: BTreeMap<u64, Value>,
}

struct G0Materialization {
    selected_references: Vec<AcceptedReference>,
    binding: BTreeMap<String, String>,
}

pub fn publish_generation(
    store: &ProposalJournal,
    request: &PublicationRequest,
) -> Result<PublishedGeneration> {
    publish_generation_with_precomputed_g0(store, request, None)
}

pub fn publish_generation_with_precomputed_g0(
    store: &ProposalJournal,
    request: &PublicationRequest,
    precomputed_g0: Option<&PrecomputedG0Admission>,
) -> Result<PublishedGeneration> {
    request.validate()?;
    let (selected_references, g0_binding) = if let Some(width) = request.g0_evaluation_width {
        let g0 = materialize_g0(
            store,
            request,
            width,
            precomputed_g0.map(|admission| admission.accepted_references.as_slice()),
        )?;
        (g0.selected_references, Some(g0.binding))
    } else {
        let mut references = request.construction_references.clone();
        references.sort_by(|left, right| left.candidate_id.cmp(&right.candidate_id));
        (references, None)
    };
    let immigrant_distribution = match precomputed_g0 {
        Some(admission) => admission.immigrant_construction_distribution.clone(),
        None => rich_immigrant_distribution(store, &request.entry_ordinals)?,
    };
    let mut population = population_template(
        request,
        &selected_references,
        g0_binding.as_ref(),
        immigrant_distribution.as_ref(),
    )?;
    // A full population can be tens or hundreds of megabytes.  For the G0
    // transaction, stream each selected rich candidate only once into a
    // private temporary file while simultaneously hashing the exact object
    // without its self-hash field.  The fixed-width placeholder is patched
    // before fsync/file hashing/write-once publication, so it can never escape
    // as a public artifact.  The non-G0 front-half retains its established
    // two-pass writer unchanged.
    let population_stream_started = Instant::now();
    let (population_sha256, population_artifact, population_source_journal_bytes_read) =
        if g0_binding.is_some() {
            population
                .as_object_mut()
                .expect("population is object")
                .insert(
                    "populationSha256".to_owned(),
                    Value::String(POPULATION_SHA256_PLACEHOLDER.to_owned()),
                );
            let mut population_sha256 = None;
            let mut source_journal_bytes_read = 0_u64;
            let artifact = store.write_canonical_once_streaming_rewritable(
                std::path::Path::new("population.json"),
                store.public_newline(),
                |writer| {
                    population_sha256 = Some(
                        stream_population_value_with_self_hash(
                            &population,
                            &selected_references,
                            store,
                            writer,
                            &mut source_journal_bytes_read,
                        )
                        .map_err(publication_error_as_journal)?,
                    );
                    Ok(())
                },
            )?;
            (
                population_sha256.ok_or_else(|| {
                    contract("G0 population stream did not produce its self-hash")
                })?,
                artifact,
                source_journal_bytes_read,
            )
        } else {
            let population_sha256 =
                population_stream_sha256(&population, &selected_references, store)?;
            population
                .as_object_mut()
                .expect("population is object")
                .insert(
                    "populationSha256".to_owned(),
                    Value::String(population_sha256.clone()),
                );
            let artifact = store.write_canonical_once_streaming(
                std::path::Path::new("population.json"),
                store.public_newline(),
                |writer| {
                    stream_population_value(&population, &selected_references, store, writer)
                        .map_err(publication_error_as_journal)
                },
            )?;
            (population_sha256, artifact, 0)
        };
    let population_stream_seconds = population_stream_started.elapsed().as_secs_f64();

    let evaluation = evaluation_value(
        request,
        &selected_references,
        &population_sha256,
        &population_artifact.file_sha256,
        g0_binding.as_ref(),
        store,
        precomputed_g0,
    )?;
    let evaluation_population_sha256 = canonical_sha256_streaming(&evaluation)?;
    let mut evaluation = evaluation;
    evaluation
        .as_object_mut()
        .expect("evaluation population is object")
        .insert(
            "evaluationPopulationSha256".to_owned(),
            Value::String(evaluation_population_sha256.clone()),
        );
    let evaluation_artifact = store.write_canonical_once(
        std::path::Path::new("evaluation-population.json"),
        &evaluation,
    )?;

    let generation_journal = generation_journal_value(
        request,
        &selected_references,
        &population_sha256,
        &population_artifact.file_sha256,
        &evaluation_population_sha256,
        g0_binding.as_ref(),
        immigrant_distribution.as_ref(),
        &evaluation,
    )?;
    let generation_journal_sha256 = canonical_sha256_streaming(&generation_journal)?;
    let mut generation_journal = generation_journal;
    generation_journal
        .as_object_mut()
        .expect("generation journal is object")
        .insert(
            "journalSha256".to_owned(),
            Value::String(generation_journal_sha256.clone()),
        );
    let journal_artifact = store.write_canonical_once(
        std::path::Path::new("generation-journal.json"),
        &generation_journal,
    )?;

    Ok(PublishedGeneration {
        population_sha256,
        population_file_sha256: population_artifact.file_sha256.clone(),
        evaluation_population_sha256,
        generation_journal_sha256,
        selected_references,
        g0_binding,
        population_artifact,
        evaluation_artifact,
        journal_artifact,
        population_source_journal_bytes_read,
        population_stream_seconds,
    })
}

fn materialize_g0(
    store: &ProposalJournal,
    request: &PublicationRequest,
    evaluation_width: u64,
    precomputed_references: Option<&[Value]>,
) -> Result<G0Materialization> {
    let construction_identity = canonical_sha256(&object([
        (
            "schemaVersion",
            Value::String("temporal_qd_g0_construction_identity_v1".to_owned()),
        ),
        ("configSha256", Value::String(request.config_sha256.clone())),
        ("generationIndex", Value::from(request.generation_index)),
        (
            "constructionPoolSize",
            Value::from(request.construction_references.len() as u64),
        ),
        ("evaluationPopulationSize", Value::from(evaluation_width)),
    ]))?;
    let mut g0_references = Vec::with_capacity(request.construction_references.len());
    let mut by_key = BTreeMap::new();
    let mut construction = request.construction_references.clone();
    construction.sort_by_key(|reference| reference.proposal_ordinal);
    for (index, reference) in construction.iter().enumerate() {
        let projected = if let Some(references) = precomputed_references {
            let value = references.get(index).ok_or_else(|| {
                contract("precomputed G0 accepted references do not bind construction count")
            })?;
            let fields = value
                .as_object()
                .ok_or_else(|| contract("precomputed G0 accepted reference is invalid"))?;
            if fields
                .get("constructionPoolIdentitySha256")
                .and_then(Value::as_str)
                != Some(construction_identity.as_str())
                || fields.get("proposalOrdinal").and_then(Value::as_u64)
                    != Some(reference.proposal_ordinal)
                || fields.get("candidateId").and_then(Value::as_str)
                    != Some(reference.candidate_id.as_str())
                || fields
                    .get("candidateIdentitySha256")
                    .and_then(Value::as_str)
                    != Some(reference.candidate_identity_sha256.as_str())
            {
                return Err(contract(
                    "precomputed G0 accepted reference diverges from construction reference",
                ));
            }
            value.clone()
        } else {
            let descriptor = reference.descriptor_projection.as_ref().ok_or_else(|| {
                contract(
                    "G0 requires an exact Dashboard descriptor projection captured during construction",
                )
            })?;
            let entry = store.read_public_entry(reference.proposal_ordinal)?;
            project_accepted_pair_entry_with_descriptor(
                &construction_identity,
                reference.proposal_ordinal,
                &format!("proposal-journal/{:08}.json", reference.proposal_ordinal),
                &entry,
                descriptor,
            )?
        };
        by_key.insert(
            (
                reference.proposal_ordinal,
                reference.candidate_id.clone(),
                reference.candidate_identity_sha256.clone(),
            ),
            reference.clone(),
        );
        g0_references.push(projected);
    }
    let pool = build_accepted_pool(&construction_identity, &g0_references)?;
    let selection = select_g0_bootstrap(&pool, evaluation_width, None)?;
    let selected_hashes = selection
        .as_object()
        .and_then(|fields| fields.get("selected"))
        .and_then(Value::as_array)
        .ok_or_else(|| contract("G0 selection lacks selected rows"))?
        .iter()
        .map(|row| {
            row.as_object()
                .and_then(|fields| fields.get("referenceSha256"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .ok_or_else(|| contract("G0 selected row lacks reference identity"))
        })
        .collect::<Result<Vec<_>>>()?;
    let ledger = materialize_campaign_ledger(&pool, &selected_hashes)?;
    store.write_canonical_once(
        std::path::Path::new("g0-bootstrap/accepted-pool.json"),
        &pool,
    )?;
    store.write_canonical_once(
        std::path::Path::new("g0-bootstrap/campaign-construction-ledger.json"),
        &ledger,
    )?;
    store.write_canonical_once(
        std::path::Path::new("g0-bootstrap/selection.json"),
        &selection,
    )?;
    let selected_rows = selection
        .as_object()
        .and_then(|fields| fields.get("selected"))
        .and_then(Value::as_array)
        .ok_or_else(|| contract("G0 selection lacks selected rows"))?;
    let mut selected = Vec::with_capacity(selected_rows.len());
    for row in selected_rows {
        let fields = row
            .as_object()
            .ok_or_else(|| contract("G0 selected row is invalid"))?;
        let ordinal = fields
            .get("proposalOrdinal")
            .and_then(Value::as_u64)
            .ok_or_else(|| contract("G0 selected row lacks ordinal"))?;
        let candidate_id = fields
            .get("candidateId")
            .and_then(Value::as_str)
            .ok_or_else(|| contract("G0 selected row lacks candidate ID"))?;
        let identity = fields
            .get("candidateIdentitySha256")
            .and_then(Value::as_str)
            .ok_or_else(|| contract("G0 selected row lacks candidate identity"))?;
        let key = (ordinal, candidate_id.to_owned(), identity.to_owned());
        selected.push(
            by_key
                .get(&key)
                .cloned()
                .ok_or_else(|| contract("G0 selection references a non-constructed candidate"))?,
        );
    }
    selected.sort_by(|left, right| left.candidate_id.cmp(&right.candidate_id));
    let binding = BTreeMap::from([
        (
            "constructionPoolIdentitySha256".to_owned(),
            construction_identity,
        ),
        (
            "acceptedPoolSha256".to_owned(),
            string_field(&pool, "acceptedPoolSha256", "G0 pool")?,
        ),
        (
            "selectionSha256".to_owned(),
            string_field(&selection, "selectionSha256", "G0 selection")?,
        ),
        (
            "ledgerSha256".to_owned(),
            string_field(&ledger, "ledgerSha256", "G0 ledger")?,
        ),
    ]);
    Ok(G0Materialization {
        selected_references: selected,
        binding,
    })
}

/// Shared public-schema envelope builder.  The historical file-backed
/// publisher streams rich candidates from a journal; the v5 transaction uses
/// the same envelope with a selected-only compact reconstruction stream.
pub(crate) fn population_template(
    request: &PublicationRequest,
    selected: &[AcceptedReference],
    g0_binding: Option<&BTreeMap<String, String>>,
    immigrant_distribution: Option<&Value>,
) -> Result<Value> {
    let mut population = object([
        ("schemaVersion", Value::String(POPULATION_SCHEMA.to_owned())),
        (
            "qdVersion",
            Value::String(request.policy.qd_version.clone()),
        ),
        (
            "policyName",
            Value::String(request.policy.policy_name.clone()),
        ),
        (
            "policySha256",
            Value::String(request.policy.policy_sha256.clone()),
        ),
        ("configSha256", Value::String(request.config_sha256.clone())),
        ("generationIndex", Value::from(request.generation_index)),
        ("targetUniqueCandidates", Value::from(selected.len() as u64)),
        (
            "maxProposalAttempts",
            Value::from(request.max_proposal_attempts),
        ),
        (
            "originCounts",
            Value::Object(
                request
                    .origin_accepted_counts
                    .iter()
                    .map(|(key, value)| (key.clone(), Value::from(*value)))
                    .collect(),
            ),
        ),
        (
            "proposalOrderCandidateIds",
            Value::Array(
                selected
                    .iter()
                    .map(|reference| Value::String(reference.candidate_id.clone()))
                    .collect(),
            ),
        ),
        ("candidateCount", Value::from(selected.len() as u64)),
        // The rich candidates are intentionally streamed from their sealed
        // proposal entries by `stream_population_value`; this placeholder
        // never reaches an artifact.
        ("candidates", Value::Null),
        ("authoredValidationBindingRequired", Value::Bool(false)),
        (
            "bidirectionalPairPolicy",
            request.policy.pair_policy.clone(),
        ),
        (
            "pairGenerationConfigSha256",
            Value::String(request.config_sha256.clone()),
        ),
        ("proposalAttempts", Value::from(request.proposal_count)),
        (
            "proposalSlots",
            proposal_slots(request, selected.len() as u64, g0_binding.is_some()),
        ),
    ]);
    if let Some(binding) = g0_binding {
        population
            .as_object_mut()
            .expect("population is object")
            .insert(
                "g0Bootstrap".to_owned(),
                Value::Object(
                    binding
                        .iter()
                        .map(|(key, value)| (key.clone(), Value::String(value.clone())))
                        .collect(),
                ),
            );
    } else if let Some(evidence) = &request.policy.predeclared_evidence_context_sha256 {
        population
            .as_object_mut()
            .expect("population is object")
            .insert(
                "predeclaredEvidenceContextSha256".to_owned(),
                Value::String(evidence.clone()),
            );
    }
    if let Some(distribution) = immigrant_distribution {
        population
            .as_object_mut()
            .expect("population is object")
            .insert(
                "immigrantConstructionDistribution".to_owned(),
                distribution.clone(),
            );
    }
    if let Some(allocation) = &request.reproduction_allocation {
        population
            .as_object_mut()
            .expect("population is object")
            .insert("reproductionAllocation".to_owned(), allocation.clone());
    }
    if let Some(accounting) = &request.reproduction_allocation_accounting {
        population
            .as_object_mut()
            .expect("population is object")
            .insert(
                "reproductionAllocationAccounting".to_owned(),
                accounting.clone(),
            );
    }
    if let Some(authority) = &request.policy.archive_policy_authority {
        population
            .as_object_mut()
            .expect("population is object")
            .insert("archivePolicyAuthority".to_owned(), authority.clone());
    }
    Ok(population)
}

fn population_stream_sha256(
    population: &Value,
    selected: &[AcceptedReference],
    store: &ProposalJournal,
) -> Result<String> {
    let mut writer = CanonicalSha256Writer::default();
    stream_population_value(population, selected, store, &mut writer)?;
    Ok(writer.finish())
}

/// One-pass G0 population writer.  The caller supplies a private seekable
/// temporary file containing a fixed-width `populationSha256` placeholder.
/// This streams the final rich candidate bytes once to both the temporary and
/// a semantic hash that omits exactly that self-hash field, then patches the
/// placeholder before the journal publisher fsyncs or exposes the file.
fn stream_population_value_with_self_hash(
    population: &Value,
    selected: &[AcceptedReference],
    store: &ProposalJournal,
    writer: &mut dyn RewritableTemporaryWrite,
    source_journal_bytes_read: &mut u64,
) -> Result<String> {
    let fields = population
        .as_object()
        .ok_or_else(|| contract("population template must be an object"))?;
    if fields.get("populationSha256").and_then(Value::as_str) != Some(POPULATION_SHA256_PLACEHOLDER)
    {
        return Err(contract(
            "G0 population temporary lacks the exact self-hash placeholder",
        ));
    }
    let ordered: BTreeMap<&str, &Value> = fields
        .iter()
        .map(|(key, value)| (key.as_str(), value))
        .collect();
    let mut semantic = CanonicalSha256Writer::default();
    stream_bytes(writer, b"{")?;
    stream_bytes(&mut semantic, b"{")?;
    let mut output_first = true;
    let mut semantic_first = true;
    let mut placeholder_offset = None;
    for (key, value) in ordered {
        if !output_first {
            stream_bytes(writer, b",")?;
        }
        output_first = false;
        write_canonical_json(&Value::String(key.to_owned()), writer)?;
        stream_bytes(writer, b":")?;

        let is_self_hash = key == "populationSha256";
        if !is_self_hash {
            if !semantic_first {
                stream_bytes(&mut semantic, b",")?;
            }
            semantic_first = false;
            write_canonical_json(&Value::String(key.to_owned()), &mut semantic)?;
            stream_bytes(&mut semantic, b":")?;
        }
        if key == "candidates" {
            stream_rich_candidate_array_tee(
                selected,
                store,
                writer,
                &mut semantic,
                source_journal_bytes_read,
            )?;
        } else if is_self_hash {
            let before_value = writer
                .stream_position()
                .map_err(JournalError::Io)
                .map_err(PublicationError::Journal)?;
            write_canonical_json(value, writer)?;
            // Canonical JSON strings are quoted and the placeholder has the
            // same 71-byte `sha256:` shape as every real identity.
            placeholder_offset = Some(
                before_value
                    .checked_add(1)
                    .ok_or_else(|| contract("G0 population placeholder offset overflow"))?,
            );
        } else {
            write_canonical_json(value, writer)?;
            write_canonical_json(value, &mut semantic)?;
        }
    }
    stream_bytes(writer, b"}")?;
    stream_bytes(&mut semantic, b"}")?;
    let population_sha256 = semantic.finish();
    if population_sha256.len() != POPULATION_SHA256_PLACEHOLDER.len() {
        return Err(contract(
            "G0 population self-hash does not match its fixed placeholder width",
        ));
    }
    let offset = placeholder_offset
        .ok_or_else(|| contract("G0 population stream never wrote self-hash placeholder"))?;
    writer
        .seek(SeekFrom::Start(offset))
        .map_err(JournalError::Io)
        .map_err(PublicationError::Journal)?;
    writer
        .write_all(population_sha256.as_bytes())
        .map_err(JournalError::Io)
        .map_err(PublicationError::Journal)?;
    writer
        .seek(SeekFrom::End(0))
        .map_err(JournalError::Io)
        .map_err(PublicationError::Journal)?;
    Ok(population_sha256)
}

/// Serialize the population object in exact canonical field order while
/// reopening one rich candidate at a time. The small envelope is still an
/// ordinary `Value`; only its high-cardinality candidate array is virtual.
fn stream_population_value(
    population: &Value,
    selected: &[AcceptedReference],
    store: &ProposalJournal,
    writer: &mut dyn Write,
) -> Result<()> {
    let fields = population
        .as_object()
        .ok_or_else(|| contract("population template must be an object"))?;
    let ordered: BTreeMap<&str, &Value> = fields
        .iter()
        .map(|(key, value)| (key.as_str(), value))
        .collect();
    stream_bytes(writer, b"{")?;
    let mut first = true;
    for (key, value) in ordered {
        if !first {
            stream_bytes(writer, b",")?;
        }
        first = false;
        write_canonical_json(&Value::String(key.to_owned()), writer)?;
        stream_bytes(writer, b":")?;
        if key == "candidates" {
            stream_rich_candidate_array(selected, store, writer)?;
        } else {
            write_canonical_json(value, writer)?;
        }
    }
    stream_bytes(writer, b"}")
}

fn stream_rich_candidate_array(
    selected: &[AcceptedReference],
    store: &ProposalJournal,
    writer: &mut dyn Write,
) -> Result<()> {
    stream_bytes(writer, b"[")?;
    for (index, reference) in selected.iter().enumerate() {
        if index != 0 {
            stream_bytes(writer, b",")?;
        }
        stream_rich_candidate(reference, store, writer)?;
    }
    stream_bytes(writer, b"]")
}

fn stream_rich_candidate_array_tee(
    selected: &[AcceptedReference],
    store: &ProposalJournal,
    output: &mut dyn RewritableTemporaryWrite,
    semantic: &mut CanonicalSha256Writer,
    source_journal_bytes_read: &mut u64,
) -> Result<()> {
    stream_bytes(output, b"[")?;
    stream_bytes(semantic, b"[")?;
    for (index, reference) in selected.iter().enumerate() {
        if index != 0 {
            stream_bytes(output, b",")?;
            stream_bytes(semantic, b",")?;
        }
        let (entry, entry_bytes) =
            store.read_public_entry_with_bytes(reference.proposal_ordinal)?;
        *source_journal_bytes_read = source_journal_bytes_read
            .checked_add(entry_bytes)
            .ok_or_else(|| contract("G0 population source journal byte counter overflow"))?;
        let fields = entry
            .as_object()
            .ok_or_else(|| contract("rich proposal entry is invalid"))?;
        if fields.get("entrySha256").and_then(Value::as_str) != Some(&reference.entry_sha256) {
            return Err(contract("accepted reference entry identity drifted"));
        }
        let candidate = fields
            .get("candidate")
            .ok_or_else(|| contract("accepted proposal entry lacks rich candidate"))?;
        if candidate
            .as_object()
            .and_then(|candidate| candidate.get("candidateId"))
            .and_then(Value::as_str)
            != Some(&reference.candidate_id)
        {
            return Err(contract("accepted reference candidate ID drifted"));
        }
        let mut writer = TeeWriter {
            left: output,
            right: semantic,
        };
        write_canonical_json(candidate, &mut writer)?;
    }
    stream_bytes(output, b"]")?;
    stream_bytes(semantic, b"]")
}

fn stream_rich_candidate(
    reference: &AcceptedReference,
    store: &ProposalJournal,
    writer: &mut dyn Write,
) -> Result<()> {
    let entry = store.read_public_entry(reference.proposal_ordinal)?;
    let fields = entry
        .as_object()
        .ok_or_else(|| contract("rich proposal entry is invalid"))?;
    if fields.get("entrySha256").and_then(Value::as_str) != Some(&reference.entry_sha256) {
        return Err(contract("accepted reference entry identity drifted"));
    }
    let candidate = fields
        .get("candidate")
        .ok_or_else(|| contract("accepted proposal entry lacks rich candidate"))?;
    if candidate
        .as_object()
        .and_then(|candidate| candidate.get("candidateId"))
        .and_then(Value::as_str)
        != Some(&reference.candidate_id)
    {
        return Err(contract("accepted reference candidate ID drifted"));
    }
    write_canonical_json(candidate, writer)?;
    Ok(())
}

fn stream_bytes<W: Write + ?Sized>(writer: &mut W, bytes: &[u8]) -> Result<()> {
    writer
        .write_all(bytes)
        .map_err(JournalError::Io)
        .map_err(PublicationError::Journal)
}

struct TeeWriter<'a, Left: Write + ?Sized, Right: Write + ?Sized> {
    left: &'a mut Left,
    right: &'a mut Right,
}

impl<Left: Write + ?Sized, Right: Write + ?Sized> Write for TeeWriter<'_, Left, Right> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.left.write_all(bytes)?;
        self.right.write_all(bytes)?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.left.flush()?;
        self.right.flush()
    }
}

fn evaluation_value(
    request: &PublicationRequest,
    selected: &[AcceptedReference],
    population_sha256: &str,
    population_file_sha256: &str,
    g0_binding: Option<&BTreeMap<String, String>>,
    store: &ProposalJournal,
    precomputed_g0: Option<&PrecomputedG0Admission>,
) -> Result<Value> {
    let candidates = match precomputed_g0 {
        Some(admission) => selected
            .iter()
            .map(|reference| {
                let candidate = admission
                    .evaluation_candidates
                    .get(&reference.proposal_ordinal)
                    .cloned()
                    .ok_or_else(|| contract("precomputed G0 evaluation candidate is missing"))?;
                validate_precomputed_evaluation_candidate(&candidate, reference)?;
                Ok(candidate)
            })
            .collect::<Result<Vec<_>>>()?,
        None => selected
            .iter()
            .map(|reference| evaluation_candidate(store, reference))
            .collect::<Result<Vec<_>>>()?,
    };
    let funnel_ordinals = if g0_binding.is_some() {
        selected
            .iter()
            .map(|reference| reference.proposal_ordinal)
            .collect::<Vec<_>>()
    } else {
        request.entry_ordinals.clone()
    };
    let funnel_entries = match precomputed_g0 {
        Some(admission) if g0_binding.is_some() => selected
            .iter()
            .map(|reference| {
                let funnel = admission
                    .funnel_entries
                    .get(&reference.proposal_ordinal)
                    .cloned()
                    .ok_or_else(|| contract("precomputed G0 funnel entry is missing"))?;
                validate_precomputed_funnel_entry(&funnel, reference)?;
                Ok(funnel)
            })
            .collect::<Result<Vec<_>>>()?,
        _ => funnel_ordinals
            .iter()
            .map(|ordinal| funnel_entry(store, *ordinal))
            .collect::<Result<Vec<_>>>()?,
    };
    let mut value = object([
        (
            "schemaVersion",
            Value::String(EVALUATION_POPULATION_SCHEMA.to_owned()),
        ),
        ("generationIndex", Value::from(request.generation_index)),
        ("candidateCount", Value::from(candidates.len() as u64)),
        (
            "populationSha256",
            Value::String(population_sha256.to_owned()),
        ),
        (
            "populationFileSha256",
            Value::String(population_file_sha256.to_owned()),
        ),
        (
            "pairGenerationConfigSha256",
            Value::String(request.config_sha256.clone()),
        ),
        (
            "policyName",
            Value::String(request.policy.policy_name.clone()),
        ),
        (
            "policySha256",
            Value::String(request.policy.policy_sha256.clone()),
        ),
        (
            "bidirectionalPairPolicy",
            request.policy.pair_policy.clone(),
        ),
        (
            "pairPolicySha256",
            Value::String(canonical_sha256(&request.policy.pair_policy)?),
        ),
        (
            "operatorImplementationSha256",
            Value::String(canonical_sha256(
                &request.policy.operator_implementation_identity,
            )?),
        ),
        (
            "predeclaredEvidenceContextSha256",
            request
                .policy
                .predeclared_evidence_context_sha256
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        ),
        ("candidates", Value::Array(candidates)),
        ("proposalAttempts", Value::from(funnel_entries.len() as u64)),
        ("funnelEntries", Value::Array(funnel_entries)),
    ]);
    if let Some(binding) = g0_binding {
        value.as_object_mut().expect("evaluation is object").insert(
            "g0Bootstrap".to_owned(),
            Value::Object(
                binding
                    .iter()
                    .map(|(key, value)| (key.clone(), Value::String(value.clone())))
                    .collect(),
            ),
        );
    }
    if let Some(authority) = &request.policy.archive_policy_authority {
        value
            .as_object_mut()
            .expect("evaluation is object")
            .insert("archivePolicyAuthority".to_owned(), authority.clone());
    }
    Ok(value)
}

#[allow(clippy::too_many_arguments)]
fn generation_journal_value(
    request: &PublicationRequest,
    selected: &[AcceptedReference],
    population_sha256: &str,
    population_file_sha256: &str,
    evaluation_population_sha256: &str,
    g0_binding: Option<&BTreeMap<String, String>>,
    immigrant_distribution: Option<&Value>,
    evaluation: &Value,
) -> Result<Value> {
    let g0 = g0_binding.is_some();
    let journal_proposal_count = if g0 {
        selected.len() as u64
    } else {
        request.proposal_count
    };
    let journal_origin_accepted_counts = if g0 {
        BTreeMap::from([("random_immigrant".to_owned(), selected.len() as u64)])
    } else {
        request.origin_accepted_counts.clone()
    };
    let journal_entry_sha256s = if g0 {
        selected
            .iter()
            .map(|reference| reference.entry_sha256.clone())
            .collect::<Vec<_>>()
    } else {
        request.entry_sha256s.clone()
    };
    let candidate_bindings = evaluation
        .as_object()
        .and_then(|fields| fields.get("candidates"))
        .and_then(Value::as_array)
        .ok_or_else(|| contract("evaluation population lacks candidates"))?
        .iter()
        .map(|candidate| {
            let fields = candidate
                .as_object()
                .ok_or_else(|| contract("evaluation candidate is invalid"))?;
            let candidate_id = fields
                .get("candidateId")
                .and_then(Value::as_str)
                .ok_or_else(|| contract("evaluation candidate lacks candidate ID"))?;
            let ordinal = fields
                .get("proposalOrdinal")
                .and_then(Value::as_u64)
                .ok_or_else(|| contract("evaluation candidate lacks proposal ordinal"))?;
            let entry_sha = fields
                .get("proposalEntrySha256")
                .and_then(Value::as_str)
                .ok_or_else(|| contract("evaluation candidate lacks proposal entry SHA"))?;
            Ok(object([
                ("candidateId", Value::String(candidate_id.to_owned())),
                ("proposalOrdinal", Value::from(ordinal)),
                ("proposalEntrySha256", Value::String(entry_sha.to_owned())),
                (
                    "candidateProjectionSha256",
                    Value::String(canonical_sha256(candidate)?),
                ),
            ]))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut journal = object([
        (
            "schemaVersion",
            Value::String(GENERATION_JOURNAL_SCHEMA.to_owned()),
        ),
        (
            "qdVersion",
            Value::String(request.policy.qd_version.clone()),
        ),
        (
            "policyName",
            Value::String(request.policy.policy_name.clone()),
        ),
        (
            "policySha256",
            Value::String(request.policy.policy_sha256.clone()),
        ),
        ("configSha256", Value::String(request.config_sha256.clone())),
        ("generationIndex", Value::from(request.generation_index)),
        ("proposalCount", Value::from(journal_proposal_count)),
        ("acceptedCount", Value::from(selected.len() as u64)),
        (
            "maxProposalAttempts",
            Value::from(request.max_proposal_attempts),
        ),
        ("nextImmigrantContinuationOrdinal", Value::from(0_u64)),
        (
            "originProposalCounts",
            counts_value(&request.origin_proposal_counts),
        ),
        (
            "originAcceptedCounts",
            counts_value(&journal_origin_accepted_counts),
        ),
        (
            "dispositionCounts",
            counts_value(&request.disposition_counts),
        ),
        (
            "proposalSlots",
            proposal_slots(request, selected.len() as u64, g0_binding.is_some()),
        ),
        (
            "uniqueIdentityCounts",
            object([
                ("candidateIdentity", Value::from(selected.len() as u64)),
                (
                    "pairGenome",
                    Value::from(
                        request
                            .unique_pair_genome_count
                            .unwrap_or(request.construction_references.len() as u64),
                    ),
                ),
            ]),
        ),
        (
            "duplicateCounters",
            object([
                (
                    "candidateIdentity",
                    Value::from(
                        request
                            .disposition_counts
                            .get("duplicate_candidate_identity")
                            .copied()
                            .unwrap_or(0),
                    ),
                ),
                (
                    "pairGenome",
                    Value::from(
                        request
                            .disposition_counts
                            .get("duplicate_pair_genome")
                            .copied()
                            .unwrap_or(0),
                    ),
                ),
                (
                    "pairGenomeGlobal",
                    Value::from(
                        request
                            .disposition_counts
                            .get("duplicate_pair_genome_global")
                            .copied()
                            .unwrap_or(0),
                    ),
                ),
            ]),
        ),
        (
            "proposalSlotCounters",
            object([
                ("proposalsObserved", Value::from(request.proposal_count)),
                (
                    "maxProposalAttempts",
                    Value::from(request.max_proposal_attempts),
                ),
            ]),
        ),
        (
            "entrySha256s",
            Value::Array(
                journal_entry_sha256s
                    .into_iter()
                    .map(Value::String)
                    .collect(),
            ),
        ),
        (
            "evaluationCandidateBindings",
            Value::Array(candidate_bindings),
        ),
        (
            "operatorImplementation",
            request.policy.operator_implementation_identity.clone(),
        ),
        (
            "populationSha256",
            Value::String(population_sha256.to_owned()),
        ),
        (
            "populationFileSha256",
            Value::String(population_file_sha256.to_owned()),
        ),
        (
            "evaluationPopulationSha256",
            Value::String(evaluation_population_sha256.to_owned()),
        ),
        (
            "predeclaredEvidenceContextSha256",
            request
                .policy
                .predeclared_evidence_context_sha256
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        ),
    ]);
    if let Some(binding) = g0_binding {
        journal.as_object_mut().expect("journal is object").insert(
            "g0Bootstrap".to_owned(),
            Value::Object(
                binding
                    .iter()
                    .map(|(key, value)| (key.clone(), Value::String(value.clone())))
                    .collect(),
            ),
        );
        let fields = journal.as_object_mut().expect("journal is object");
        fields.insert(
            "constructionProposalCount".to_owned(),
            Value::from(request.proposal_count),
        );
        fields.insert(
            "constructedAcceptedCount".to_owned(),
            Value::from(request.construction_references.len() as u64),
        );
        fields.insert(
            "constructionOriginAcceptedCounts".to_owned(),
            counts_value(&request.origin_accepted_counts),
        );
        fields.insert(
            "constructionEntrySha256s".to_owned(),
            Value::Array(
                request
                    .entry_sha256s
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
    }
    if let Some(distribution) = immigrant_distribution {
        journal.as_object_mut().expect("journal is object").insert(
            "immigrantConstructionDistribution".to_owned(),
            distribution.clone(),
        );
    }
    if let Some(allocation) = &request.reproduction_allocation {
        journal
            .as_object_mut()
            .expect("journal is object")
            .insert("reproductionAllocation".to_owned(), allocation.clone());
    }
    if let Some(accounting) = &request.reproduction_allocation_accounting {
        journal.as_object_mut().expect("journal is object").insert(
            "reproductionAllocationAccounting".to_owned(),
            accounting.clone(),
        );
    }
    if let Some(authority) = &request.policy.archive_policy_authority {
        journal
            .as_object_mut()
            .expect("journal is object")
            .insert("archivePolicyAuthority".to_owned(), authority.clone());
    }
    if let Some(global_identity_ledger) = &request.global_identity_ledger {
        journal.as_object_mut().expect("journal is object").insert(
            "globalIdentityLedger".to_owned(),
            global_identity_ledger.clone(),
        );
    }
    Ok(journal)
}

/// Streaming reducer for the constructor audit retained in the rich proposal
/// journal.  The post-construction G0 transaction feeds this reducer while it
/// is already admitting each durable entry, so it never has to reopen the
/// whole journal merely to produce the public breadth artifact.
#[derive(Default)]
pub struct RichImmigrantDistributionAccumulator {
    attempted: RichImmigrantDistribution,
    accepted: RichImmigrantDistribution,
}

#[derive(Default)]
struct RichImmigrantDistributionSide {
    module_count: u64,
    seed_name_counts: BTreeMap<String, u64>,
    evidence_group_counts: BTreeMap<String, u64>,
    event_binding_counts: BTreeMap<String, u64>,
    hold_kind_counts: BTreeMap<String, u64>,
    planned_grammar_depth_counts: BTreeMap<String, u64>,
    applied_grammar_depth_counts: BTreeMap<String, u64>,
    grammar_operation_family_counts: BTreeMap<String, u64>,
    planned_indicator_depth_counts: BTreeMap<String, u64>,
    applied_indicator_depth_counts: BTreeMap<String, u64>,
    indicator_operator_counts: BTreeMap<String, u64>,
    indicator_construction_kind_counts: BTreeMap<String, u64>,
    indicator_count_counts: BTreeMap<String, u64>,
    evidence_group_member_shape_counts: BTreeMap<String, u64>,
}

impl RichImmigrantDistributionSide {
    fn merge(&mut self, other: Self) -> Result<()> {
        self.module_count = self
            .module_count
            .checked_add(other.module_count)
            .ok_or_else(|| contract("rich immigrant distribution module count overflow"))?;
        merge_count_map(&mut self.seed_name_counts, other.seed_name_counts)?;
        merge_count_map(&mut self.evidence_group_counts, other.evidence_group_counts)?;
        merge_count_map(&mut self.event_binding_counts, other.event_binding_counts)?;
        merge_count_map(&mut self.hold_kind_counts, other.hold_kind_counts)?;
        merge_count_map(
            &mut self.planned_grammar_depth_counts,
            other.planned_grammar_depth_counts,
        )?;
        merge_count_map(
            &mut self.applied_grammar_depth_counts,
            other.applied_grammar_depth_counts,
        )?;
        merge_count_map(
            &mut self.grammar_operation_family_counts,
            other.grammar_operation_family_counts,
        )?;
        merge_count_map(
            &mut self.planned_indicator_depth_counts,
            other.planned_indicator_depth_counts,
        )?;
        merge_count_map(
            &mut self.applied_indicator_depth_counts,
            other.applied_indicator_depth_counts,
        )?;
        merge_count_map(
            &mut self.indicator_operator_counts,
            other.indicator_operator_counts,
        )?;
        merge_count_map(
            &mut self.indicator_construction_kind_counts,
            other.indicator_construction_kind_counts,
        )?;
        merge_count_map(
            &mut self.indicator_count_counts,
            other.indicator_count_counts,
        )?;
        merge_count_map(
            &mut self.evidence_group_member_shape_counts,
            other.evidence_group_member_shape_counts,
        )?;
        Ok(())
    }

    fn value(self) -> Value {
        object([
            ("moduleCount", Value::from(self.module_count)),
            ("seedNameCounts", counts_value(&self.seed_name_counts)),
            (
                "evidenceGroupCounts",
                counts_value(&self.evidence_group_counts),
            ),
            (
                "eventBindingCounts",
                counts_value(&self.event_binding_counts),
            ),
            ("holdKindCounts", counts_value(&self.hold_kind_counts)),
            (
                "plannedGrammarDepthCounts",
                counts_value(&self.planned_grammar_depth_counts),
            ),
            (
                "appliedGrammarDepthCounts",
                counts_value(&self.applied_grammar_depth_counts),
            ),
            (
                "grammarOperationFamilyCounts",
                counts_value(&self.grammar_operation_family_counts),
            ),
            (
                "plannedIndicatorDepthCounts",
                counts_value(&self.planned_indicator_depth_counts),
            ),
            (
                "appliedIndicatorDepthCounts",
                counts_value(&self.applied_indicator_depth_counts),
            ),
            (
                "indicatorOperatorCounts",
                counts_value(&self.indicator_operator_counts),
            ),
            (
                "indicatorConstructionKindCounts",
                counts_value(&self.indicator_construction_kind_counts),
            ),
            (
                "indicatorCountCounts",
                counts_value(&self.indicator_count_counts),
            ),
            (
                "evidenceGroupMemberShapeCounts",
                counts_value(&self.evidence_group_member_shape_counts),
            ),
        ])
    }

    fn add_module(&mut self, module: &Map<String, Value>) -> Result<()> {
        self.module_count += 1;
        let selector = object_field(module, "selector");
        increment_counter(
            &mut self.seed_name_counts,
            value_field(selector, "seedName"),
        )?;
        increment_counter(
            &mut self.evidence_group_counts,
            value_field(selector, "groupId"),
        )?;
        increment_counter(
            &mut self.event_binding_counts,
            value_field(selector, "eventId"),
        )?;

        let grammar = object_field(module, "grammar");
        let indicator = object_field(module, "indicator");
        let shape = object_field(module, "profileShape");
        increment_counter(&mut self.hold_kind_counts, value_field(shape, "holdKind"))?;
        increment_counter(
            &mut self.planned_grammar_depth_counts,
            value_field(grammar, "plannedDepth"),
        )?;
        increment_counter(
            &mut self.applied_grammar_depth_counts,
            value_field(grammar, "appliedDepth"),
        )?;
        if let Some(steps) = grammar.and_then(|fields| fields.get("steps")) {
            if let Some(steps) = steps.as_array() {
                for step in steps {
                    if let Some(step) = step.as_object() {
                        increment_counter(
                            &mut self.grammar_operation_family_counts,
                            step.get("operationFamily"),
                        )?;
                    }
                }
            }
        }
        increment_counter(
            &mut self.planned_indicator_depth_counts,
            value_field(indicator, "plannedDepth"),
        )?;
        increment_counter(
            &mut self.applied_indicator_depth_counts,
            value_field(indicator, "appliedDepth"),
        )?;
        if let Some(steps) = indicator.and_then(|fields| fields.get("steps")) {
            if let Some(steps) = steps.as_array() {
                for step in steps {
                    if let Some(step) = step.as_object() {
                        increment_counter(
                            &mut self.indicator_operator_counts,
                            step.get("operatorId"),
                        )?;
                        increment_counter(
                            &mut self.indicator_construction_kind_counts,
                            step.get("constructionKind"),
                        )?;
                    }
                }
            }
        }
        increment_counter(
            &mut self.indicator_count_counts,
            value_field(shape, "indicatorCount"),
        )?;
        if let Some(members) = shape
            .and_then(|fields| fields.get("evidenceGroupMemberCounts"))
            .filter(|value| !value.is_null())
        {
            increment_counter(&mut self.evidence_group_member_shape_counts, Some(members))?;
        } else {
            let empty_members = Value::Array(Vec::new());
            increment_counter(
                &mut self.evidence_group_member_shape_counts,
                Some(&empty_members),
            )?;
        }
        Ok(())
    }
}

#[derive(Default)]
struct RichImmigrantDistribution {
    proposal_count: u64,
    long: RichImmigrantDistributionSide,
    short: RichImmigrantDistributionSide,
}

impl RichImmigrantDistribution {
    fn merge(&mut self, other: Self) -> Result<()> {
        self.proposal_count = self
            .proposal_count
            .checked_add(other.proposal_count)
            .ok_or_else(|| contract("rich immigrant distribution proposal count overflow"))?;
        self.long.merge(other.long)?;
        self.short.merge(other.short)
    }

    fn value(self) -> Value {
        object([
            ("proposalCount", Value::from(self.proposal_count)),
            (
                "sides",
                object([("long", self.long.value()), ("short", self.short.value())]),
            ),
        ])
    }

    fn add(&mut self, modules: &Map<String, Value>) -> Result<()> {
        self.proposal_count += 1;
        if let Some(module) = modules.get("long").and_then(Value::as_object) {
            self.long.add_module(module)?;
        }
        if let Some(module) = modules.get("short").and_then(Value::as_object) {
            self.short.add_module(module)?;
        }
        Ok(())
    }
}

fn object_field<'a>(fields: &'a Map<String, Value>, name: &str) -> Option<&'a Map<String, Value>> {
    fields.get(name).and_then(Value::as_object)
}

fn value_field<'a>(fields: Option<&'a Map<String, Value>>, name: &str) -> Option<&'a Value> {
    fields.and_then(|fields| fields.get(name))
}

fn python_counter_key(value: Option<&Value>) -> Result<String> {
    match value.unwrap_or(&Value::Null) {
        Value::Null => Ok("None".to_owned()),
        Value::Bool(value) => Ok(if *value { "True" } else { "False" }.to_owned()),
        Value::String(value) => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        rich @ (Value::Array(_) | Value::Object(_)) => Ok(canonical_json(rich)?),
    }
}

fn increment_counter(target: &mut BTreeMap<String, u64>, value: Option<&Value>) -> Result<()> {
    let key = python_counter_key(value)?;
    *target.entry(key).or_default() += 1;
    Ok(())
}

fn merge_count_map(
    target: &mut BTreeMap<String, u64>,
    source: BTreeMap<String, u64>,
) -> Result<()> {
    for (key, count) in source {
        let current = target.entry(key).or_default();
        *current = current
            .checked_add(count)
            .ok_or_else(|| contract("rich immigrant distribution count overflow"))?;
    }
    Ok(())
}

impl RichImmigrantDistributionAccumulator {
    /// Observe one immutable proposal entry.  Entries without a rich factory
    /// audit intentionally do not participate, matching the Python oracle's
    /// historical optional-distribution behavior.
    pub fn observe(&mut self, entry: &Value) -> Result<()> {
        let modules = entry
            .as_object()
            .and_then(|entry| entry.get("proposal"))
            .and_then(Value::as_object)
            .and_then(|proposal| proposal.get("factoryConstructionAudit"))
            .and_then(Value::as_object)
            .and_then(|audit| audit.get("sides"))
            .and_then(Value::as_object);
        let Some(modules) = modules else {
            return Ok(());
        };
        self.attempted.add(modules)?;
        if entry
            .as_object()
            .and_then(|fields| fields.get("disposition"))
            .and_then(Value::as_str)
            == Some("accepted")
        {
            self.accepted.add(modules)?;
        }
        Ok(())
    }

    /// Merge independently admitted journal ranges.  All exposed state is a
    /// commutative count reducer, so this preserves the exact serial public
    /// distribution while allowing bounded parallel source admission.
    pub fn merge(&mut self, other: Self) -> Result<()> {
        self.attempted.merge(other.attempted)?;
        self.accepted.merge(other.accepted)
    }

    pub fn finish(self) -> Result<Option<Value>> {
        if self.attempted.proposal_count == 0 {
            return Ok(None);
        }
        let mut distribution = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_rich_immigrant_distribution_v1".to_owned()),
            ),
            ("attempted", self.attempted.value()),
            ("accepted", self.accepted.value()),
        ]);
        let distribution_sha256 = canonical_sha256(&distribution)?;
        distribution
            .as_object_mut()
            .expect("immigrant distribution is object")
            .insert(
                "distributionSha256".to_owned(),
                Value::String(distribution_sha256),
            );
        Ok(Some(distribution))
    }
}

/// Reduce the constructor audit retained in the rich proposal journal into the
/// legacy immigrant-breadth artifact.  This deliberately reads the durable
/// rich entries rather than construction-time summaries: the public aggregate
/// must describe exactly the bytes that were admitted to the journal.
pub fn rich_immigrant_distribution(
    store: &ProposalJournal,
    entry_ordinals: &[u64],
) -> Result<Option<Value>> {
    let mut distribution = RichImmigrantDistributionAccumulator::default();
    for ordinal in entry_ordinals {
        let entry = store.read_public_entry(*ordinal)?;
        distribution.observe(&entry)?;
    }
    distribution.finish()
}

fn rich_candidate(store: &ProposalJournal, reference: &AcceptedReference) -> Result<Value> {
    let entry = store.read_public_entry(reference.proposal_ordinal)?;
    rich_candidate_from_entry(&entry, reference)
}

fn rich_candidate_from_entry(entry: &Value, reference: &AcceptedReference) -> Result<Value> {
    let fields = entry
        .as_object()
        .ok_or_else(|| contract("rich proposal entry is invalid"))?;
    if fields.get("entrySha256").and_then(Value::as_str) != Some(&reference.entry_sha256) {
        return Err(contract("accepted reference entry identity drifted"));
    }
    let candidate = fields
        .get("candidate")
        .cloned()
        .ok_or_else(|| contract("accepted proposal entry lacks rich candidate"))?;
    if candidate
        .as_object()
        .and_then(|candidate| candidate.get("candidateId"))
        .and_then(Value::as_str)
        != Some(&reference.candidate_id)
    {
        return Err(contract("accepted reference candidate ID drifted"));
    }
    Ok(candidate)
}

fn evaluation_candidate(store: &ProposalJournal, reference: &AcceptedReference) -> Result<Value> {
    let candidate = rich_candidate(store, reference)?;
    evaluation_candidate_from_rich_candidate(&candidate, reference)
}

/// Derive the compact evaluation projection at the same point the caller has
/// already authenticated an immutable journal row.  This is crate-visible so
/// the native G0 admission workers can retain only the projection instead of
/// reopening the rich source entry during publication.
pub(crate) fn evaluation_candidate_from_entry(
    entry: &Value,
    reference: &AcceptedReference,
) -> Result<Value> {
    let candidate = rich_candidate_from_entry(entry, reference)?;
    evaluation_candidate_from_rich_candidate(&candidate, reference)
}

fn evaluation_candidate_from_rich_candidate(
    candidate: &Value,
    reference: &AcceptedReference,
) -> Result<Value> {
    let fields = candidate
        .as_object()
        .ok_or_else(|| contract("rich candidate is invalid"))?;
    let required = |name: &str| {
        fields
            .get(name)
            .cloned()
            .ok_or_else(|| contract(format!("rich candidate lacks {name}")))
    };
    let mut projection = object([
        ("candidateId", required("candidateId")?),
        ("sourceMode", required("sourceMode")?),
        ("seedId", required("seedId")?),
        (
            "candidateIdentitySha256",
            required("candidateIdentitySha256")?,
        ),
        ("programSha256", required("programSha256")?),
        ("sourceProfile", required("sourceProfile")?),
        ("sourceProfileSha256", required("sourceProfileSha256")?),
        (
            "structuralOperatorHistory",
            fields
                .get("structuralOperatorHistory")
                .cloned()
                .unwrap_or_else(|| Value::Array(Vec::new())),
        ),
        ("proposalOrdinal", Value::from(reference.proposal_ordinal)),
        (
            "proposalEntrySha256",
            Value::String(reference.entry_sha256.clone()),
        ),
    ]);
    for optional in ["profileSnapshotSha256", "canonicalEvidenceIdentitySha256"] {
        if let Some(value) = fields.get(optional) {
            projection
                .as_object_mut()
                .expect("evaluation candidate is object")
                .insert(optional.to_owned(), value.clone());
        }
    }
    Ok(projection)
}

fn funnel_entry(store: &ProposalJournal, ordinal: u64) -> Result<Value> {
    let entry = store.read_public_entry(ordinal)?;
    funnel_entry_from_entry(&entry)
}

/// Compact the public funnel row from an already authenticated source entry.
/// The value is deliberately exact to the historical reread implementation.
pub(crate) fn funnel_entry_from_entry(entry: &Value) -> Result<Value> {
    let fields = entry
        .as_object()
        .ok_or_else(|| contract("proposal journal entry is invalid"))?;
    let required = |name: &str| {
        fields
            .get(name)
            .cloned()
            .ok_or_else(|| contract(format!("proposal journal entry lacks {name}")))
    };
    let mut funnel = object([
        ("entrySha256", required("entrySha256")?),
        ("proposalOrdinal", required("proposalOrdinal")?),
        ("originKind", required("originKind")?),
        ("disposition", required("disposition")?),
    ]);
    if let Some(candidate) = fields.get("candidate").and_then(Value::as_object) {
        let mut candidate_projection = Map::new();
        for field in ["candidateId", "sourceProfileSha256"] {
            if let Some(value) = candidate.get(field) {
                candidate_projection.insert(field.to_owned(), value.clone());
            }
        }
        funnel
            .as_object_mut()
            .expect("funnel is object")
            .insert("candidate".to_owned(), Value::Object(candidate_projection));
    }
    if let Some(proposal) = fields.get("proposal").and_then(Value::as_object) {
        let mut proposal_projection = Map::new();
        for field in ["candidateId", "rawSourceProfileSha256"] {
            if let Some(value) = proposal.get(field) {
                proposal_projection.insert(field.to_owned(), value.clone());
            }
        }
        funnel
            .as_object_mut()
            .expect("funnel is object")
            .insert("proposal".to_owned(), Value::Object(proposal_projection));
    }
    if let Some(value) = fields.get("funnelCandidate") {
        funnel
            .as_object_mut()
            .expect("funnel is object")
            .insert("funnelCandidate".to_owned(), value.clone());
    }
    Ok(funnel)
}

fn validate_precomputed_evaluation_candidate(
    candidate: &Value,
    reference: &AcceptedReference,
) -> Result<()> {
    let fields = candidate
        .as_object()
        .ok_or_else(|| contract("precomputed G0 evaluation candidate is invalid"))?;
    if fields.get("candidateId").and_then(Value::as_str) != Some(&reference.candidate_id)
        || fields
            .get("candidateIdentitySha256")
            .and_then(Value::as_str)
            != Some(&reference.candidate_identity_sha256)
        || fields.get("proposalOrdinal").and_then(Value::as_u64) != Some(reference.proposal_ordinal)
        || fields.get("proposalEntrySha256").and_then(Value::as_str)
            != Some(&reference.entry_sha256)
    {
        return Err(contract(
            "precomputed G0 evaluation candidate diverges from selected reference",
        ));
    }
    Ok(())
}

fn validate_precomputed_funnel_entry(funnel: &Value, reference: &AcceptedReference) -> Result<()> {
    let fields = funnel
        .as_object()
        .ok_or_else(|| contract("precomputed G0 funnel entry is invalid"))?;
    if fields.get("entrySha256").and_then(Value::as_str) != Some(&reference.entry_sha256)
        || fields.get("proposalOrdinal").and_then(Value::as_u64) != Some(reference.proposal_ordinal)
    {
        return Err(contract(
            "precomputed G0 funnel entry diverges from selected reference",
        ));
    }
    Ok(())
}

fn proposal_slots(request: &PublicationRequest, accepted: u64, g0: bool) -> Value {
    let mut slots = Map::new();
    slots.insert("targetUniqueCandidates".to_owned(), Value::from(accepted));
    slots.insert("acceptedUniqueCandidates".to_owned(), Value::from(accepted));
    slots.insert(
        "proposalAttempts".to_owned(),
        Value::from(request.proposal_count),
    );
    slots.insert(
        "maxProposalAttempts".to_owned(),
        Value::from(request.max_proposal_attempts),
    );
    slots.insert(
        "remainingUniqueCandidateSlots".to_owned(),
        Value::from(request.target_unique_candidates.saturating_sub(accepted)),
    );
    if g0 {
        slots.insert(
            "constructionPoolSize".to_owned(),
            Value::from(request.target_unique_candidates),
        );
        slots.insert(
            "constructedAcceptedCount".to_owned(),
            Value::from(request.construction_references.len() as u64),
        );
        slots.insert("evaluationPopulationSize".to_owned(), Value::from(accepted));
    }
    Value::Object(slots)
}

fn counts_value(counts: &BTreeMap<String, u64>) -> Value {
    Value::Object(
        counts
            .iter()
            .map(|(key, value)| (key.clone(), Value::from(*value)))
            .collect(),
    )
}

fn string_field(value: &Value, field: &str, label: &str) -> Result<String> {
    let result = value
        .as_object()
        .and_then(|fields| fields.get(field))
        .and_then(Value::as_str)
        .ok_or_else(|| contract(format!("{label} lacks {field}")))?
        .to_owned();
    sha(&result, field)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use temporal_qd_contract::{canonical_json_bytes, canonical_sha256};

    use super::*;

    #[test]
    fn population_stream_matches_the_value_backed_canonical_bytes() {
        let root = std::env::temp_dir().join(format!(
            "temporal-qd-population-stream-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        let store = ProposalJournal::open(&root, crate::journal::FinalNewline::Lf).unwrap();
        let mut entry = object([
            (
                "schemaVersion",
                Value::String(crate::proposal::PROPOSAL_ENTRY_SCHEMA.to_owned()),
            ),
            (
                "candidate",
                object([("candidateId", Value::String("candidate-1".to_owned()))]),
            ),
        ]);
        let entry_sha256 = canonical_sha256(&entry).unwrap();
        entry.as_object_mut().unwrap().insert(
            "entrySha256".to_owned(),
            Value::String(entry_sha256.clone()),
        );
        store.write_public_entry(0, &entry).unwrap();
        let references = vec![AcceptedReference {
            proposal_ordinal: 0,
            candidate_id: "candidate-1".to_owned(),
            candidate_identity_sha256:
                "sha256:1111111111111111111111111111111111111111111111111111111111111111".to_owned(),
            executable_semantic_sha256:
                "sha256:2222222222222222222222222222222222222222222222222222222222222222".to_owned(),
            entry_sha256,
            descriptor_projection: None,
        }];
        let template = object([
            (
                "schemaVersion",
                Value::String("population_test_v1".to_owned()),
            ),
            ("candidates", Value::Null),
            ("z", Value::String("\u{007f}".to_owned())),
        ]);
        let mut streamed = Vec::new();
        stream_population_value(&template, &references, &store, &mut streamed).unwrap();

        let mut expected = template;
        expected.as_object_mut().unwrap().insert(
            "candidates".to_owned(),
            Value::Array(vec![object([(
                "candidateId",
                Value::String("candidate-1".to_owned()),
            )])]),
        );
        assert_eq!(streamed, canonical_json_bytes(&expected).unwrap());
        assert_eq!(
            population_stream_sha256(&expected, &references, &store).unwrap(),
            canonical_sha256(&expected).unwrap(),
        );
        fs::remove_dir_all(root).unwrap();
    }
}
