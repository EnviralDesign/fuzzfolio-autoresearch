//! One bounded, native-owned post-construction G0 transaction.
//!
//! Python's v5 authority remains responsible for producing immutable proposal
//! entries.  Once the construction target is durable, this module is the only
//! production owner of admission, G0 selection, publication, and restart
//! adoption.  It deliberately accepts a compact request: neither an expected
//! entry list nor a selected-candidate list crosses the language boundary.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Read,
    path::{Path, PathBuf},
    thread,
    time::Instant,
};

use sha2::{Digest, Sha256};
use temporal_qd_contract::{
    ContractError, Map, Value, canonical_sha256, canonical_sha256_without_object_field,
};

use crate::{
    g0::{
        admit_accepted_pair_entry_bound_to_operator, project_admitted_pair_entry,
        validate_accepted_pool, verify_campaign_ledger, verify_g0_bootstrap_selection,
    },
    journal::{AcceptedReference, FinalNewline, JournalError, ProposalJournal, WrittenArtifact},
    publication::{
        PrecomputedG0Admission, PublicationPolicy, PublicationRequest,
        RichImmigrantDistributionAccumulator, evaluation_candidate_from_entry,
        funnel_entry_from_entry, publish_generation_with_precomputed_g0,
    },
};

pub const G0_FUNNEL_RECEIPT_SCHEMA: &str = "temporal_qd_native_g0_funnel_receipt_v2";
pub const G0_CONSTRUCTION_HANDOFF_SCHEMA: &str = "temporal_qd_native_g0_construction_handoff_v1";
pub const G0_FUNNEL_RESULT_SCHEMA: &str = "temporal_qd_native_g0_funnel_result_v2";
pub const G0_FUNNEL_RECEIPT_PATH: &str = "internal/g0-funnel/receipt.json";
pub const G0_CONSTRUCTION_HANDOFF_PATH: &str = "internal/g0-funnel/construction-handoff.json";
/// The ordinary production setting.  The boundary may lower this for a
/// deterministic single-thread diagnostic or raise it only to the hard cap.
/// This operational knob never enters public artifact identities because the
/// admission merge is ordinal-stable.
pub const DEFAULT_G0_ADMISSION_THREAD_CAP: usize = 8;
pub const MAX_G0_ADMISSION_THREAD_CAP: usize = 8;

#[derive(Debug, thiserror::Error)]
pub enum G0FunnelError {
    #[error("G0 funnel canonical contract failure: {0}")]
    Canonical(#[from] ContractError),
    #[error("G0 funnel journal failure: {0}")]
    Journal(#[from] JournalError),
    #[error("G0 funnel publication failure: {0}")]
    Publication(#[from] crate::publication::PublicationError),
    #[error("G0 funnel failure: {0}")]
    Contract(String),
}

pub type Result<T> = std::result::Result<T, G0FunnelError>;

fn contract(message: impl Into<String>) -> G0FunnelError {
    G0FunnelError::Contract(message.into())
}

fn object(entries: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect(),
    )
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

fn string<'a>(fields: &'a Map<String, Value>, field: &str, label: &str) -> Result<&'a str> {
    fields
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| contract(format!("{label} lacks nonempty {field}")))
}

fn number(fields: &Map<String, Value>, field: &str, label: &str) -> Result<u64> {
    fields
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| contract(format!("{label} lacks integer {field}")))
}

fn exact_fields(
    fields: &Map<String, Value>,
    required: &[&str],
    optional: &[&str],
    label: &str,
) -> Result<()> {
    if !required.iter().all(|field| fields.contains_key(*field))
        || fields.len()
            != required.len()
                + optional
                    .iter()
                    .filter(|field| fields.contains_key(**field))
                    .count()
    {
        return Err(contract(format!("{label} fields are not exact")));
    }
    Ok(())
}

fn self_hashed(value: &Value, field: &str, schema: &str, label: &str) -> Result<String> {
    let fields = value
        .as_object()
        .ok_or_else(|| contract(format!("{label} must be an object")))?;
    if fields.get("schemaVersion").and_then(Value::as_str) != Some(schema) {
        return Err(contract(format!("{label} schema is incompatible")));
    }
    let identity = string(fields, field, label)?.to_owned();
    sha(&identity, field)?;
    if canonical_sha256_without_object_field(value, field)? != identity {
        return Err(contract(format!("{label} self-hash mismatched")));
    }
    Ok(identity)
}

#[derive(Clone, Debug)]
pub struct G0FunnelRequest {
    /// The generation's `proposal` directory.  All native artifacts remain
    /// below this root and journal discovery never follows a caller path.
    pub output_root: PathBuf,
    pub final_newline: FinalNewline,
    /// Identity of the closed manifest.  It binds the private receipt without
    /// embedding any rich journal data in the request.
    pub request_sha256: String,
    /// Frozen runtime authority for this post-construction operation.  It is
    /// intentionally separate from the Python-owned construction runtime.
    pub authority_sha256: String,
    /// Closed object binding the frozen G0 runtime to the concrete qd-batch
    /// executable and source authority.  It is embedded in the sealed receipt
    /// so restart adoption cannot cross a native build drift.
    pub execution_authority: Value,
    pub config: Value,
    pub config_sha256: String,
    pub generation_index: u64,
    pub construction_pool_size: u64,
    pub evaluation_population_size: u64,
    pub max_proposal_attempts: u64,
    /// Bounded operational concurrency for independent sealed-row admission.
    /// It is supplied by the native process boundary and deliberately remains
    /// outside the compact manifest/receipt semantic authority.
    pub admission_thread_cap: usize,
    pub publication_policy: PublicationPolicy,
    /// Compact path/policy pointer to the mutable global ledger. Rust is the
    /// sole finalization reader/reducer; Python must never send its rows or a
    /// precomputed summary across the G0 boundary.
    pub identity_ledger: Option<Value>,
    /// Test-only compatibility projection for direct kernel fixtures. The
    /// closed batch manifest never carries this field; production must use
    /// `identity_ledger` so Rust owns the durable ledger reduction.
    pub global_identity_ledger: Option<Value>,
    /// An explicit audit re-reads the construction journal even after a sealed
    /// receipt.  Ordinary restart adoption must leave those bytes untouched.
    pub audit: bool,
}

impl G0FunnelRequest {
    pub fn validate(&self) -> Result<()> {
        sha(&self.request_sha256, "G0 funnel request SHA-256")?;
        sha(
            &self.authority_sha256,
            "G0 funnel runtime authority SHA-256",
        )?;
        sha(&self.config_sha256, "G0 funnel config SHA-256")?;
        if self.generation_index != 1
            || self.construction_pool_size == 0
            || self.evaluation_population_size == 0
            || self.evaluation_population_size > self.construction_pool_size
            || self.max_proposal_attempts < self.construction_pool_size
            || self.admission_thread_cap == 0
            || self.admission_thread_cap > MAX_G0_ADMISSION_THREAD_CAP
        {
            return Err(contract("G0 funnel widths or proposal ceiling are invalid"));
        }
        self.publication_policy.validate()?;
        let config = self
            .config
            .as_object()
            .ok_or_else(|| contract("G0 funnel config must be an object"))?;
        if config.get("schemaVersion").and_then(Value::as_str)
            != Some("temporal_qd_pair_generation_v2")
            || config.get("configSha256").and_then(Value::as_str)
                != Some(self.config_sha256.as_str())
            || number(config, "generationIndex", "G0 funnel config")? != self.generation_index
            || number(config, "targetUniqueCandidates", "G0 funnel config")?
                != self.construction_pool_size
            || number(config, "maxProposalAttempts", "G0 funnel config")?
                != self.max_proposal_attempts
            || canonical_sha256_without_object_field(&self.config, "configSha256")?
                != self.config_sha256
        {
            return Err(contract("G0 funnel config binding drifted"));
        }
        let allocation = config
            .get("reproductionAllocation")
            .ok_or_else(|| contract("G0 funnel config lacks reproduction allocation"))?;
        validate_reproduction_allocation(allocation)?;
        if self.identity_ledger.is_none() {
            if let Some(ledger) = &self.global_identity_ledger {
                validate_global_identity_ledger_summary(ledger)?;
            }
        } else if self.global_identity_ledger.is_some() {
            return Err(contract(
                "G0 request cannot mix a ledger binding with a summary projection",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub enum G0FunnelOutcome {
    /// The durable journal has not reached the construction target yet.  This
    /// is not a Python-finalization fallback; Python may only resume proposal
    /// construction after this outcome.
    ConstructionIncomplete {
        proposal_count: u64,
        accepted_count: u64,
    },
    /// Fresh native finalization or audited recovery completed.
    Completed {
        pair_generation_result: Value,
        receipt: Value,
    },
    /// A sealed receipt was verified without reading a proposal-journal byte.
    Adopted {
        pair_generation_result: Value,
        receipt: Value,
        adoption_verification: Value,
    },
}

impl G0FunnelOutcome {
    pub fn result_value(&self) -> Value {
        match self {
            Self::ConstructionIncomplete {
                proposal_count,
                accepted_count,
            } => object([
                (
                    "schemaVersion",
                    Value::String(G0_FUNNEL_RESULT_SCHEMA.to_owned()),
                ),
                (
                    "status",
                    Value::String("construction_incomplete".to_owned()),
                ),
                ("proposalCount", Value::from(*proposal_count)),
                ("acceptedCount", Value::from(*accepted_count)),
            ]),
            Self::Completed {
                pair_generation_result,
                receipt,
            }
            | Self::Adopted {
                pair_generation_result,
                receipt,
                ..
            } => {
                let mut result = object([
                    (
                        "schemaVersion",
                        Value::String(G0_FUNNEL_RESULT_SCHEMA.to_owned()),
                    ),
                    (
                        "status",
                        Value::String(
                            if matches!(self, Self::Adopted { .. }) {
                                "adopted"
                            } else {
                                "completed"
                            }
                            .to_owned(),
                        ),
                    ),
                    ("pairGenerationResult", pair_generation_result.clone()),
                    ("receipt", receipt.clone()),
                ]);
                if let Self::Adopted {
                    adoption_verification,
                    ..
                } = self
                {
                    result
                        .as_object_mut()
                        .expect("G0 funnel result is object")
                        .insert(
                            "adoptionVerification".to_owned(),
                            adoption_verification.clone(),
                        );
                }
                result
            }
        }
    }
}

#[derive(Debug)]
struct ScannedJournal {
    entry_sha256s: Vec<String>,
    entry_ordinals: Vec<u64>,
    construction_references: Vec<AcceptedReference>,
    precomputed_references: Vec<Value>,
    evaluation_candidates: BTreeMap<u64, Value>,
    funnel_entries: BTreeMap<u64, Value>,
    origin_proposal_counts: BTreeMap<String, u64>,
    origin_accepted_counts: BTreeMap<String, u64>,
    origin_materialized_counts: BTreeMap<String, u64>,
    rejected_by_accounting_origin: BTreeMap<String, BTreeMap<String, u64>>,
    disposition_counts: BTreeMap<String, u64>,
    unique_pair_genome_count: u64,
    /// Canonical exact set used to bind the external global identity ledger;
    /// each executable semantic may map to exactly one candidate identity.
    executable_semantic_candidates: BTreeMap<String, String>,
    immigrant_distribution: Option<Value>,
    journal_inventory_sha256: String,
}

/// Untrusted operational accounting emitted only on stderr by the native
/// batch process path.  It is intentionally kept out of semantic receipts and
/// public artifacts so timing and I/O measurement cannot perturb parity.
#[derive(Clone, Debug, Default)]
struct JournalAdmissionTelemetry {
    entry_count: u64,
    accepted_count: u64,
    source_bytes_read: u64,
    worker_count: usize,
    enumeration_seconds: f64,
    admission_seconds: f64,
    merge_seconds: f64,
}

struct JournalScan {
    scanned: ScannedJournal,
    telemetry: JournalAdmissionTelemetry,
}

struct AdmittedJournalChunk {
    entries: Vec<ParsedJournalEntry>,
    distribution: RichImmigrantDistributionAccumulator,
    source_bytes_read: u64,
}

struct ParsedJournalEntry {
    ordinal: u64,
    entry_sha256: String,
    origin: String,
    disposition: String,
    accounting_origin: &'static str,
    materialized: bool,
    accepted: Option<ParsedAcceptedEntry>,
}

struct ParsedAcceptedEntry {
    reference: AcceptedReference,
    birth_ordinal: u64,
    precomputed_reference: Value,
    evaluation_candidate: Value,
    funnel_entry: Value,
}

fn emit_g0_diagnostics(
    mode: &str,
    request: &G0FunnelRequest,
    total_started: Instant,
    configuration_binding_seconds: f64,
    journal: Option<&JournalAdmissionTelemetry>,
    population_source_journal_bytes_read: u64,
    population_stream_seconds: f64,
    reduction_seconds: f64,
    publication_seconds: f64,
    sealing_seconds: f64,
    output_artifact_bytes: u64,
    output_bytes_hashed: u64,
) {
    let journal = journal.map_or(Value::Null, |telemetry| {
        object([
            ("entryCount", Value::from(telemetry.entry_count)),
            ("acceptedCount", Value::from(telemetry.accepted_count)),
            (
                "sourceBytesRead",
                Value::from(
                    telemetry
                        .source_bytes_read
                        .saturating_add(population_source_journal_bytes_read),
                ),
            ),
            (
                "admissionSourceBytesRead",
                Value::from(telemetry.source_bytes_read),
            ),
            (
                "populationSourceBytesRead",
                Value::from(population_source_journal_bytes_read),
            ),
            ("workerCount", Value::from(telemetry.worker_count as u64)),
            (
                "enumerationMilliseconds",
                Value::from((telemetry.enumeration_seconds * 1000.0) as u64),
            ),
            (
                "admissionMilliseconds",
                Value::from((telemetry.admission_seconds * 1000.0) as u64),
            ),
            (
                "mergeMilliseconds",
                Value::from((telemetry.merge_seconds * 1000.0) as u64),
            ),
        ])
    });
    let value = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_native_g0_diagnostics_v1".to_owned()),
        ),
        ("mode", Value::String(mode.to_owned())),
        (
            "threadCap",
            Value::from(request.admission_thread_cap as u64),
        ),
        (
            "totalElapsedMilliseconds",
            Value::from(total_started.elapsed().as_millis() as u64),
        ),
        (
            "configurationBindingMilliseconds",
            Value::from((configuration_binding_seconds * 1000.0) as u64),
        ),
        ("journalAdmission", journal),
        (
            "populationStreamingMilliseconds",
            Value::from((population_stream_seconds * 1000.0) as u64),
        ),
        (
            "reductionMilliseconds",
            Value::from((reduction_seconds * 1000.0) as u64),
        ),
        (
            "publicationMilliseconds",
            Value::from((publication_seconds * 1000.0) as u64),
        ),
        (
            "sealingMilliseconds",
            Value::from((sealing_seconds * 1000.0) as u64),
        ),
        ("outputArtifactBytes", Value::from(output_artifact_bytes)),
        ("outputBytesHashed", Value::from(output_bytes_hashed)),
    ]);
    if let Ok(line) = serde_json::to_string(&value) {
        eprintln!("TEMPORAL_QD_G0_DIAGNOSTICS {line}");
    }
}

fn final_public_output_bytes(store: &ProposalJournal) -> Result<u64> {
    [
        "g0-bootstrap/accepted-pool.json",
        "g0-bootstrap/campaign-construction-ledger.json",
        "g0-bootstrap/selection.json",
        "population.json",
        "evaluation-population.json",
        "generation-journal.json",
    ]
    .into_iter()
    .try_fold(0_u64, |total, relative| {
        total
            .checked_add(store.artifact_encoded_bytes(Path::new(relative))?)
            .ok_or_else(|| contract("G0 public output byte counter overflow"))
    })
}

/// Execute one complete post-construction G0 transaction.  The only regular
/// language crossing is the compact manifest/result pair at qd-batch; this
/// function discovers the journal inventory and selected cohort itself.
pub fn finalize_g0(request: &G0FunnelRequest) -> Result<G0FunnelOutcome> {
    let total_started = Instant::now();
    request.validate()?;
    let configuration_started = Instant::now();
    let store = ProposalJournal::open(&request.output_root, request.final_newline)?;
    // `pair-config.json` is the small, durable construction authority.  Bind
    // it before both the fast-adoption and slow-audit paths so an old receipt
    // can never be adopted against a caller-supplied lookalike config.
    let persisted_config = store.read_artifact(Path::new("pair-config.json"))?;
    if persisted_config != request.config {
        return Err(contract(
            "persisted pair configuration differs from the G0 funnel request",
        ));
    }
    let configuration_binding_seconds = configuration_started.elapsed().as_secs_f64();
    let receipt_path = Path::new(G0_FUNNEL_RECEIPT_PATH);
    if !request.audit && store.safe_existing_artifact(receipt_path)?.is_some() {
        let receipt = store.read_artifact(receipt_path)?;
        let (pair_generation_result, adoption_verification) =
            verify_receipt_for_adoption(&store, request, &receipt)?;
        emit_g0_diagnostics(
            "adopted",
            request,
            total_started,
            configuration_binding_seconds,
            None,
            0,
            0.0,
            0.0,
            0.0,
            0.0,
            final_public_output_bytes(&store)?,
            adoption_verification
                .get("outputBytesHashed")
                .and_then(Value::as_u64)
                .unwrap_or(0),
        );
        return Ok(G0FunnelOutcome::Adopted {
            pair_generation_result,
            receipt,
            adoption_verification,
        });
    }

    let final_artifacts_present = final_artifact_presence(&store)?;
    let JournalScan {
        scanned,
        telemetry: journal_telemetry,
    } = scan_journal(&store, request)?;
    if (scanned.construction_references.len() as u64) < request.construction_pool_size {
        if final_artifacts_present.any() {
            return Err(contract(
                "G0 finalization found a partial public artifact prefix before construction completed",
            ));
        }
        emit_g0_diagnostics(
            "construction_incomplete",
            request,
            total_started,
            configuration_binding_seconds,
            Some(&journal_telemetry),
            0,
            0.0,
            0.0,
            0.0,
            0.0,
            0,
            0,
        );
        return Ok(G0FunnelOutcome::ConstructionIncomplete {
            proposal_count: scanned.entry_ordinals.len() as u64,
            accepted_count: scanned.construction_references.len() as u64,
        });
    }
    if scanned.construction_references.len() as u64 != request.construction_pool_size {
        return Err(contract(
            "G0 construction journal accepted count differs from its frozen target",
        ));
    }
    let reduction_started = Instant::now();
    let global_identity_ledger = load_and_verify_identity_ledger(
        request.identity_ledger.as_ref(),
        Some(&scanned.executable_semantic_candidates),
    )?
    .or_else(|| {
        request
            .global_identity_ledger
            .clone()
            .map(|summary| IdentityLedgerState {
                binding: Value::Null,
                summary,
            })
    });
    let handoff = load_and_verify_handoff(&store, request, &scanned)?;
    // A completed construction may have been interrupted after any public
    // artifact write and before the receipt commit.  Publication writes are
    // deterministic `write_once` operations: this slow native audit may
    // therefore adopt an exact prefix and finish the suffix, while any
    // divergent existing byte immediately fails closed.

    let allocation = request
        .config
        .get("reproductionAllocation")
        .expect("request validation requires reproduction allocation")
        .clone();
    let accounting = reproduction_allocation_accounting(
        &allocation,
        &scanned.origin_proposal_counts,
        &scanned.origin_accepted_counts,
        &scanned.origin_materialized_counts,
        &scanned.rejected_by_accounting_origin,
    )?;
    if accounting.get("complete").and_then(Value::as_bool) != Some(true) {
        return Err(contract(
            "frozen reproduction allocation completed with an origin quota deficit",
        ));
    }
    let reduction_seconds = reduction_started.elapsed().as_secs_f64();

    let ScannedJournal {
        entry_sha256s,
        entry_ordinals,
        construction_references,
        precomputed_references,
        evaluation_candidates,
        funnel_entries,
        origin_proposal_counts,
        origin_accepted_counts,
        origin_materialized_counts,
        rejected_by_accounting_origin,
        disposition_counts,
        unique_pair_genome_count,
        executable_semantic_candidates,
        immigrant_distribution,
        journal_inventory_sha256,
    } = scanned;
    let publication_request = PublicationRequest {
        request_sha256: request.request_sha256.clone(),
        config_sha256: request.config_sha256.clone(),
        generation_index: request.generation_index,
        target_unique_candidates: request.construction_pool_size,
        max_proposal_attempts: request.max_proposal_attempts,
        proposal_count: entry_ordinals.len() as u64,
        origin_proposal_counts: origin_proposal_counts.clone(),
        origin_accepted_counts: origin_accepted_counts.clone(),
        disposition_counts: disposition_counts.clone(),
        entry_sha256s: entry_sha256s.clone(),
        entry_ordinals: entry_ordinals.clone(),
        construction_references,
        g0_evaluation_width: Some(request.evaluation_population_size),
        global_identity_ledger: global_identity_ledger
            .as_ref()
            .map(|ledger| ledger.summary.clone()),
        reproduction_allocation: Some(allocation),
        reproduction_allocation_accounting: Some(accounting),
        unique_pair_genome_count: Some(unique_pair_genome_count),
        policy: request.publication_policy.clone(),
    };
    let publication_started = Instant::now();
    let published = publish_generation_with_precomputed_g0(
        &store,
        &publication_request,
        Some(&PrecomputedG0Admission {
            accepted_references: precomputed_references,
            immigrant_construction_distribution: immigrant_distribution,
            evaluation_candidates,
            funnel_entries,
        }),
    )?;
    let journal = store.read_artifact(Path::new("generation-journal.json"))?;
    let pair_generation_result = pair_generation_result_from_journal(&journal)?;
    let publication_seconds = publication_started.elapsed().as_secs_f64();
    let scanned = ScannedJournal {
        entry_sha256s,
        entry_ordinals,
        construction_references: Vec::new(),
        precomputed_references: Vec::new(),
        evaluation_candidates: BTreeMap::new(),
        funnel_entries: BTreeMap::new(),
        origin_proposal_counts,
        origin_accepted_counts,
        origin_materialized_counts,
        rejected_by_accounting_origin,
        disposition_counts,
        unique_pair_genome_count,
        executable_semantic_candidates,
        immigrant_distribution: None,
        journal_inventory_sha256,
    };
    let sealing_started = Instant::now();
    let receipt = build_receipt(
        request,
        &scanned,
        global_identity_ledger.as_ref(),
        handoff.as_ref(),
        &published.population_artifact,
        &published.evaluation_artifact,
        &published.journal_artifact,
        &published.population_sha256,
        &published.evaluation_population_sha256,
        &published.generation_journal_sha256,
        published.g0_binding.as_ref(),
        &pair_generation_result,
    )?;
    // This is intentionally last: presence of a receipt means every public
    // artifact has been installed and exact-existing verification has passed.
    inject_test_crash_before_receipt()?;
    store.write_canonical_once(receipt_path, &receipt)?;
    emit_g0_diagnostics(
        "completed",
        request,
        total_started,
        configuration_binding_seconds,
        Some(&journal_telemetry),
        published.population_source_journal_bytes_read,
        published.population_stream_seconds,
        reduction_seconds,
        publication_seconds,
        sealing_started.elapsed().as_secs_f64(),
        final_public_output_bytes(&store)?,
        0,
    );
    Ok(G0FunnelOutcome::Completed {
        pair_generation_result,
        receipt,
    })
}

/// Debug-only crash boundary used by the hermetic resume test.  The receipt is
/// the commit record, so a loss immediately before this write must leave an
/// exact public prefix that a later native slow audit can finish.
fn inject_test_crash_before_receipt() -> Result<()> {
    #[cfg(debug_assertions)]
    if crate::journal::g0_test_should_crash_after_artifact(Path::new("before-receipt")) {
        return Err(contract("injected G0 crash before receipt publication"));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct FinalArtifactPresence {
    accepted_pool: bool,
    selection: bool,
    ledger: bool,
    population: bool,
    evaluation: bool,
    journal: bool,
}

impl FinalArtifactPresence {
    fn any(self) -> bool {
        self.accepted_pool
            || self.selection
            || self.ledger
            || self.population
            || self.evaluation
            || self.journal
    }
}

fn final_artifact_presence(store: &ProposalJournal) -> Result<FinalArtifactPresence> {
    Ok(FinalArtifactPresence {
        accepted_pool: store
            .safe_existing_artifact(Path::new("g0-bootstrap/accepted-pool.json"))?
            .is_some(),
        selection: store
            .safe_existing_artifact(Path::new("g0-bootstrap/selection.json"))?
            .is_some(),
        ledger: store
            .safe_existing_artifact(Path::new("g0-bootstrap/campaign-construction-ledger.json"))?
            .is_some(),
        population: store
            .safe_existing_artifact(Path::new("population.json"))?
            .is_some(),
        evaluation: store
            .safe_existing_artifact(Path::new("evaluation-population.json"))?
            .is_some(),
        journal: store
            .safe_existing_artifact(Path::new("generation-journal.json"))?
            .is_some(),
    })
}

#[cfg(windows)]
fn is_reparse_point(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.file_attributes() & 0x0400 != 0
}

#[cfg(not(windows))]
fn is_reparse_point(_metadata: &fs::Metadata) -> bool {
    false
}

fn scan_journal(store: &ProposalJournal, request: &G0FunnelRequest) -> Result<JournalScan> {
    let enumeration_started = Instant::now();
    let journal_dir = store.safe_existing_directory(Path::new("proposal-journal"))?;
    let mut names = Vec::new();
    for item in fs::read_dir(&journal_dir).map_err(JournalError::Io)? {
        let item = item.map_err(JournalError::Io)?;
        let name = item
            .file_name()
            .to_str()
            .map(ToOwned::to_owned)
            .ok_or_else(|| contract("proposal journal filename is not UTF-8"))?;
        if store
            .safe_existing_artifact(&PathBuf::from("proposal-journal").join(&name))?
            .is_none()
        {
            continue;
        }
        let ordinal = parse_canonical_journal_name(&name)?;
        names.push((ordinal, name));
    }
    names.sort_by_key(|(ordinal, _)| *ordinal);
    for (expected, (ordinal, _)) in names.iter().enumerate() {
        if *ordinal != expected as u64 {
            return Err(contract(
                "proposal journal ordinals are not contiguous from zero",
            ));
        }
    }
    if names.len() as u64 > request.max_proposal_attempts {
        return Err(contract(
            "G0 proposal journal exceeds its frozen maxProposalAttempts",
        ));
    }

    let expected_operator_sha256 =
        canonical_sha256(&request.publication_policy.operator_implementation_identity)?;
    let construction_identity = construction_pool_identity(request)?;
    let ordinals = names
        .into_iter()
        .map(|(ordinal, _)| ordinal)
        .collect::<Vec<_>>();
    let journal_entry_count = ordinals.len();
    let worker_count = request.admission_thread_cap.min(journal_entry_count.max(1));
    let chunk_size = journal_entry_count.div_ceil(worker_count).max(1);
    let enumeration_seconds = enumeration_started.elapsed().as_secs_f64();
    let admission_started = Instant::now();
    let joined = thread::scope(|scope| {
        let mut handles = Vec::new();
        for chunk in ordinals.chunks(chunk_size) {
            handles.push(scope.spawn(|| {
                admit_journal_chunk(
                    store,
                    request,
                    chunk,
                    &expected_operator_sha256,
                    &construction_identity,
                )
            }));
        }
        handles
            .into_iter()
            .map(|handle| handle.join())
            .collect::<Vec<_>>()
    });
    let mut chunks = Vec::with_capacity(joined.len());
    for joined_chunk in joined {
        let chunk = joined_chunk
            .map_err(|_| contract("parallel G0 journal admission worker panicked"))??;
        chunks.push(chunk);
    }
    let admission_seconds = admission_started.elapsed().as_secs_f64();
    let merge_started = Instant::now();
    let mut entry_sha256s = Vec::with_capacity(journal_entry_count);
    let mut entry_ordinals = Vec::with_capacity(journal_entry_count);
    let mut construction_references = Vec::new();
    let mut precomputed_references = Vec::new();
    let mut evaluation_candidates = BTreeMap::new();
    let mut funnel_entries = BTreeMap::new();
    let mut origin_proposal_counts = BTreeMap::new();
    let mut origin_accepted_counts = BTreeMap::new();
    let mut origin_materialized_counts = BTreeMap::new();
    let mut rejected_by_accounting_origin: BTreeMap<String, BTreeMap<String, u64>> =
        BTreeMap::new();
    let mut disposition_counts = BTreeMap::new();
    let mut candidate_identities = BTreeSet::new();
    let mut candidate_ids = BTreeSet::new();
    let mut executable_semantics = BTreeSet::new();
    let mut executable_semantic_candidates = BTreeMap::new();
    let mut birth_ordinals = BTreeSet::new();
    let mut distribution = RichImmigrantDistributionAccumulator::default();

    let mut source_bytes_read = 0_u64;
    for chunk in chunks {
        source_bytes_read = source_bytes_read
            .checked_add(chunk.source_bytes_read)
            .ok_or_else(|| contract("G0 source journal byte counter overflow"))?;
        distribution.merge(chunk.distribution)?;
        for parsed in chunk.entries {
            let index = entry_ordinals.len();
            if parsed.ordinal != index as u64 {
                return Err(contract(
                    "parallel G0 journal admission did not preserve ordinal order",
                ));
            }
            *origin_proposal_counts
                .entry(parsed.origin.clone())
                .or_default() += 1;
            *disposition_counts
                .entry(parsed.disposition.clone())
                .or_default() += 1;
            if parsed.materialized {
                *origin_materialized_counts
                    .entry(parsed.accounting_origin.to_owned())
                    .or_default() += 1;
            }
            if parsed.disposition != "accepted" {
                *rejected_by_accounting_origin
                    .entry(parsed.accounting_origin.to_owned())
                    .or_default()
                    .entry(parsed.disposition.clone())
                    .or_default() += 1;
            }
            if let Some(accepted) = parsed.accepted {
                let reference = accepted.reference;
                if reference.proposal_ordinal != parsed.ordinal
                    || reference.entry_sha256 != parsed.entry_sha256
                    || !candidate_ids.insert(reference.candidate_id.clone())
                    || !candidate_identities.insert(reference.candidate_identity_sha256.clone())
                    || !executable_semantics.insert(reference.executable_semantic_sha256.clone())
                    || !birth_ordinals.insert(accepted.birth_ordinal)
                {
                    return Err(contract(
                        "accepted G0 journal uniqueness or source binding drifted",
                    ));
                }
                if executable_semantic_candidates
                    .insert(
                        reference.executable_semantic_sha256.clone(),
                        reference.candidate_identity_sha256.clone(),
                    )
                    .is_some()
                {
                    return Err(contract(
                        "accepted G0 journal repeats an executable semantic ledger binding",
                    ));
                }
                if evaluation_candidates
                    .insert(parsed.ordinal, accepted.evaluation_candidate)
                    .is_some()
                    || funnel_entries
                        .insert(parsed.ordinal, accepted.funnel_entry)
                        .is_some()
                {
                    return Err(contract(
                        "accepted G0 journal repeats a compact publication projection",
                    ));
                }
                *origin_accepted_counts.entry(parsed.origin).or_default() += 1;
                precomputed_references.push(accepted.precomputed_reference);
                construction_references.push(reference);
                if construction_references.len() as u64 == request.construction_pool_size
                    && index + 1 != journal_entry_count
                {
                    return Err(contract(
                        "G0 proposal journal contains entries after the construction target completed",
                    ));
                }
            }
            entry_ordinals.push(parsed.ordinal);
            entry_sha256s.push(parsed.entry_sha256);
        }
    }
    for (expected, ordinal) in birth_ordinals.iter().enumerate() {
        if *ordinal != expected as u64 {
            return Err(contract(
                "accepted G0 birth ordinals are not contiguous from zero",
            ));
        }
    }
    let journal_inventory_sha256 = canonical_sha256(&object([
        (
            "schemaVersion",
            Value::String("temporal_qd_native_g0_journal_inventory_v1".to_owned()),
        ),
        ("configSha256", Value::String(request.config_sha256.clone())),
        ("generationIndex", Value::from(request.generation_index)),
        (
            "operatorImplementationSha256",
            Value::String(expected_operator_sha256),
        ),
        (
            "entrySha256s",
            Value::Array(entry_sha256s.iter().cloned().map(Value::String).collect()),
        ),
    ]))?;
    let telemetry = JournalAdmissionTelemetry {
        entry_count: journal_entry_count as u64,
        accepted_count: construction_references.len() as u64,
        source_bytes_read,
        worker_count,
        enumeration_seconds,
        admission_seconds,
        merge_seconds: merge_started.elapsed().as_secs_f64(),
    };
    Ok(JournalScan {
        scanned: ScannedJournal {
            entry_sha256s,
            entry_ordinals,
            construction_references,
            precomputed_references,
            evaluation_candidates,
            funnel_entries,
            origin_proposal_counts,
            origin_accepted_counts,
            origin_materialized_counts,
            rejected_by_accounting_origin,
            disposition_counts,
            unique_pair_genome_count: executable_semantics.len() as u64,
            executable_semantic_candidates,
            immigrant_distribution: distribution.finish()?,
            journal_inventory_sha256,
        },
        telemetry,
    })
}

fn admit_journal_chunk(
    store: &ProposalJournal,
    request: &G0FunnelRequest,
    ordinals: &[u64],
    expected_operator_sha256: &str,
    construction_identity: &str,
) -> Result<AdmittedJournalChunk> {
    let mut entries = Vec::with_capacity(ordinals.len());
    let mut distribution = RichImmigrantDistributionAccumulator::default();
    let mut source_bytes_read = 0_u64;
    for ordinal in ordinals {
        let (entry, entry_bytes) = store.read_public_entry_with_bytes(*ordinal)?;
        source_bytes_read = source_bytes_read
            .checked_add(entry_bytes)
            .ok_or_else(|| contract("G0 source journal byte counter overflow"))?;
        let fields = entry
            .as_object()
            .ok_or_else(|| contract("proposal journal entry must be an object"))?;
        if number(fields, "proposalOrdinal", "proposal journal entry")? != *ordinal
            || number(fields, "generationIndex", "proposal journal entry")?
                != request.generation_index
            || string(fields, "configSha256", "proposal journal entry")? != request.config_sha256
            || string(
                fields,
                "operatorImplementationSha256",
                "proposal journal entry",
            )? != expected_operator_sha256
        {
            return Err(contract("proposal journal entry source binding drifted"));
        }
        let entry_sha256 = string(fields, "entrySha256", "proposal journal entry")?.to_owned();
        sha(&entry_sha256, "proposal journal entry SHA-256")?;
        let origin = string(fields, "originKind", "proposal journal entry")?.to_owned();
        let disposition = string(fields, "disposition", "proposal journal entry")?.to_owned();
        let accounting_origin = if origin == "random_immigrant" {
            "random_immigrant"
        } else {
            "structural_offspring"
        };
        let materialized = fields
            .get("proposal")
            .and_then(Value::as_object)
            .and_then(|proposal| proposal.get("disposition"))
            .and_then(Value::as_str)
            == Some("materialized");
        distribution.observe(&entry)?;
        let accepted = if disposition == "accepted" {
            let admitted = admit_accepted_pair_entry_bound_to_operator(
                &entry,
                &request.publication_policy.operator_implementation_identity,
            )
            .map_err(|error| {
                contract(format!(
                    "accepted G0 journal entry {ordinal} failed admission: {error}"
                ))
            })?;
            if admitted.generation_index != request.generation_index
                || admitted.proposal_ordinal != *ordinal
                || admitted.entry_sha256 != entry_sha256
            {
                return Err(contract("accepted G0 journal entry source binding drifted"));
            }
            let reference = AcceptedReference {
                proposal_ordinal: admitted.proposal_ordinal,
                candidate_id: admitted.candidate_id.clone(),
                candidate_identity_sha256: admitted.candidate_identity_sha256.clone(),
                executable_semantic_sha256: admitted.executable_semantic_sha256.clone(),
                entry_sha256: admitted.entry_sha256.clone(),
                descriptor_projection: Some(admitted.descriptor_projection.clone()),
            };
            let precomputed_reference = project_admitted_pair_entry(
                construction_identity,
                &format!("proposal-journal/{ordinal:08}.json"),
                &admitted,
            )
            .map_err(|error| {
                contract(format!(
                    "accepted G0 journal entry {ordinal} projection failed: {error}"
                ))
            })?;
            let evaluation_candidate = evaluation_candidate_from_entry(&entry, &reference)
                .map_err(|error| {
                    contract(format!(
                        "accepted G0 journal entry {ordinal} evaluation projection failed: {error}"
                    ))
                })?;
            let funnel_entry = funnel_entry_from_entry(&entry).map_err(|error| {
                contract(format!(
                    "accepted G0 journal entry {ordinal} funnel projection failed: {error}"
                ))
            })?;
            Some(ParsedAcceptedEntry {
                reference,
                birth_ordinal: admitted.birth_ordinal,
                precomputed_reference,
                evaluation_candidate,
                funnel_entry,
            })
        } else {
            validate_rejected_entry(&entry, request, *ordinal, expected_operator_sha256)?;
            None
        };
        entries.push(ParsedJournalEntry {
            ordinal: *ordinal,
            entry_sha256,
            origin,
            disposition,
            accounting_origin,
            materialized,
            accepted,
        });
    }
    Ok(AdmittedJournalChunk {
        entries,
        distribution,
        source_bytes_read,
    })
}

/// Rejected construction rows still influence allocation and completion. They
/// are not harmless opaque telemetry: admit their closed source/proposal/audit
/// surface before accounting them, so a self-rehashed malformed rejection
/// cannot steer the native G0 transaction.
fn validate_rejected_entry(
    entry: &Value,
    request: &G0FunnelRequest,
    ordinal: u64,
    expected_operator_sha256: &str,
) -> Result<()> {
    let fields = entry
        .as_object()
        .ok_or_else(|| contract("rejected G0 journal entry must be an object"))?;
    let required = [
        "schemaVersion",
        "configSha256",
        "generationIndex",
        "proposalOrdinal",
        "originKind",
        "proposal",
        "operatorImplementationSha256",
        "disposition",
        "entrySha256",
    ];
    let optional = ["identityChecks", "predeclaredLakeScope"];
    exact_fields(fields, &required, &optional, "rejected G0 journal entry")?;
    if fields.get("schemaVersion").and_then(Value::as_str) != Some("temporal_qd_proposal_entry_v3")
        || string(fields, "configSha256", "rejected G0 journal entry")? != request.config_sha256
        || number(fields, "generationIndex", "rejected G0 journal entry")?
            != request.generation_index
        || number(fields, "proposalOrdinal", "rejected G0 journal entry")? != ordinal
        || string(
            fields,
            "operatorImplementationSha256",
            "rejected G0 journal entry",
        )? != expected_operator_sha256
        || string(fields, "originKind", "rejected G0 journal entry")? != "random_immigrant"
        || ![
            "predeclared_lake_scope_rejected",
            "duplicate_pair_genome",
            "duplicate_candidate_identity",
            "duplicate_pair_genome_global",
            "duplicate_program",
            "duplicate_source_profile",
            "duplicate_profile_snapshot",
            "duplicate_canonical_evidence",
        ]
        .contains(&string(fields, "disposition", "rejected G0 journal entry")?)
    {
        return Err(contract("rejected G0 journal entry source binding drifted"));
    }
    self_hashed(
        entry,
        "entrySha256",
        "temporal_qd_proposal_entry_v3",
        "rejected G0 journal entry",
    )?;
    if let Some(identity_checks) = fields.get("identityChecks") {
        if !identity_checks.is_object() {
            return Err(contract(
                "rejected G0 journal entry identity checks are invalid",
            ));
        }
    }
    if let Some(scope) = fields.get("predeclaredLakeScope") {
        if !scope.is_object() {
            return Err(contract("rejected G0 journal entry scope audit is invalid"));
        }
    }
    let proposal_value = fields
        .get("proposal")
        .ok_or_else(|| contract("rejected G0 journal entry lacks proposal"))?;
    let proposal = proposal_value
        .as_object()
        .ok_or_else(|| contract("rejected G0 proposal must be an object"))?;
    let proposal_required = [
        "schemaVersion",
        "proposalSeed",
        "originKind",
        "side",
        "factoryPair",
        "pairIdentitySha256",
        "disposition",
        "proposalSha256",
    ];
    exact_fields(
        proposal,
        &proposal_required,
        &["factoryConstructionAudit"],
        "rejected G0 immigrant proposal",
    )?;
    if proposal.get("schemaVersion").and_then(Value::as_str) != Some("temporal_qd_pair_proposal_v2")
        || proposal.get("originKind") != fields.get("originKind")
        || proposal.get("originKind").and_then(Value::as_str) != Some("random_immigrant")
        || string(proposal, "proposalSeed", "rejected G0 proposal")?.is_empty()
        || string(proposal, "side", "rejected G0 proposal")?.is_empty()
        || proposal.get("disposition").and_then(Value::as_str) != Some("materialized")
        || !proposal.get("factoryPair").is_some_and(Value::is_object)
    {
        return Err(contract("rejected G0 proposal source surface drifted"));
    }
    let proposal_pair_identity = string(proposal, "pairIdentitySha256", "rejected G0 proposal")?;
    sha(
        proposal_pair_identity,
        "rejected G0 proposal pairIdentitySha256",
    )?;
    self_hashed(
        proposal_value,
        "proposalSha256",
        "temporal_qd_pair_proposal_v2",
        "rejected G0 proposal",
    )?;
    if let Some(audit) = proposal.get("factoryConstructionAudit") {
        let audit_fields = audit
            .as_object()
            .ok_or_else(|| contract("rejected G0 proposal construction audit is invalid"))?;
        match audit_fields.get("schemaVersion").and_then(Value::as_str) {
            Some("temporal_qd_rich_immigrant_pair_construction_v1") => {
                exact_fields(
                    audit_fields,
                    &[
                        "schemaVersion",
                        "pairIdentitySha256",
                        "sides",
                        "auditSha256",
                    ],
                    &[],
                    "rejected G0 rich construction audit",
                )?;
                if string(
                    audit_fields,
                    "pairIdentitySha256",
                    "rejected G0 rich construction audit",
                )? != proposal_pair_identity
                    || !audit_fields.get("sides").is_some_and(Value::is_object)
                {
                    return Err(contract(
                        "rejected G0 rich construction audit pair binding drifted",
                    ));
                }
                self_hashed(
                    audit,
                    "auditSha256",
                    "temporal_qd_rich_immigrant_pair_construction_v1",
                    "rejected G0 rich construction audit",
                )?;
            }
            Some("temporal_qd_evolvable_module_factory_audit_v1") => {
                exact_fields(
                    audit_fields,
                    &[
                        "schemaVersion",
                        "authoritySha256",
                        "pairIdentitySha256",
                        "sides",
                        "auditSha256",
                    ],
                    &[],
                    "rejected G0 evolvable construction audit",
                )?;
                self_hashed(
                    audit,
                    "auditSha256",
                    "temporal_qd_evolvable_module_factory_audit_v1",
                    "rejected G0 evolvable construction audit",
                )?;
                for field in ["authoritySha256", "pairIdentitySha256"] {
                    sha(
                        string(
                            audit_fields,
                            field,
                            "rejected G0 evolvable construction audit",
                        )?,
                        "rejected G0 evolvable audit SHA-256",
                    )?;
                }
                if string(
                    audit_fields,
                    "pairIdentitySha256",
                    "rejected G0 evolvable construction audit",
                )? != proposal_pair_identity
                    || !audit_fields.get("sides").is_some_and(Value::is_object)
                {
                    return Err(contract(
                        "rejected G0 evolvable construction audit pair binding drifted",
                    ));
                }
                let operator = request
                    .publication_policy
                    .operator_implementation_identity
                    .as_object()
                    .ok_or_else(|| contract("G0 publication operator must be an object"))?;
                let expected_authority = string(
                    operator,
                    "authoritySha256",
                    "G0 publication evolvable operator",
                )?;
                let expected_compiler_policy = string(
                    operator,
                    "compilerPolicySha256",
                    "G0 publication evolvable operator",
                )?;
                if string(
                    audit_fields,
                    "authoritySha256",
                    "rejected G0 evolvable construction audit",
                )? != expected_authority
                {
                    return Err(contract(
                        "rejected G0 evolvable audit authority does not bind publication operator",
                    ));
                }
                let lineage = proposal
                    .get("factoryPair")
                    .and_then(Value::as_object)
                    .and_then(|pair| pair.get("sideTargetedLineage"))
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        contract("rejected G0 evolvable audit lacks frozen side-targeted lineage")
                    })?;
                if lineage.len() != 2
                    || lineage.iter().any(|row| {
                        let Some(row) = row.as_object() else {
                            return true;
                        };
                        row.get("authoritySha256").and_then(Value::as_str)
                            != Some(expected_authority)
                            || row.get("compilerPolicySha256").and_then(Value::as_str)
                                != Some(expected_compiler_policy)
                    })
                {
                    return Err(contract(
                        "rejected G0 evolvable lineage does not bind publication operator",
                    ));
                }
            }
            _ => {
                return Err(contract(
                    "rejected G0 proposal construction audit schema is incompatible",
                ));
            }
        }
    }
    Ok(())
}

fn parse_canonical_journal_name(name: &str) -> Result<u64> {
    let Some(stem) = name.strip_suffix(".json") else {
        return Err(contract(
            "proposal journal contains a noncanonical filename",
        ));
    };
    if stem.len() != 8 || !stem.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(contract(
            "proposal journal filename must be eight decimal digits plus .json",
        ));
    }
    stem.parse::<u64>()
        .map_err(|_| contract("proposal journal filename ordinal is invalid"))
}

fn construction_pool_identity(request: &G0FunnelRequest) -> Result<String> {
    canonical_sha256(&object([
        (
            "schemaVersion",
            Value::String("temporal_qd_g0_construction_identity_v1".to_owned()),
        ),
        ("configSha256", Value::String(request.config_sha256.clone())),
        ("generationIndex", Value::from(request.generation_index)),
        (
            "constructionPoolSize",
            Value::from(request.construction_pool_size),
        ),
        (
            "evaluationPopulationSize",
            Value::from(request.evaluation_population_size),
        ),
    ]))
    .map_err(Into::into)
}

/// Validate the sealed allocation object used by both the historical G0
/// funnel and the write-neutral v5 publication stream.  Keeping this one
/// reducer crate-visible prevents the v5 path from growing a subtly different
/// allocation schema.
pub(crate) fn validate_reproduction_allocation(allocation: &Value) -> Result<()> {
    let fields = allocation
        .as_object()
        .ok_or_else(|| contract("reproduction allocation must be an object"))?;
    let accepted = fields.get("schemaVersion").and_then(Value::as_str)
        == Some("temporal_qd_reproduction_allocation_v2");
    let legacy = fields.get("schemaVersion").and_then(Value::as_str)
        == Some("temporal_qd_reproduction_allocation_v1");
    if !accepted && !legacy {
        return Err(contract("reproduction allocation schema is incompatible"));
    }
    let (target, offspring, immigrant) = if accepted {
        (
            "targetAcceptedCandidates",
            "desiredAcceptedOffspringCount",
            "desiredAcceptedImmigrantCount",
        )
    } else {
        (
            "targetEvaluatedCandidates",
            "desiredEvaluatedOffspringCount",
            "desiredEvaluatedImmigrantCount",
        )
    };
    let total = number(fields, target, "reproduction allocation")?;
    if total == 0
        || number(fields, offspring, "reproduction allocation")?
            + number(fields, immigrant, "reproduction allocation")?
            != total
    {
        return Err(contract("reproduction allocation target is inconsistent"));
    }
    let identity = string(fields, "allocationSha256", "reproduction allocation")?;
    sha(identity, "reproduction allocation SHA-256")?;
    if canonical_sha256_without_object_field(allocation, "allocationSha256")? != identity {
        return Err(contract("reproduction allocation identity drifted"));
    }
    Ok(())
}

/// Deterministically reduce compact proposal dispositions into the exact
/// public allocation-accounting value.  File-backed callers live in
/// `g0_funnel`; the v5 transaction reuses this pure reducer without opening a
/// journal or writing a path.
pub(crate) fn reproduction_allocation_accounting(
    allocation: &Value,
    origins: &BTreeMap<String, u64>,
    accepted_origins: &BTreeMap<String, u64>,
    materialized_origins: &BTreeMap<String, u64>,
    rejected_by_origin: &BTreeMap<String, BTreeMap<String, u64>>,
) -> Result<Value> {
    let fields = allocation
        .as_object()
        .ok_or_else(|| contract("reproduction allocation must be an object"))?;
    let accepted_terms = fields.get("schemaVersion").and_then(Value::as_str)
        == Some("temporal_qd_reproduction_allocation_v2");
    let (offspring_field, immigrant_field) = if accepted_terms {
        (
            "desiredAcceptedOffspringCount",
            "desiredAcceptedImmigrantCount",
        )
    } else {
        (
            "desiredEvaluatedOffspringCount",
            "desiredEvaluatedImmigrantCount",
        )
    };
    let targets = BTreeMap::from([
        (
            "structural_offspring".to_owned(),
            number(fields, offspring_field, "reproduction allocation")?,
        ),
        (
            "random_immigrant".to_owned(),
            number(fields, immigrant_field, "reproduction allocation")?,
        ),
    ]);
    let mut rows = Map::new();
    let mut total_handoff = 0_u64;
    for (origin, scheduled) in &targets {
        let attempted = if origin == "random_immigrant" {
            origins.get("random_immigrant").copied().unwrap_or(0)
        } else {
            origins
                .iter()
                .filter(|(kind, _)| kind.as_str() != "random_immigrant")
                .map(|(_, count)| *count)
                .sum()
        };
        let accepted = if origin == "random_immigrant" {
            accepted_origins
                .get("random_immigrant")
                .copied()
                .unwrap_or(0)
        } else {
            accepted_origins
                .iter()
                .filter(|(kind, _)| kind.as_str() != "random_immigrant")
                .map(|(_, count)| *count)
                .sum()
        };
        let materialized = materialized_origins.get(origin).copied().unwrap_or(0);
        let rejected_by_reason = rejected_by_origin.get(origin).cloned().unwrap_or_default();
        total_handoff += accepted;
        let mut row = Map::new();
        if accepted_terms {
            row.insert("targetAccepted".to_owned(), Value::from(*scheduled));
            row.insert("materialized".to_owned(), Value::from(materialized));
            row.insert("acceptedForEvaluation".to_owned(), Value::from(accepted));
            row.insert(
                "deficitAccepted".to_owned(),
                Value::from(scheduled.saturating_sub(accepted)),
            );
        } else {
            row.insert("scheduled".to_owned(), Value::from(*scheduled));
            row.insert("valid".to_owned(), Value::from(materialized));
            row.insert("accepted".to_owned(), Value::from(accepted));
            row.insert("evaluated".to_owned(), Value::from(accepted));
            row.insert(
                "deficit".to_owned(),
                Value::from(scheduled.saturating_sub(accepted)),
            );
        }
        row.insert("attempted".to_owned(), Value::from(attempted));
        row.insert(
            "rejected".to_owned(),
            Value::from(attempted.saturating_sub(accepted)),
        );
        row.insert(
            "rejectedByReason".to_owned(),
            Value::Object(
                rejected_by_reason
                    .into_iter()
                    .map(|(reason, count)| (reason, Value::from(count)))
                    .collect(),
            ),
        );
        row.insert(
            "backfilled".to_owned(),
            Value::from(attempted.saturating_sub(*scheduled)),
        );
        rows.insert(origin.clone(), Value::Object(row));
    }
    let ratio_key = if accepted_terms {
        "realizedAcceptedForEvaluationRatios"
    } else {
        "realizedRatios"
    };
    let mut ratios = Map::new();
    for (origin, row) in &rows {
        let accepted = if accepted_terms {
            row.get("acceptedForEvaluation")
        } else {
            row.get("evaluated")
        }
        .and_then(Value::as_u64)
        .expect("accounting rows have accepted count");
        ratios.insert(
            origin.clone(),
            Value::from(if total_handoff == 0 {
                0.0
            } else {
                accepted as f64 / total_handoff as f64
            }),
        );
    }
    let complete = rows.values().all(|row| {
        row.get(if accepted_terms {
            "deficitAccepted"
        } else {
            "deficit"
        })
        .and_then(Value::as_u64)
            == Some(0)
    });
    let mut result = object([
        (
            "schemaVersion",
            Value::String(
                if accepted_terms {
                    "temporal_qd_reproduction_allocation_accounting_v2"
                } else {
                    "temporal_qd_reproduction_allocation_accounting_v1"
                }
                .to_owned(),
            ),
        ),
        (
            "allocationSha256",
            Value::String(
                string(fields, "allocationSha256", "reproduction allocation")?.to_owned(),
            ),
        ),
        ("origins", Value::Object(rows)),
        (ratio_key, Value::Object(ratios)),
        ("complete", Value::Bool(complete)),
    ]);
    let hash = canonical_sha256(&result)?;
    result
        .as_object_mut()
        .expect("accounting result is object")
        .insert("accountingSha256".to_owned(), Value::String(hash));
    Ok(result)
}

#[derive(Clone, Debug)]
struct IdentityLedgerState {
    binding: Value,
    summary: Value,
}

fn validate_global_identity_ledger_summary(ledger: &Value) -> Result<()> {
    let fields = ledger
        .as_object()
        .ok_or_else(|| contract("global identity ledger summary must be an object"))?;
    let keys = [
        "pairExecutableSemanticCount",
        "pairExecutableSemanticDuplicateRejections",
        "identityLedgerSha256",
    ];
    if fields.len() != keys.len() || keys.iter().any(|key| !fields.contains_key(*key)) {
        return Err(contract(
            "global identity ledger summary fields are not exact",
        ));
    }
    number(
        fields,
        "pairExecutableSemanticCount",
        "global identity ledger summary",
    )?;
    number(
        fields,
        "pairExecutableSemanticDuplicateRejections",
        "global identity ledger summary",
    )?;
    sha(
        string(
            fields,
            "identityLedgerSha256",
            "global identity ledger summary",
        )?,
        "global identity ledger summary SHA-256",
    )
}

fn safe_absolute_regular_file(path: &Path, label: &str) -> Result<()> {
    if !path.is_absolute() {
        return Err(contract(format!("{label} path must be absolute")));
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        use std::path::Component;
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                current.push(component.as_os_str())
            }
            Component::CurDir | Component::ParentDir => {
                return Err(contract(format!(
                    "{label} path contains an unsafe component"
                )));
            }
        }
        let metadata = fs::symlink_metadata(&current).map_err(JournalError::Io)?;
        if metadata.file_type().is_symlink() || is_reparse_point(&metadata) {
            return Err(contract(format!(
                "{label} path contains a reparse component"
            )));
        }
    }
    if !fs::symlink_metadata(path)
        .map_err(JournalError::Io)?
        .is_file()
    {
        return Err(contract(format!("{label} must be a regular file")));
    }
    Ok(())
}

fn stable_read_external_json(path: &Path, label: &str) -> Result<Value> {
    safe_absolute_regular_file(path, label)?;
    let before = fs::metadata(path).map_err(JournalError::Io)?;
    let bytes = fs::read(path).map_err(JournalError::Io)?;
    let after = fs::metadata(path).map_err(JournalError::Io)?;
    if before.len() != after.len() || before.modified().ok() != after.modified().ok() {
        return Err(contract(format!("{label} changed while being read")));
    }
    serde_json::from_slice(&bytes)
        .map_err(|error| contract(format!("{label} must be UTF-8 JSON: {error}")))
}

fn exact_integer_map(
    value: &Value,
    expected: &[&str],
    label: &str,
) -> Result<BTreeMap<String, u64>> {
    let fields = value
        .as_object()
        .ok_or_else(|| contract(format!("{label} must be an object")))?;
    if fields.len() != expected.len() || expected.iter().any(|field| !fields.contains_key(*field)) {
        return Err(contract(format!("{label} fields are not exact")));
    }
    expected
        .iter()
        .map(|field| Ok(((*field).to_owned(), number(fields, field, label)?)))
        .collect()
}

fn load_and_verify_identity_ledger(
    binding: Option<&Value>,
    expected_pairs: Option<&BTreeMap<String, String>>,
) -> Result<Option<IdentityLedgerState>> {
    let Some(binding) = binding else {
        return Ok(None);
    };
    let binding_fields = binding
        .as_object()
        .ok_or_else(|| contract("G0 identity ledger binding must be an object"))?;
    let binding_keys = [
        "schemaVersion",
        "ledgerPath",
        "policyName",
        "policySha256",
        "identityPolicy",
        "identityPolicySha256",
    ];
    if binding_fields.len() != binding_keys.len()
        || binding_keys
            .iter()
            .any(|field| !binding_fields.contains_key(*field))
        || binding_fields.get("schemaVersion").and_then(Value::as_str)
            != Some("temporal_qd_native_g0_identity_ledger_binding_v1")
    {
        return Err(contract("G0 identity ledger binding fields are not exact"));
    }
    let ledger_path = PathBuf::from(string(
        binding_fields,
        "ledgerPath",
        "G0 identity ledger binding",
    )?);
    let policy_name = string(binding_fields, "policyName", "G0 identity ledger binding")?;
    let policy_sha = string(binding_fields, "policySha256", "G0 identity ledger binding")?;
    let identity_policy = binding_fields
        .get("identityPolicy")
        .filter(|value| value.is_object())
        .ok_or_else(|| contract("G0 identity ledger binding identity policy is invalid"))?;
    let identity_policy_sha = string(
        binding_fields,
        "identityPolicySha256",
        "G0 identity ledger binding",
    )?;
    sha(policy_sha, "G0 identity ledger policy SHA-256")?;
    sha(
        identity_policy_sha,
        "G0 identity ledger identity policy SHA-256",
    )?;
    if canonical_sha256(identity_policy)? != identity_policy_sha {
        return Err(contract(
            "G0 identity ledger identity policy binding drifted",
        ));
    }

    let ledger = stable_read_external_json(&ledger_path, "G0 global identity ledger")?;
    let fields = ledger
        .as_object()
        .ok_or_else(|| contract("G0 global identity ledger must be an object"))?;
    let keys = [
        "schemaVersion",
        "qdVersion",
        "policyName",
        "policySha256",
        "identityPolicy",
        "records",
        "uniqueCounts",
        "duplicateCounters",
        "proposalSlotCounters",
        "pairExecutableSemantics",
        "pairExecutableSemanticDuplicateRejections",
        "ledgerSha256",
    ];
    if fields.len() != keys.len()
        || keys.iter().any(|field| !fields.contains_key(*field))
        || fields.get("schemaVersion").and_then(Value::as_str)
            != Some("temporal_qd_identity_ledger_v3")
        || fields.get("qdVersion").and_then(Value::as_str) != Some("temporal_qd_evolution_v3")
        || string(fields, "policyName", "G0 global identity ledger")? != policy_name
        || string(fields, "policySha256", "G0 global identity ledger")? != policy_sha
        || fields.get("identityPolicy") != Some(identity_policy)
    {
        return Err(contract("G0 global identity ledger policy binding drifted"));
    }
    let ledger_sha = self_hashed(
        &ledger,
        "ledgerSha256",
        "temporal_qd_identity_ledger_v3",
        "G0 global identity ledger",
    )?;
    let records = fields
        .get("records")
        .and_then(Value::as_array)
        .ok_or_else(|| contract("G0 global identity ledger records are invalid"))?;
    let generic_keys = [
        "candidateIdentitySha256",
        "programSha256",
        "sourceProfileSha256",
        "profileSnapshotSha256",
        "canonicalEvidenceIdentitySha256",
    ];
    let mut generic_sets: BTreeMap<String, BTreeSet<String>> = generic_keys
        .iter()
        .map(|key| ((*key).to_owned(), BTreeSet::new()))
        .collect();
    for record in records {
        let record = record
            .as_object()
            .ok_or_else(|| contract("G0 global identity ledger record is invalid"))?;
        if record.len() != generic_keys.len()
            || generic_keys
                .iter()
                .any(|field| !record.contains_key(*field))
        {
            return Err(contract(
                "G0 global identity ledger record fields are not exact",
            ));
        }
        for field in generic_keys {
            let identity = string(record, field, "G0 global identity ledger record")?;
            sha(identity, "G0 global identity ledger record SHA-256")?;
            generic_sets
                .get_mut(field)
                .expect("closed generic field")
                .insert(identity.to_owned());
        }
    }
    let unique_counts = exact_integer_map(
        fields
            .get("uniqueCounts")
            .expect("closed ledger has uniqueCounts"),
        &[
            "candidateIdentity",
            "program",
            "sourceProfile",
            "profileSnapshot",
            "canonicalEvidence",
        ],
        "G0 global identity ledger unique counts",
    )?;
    for (count_field, record_field) in [
        ("candidateIdentity", "candidateIdentitySha256"),
        ("program", "programSha256"),
        ("sourceProfile", "sourceProfileSha256"),
        ("profileSnapshot", "profileSnapshotSha256"),
        ("canonicalEvidence", "canonicalEvidenceIdentitySha256"),
    ] {
        if unique_counts.get(count_field)
            != generic_sets
                .get(record_field)
                .map(|set| set.len() as u64)
                .as_ref()
        {
            return Err(contract("G0 global identity ledger unique count drifted"));
        }
    }
    exact_integer_map(
        fields
            .get("duplicateCounters")
            .expect("closed ledger has duplicateCounters"),
        &[
            "candidateIdentity",
            "program",
            "sourceProfile",
            "profileSnapshot",
            "canonicalEvidence",
            "programDifferentEvidenceAllowed",
        ],
        "G0 global identity ledger duplicate counters",
    )?;
    exact_integer_map(
        fields
            .get("proposalSlotCounters")
            .expect("closed ledger has proposalSlotCounters"),
        &[
            "proposalsObserved",
            "acceptedUniqueProposalSlots",
            "duplicateRejections",
        ],
        "G0 global identity ledger proposal slot counters",
    )?;
    let pair_records = fields
        .get("pairExecutableSemantics")
        .and_then(Value::as_array)
        .ok_or_else(|| contract("G0 global identity ledger pair semantics are invalid"))?;
    let mut pairs = BTreeMap::new();
    for record in pair_records {
        let record = record
            .as_object()
            .ok_or_else(|| contract("G0 global identity ledger pair record is invalid"))?;
        let pair_keys = [
            "schemaVersion",
            "pairGenomeSemanticSha256",
            "candidateIdentitySha256",
        ];
        if record.len() != pair_keys.len()
            || pair_keys.iter().any(|field| !record.contains_key(*field))
            || record.get("schemaVersion").and_then(Value::as_str)
                != Some("temporal_qd_pair_executable_semantic_record_v1")
        {
            return Err(contract(
                "G0 global identity ledger pair record fields are not exact",
            ));
        }
        let semantic = string(
            record,
            "pairGenomeSemanticSha256",
            "G0 global identity ledger pair record",
        )?;
        let candidate = string(
            record,
            "candidateIdentitySha256",
            "G0 global identity ledger pair record",
        )?;
        sha(semantic, "G0 global identity ledger semantic SHA-256")?;
        sha(candidate, "G0 global identity ledger candidate SHA-256")?;
        if pairs
            .insert(semantic.to_owned(), candidate.to_owned())
            .is_some()
        {
            return Err(contract(
                "G0 global identity ledger repeats executable semantics",
            ));
        }
    }
    let duplicate_rejections = number(
        fields,
        "pairExecutableSemanticDuplicateRejections",
        "G0 global identity ledger",
    )?;
    if let Some(expected_pairs) = expected_pairs {
        if &pairs != expected_pairs {
            return Err(contract(
                "G0 global identity ledger exact semantic/candidate bindings differ from admitted construction",
            ));
        }
    }
    let summary = object([
        (
            "pairExecutableSemanticCount",
            Value::from(pairs.len() as u64),
        ),
        (
            "pairExecutableSemanticDuplicateRejections",
            Value::from(duplicate_rejections),
        ),
        ("identityLedgerSha256", Value::String(ledger_sha)),
    ]);
    Ok(Some(IdentityLedgerState {
        binding: binding.clone(),
        summary,
    }))
}

fn load_and_verify_handoff(
    store: &ProposalJournal,
    request: &G0FunnelRequest,
    scanned: &ScannedJournal,
) -> Result<Option<Value>> {
    if store
        .safe_existing_artifact(Path::new(G0_CONSTRUCTION_HANDOFF_PATH))?
        .is_none()
    {
        return Ok(None);
    }
    let handoff = store.read_artifact(Path::new(G0_CONSTRUCTION_HANDOFF_PATH))?;
    let binding = verify_handoff_surface(request, &handoff)?;
    if binding.accepted_count != scanned.construction_references.len() as u64
        || binding.proposal_count != scanned.entry_ordinals.len() as u64
        || binding.journal_inventory_sha256 != scanned.journal_inventory_sha256
    {
        return Err(contract("G0 construction handoff journal binding drifted"));
    }
    Ok(Some(handoff))
}

#[derive(Debug)]
struct HandoffBinding {
    proposal_count: u64,
    accepted_count: u64,
    journal_inventory_sha256: String,
}

/// Validate the compact completion marker without opening proposal-journal
/// bytes.  Fast adoption relies on this plus the sealed receipt; slow audit
/// additionally compares the returned inventory against a fresh stream scan.
fn verify_handoff_surface(request: &G0FunnelRequest, handoff: &Value) -> Result<HandoffBinding> {
    self_hashed(
        handoff,
        "handoffSha256",
        G0_CONSTRUCTION_HANDOFF_SCHEMA,
        "G0 construction handoff",
    )?;
    let fields = handoff.as_object().expect("self-hashed handoff is object");
    let expected_fields = [
        "schemaVersion",
        "configSha256",
        "generationIndex",
        "constructionPoolSize",
        "evaluationPopulationSize",
        "acceptedCount",
        "proposalCount",
        "lastProposalOrdinal",
        "constructionComplete",
        "journalInventorySha256",
        "operatorImplementationSha256",
        "handoffSha256",
    ];
    if fields.len() != expected_fields.len()
        || expected_fields
            .iter()
            .any(|field| !fields.contains_key(*field))
    {
        return Err(contract("G0 construction handoff fields are not exact"));
    }
    let proposal_count = number(fields, "proposalCount", "G0 construction handoff")?;
    let accepted_count = number(fields, "acceptedCount", "G0 construction handoff")?;
    let journal_inventory_sha256 =
        string(fields, "journalInventorySha256", "G0 construction handoff")?.to_owned();
    sha(
        &journal_inventory_sha256,
        "G0 construction handoff journal inventory SHA-256",
    )?;
    if string(fields, "configSha256", "G0 construction handoff")? != request.config_sha256
        || number(fields, "generationIndex", "G0 construction handoff")? != request.generation_index
        || number(fields, "constructionPoolSize", "G0 construction handoff")?
            != request.construction_pool_size
        || number(
            fields,
            "evaluationPopulationSize",
            "G0 construction handoff",
        )? != request.evaluation_population_size
        || accepted_count != request.construction_pool_size
        || proposal_count == 0
        || number(fields, "lastProposalOrdinal", "G0 construction handoff")? != proposal_count - 1
        || fields.get("constructionComplete").and_then(Value::as_bool) != Some(true)
        || string(
            fields,
            "operatorImplementationSha256",
            "G0 construction handoff",
        )? != canonical_sha256(&request.publication_policy.operator_implementation_identity)?
    {
        return Err(contract("G0 construction handoff binding drifted"));
    }
    Ok(HandoffBinding {
        proposal_count,
        accepted_count,
        journal_inventory_sha256,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_receipt(
    request: &G0FunnelRequest,
    scanned: &ScannedJournal,
    global_identity_ledger: Option<&IdentityLedgerState>,
    handoff: Option<&Value>,
    population: &WrittenArtifact,
    evaluation: &WrittenArtifact,
    journal: &WrittenArtifact,
    population_sha256: &str,
    evaluation_population_sha256: &str,
    generation_journal_sha256: &str,
    g0_binding: Option<&BTreeMap<String, String>>,
    pair_generation_result: &Value,
) -> Result<Value> {
    let binding = g0_binding.ok_or_else(|| contract("G0 publication lacks a G0 binding"))?;
    let string_binding = |field: &str| {
        binding
            .get(field)
            .cloned()
            .ok_or_else(|| contract(format!("G0 publication binding lacks {field}")))
    };
    let mut receipt = object([
        (
            "schemaVersion",
            Value::String(G0_FUNNEL_RECEIPT_SCHEMA.to_owned()),
        ),
        (
            "requestSha256",
            Value::String(request.request_sha256.clone()),
        ),
        (
            "authoritySha256",
            Value::String(request.authority_sha256.clone()),
        ),
        ("executionAuthority", request.execution_authority.clone()),
        ("configSha256", Value::String(request.config_sha256.clone())),
        ("generationIndex", Value::from(request.generation_index)),
        (
            "constructionPoolSize",
            Value::from(request.construction_pool_size),
        ),
        (
            "evaluationPopulationSize",
            Value::from(request.evaluation_population_size),
        ),
        (
            "operatorImplementationSha256",
            Value::String(canonical_sha256(
                &request.publication_policy.operator_implementation_identity,
            )?),
        ),
        (
            "archivePolicyAuthoritySha256",
            request
                .publication_policy
                .archive_policy_authority
                .as_ref()
                .map(canonical_sha256)
                .transpose()?
                .map(Value::String)
                .unwrap_or(Value::Null),
        ),
        (
            "journalInventorySha256",
            Value::String(scanned.journal_inventory_sha256.clone()),
        ),
        (
            "sourceHandoffSha256",
            handoff
                .and_then(|value| value.get("handoffSha256"))
                .cloned()
                .unwrap_or(Value::Null),
        ),
        (
            "globalIdentityLedger",
            global_identity_ledger
                .map(|ledger| ledger.summary.clone())
                .unwrap_or(Value::Null),
        ),
        (
            "identityLedgerBinding",
            global_identity_ledger
                .map(|ledger| ledger.binding.clone())
                .unwrap_or(Value::Null),
        ),
        (
            "g0Bootstrap",
            Value::Object(
                binding
                    .iter()
                    .map(|(key, value)| (key.clone(), Value::String(value.clone())))
                    .collect(),
            ),
        ),
        (
            "population",
            artifact_receipt_value(population, population_sha256),
        ),
        (
            "evaluationPopulation",
            artifact_receipt_value(evaluation, evaluation_population_sha256),
        ),
        (
            "generationJournal",
            artifact_receipt_value(journal, generation_journal_sha256),
        ),
        ("pairGenerationResult", pair_generation_result.clone()),
        (
            "constructionPoolIdentitySha256",
            Value::String(string_binding("constructionPoolIdentitySha256")?),
        ),
        (
            "acceptedPoolSha256",
            Value::String(string_binding("acceptedPoolSha256")?),
        ),
        (
            "selectionSha256",
            Value::String(string_binding("selectionSha256")?),
        ),
        (
            "ledgerSha256",
            Value::String(string_binding("ledgerSha256")?),
        ),
    ]);
    let hash = canonical_sha256(&receipt)?;
    receipt
        .as_object_mut()
        .expect("receipt is object")
        .insert("receiptSha256".to_owned(), Value::String(hash));
    Ok(receipt)
}

fn artifact_receipt_value(artifact: &WrittenArtifact, semantic_sha256: &str) -> Value {
    object([
        (
            "relativePath",
            Value::String(
                artifact
                    .path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default()
                    .to_owned(),
            ),
        ),
        ("semanticSha256", Value::String(semantic_sha256.to_owned())),
        ("fileSha256", Value::String(artifact.file_sha256.clone())),
        ("encodedBytes", Value::from(artifact.encoded_bytes)),
    ])
}

fn pair_generation_result_from_journal(journal: &Value) -> Result<Value> {
    let fields = journal
        .as_object()
        .ok_or_else(|| contract("generation journal must be an object"))?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some("temporal_qd_generation_journal_v3")
    {
        return Err(contract("generation journal schema is incompatible"));
    }
    let required = |field: &str| {
        fields
            .get(field)
            .cloned()
            .ok_or_else(|| contract(format!("generation journal lacks {field}")))
    };
    let proposal_slots = required("proposalSlots")?;
    let constructed_pool_size = proposal_slots
        .as_object()
        .and_then(|slots| slots.get("constructionPoolSize"))
        .cloned()
        .ok_or_else(|| contract("G0 generation journal lacks construction pool size"))?;
    let mut result = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_pair_generation_result_v1".to_owned()),
        ),
        ("configSha256", required("configSha256")?),
        ("populationSha256", required("populationSha256")?),
        (
            "evaluationPopulationSha256",
            required("evaluationPopulationSha256")?,
        ),
        ("journalSha256", required("journalSha256")?),
        ("proposalCount", required("proposalCount")?),
        ("candidateCount", required("acceptedCount")?),
        ("constructionPoolSize", constructed_pool_size),
        (
            "constructedAcceptedCount",
            required("constructedAcceptedCount")?,
        ),
        ("g0Bootstrap", required("g0Bootstrap")?),
        ("originProposalCounts", required("originProposalCounts")?),
        ("originAcceptedCounts", required("originAcceptedCounts")?),
        (
            "reproductionAllocation",
            required("reproductionAllocation")?,
        ),
        (
            "reproductionAllocationAccounting",
            required("reproductionAllocationAccounting")?,
        ),
        ("proposalSlots", required("proposalSlots")?),
        ("uniqueIdentityCounts", required("uniqueIdentityCounts")?),
        ("duplicateCounters", required("duplicateCounters")?),
        ("proposalSlotCounters", required("proposalSlotCounters")?),
        (
            "nextImmigrantContinuationOrdinal",
            required("nextImmigrantContinuationOrdinal")?,
        ),
        ("completed", Value::Bool(true)),
    ]);
    if let Some(distribution) = fields.get("immigrantConstructionDistribution") {
        result
            .as_object_mut()
            .expect("pair result is object")
            .insert(
                "immigrantConstructionDistribution".to_owned(),
                distribution.clone(),
            );
    }
    Ok(result)
}

fn verify_receipt_for_adoption(
    store: &ProposalJournal,
    request: &G0FunnelRequest,
    receipt: &Value,
) -> Result<(Value, Value)> {
    let started = Instant::now();
    self_hashed(
        receipt,
        "receiptSha256",
        G0_FUNNEL_RECEIPT_SCHEMA,
        "G0 funnel receipt",
    )?;
    let fields = receipt.as_object().expect("self-hashed receipt is object");
    let expected_fields = [
        "schemaVersion",
        "requestSha256",
        "authoritySha256",
        "executionAuthority",
        "configSha256",
        "generationIndex",
        "constructionPoolSize",
        "evaluationPopulationSize",
        "operatorImplementationSha256",
        "archivePolicyAuthoritySha256",
        "journalInventorySha256",
        "sourceHandoffSha256",
        "globalIdentityLedger",
        "identityLedgerBinding",
        "g0Bootstrap",
        "population",
        "evaluationPopulation",
        "generationJournal",
        "pairGenerationResult",
        "constructionPoolIdentitySha256",
        "acceptedPoolSha256",
        "selectionSha256",
        "ledgerSha256",
        "receiptSha256",
    ];
    if fields.len() != expected_fields.len()
        || expected_fields
            .iter()
            .any(|field| !fields.contains_key(*field))
    {
        return Err(contract("G0 funnel receipt fields are not exact"));
    }
    if string(fields, "requestSha256", "G0 funnel receipt")? != request.request_sha256
        || string(fields, "authoritySha256", "G0 funnel receipt")? != request.authority_sha256
        || fields.get("executionAuthority") != Some(&request.execution_authority)
        || string(fields, "configSha256", "G0 funnel receipt")? != request.config_sha256
        || number(fields, "generationIndex", "G0 funnel receipt")? != request.generation_index
        || number(fields, "constructionPoolSize", "G0 funnel receipt")?
            != request.construction_pool_size
        || number(fields, "evaluationPopulationSize", "G0 funnel receipt")?
            != request.evaluation_population_size
        || string(fields, "operatorImplementationSha256", "G0 funnel receipt")?
            != canonical_sha256(&request.publication_policy.operator_implementation_identity)?
    {
        return Err(contract("G0 funnel receipt request binding drifted"));
    }
    let expected_archive = request
        .publication_policy
        .archive_policy_authority
        .as_ref()
        .map(canonical_sha256)
        .transpose()?;
    match (fields.get("archivePolicyAuthoritySha256"), expected_archive) {
        (Some(Value::Null), None) => {}
        (Some(Value::String(actual)), Some(expected)) if actual == &expected => {}
        _ => return Err(contract("G0 funnel receipt archive policy binding drifted")),
    }
    let loaded_ledger = load_and_verify_identity_ledger(request.identity_ledger.as_ref(), None)?
        .or_else(|| {
            request
                .global_identity_ledger
                .clone()
                .map(|summary| IdentityLedgerState {
                    binding: Value::Null,
                    summary,
                })
        });
    let null_ledger = Value::Null;
    let expected_ledger_summary = loaded_ledger
        .as_ref()
        .map(|ledger| &ledger.summary)
        .unwrap_or(&null_ledger);
    let expected_ledger_binding = loaded_ledger
        .as_ref()
        .map(|ledger| &ledger.binding)
        .unwrap_or(&null_ledger);
    if fields.get("globalIdentityLedger") != Some(expected_ledger_summary)
        || fields.get("identityLedgerBinding") != Some(expected_ledger_binding)
    {
        return Err(contract(
            "G0 funnel receipt global identity ledger binding drifted",
        ));
    }
    let journal_inventory_sha256 = string(fields, "journalInventorySha256", "G0 funnel receipt")?;
    sha(
        journal_inventory_sha256,
        "G0 funnel receipt journal inventory SHA-256",
    )?;
    match fields.get("sourceHandoffSha256") {
        Some(Value::Null) => {
            if store
                .safe_existing_artifact(Path::new(G0_CONSTRUCTION_HANDOFF_PATH))?
                .is_some()
            {
                return Err(contract(
                    "sealed G0 receipt omitted an existing construction handoff",
                ));
            }
        }
        Some(Value::String(expected_handoff_sha)) => {
            sha(
                expected_handoff_sha,
                "G0 funnel receipt source handoff SHA-256",
            )?;
            let handoff = store.read_artifact(Path::new(G0_CONSTRUCTION_HANDOFF_PATH))?;
            let binding = verify_handoff_surface(request, &handoff)?;
            if handoff.get("handoffSha256").and_then(Value::as_str) != Some(expected_handoff_sha)
                || binding.journal_inventory_sha256 != journal_inventory_sha256
            {
                return Err(contract("sealed G0 receipt handoff binding drifted"));
            }
        }
        _ => return Err(contract("G0 funnel receipt source handoff is invalid")),
    }
    let pool = store.read_artifact(Path::new("g0-bootstrap/accepted-pool.json"))?;
    let selection = store.read_artifact(Path::new("g0-bootstrap/selection.json"))?;
    let ledger =
        store.read_artifact(Path::new("g0-bootstrap/campaign-construction-ledger.json"))?;
    let references = validate_accepted_pool(&pool)
        .map_err(|error| contract(format!("sealed G0 pool validation failed: {error}")))?;
    if references.len() as u64 != request.construction_pool_size {
        return Err(contract("sealed G0 pool width drifted"));
    }
    let selection = verify_g0_bootstrap_selection(&selection, &pool)
        .map_err(|error| contract(format!("sealed G0 selection validation failed: {error}")))?;
    let selected_reference_sha256s = selection
        .get("selected")
        .and_then(Value::as_array)
        .ok_or_else(|| contract("sealed G0 selection lacks selected references"))?
        .iter()
        .map(|row| {
            row.get("referenceSha256")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .ok_or_else(|| contract("sealed G0 selection reference is invalid"))
        })
        .collect::<Result<Vec<_>>>()?;
    verify_campaign_ledger(&ledger, &pool, &selected_reference_sha256s)
        .map_err(|error| contract(format!("sealed G0 ledger validation failed: {error}")))?;
    for (field, artifact, hash_field) in [
        ("acceptedPoolSha256", &pool, "acceptedPoolSha256"),
        ("selectionSha256", &selection, "selectionSha256"),
        ("ledgerSha256", &ledger, "ledgerSha256"),
    ] {
        let actual = artifact
            .get(hash_field)
            .and_then(Value::as_str)
            .ok_or_else(|| contract("sealed G0 artifact lacks identity"))?;
        if string(fields, field, "G0 funnel receipt")? != actual {
            return Err(contract("sealed G0 artifact receipt identity drifted"));
        }
    }
    let mut output_bytes_hashed =
        stream_verify_cached_output(store, fields, "population", "population.json")?;
    output_bytes_hashed += stream_verify_cached_output(
        store,
        fields,
        "evaluationPopulation",
        "evaluation-population.json",
    )?;
    output_bytes_hashed += stream_verify_cached_output(
        store,
        fields,
        "generationJournal",
        "generation-journal.json",
    )?;
    let journal = store.read_artifact(Path::new("generation-journal.json"))?;
    let journal_sha = self_hashed(
        &journal,
        "journalSha256",
        "temporal_qd_generation_journal_v3",
        "sealed G0 generation journal",
    )?;
    if fields
        .get("generationJournal")
        .and_then(Value::as_object)
        .and_then(|artifact| artifact.get("semanticSha256"))
        .and_then(Value::as_str)
        != Some(journal_sha.as_str())
    {
        return Err(contract(
            "sealed G0 generation journal semantic identity drifted",
        ));
    }
    let evaluation = store.read_artifact(Path::new("evaluation-population.json"))?;
    let evaluation_sha = self_hashed(
        &evaluation,
        "evaluationPopulationSha256",
        "temporal_qd_evaluation_population_v1",
        "sealed G0 evaluation population",
    )?;
    if fields
        .get("evaluationPopulation")
        .and_then(Value::as_object)
        .and_then(|artifact| artifact.get("semanticSha256"))
        .and_then(Value::as_str)
        != Some(evaluation_sha.as_str())
    {
        return Err(contract(
            "sealed G0 evaluation population semantic identity drifted",
        ));
    }
    let result = pair_generation_result_from_journal(&journal)?;
    for (receipt_field, journal_field) in [
        ("population", "populationSha256"),
        ("evaluationPopulation", "evaluationPopulationSha256"),
        ("generationJournal", "journalSha256"),
    ] {
        let receipt_semantic = fields
            .get(receipt_field)
            .and_then(Value::as_object)
            .and_then(|artifact| artifact.get("semanticSha256"))
            .and_then(Value::as_str);
        let journal_semantic = journal.get(journal_field).and_then(Value::as_str);
        if receipt_semantic != journal_semantic {
            return Err(contract(
                "sealed G0 public semantic receipt binding drifted",
            ));
        }
    }
    if fields.get("pairGenerationResult") != Some(&result) {
        return Err(contract("sealed G0 receipt result binding drifted"));
    }
    let adoption_verification = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_native_g0_adoption_verification_v1".to_owned()),
        ),
        ("outputBytesHashed", Value::from(output_bytes_hashed)),
        (
            "outputHashElapsedMilliseconds",
            Value::from(started.elapsed().as_millis() as u64),
        ),
        ("proposalJournalBytesRead", Value::from(0_u64)),
    ]);
    Ok((result, adoption_verification))
}

fn stream_verify_cached_output(
    store: &ProposalJournal,
    receipt: &Map<String, Value>,
    field: &str,
    expected_relative: &str,
) -> Result<u64> {
    let artifact = receipt
        .get(field)
        .and_then(Value::as_object)
        .ok_or_else(|| contract("G0 funnel receipt artifact binding is invalid"))?;
    if artifact.get("relativePath").and_then(Value::as_str)
        != Some(
            Path::new(expected_relative)
                .file_name()
                .and_then(|value| value.to_str())
                .expect("fixed file name"),
        )
    {
        return Err(contract("G0 funnel receipt artifact path drifted"));
    }
    let expected_bytes = number(artifact, "encodedBytes", "G0 funnel receipt artifact")?;
    let expected_file_sha = string(artifact, "fileSha256", "G0 funnel receipt artifact")?;
    let expected_semantic_sha = string(artifact, "semanticSha256", "G0 funnel receipt artifact")?;
    sha(expected_file_sha, "G0 funnel receipt artifact file SHA-256")?;
    sha(
        expected_semantic_sha,
        "G0 funnel receipt artifact semantic SHA-256",
    )?;
    let path = store
        .safe_existing_artifact(Path::new(expected_relative))?
        .ok_or_else(|| contract("sealed public artifact disappeared"))?;
    let metadata = fs::metadata(&path).map_err(JournalError::Io)?;
    if metadata.len() != expected_bytes {
        return Err(contract("sealed public artifact byte length drifted"));
    }
    let mut file = fs::File::open(&path).map_err(JournalError::Io)?;
    let mut digest = Sha256::new();
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(JournalError::Io)?;
        if read == 0 {
            break;
        }
        bytes += read as u64;
        digest.update(&buffer[..read]);
    }
    let after = fs::metadata(&path).map_err(JournalError::Io)?;
    if metadata.len() != after.len()
        || metadata.modified().ok() != after.modified().ok()
        || bytes != expected_bytes
    {
        return Err(contract(
            "sealed public artifact changed while being hashed",
        ));
    }
    let actual_file_sha = format!("sha256:{:x}", digest.finalize());
    if actual_file_sha != expected_file_sha {
        return Err(contract("sealed public artifact file SHA-256 drifted"));
    }
    Ok(bytes)
}
