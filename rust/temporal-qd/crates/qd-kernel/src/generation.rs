//! One-call pre-market Temporal-QD generation transaction.
//!
//! This stops at the durable evaluation-population handoff.  Campaign creation,
//! worker dispatch, replay, and archive reduction remain outside the native
//! generation boundary.

use std::{collections::BTreeSet, fs, path::PathBuf};

use temporal_qd_contract::{ContractError, Map, Value, canonical_json_bytes, canonical_sha256};

use crate::{
    factory::{NativeConstructionContext, NativePairAuthority, PairFactory},
    journal::{
        AcceptedReference, CheckpointInput, FinalNewline, ProposalJournal, SegmentInput,
        self_hashed_generation_head, verify_self_hash,
    },
    proposal::{
        AcceptedProposal, IdentityLedger, PROPOSAL_ENTRY_SCHEMA, ParentSelector, PreparedProposal,
        ProposalAssembler, ProposalPlanner, ProposalSchedule, ProposalState,
    },
    publication::{
        FRONT_GENERATION_RESULT_SCHEMA, PublicationPolicy, PublicationRequest, publish_generation,
    },
};

pub const GENERATION_REQUEST_SCHEMA: &str = "temporal_qd_front_generation_request_v1";
pub const GENERATION_PROGRESS_SCHEMA: &str = "temporal_qd_front_generation_progress_v1";

// `temporal_qd_pair_generation_v2` predates the accepted-quota allocation
// record.  Only the two already-sealed evolution campaigns below are allowed
// to use that historical shape.  New v2/v5 material must carry the explicit
// allocation; otherwise a caller could accidentally recreate an old,
// ordinal-scheduled campaign under the new selection semantics.
const PAIR_GENERATION_SCHEMA_LEGACY: &str = "temporal_qd_pair_generation_v1";
const PAIR_GENERATION_SCHEMA: &str = "temporal_qd_pair_generation_v2";
const FROZEN_LEGACY_QD_VERSIONS: &[&str] =
    &["temporal_qd_evolution_v3", "temporal_qd_evolution_v4"];
const REPRODUCTION_ALLOCATION_SCHEMA: &str = "temporal_qd_reproduction_allocation_v1";
const REPRODUCTION_ALLOCATION_SCHEMA_ACCEPTED: &str = "temporal_qd_reproduction_allocation_v2";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReproductionPolicy {
    /// A campaign sealed before accepted-quota allocation was introduced.
    /// Its parent schedule is still checked against the frozen pair config;
    /// the original kernel quota projection remains its recovery behavior.
    LegacyProjection,
    /// A fresh campaign has committed its exact accepted-population quota.
    FrozenAcceptedQuota {
        desired_offspring: u64,
        desired_immigrants: u64,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum GenerationError {
    #[error("generation filesystem failure: {0}")]
    Io(#[from] std::io::Error),
    #[error("canonical contract failure: {0}")]
    Canonical(#[from] ContractError),
    #[error("proposal failure: {0}")]
    Proposal(#[from] crate::proposal::ProposalError),
    #[error("factory failure: {0}")]
    Factory(#[from] crate::factory::FactoryError),
    #[error("journal failure: {0}")]
    Journal(#[from] crate::journal::JournalError),
    #[error("publication failure: {0}")]
    Publication(#[from] crate::publication::PublicationError),
    #[error("generation contract failure: {0}")]
    Contract(String),
    #[error(
        "a durable proposal segment exists without a checkpoint; recovery needs the explicit tail-replay adapter before native work can continue"
    )]
    UncheckpointedSegmentRecoveryRequired,
}

pub type Result<T> = std::result::Result<T, GenerationError>;

fn contract(message: impl Into<String>) -> GenerationError {
    GenerationError::Contract(message.into())
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

fn u64_field(fields: &Map<String, Value>, name: &str, label: &str) -> Result<u64> {
    fields
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| contract(format!("{label} {name} is invalid")))
}

/// Decode the *already frozen* schedule object in a pair config.  This is
/// deliberately kept in the kernel rather than trusting the caller's compact
/// schedule: the pair-config self-hash is the restart authority.
fn frozen_parent_schedule(value: &Value) -> Result<crate::schedule::RotatingParentSchedule> {
    let fields = value
        .as_object()
        .ok_or_else(|| contract("pair config parentSchedule must be an object"))?;
    let breeder_width = u64_field(fields, "breederWidth", "pair config parentSchedule")?;
    let breeder_parent_count =
        u64_field(fields, "breederParentCount", "pair config parentSchedule")?;
    match fields.get("schemaVersion").and_then(Value::as_str) {
        Some("temporal_qd_rotating_parent_schedule_v1") => {
            let numerator = u64_field(fields, "offspringNumerator", "legacy parentSchedule")?;
            let denominator = u64_field(fields, "offspringDenominator", "legacy parentSchedule")?;
            let expected = crate::schedule::RotatingParentSchedule::legacy_schedule_sha256(
                breeder_width,
                breeder_parent_count,
                numerator,
                denominator,
            );
            if fields.get("scheduleSha256").and_then(Value::as_str) != Some(expected.as_str()) {
                return Err(contract(
                    "legacy pair config parentSchedule identity mismatch",
                ));
            }
            crate::schedule::RotatingParentSchedule::validated_legacy_fields(
                breeder_width,
                breeder_parent_count,
                numerator,
                denominator,
            )
            .map_err(|error| {
                contract(format!(
                    "legacy pair config parentSchedule is invalid: {error}"
                ))
            })
        }
        Some("temporal_qd_rotating_parent_schedule_v2") => {
            if fields
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
                    != Some(crate::schedule::RATIONAL_PREFIX_BALANCE_METHOD)
            {
                return Err(contract("pair config parentSchedule v2 policy is invalid"));
            }
            let schedule = crate::schedule::RotatingParentSchedule::from_counts(
                breeder_width,
                breeder_parent_count,
            )
            .map_err(|error| contract(format!("pair config parentSchedule is invalid: {error}")))?;
            if fields.get("scheduleSha256").and_then(Value::as_str)
                != Some(schedule.schedule_sha256().as_str())
            {
                return Err(contract("pair config parentSchedule identity mismatch"));
            }
            Ok(schedule)
        }
        _ => Err(contract("pair config parentSchedule schema is invalid")),
    }
}

fn frozen_qd_version(fields: &Map<String, Value>) -> Option<&str> {
    fields
        .get("runConfig")
        .and_then(Value::as_object)
        .and_then(|run| run.get("parameters"))
        .and_then(Value::as_object)
        .and_then(|parameters| parameters.get("version"))
        .and_then(Value::as_str)
}

#[derive(Clone, Debug)]
pub struct GenerateGenerationRequest {
    pub output_root: PathBuf,
    pub final_newline: FinalNewline,
    /// Exact Python-authored config document, including its self-hash field.
    pub pair_config: Value,
    pub config_sha256: String,
    pub generation_index: u64,
    pub target_unique_candidates: u64,
    pub max_proposal_attempts: u64,
    /// A bounded continuation used by the split-restart gate.  It may stop a
    /// call before population publication but must still seal every completed
    /// proposal and checkpoint.
    pub max_new_proposals: Option<u64>,
    pub parent_schedule: Option<crate::schedule::RotatingParentSchedule>,
    pub expected_native_authority_sha256: String,
    pub publication_policy: PublicationPolicy,
    pub g0_evaluation_width: Option<u64>,
    pub evidence_identity_context: Option<Value>,
    pub frozen_construction_catalog: Option<Value>,
    pub factory_construction_policy: Option<Value>,
}

impl GenerateGenerationRequest {
    fn reproduction_policy(
        &self,
        config_fields: &Map<String, Value>,
    ) -> Result<ReproductionPolicy> {
        // The compact request schedule is a convenience for the planner, not
        // an authority.  If a schedule was sealed in the config, it must
        // decode and project to exactly the same compact value before a
        // restart can touch a journal.
        match config_fields.get("parentSchedule") {
            Some(Value::Null) | None => {
                if self.parent_schedule.is_some() {
                    return Err(contract(
                        "pair config lacks the frozen parentSchedule requested by generation",
                    ));
                }
            }
            Some(raw) => {
                let frozen = frozen_parent_schedule(raw)?;
                if self.parent_schedule != Some(frozen) {
                    return Err(contract(
                        "pair config parentSchedule diverged from generation request",
                    ));
                }
            }
        }

        let schema = config_fields.get("schemaVersion").and_then(Value::as_str);
        let allocation = config_fields.get("reproductionAllocation");
        let legacy = schema == Some(PAIR_GENERATION_SCHEMA_LEGACY)
            || (schema == Some(PAIR_GENERATION_SCHEMA)
                && allocation.is_none()
                && frozen_qd_version(config_fields)
                    .is_some_and(|version| FROZEN_LEGACY_QD_VERSIONS.contains(&version)));
        if legacy {
            if allocation.is_some() {
                return Err(contract(
                    "legacy pair config must not carry a partial reproduction allocation",
                ));
            }
            return Ok(ReproductionPolicy::LegacyProjection);
        }
        if schema != Some(PAIR_GENERATION_SCHEMA) {
            return Err(contract(
                "pair config schema is not admitted for native generation",
            ));
        }
        let allocation = allocation
            .and_then(Value::as_object)
            .ok_or_else(|| contract("pair config lacks frozen reproduction allocation"))?;
        let has_supported_parents = self
            .parent_schedule
            .is_some_and(|schedule| schedule.breeder_parent_count > 0);
        let expected_immigrants = crate::schedule::accepted_quota_immigrant_count(
            self.target_unique_candidates,
            has_supported_parents,
        );
        let desired_offspring = self.target_unique_candidates - expected_immigrants;
        let accepted_terms = allocation.get("schemaVersion").and_then(Value::as_str)
            == Some(REPRODUCTION_ALLOCATION_SCHEMA_ACCEPTED);
        if (!accepted_terms
            && allocation.get("schemaVersion").and_then(Value::as_str)
                != Some(REPRODUCTION_ALLOCATION_SCHEMA))
            || allocation
                .get(if accepted_terms {
                    "targetAcceptedCandidates"
                } else {
                    "targetEvaluatedCandidates"
                })
                .and_then(Value::as_u64)
                != Some(self.target_unique_candidates)
            || allocation
                .get(if accepted_terms {
                    "desiredAcceptedImmigrantCount"
                } else {
                    "desiredEvaluatedImmigrantCount"
                })
                .and_then(Value::as_u64)
                != Some(expected_immigrants)
            || allocation
                .get(if accepted_terms {
                    "desiredAcceptedOffspringCount"
                } else {
                    "desiredEvaluatedOffspringCount"
                })
                .and_then(Value::as_u64)
                != Some(desired_offspring)
            || allocation
                .get("minimumImmigrantNumerator")
                .and_then(Value::as_u64)
                != Some(1)
            || allocation
                .get("minimumImmigrantDenominator")
                .and_then(Value::as_u64)
                != Some(5)
            || allocation.get("parentSampling").and_then(Value::as_str)
                != Some("with_replacement_supported_parents_v1")
            || allocation
                .get("unsupportedParentPolicy")
                .and_then(Value::as_str)
                != Some("immigrant_only_authority_bound_v1")
            || allocation.get("allocationMethod").and_then(Value::as_str)
                != Some("accepted_quota_prefix_balance_v1")
        {
            return Err(contract("pair config reproduction allocation is invalid"));
        }
        Ok(ReproductionPolicy::FrozenAcceptedQuota {
            desired_offspring,
            desired_immigrants: expected_immigrants,
        })
    }

    fn reproduction_allocation_for_parents(&self, has_parents: bool) -> Result<(u64, u64)> {
        let mut material = self.pair_config.clone();
        let fields = material
            .as_object_mut()
            .ok_or_else(|| contract("pair config must be an object"))?;
        fields.remove("configSha256");
        match self.reproduction_policy(fields)? {
            ReproductionPolicy::LegacyProjection => {
                let immigrants = if has_parents {
                    if self.target_unique_candidates < 5 {
                        0
                    } else {
                        self.target_unique_candidates.div_ceil(5)
                    }
                } else {
                    self.target_unique_candidates
                };
                Ok((self.target_unique_candidates - immigrants, immigrants))
            }
            ReproductionPolicy::FrozenAcceptedQuota {
                desired_offspring,
                desired_immigrants,
            } => {
                let expected_immigrants = crate::schedule::accepted_quota_immigrant_count(
                    self.target_unique_candidates,
                    has_parents,
                );
                if desired_immigrants != expected_immigrants
                    || desired_offspring != self.target_unique_candidates - expected_immigrants
                {
                    return Err(contract(
                        "frozen reproduction allocation disagrees with the supplied parent selector",
                    ));
                }
                Ok((desired_offspring, desired_immigrants))
            }
        }
    }

    pub fn validate(&self) -> Result<()> {
        sha(&self.config_sha256, "generation config SHA-256")?;
        let mut config_material = self.pair_config.clone();
        let config_fields = config_material
            .as_object_mut()
            .ok_or_else(|| contract("pair config must be an object"))?;
        if config_fields.get("configSha256").and_then(Value::as_str)
            != Some(self.config_sha256.as_str())
        {
            return Err(contract(
                "pair config does not carry the requested config identity",
            ));
        }
        config_fields.remove("configSha256");
        if canonical_sha256(&config_material)? != self.config_sha256 {
            return Err(contract("pair config self-hash is invalid"));
        }
        if config_material
            .as_object()
            .and_then(|fields| fields.get("immigrantConstructionPolicy"))
            != self.factory_construction_policy.as_ref()
        {
            return Err(contract(
                "pair config immigrant construction policy diverged from request",
            ));
        }
        let policy_fields = config_material
            .as_object()
            .ok_or_else(|| contract("pair config must be an object"))?;
        let _ = self.reproduction_policy(policy_fields)?;
        sha(
            &self.expected_native_authority_sha256,
            "native proposal authority SHA-256",
        )?;
        self.publication_policy
            .validate()
            .map_err(|error| contract(error.to_string()))?;
        if self.target_unique_candidates == 0 || self.max_proposal_attempts == 0 {
            return Err(contract(
                "generation target and proposal ceiling must be positive",
            ));
        }
        if let Some(width) = self.g0_evaluation_width {
            if self.generation_index != 1 || width == 0 || width > self.target_unique_candidates {
                return Err(contract("G0 width is invalid for this generation"));
            }
        }
        for (value, label) in [
            (
                self.evidence_identity_context.as_ref(),
                "evidence identity context",
            ),
            (
                self.frozen_construction_catalog.as_ref(),
                "frozen construction catalog",
            ),
            (
                self.factory_construction_policy.as_ref(),
                "factory construction policy",
            ),
        ] {
            if let Some(value) = value {
                if !value.is_object() {
                    return Err(contract(format!("{label} must be an object")));
                }
            }
        }
        // The immutable construction policy carries the immigrant collision
        // tripwire.  Validate it before any checkpoint or proposal write so a
        // malformed policy cannot create a resumable partial generation.
        let _ = collision_tripwire_policy(self)?;
        Ok(())
    }

    pub fn compatibility_request_identity(&self, ledger_identity: &Value) -> Result<String> {
        let parent_schedule = self.parent_schedule.map(|schedule| {
            object([
                ("breederWidth", Value::from(schedule.breeder_width)),
                (
                    "breederParentCount",
                    Value::from(schedule.breeder_parent_count),
                ),
                (
                    "offspringNumerator",
                    Value::from(schedule.offspring_numerator),
                ),
                (
                    "offspringDenominator",
                    Value::from(schedule.offspring_denominator),
                ),
                ("scheduleSha256", Value::String(schedule.schedule_sha256())),
            ])
        });
        let value = object([
            (
                "schemaVersion",
                Value::String(GENERATION_REQUEST_SCHEMA.to_owned()),
            ),
            ("configSha256", Value::String(self.config_sha256.clone())),
            ("generationIndex", Value::from(self.generation_index)),
            (
                "targetUniqueCandidates",
                Value::from(self.target_unique_candidates),
            ),
            (
                "maxProposalAttempts",
                Value::from(self.max_proposal_attempts),
            ),
            (
                "nativeAuthoritySha256",
                Value::String(self.expected_native_authority_sha256.clone()),
            ),
            ("parentSchedule", parent_schedule.unwrap_or(Value::Null)),
            (
                "g0EvaluationWidth",
                self.g0_evaluation_width
                    .map(Value::from)
                    .unwrap_or(Value::Null),
            ),
            (
                "evidenceIdentityContext",
                self.evidence_identity_context
                    .clone()
                    .unwrap_or(Value::Null),
            ),
            (
                "frozenConstructionCatalog",
                self.frozen_construction_catalog
                    .clone()
                    .unwrap_or(Value::Null),
            ),
            (
                "factoryConstructionPolicy",
                self.factory_construction_policy
                    .clone()
                    .unwrap_or(Value::Null),
            ),
            (
                "publicationPolicy",
                object([
                    (
                        "qdVersion",
                        Value::String(self.publication_policy.qd_version.clone()),
                    ),
                    (
                        "policyName",
                        Value::String(self.publication_policy.policy_name.clone()),
                    ),
                    (
                        "policySha256",
                        Value::String(self.publication_policy.policy_sha256.clone()),
                    ),
                    ("pairPolicy", self.publication_policy.pair_policy.clone()),
                    (
                        "operatorImplementation",
                        self.publication_policy
                            .operator_implementation_identity
                            .clone(),
                    ),
                    (
                        "predeclaredEvidenceContextSha256",
                        self.publication_policy
                            .predeclared_evidence_context_sha256
                            .clone()
                            .map(Value::String)
                            .unwrap_or(Value::Null),
                    ),
                ]),
            ),
            ("ledgerAuthority", ledger_identity.clone()),
        ]);
        Ok(canonical_sha256(&value)?)
    }
}

fn collision_tripwire_policy(request: &GenerateGenerationRequest) -> Result<Option<(u64, f64)>> {
    let Some(policy) = &request.factory_construction_policy else {
        return Ok(None);
    };
    let tripwire = policy
        .as_object()
        .and_then(|fields| fields.get("collisionTripwire"))
        .and_then(Value::as_object)
        .ok_or_else(|| contract("pair immigrant collision tripwire policy is invalid"))?;
    let minimum_attempts = tripwire
        .get("minimumImmigrantAttempts")
        .and_then(Value::as_u64)
        .filter(|value| *value >= 1)
        .ok_or_else(|| contract("pair immigrant collision tripwire policy is invalid"))?;
    let minimum_ratio = tripwire
        .get("minimumAcceptedRatio")
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite() && *value > 0.0 && *value <= 1.0)
        .ok_or_else(|| contract("pair immigrant collision tripwire policy is invalid"))?;
    Ok(Some((minimum_attempts, minimum_ratio)))
}

fn enforce_immigrant_collision_tripwire(
    store: &ProposalJournal,
    request: &GenerateGenerationRequest,
    runtime: &RuntimeState,
    ledger: &dyn IdentityLedger,
    immigrant_only_bootstrap: bool,
) -> Result<()> {
    let Some((minimum_attempts, minimum_ratio)) = collision_tripwire_policy(request)? else {
        return Ok(());
    };
    if !immigrant_only_bootstrap || runtime.proposal_state.immigrant_attempts < minimum_attempts {
        return Ok(());
    }
    let accepted_ratio = runtime.proposal_state.immigrant_accepted as f64
        / runtime.proposal_state.immigrant_attempts as f64;
    if accepted_ratio >= minimum_ratio {
        return Ok(());
    }
    let global_pair_semantic_count = if let Some(public_ledger) = ledger.public_ledger() {
        public_identity_ledger_summary(&public_ledger)?
            .as_object()
            .and_then(|fields| fields.get("pairExecutableSemanticCount"))
            .and_then(Value::as_u64)
            .ok_or_else(|| contract("public identity ledger summary is invalid"))?
    } else {
        runtime.proposal_state.local_executable_semantics.len() as u64
    };
    let mut failure = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_immigrant_collision_tripwire_v1".to_owned()),
        ),
        ("configSha256", Value::String(request.config_sha256.clone())),
        ("generationIndex", Value::from(request.generation_index)),
        (
            "immigrantAttempts",
            Value::from(runtime.proposal_state.immigrant_attempts),
        ),
        (
            "immigrantAccepted",
            Value::from(runtime.proposal_state.immigrant_accepted),
        ),
        ("acceptedRatio", Value::from(accepted_ratio)),
        ("minimumAcceptedRatio", Value::from(minimum_ratio)),
        ("minimumImmigrantAttempts", Value::from(minimum_attempts)),
        (
            "dispositionCounts",
            Value::Object(
                runtime
                    .proposal_state
                    .disposition_counts
                    .iter()
                    .map(|(key, value)| (key.clone(), Value::from(*value)))
                    .collect(),
            ),
        ),
        (
            "globalPairSemanticCount",
            Value::from(global_pair_semantic_count),
        ),
        (
            "reason",
            Value::String("rich_immigrant_semantic_acceptance_collapsed".to_owned()),
        ),
    ]);
    let failure_sha256 = canonical_sha256(&failure)?;
    failure
        .as_object_mut()
        .expect("collision tripwire failure is object")
        .insert("tripwireSha256".to_owned(), Value::String(failure_sha256));
    store.write_canonical_once(
        std::path::Path::new("immigrant-collision-tripwire.json"),
        &failure,
    )?;
    Err(contract(
        "rich immigrant semantic acceptance collapsed below the frozen collision tripwire",
    ))
}

#[derive(Default)]
struct RuntimeState {
    proposal_state: ProposalState,
    accepted_references: Vec<AcceptedReference>,
    last_segment_sha256: Option<String>,
    checkpoint_sha256: Option<String>,
    public_ledger_sha256: Option<String>,
}

#[derive(Clone)]
struct RecoveredSegment {
    proposal_ordinal: u64,
    segment_sha256: String,
    entry_sha256: String,
    ledger_delta: Value,
    accepted_reference: Option<AcceptedReference>,
    origin_kind: String,
    disposition: String,
    structural_parent_draws: u64,
}

/// Execute or resume one complete generation.  The native authority and ledger
/// are injected to prevent this crate from becoming a Dashboard interpreter or
/// a second undocumented duplicate-policy implementation.
pub fn generate_generation(
    request: &GenerateGenerationRequest,
    authority: &mut dyn NativePairAuthority,
    parents: &mut dyn ParentSelector,
    ledger: &mut dyn IdentityLedger,
) -> Result<Value> {
    request.validate()?;
    let store = ProposalJournal::open(&request.output_root, request.final_newline)?;
    store.write_canonical_once(
        std::path::Path::new("pair-config.json"),
        &request.pair_config,
    )?;
    let request_sha256 = request.compatibility_request_identity(ledger.identity())?;
    let mut runtime = recover_runtime(&store, ledger, parents, &request_sha256)?;
    if runtime.checkpoint_sha256.is_none() {
        runtime.public_ledger_sha256 = synchronize_public_identity_ledger(&store, ledger, true)?
            .map(public_identity_ledger_sha256)
            .transpose()?;
        let (_, checkpoint_sha256) = store.write_checkpoint(&CheckpointInput {
            request_sha256: request_sha256.clone(),
            next_proposal_ordinal: 0,
            last_segment_sha256: None,
            proposal_state: runtime.proposal_state.compact_value(),
            parent_selector_state: parents.compact_state(),
            ledger_state: ledger.compact_state(),
            public_ledger_sha256: runtime.public_ledger_sha256.clone(),
            accepted_references: Vec::new(),
            pending_public_entries: Vec::new(),
        })?;
        runtime.checkpoint_sha256 = Some(checkpoint_sha256);
    }
    repair_pending_public_entries(&store, &runtime)?;
    let immigrant_only_bootstrap = !parents.has_parents();
    enforce_immigrant_collision_tripwire(
        &store,
        request,
        &runtime,
        ledger,
        immigrant_only_bootstrap,
    )?;
    if let Some(result) = reopen_completed_generation(&store, request, &request_sha256, ledger)? {
        return Ok(result);
    }

    if request.g0_evaluation_width.is_some() && parents.has_parents() {
        return Err(contract(
            "G0 is an initial random-immigrant handoff and cannot run with parent sources",
        ));
    }
    let (desired_evaluated_offspring, desired_evaluated_immigrants) =
        request.reproduction_allocation_for_parents(parents.has_parents())?;
    let schedule = ProposalSchedule {
        config_sha256: request.config_sha256.clone(),
        generation_index: request.generation_index,
        parent_schedule: request.parent_schedule,
        desired_evaluated_offspring,
        desired_evaluated_immigrants,
    };
    let mut factory =
        PairFactory::new(authority, request.expected_native_authority_sha256.clone())?;
    let mut made = 0_u64;
    while (runtime.accepted_references.len() as u64) < request.target_unique_candidates
        && runtime.proposal_state.next_proposal_ordinal < request.max_proposal_attempts
        && request
            .max_new_proposals
            .is_none_or(|maximum| made < maximum)
    {
        let planned = {
            let mut planner = ProposalPlanner {
                schedule: schedule.clone(),
                parents,
            };
            planner.plan_next(&mut runtime.proposal_state)?
        };
        let native = factory.execute(
            &planned.intent,
            &NativeConstructionContext {
                generation_index: request.generation_index,
                birth_ordinal: runtime.accepted_references.len() as u64,
                proposal_ordinal: planned.proposal_ordinal,
                pair_policy: request.publication_policy.pair_policy.clone(),
                evidence_identity_context: request.evidence_identity_context.clone(),
                frozen_construction_catalog: request.frozen_construction_catalog.clone(),
                g0_evaluation_width: request.g0_evaluation_width,
                factory_construction_policy: request.factory_construction_policy.clone(),
            },
        )?;
        let prepared = {
            let assembler = ProposalAssembler {
                schedule: &schedule,
                operator_implementation_identity: &request
                    .publication_policy
                    .operator_implementation_identity,
                ledger: &*ledger,
                g0_evaluation_width: request.g0_evaluation_width,
            };
            assembler.prepare(&runtime.proposal_state, &planned, native)?
        };
        seal_prepared_proposal(
            &store,
            &mut runtime,
            ledger,
            parents,
            &request_sha256,
            prepared,
        )?;
        enforce_immigrant_collision_tripwire(
            &store,
            request,
            &runtime,
            ledger,
            immigrant_only_bootstrap,
        )?;
        made += 1;
    }

    if (runtime.accepted_references.len() as u64) < request.target_unique_candidates {
        let reason =
            if runtime.proposal_state.next_proposal_ordinal >= request.max_proposal_attempts {
                "max_proposal_attempts_reached"
            } else {
                "max_new_proposals_reached"
            };
        return Ok(object([
            (
                "schemaVersion",
                Value::String(GENERATION_PROGRESS_SCHEMA.to_owned()),
            ),
            ("configSha256", Value::String(request.config_sha256.clone())),
            (
                "proposalCount",
                Value::from(runtime.proposal_state.next_proposal_ordinal),
            ),
            (
                "acceptedCount",
                Value::from(runtime.accepted_references.len() as u64),
            ),
            (
                "maxProposalAttempts",
                Value::from(request.max_proposal_attempts),
            ),
            ("terminationReason", Value::String(reason.to_owned())),
            ("completed", Value::Bool(false)),
        ]));
    }

    let global_identity_ledger = current_public_identity_ledger_binding(
        &store,
        ledger,
        runtime.public_ledger_sha256.as_deref(),
    )?;
    let published = publish_generation(
        &store,
        &PublicationRequest {
            request_sha256: request_sha256.clone(),
            config_sha256: request.config_sha256.clone(),
            generation_index: request.generation_index,
            target_unique_candidates: request.target_unique_candidates,
            max_proposal_attempts: request.max_proposal_attempts,
            proposal_count: runtime.proposal_state.next_proposal_ordinal,
            origin_proposal_counts: runtime.proposal_state.origin_proposal_counts.clone(),
            origin_accepted_counts: runtime.proposal_state.origin_accepted_counts.clone(),
            disposition_counts: runtime.proposal_state.disposition_counts.clone(),
            entry_sha256s: runtime.proposal_state.entry_sha256s.clone(),
            entry_ordinals: (0..runtime.proposal_state.next_proposal_ordinal).collect(),
            construction_references: runtime.accepted_references.clone(),
            g0_evaluation_width: request.g0_evaluation_width,
            global_identity_ledger,
            reproduction_allocation: request.pair_config.get("reproductionAllocation").cloned(),
            // The native front-half keeps its allocation counters in the
            // compact proposal state.  Historical callers that do not yet
            // expose a byte-identical accounting facade leave this absent;
            // the dedicated v5 G0 funnel supplies the exact reduction.
            reproduction_allocation_accounting: None,
            unique_pair_genome_count: Some(
                runtime.proposal_state.local_executable_semantics.len() as u64
            ),
            policy: request.publication_policy.clone(),
        },
    )?;
    let checkpoint_sha256 = runtime
        .checkpoint_sha256
        .as_deref()
        .ok_or_else(|| contract("completed generation lacks a sealed checkpoint"))?;
    let head = self_hashed_generation_head(
        request.generation_index,
        &request_sha256,
        checkpoint_sha256,
        &published.population_sha256,
        &published.evaluation_population_sha256,
        &published.generation_journal_sha256,
    )?;
    let head_sha256 = head
        .as_object()
        .and_then(|fields| fields.get("generationHeadSha256"))
        .and_then(Value::as_str)
        .ok_or_else(|| contract("generation head omitted its identity"))?
        .to_owned();
    store.write_generation_head(&head)?;
    let pair_generation_result = python_compatible_generation_result(
        &store,
        request,
        &published.population_sha256,
        &published.evaluation_population_sha256,
        &published.generation_journal_sha256,
    )?;

    Ok(object([
        (
            "schemaVersion",
            Value::String(FRONT_GENERATION_RESULT_SCHEMA.to_owned()),
        ),
        ("completed", Value::Bool(true)),
        ("generationIndex", Value::from(request.generation_index)),
        ("configSha256", Value::String(request.config_sha256.clone())),
        (
            "populationSha256",
            Value::String(published.population_sha256),
        ),
        (
            "populationFileSha256",
            Value::String(published.population_file_sha256),
        ),
        (
            "evaluationPopulationSha256",
            Value::String(published.evaluation_population_sha256),
        ),
        (
            "generationJournalSha256",
            Value::String(published.generation_journal_sha256),
        ),
        ("sealedGenerationHeadSha256", Value::String(head_sha256)),
        (
            "proposalCount",
            Value::from(runtime.proposal_state.next_proposal_ordinal),
        ),
        (
            "candidateCount",
            Value::from(published.selected_references.len() as u64),
        ),
        (
            "nextContinuationOrdinal",
            // A completed Python-compatible generation has consumed its
            // immigrant continuation stream.  The sealed journal owns this
            // public result value (zero), rather than the private next
            // proposal ordinal retained in the checkpoint.
            Value::from(0_u64),
        ),
        (
            "g0Bootstrap",
            published
                .g0_binding
                .map(|binding| {
                    Value::Object(
                        binding
                            .into_iter()
                            .map(|(key, value)| (key, Value::String(value)))
                            .collect(),
                    )
                })
                .unwrap_or(Value::Null),
        ),
        ("pairGenerationResult", pair_generation_result),
    ]))
}

fn python_compatible_generation_result(
    store: &ProposalJournal,
    request: &GenerateGenerationRequest,
    population_sha256: &str,
    evaluation_population_sha256: &str,
    journal_sha256: &str,
) -> Result<Value> {
    let journal = store.read_artifact(std::path::Path::new("generation-journal.json"))?;
    let fields = journal
        .as_object()
        .ok_or_else(|| contract("generation journal is invalid"))?;
    let required = |field: &str| {
        fields
            .get(field)
            .cloned()
            .ok_or_else(|| contract(format!("generation journal lacks {field}")))
    };
    let mut result = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_pair_generation_result_v1".to_owned()),
        ),
        ("configSha256", Value::String(request.config_sha256.clone())),
        (
            "populationSha256",
            Value::String(population_sha256.to_owned()),
        ),
        (
            "evaluationPopulationSha256",
            Value::String(evaluation_population_sha256.to_owned()),
        ),
        ("journalSha256", Value::String(journal_sha256.to_owned())),
        ("proposalCount", required("proposalCount")?),
        ("candidateCount", required("acceptedCount")?),
        ("originProposalCounts", required("originProposalCounts")?),
        ("originAcceptedCounts", required("originAcceptedCounts")?),
        ("proposalSlots", required("proposalSlots")?),
        ("uniqueIdentityCounts", required("uniqueIdentityCounts")?),
        ("duplicateCounters", required("duplicateCounters")?),
        ("proposalSlotCounters", required("proposalSlotCounters")?),
        ("nextImmigrantContinuationOrdinal", Value::from(0_u64)),
        ("completed", Value::Bool(true)),
    ]);
    for field in [
        "constructionPoolSize",
        "constructedAcceptedCount",
        "g0Bootstrap",
        "immigrantConstructionDistribution",
    ] {
        if let Some(value) = fields.get(field) {
            result
                .as_object_mut()
                .expect("generation result is object")
                .insert(field.to_owned(), value.clone());
        }
    }
    Ok(result)
}

fn seal_prepared_proposal(
    store: &ProposalJournal,
    runtime: &mut RuntimeState,
    ledger: &mut dyn IdentityLedger,
    parents: &dyn ParentSelector,
    request_sha256: &str,
    prepared: PreparedProposal,
) -> Result<()> {
    let accepted_reference = prepared
        .accepted
        .as_ref()
        .map(|accepted| AcceptedReference {
            proposal_ordinal: prepared.proposal_ordinal,
            candidate_id: accepted.candidate_id.clone(),
            candidate_identity_sha256: accepted.candidate_identity_sha256.clone(),
            executable_semantic_sha256: accepted.executable_semantic_sha256.clone(),
            entry_sha256: prepared.entry_sha256.clone(),
            descriptor_projection: accepted.descriptor_projection.clone(),
        });
    let ledger_delta = prepared.ledger_delta.clone();
    let (_, segment_sha256) = store.write_segment(&SegmentInput {
        proposal_ordinal: prepared.proposal_ordinal,
        previous_segment_sha256: runtime.last_segment_sha256.clone(),
        entry: prepared.entry.clone(),
        entry_sha256: prepared.entry_sha256.clone(),
        ledger_delta: Some(ledger_delta.clone()),
        descriptor_projection: accepted_reference
            .as_ref()
            .and_then(|reference| reference.descriptor_projection.clone()),
        accepted_reference: accepted_reference.clone(),
    })?;

    // The segment is the receipt.  Only now may external identity state and
    // in-memory duplicate indexes advance.
    ledger.commit_prepared_delta(&ledger_delta)?;
    runtime.public_ledger_sha256 = synchronize_public_identity_ledger(store, ledger, false)?
        .map(public_identity_ledger_sha256)
        .transpose()?;
    runtime.proposal_state.observe(&prepared)?;
    if let Some(reference) = accepted_reference {
        runtime.accepted_references.push(reference);
    }
    runtime.last_segment_sha256 = Some(segment_sha256);
    let (_, checkpoint_sha256) = store.write_checkpoint(&CheckpointInput {
        request_sha256: request_sha256.to_owned(),
        next_proposal_ordinal: runtime.proposal_state.next_proposal_ordinal,
        last_segment_sha256: runtime.last_segment_sha256.clone(),
        proposal_state: runtime.proposal_state.compact_value(),
        parent_selector_state: parents.compact_state(),
        ledger_state: ledger.compact_state(),
        public_ledger_sha256: runtime.public_ledger_sha256.clone(),
        accepted_references: runtime.accepted_references.clone(),
        pending_public_entries: vec![(prepared.proposal_ordinal, prepared.entry.clone())],
    })?;
    runtime.checkpoint_sha256 = Some(checkpoint_sha256);
    store.write_public_entry(prepared.proposal_ordinal, &prepared.entry)?;
    Ok(())
}

fn recover_runtime(
    store: &ProposalJournal,
    ledger: &mut dyn IdentityLedger,
    parents: &mut dyn ParentSelector,
    request_sha256: &str,
) -> Result<RuntimeState> {
    let checkpoint_directory = store.root().join("internal/checkpoints");
    let mut ordinals = fs::read_dir(&checkpoint_directory)?
        .map(|entry| {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| contract("checkpoint filename is not UTF-8"))?;
            let ordinal = name
                .strip_suffix(".json")
                .ok_or_else(|| contract("checkpoint filename is invalid"))?
                .parse::<u64>()
                .map_err(|_| contract("checkpoint filename is invalid"))?;
            Ok(ordinal)
        })
        .collect::<Result<Vec<_>>>()?;
    ordinals.sort_unstable();
    let checkpoint_ordinals = ordinals;
    let Some(&ordinal) = checkpoint_ordinals.last() else {
        if load_verified_segments(store)?.is_empty() {
            return Ok(RuntimeState::default());
        }
        return Err(contract(
            "proposal segments exist without the required ordinal-zero recovery checkpoint",
        ));
    };
    let checkpoint = store.load_checkpoint(ordinal)?;
    let fields = checkpoint
        .as_object()
        .ok_or_else(|| contract("checkpoint is invalid"))?;
    if fields.get("requestSha256").and_then(Value::as_str) != Some(request_sha256) {
        return Err(contract(
            "checkpoint request identity mismatched the generation request",
        ));
    }
    if fields.get("nextProposalOrdinal").and_then(Value::as_u64) != Some(ordinal) {
        return Err(contract(
            "checkpoint filename does not bind next proposal ordinal",
        ));
    }
    let proposal_state = ProposalState::from_compact_value(
        fields
            .get("proposalState")
            .ok_or_else(|| contract("checkpoint lacks proposal state"))?,
    )?;
    if proposal_state.next_proposal_ordinal != ordinal {
        return Err(contract("checkpoint proposal state ordinal drifted"));
    }
    ledger.restore_compact_state(
        fields
            .get("ledgerState")
            .ok_or_else(|| contract("checkpoint lacks ledger state"))?,
    )?;
    parents.restore_compact_state(
        fields
            .get("parentSelectorState")
            .ok_or_else(|| contract("checkpoint lacks parent selector state"))?,
    )?;
    let accepted_references = fields
        .get("acceptedReferences")
        .and_then(Value::as_array)
        .ok_or_else(|| contract("checkpoint lacks accepted references"))?
        .iter()
        .map(AcceptedReference::from_value)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let last_segment_sha256 = fields
        .get("lastSegmentSha256")
        .cloned()
        .filter(|value| !value.is_null())
        .and_then(|value| value.as_str().map(ToOwned::to_owned));
    if let Some(last) = &last_segment_sha256 {
        sha(last, "checkpoint segment SHA-256")?;
    }
    let checkpoint_sha256 = fields
        .get("checkpointSha256")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| contract("checkpoint lacks checkpoint SHA-256"))?;
    let mut runtime = RuntimeState {
        proposal_state,
        accepted_references,
        last_segment_sha256,
        checkpoint_sha256: Some(checkpoint_sha256),
        public_ledger_sha256: nullable_sha(fields, "publicLedgerSha256")?,
    };
    reconcile_checkpoint_public_identity_ledger(
        store,
        ledger,
        runtime.public_ledger_sha256.as_deref(),
    )?;
    let segments = load_verified_segments(store)?;
    validate_checkpoint_chain(store, &checkpoint_ordinals, &segments, request_sha256)?;
    validate_public_inventory(store, &segments, ordinal)?;
    for segment in segments.iter().skip(ordinal as usize) {
        apply_recovered_segment(store, &mut runtime, ledger, segment)?;
        // Reopen only the one rich receipt needed for the crash-repair
        // checkpoint and public write.  Keeping every recovered entry in the
        // segment index made a restart retain the entire rich journal.
        let segment_value = store.load_segment(segment.proposal_ordinal)?;
        let entry = segment_value
            .as_object()
            .and_then(|fields| fields.get("entry"))
            .cloned()
            .ok_or_else(|| contract("recovered durable segment lacks rich entry"))?;
        let (_, checkpoint_sha256) = store.write_checkpoint(&CheckpointInput {
            request_sha256: request_sha256.to_owned(),
            next_proposal_ordinal: runtime.proposal_state.next_proposal_ordinal,
            last_segment_sha256: runtime.last_segment_sha256.clone(),
            proposal_state: runtime.proposal_state.compact_value(),
            parent_selector_state: parents.compact_state(),
            ledger_state: ledger.compact_state(),
            public_ledger_sha256: runtime.public_ledger_sha256.clone(),
            accepted_references: runtime.accepted_references.clone(),
            pending_public_entries: vec![(segment.proposal_ordinal, entry.clone())],
        })?;
        runtime.checkpoint_sha256 = Some(checkpoint_sha256);
        store.write_public_entry(segment.proposal_ordinal, &entry)?;
    }
    Ok(runtime)
}

fn repair_pending_public_entries(store: &ProposalJournal, runtime: &RuntimeState) -> Result<()> {
    let next_ordinal = runtime.proposal_state.next_proposal_ordinal;
    let checkpoint = store.load_checkpoint(next_ordinal)?;
    let pending = checkpoint
        .as_object()
        .and_then(|fields| fields.get("pendingPublicEntries"))
        .and_then(Value::as_array)
        .ok_or_else(|| contract("checkpoint lacks pending public entries"))?;
    for row in pending {
        let fields = row
            .as_object()
            .ok_or_else(|| contract("pending public entry is invalid"))?;
        let ordinal = fields
            .get("proposalOrdinal")
            .and_then(Value::as_u64)
            .ok_or_else(|| contract("pending public entry lacks ordinal"))?;
        let entry = fields
            .get("entry")
            .ok_or_else(|| contract("pending public entry lacks entry"))?;
        store.write_public_entry(ordinal, entry)?;
    }
    Ok(())
}

fn reopen_completed_generation(
    store: &ProposalJournal,
    request: &GenerateGenerationRequest,
    request_sha256: &str,
    ledger: &dyn IdentityLedger,
) -> Result<Option<Value>> {
    let Some(head) = store.load_generation_head()? else {
        return Ok(None);
    };
    let fields = head
        .as_object()
        .ok_or_else(|| contract("generation head is invalid"))?;
    if fields.get("generationIndex").and_then(Value::as_u64) != Some(request.generation_index)
        || fields.get("requestSha256").and_then(Value::as_str) != Some(request_sha256)
    {
        return Err(contract(
            "generation head does not bind this generation request",
        ));
    }
    let population = store.read_artifact(std::path::Path::new("population.json"))?;
    verify_self_hash(
        &population,
        "populationSha256",
        crate::publication::POPULATION_SCHEMA,
        "population",
    )?;
    let evaluation = store.read_artifact(std::path::Path::new("evaluation-population.json"))?;
    verify_self_hash(
        &evaluation,
        "evaluationPopulationSha256",
        crate::publication::EVALUATION_POPULATION_SCHEMA,
        "evaluation population",
    )?;
    let journal = store.read_artifact(std::path::Path::new("generation-journal.json"))?;
    verify_self_hash(
        &journal,
        "journalSha256",
        crate::publication::GENERATION_JOURNAL_SCHEMA,
        "generation journal",
    )?;
    verify_public_identity_ledger(store, ledger, &journal)?;
    let string = |value: &Value, field: &str| {
        value
            .as_object()
            .and_then(|fields| fields.get(field))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| contract(format!("artifact lacks {field}")))
    };
    let population_sha256 = string(&population, "populationSha256")?;
    let evaluation_population_sha256 = string(&evaluation, "evaluationPopulationSha256")?;
    let generation_journal_sha256 = string(&journal, "journalSha256")?;
    for (head_field, actual, label) in [
        ("populationSha256", &population_sha256, "population"),
        (
            "evaluationPopulationSha256",
            &evaluation_population_sha256,
            "evaluation population",
        ),
        (
            "journalSha256",
            &generation_journal_sha256,
            "generation journal",
        ),
    ] {
        if fields.get(head_field).and_then(Value::as_str) != Some(actual.as_str()) {
            return Err(contract(format!(
                "generation head {label} identity drifted"
            )));
        }
    }
    let population_file_sha256 =
        store.artifact_file_sha256(std::path::Path::new("population.json"))?;
    if evaluation
        .as_object()
        .and_then(|fields| fields.get("populationFileSha256"))
        .and_then(Value::as_str)
        != Some(population_file_sha256.as_str())
    {
        return Err(contract("evaluation population file identity drifted"));
    }
    // A reopened completed request is an observation of the sealed result,
    // not a different progress response.  Rebuild the nested compatibility
    // projection from those same verified public artifacts so callers receive
    // the identical front wrapper on the first call and every later reopen.
    let pair_generation_result = python_compatible_generation_result(
        store,
        request,
        &population_sha256,
        &evaluation_population_sha256,
        &generation_journal_sha256,
    )?;
    Ok(Some(object([
        (
            "schemaVersion",
            Value::String(FRONT_GENERATION_RESULT_SCHEMA.to_owned()),
        ),
        ("completed", Value::Bool(true)),
        ("generationIndex", Value::from(request.generation_index)),
        ("configSha256", Value::String(request.config_sha256.clone())),
        ("populationSha256", Value::String(population_sha256)),
        (
            "populationFileSha256",
            Value::String(population_file_sha256),
        ),
        (
            "evaluationPopulationSha256",
            Value::String(evaluation_population_sha256),
        ),
        (
            "generationJournalSha256",
            Value::String(generation_journal_sha256),
        ),
        (
            "sealedGenerationHeadSha256",
            fields
                .get("generationHeadSha256")
                .cloned()
                .ok_or_else(|| contract("generation head lacks its identity"))?,
        ),
        (
            "proposalCount",
            journal
                .as_object()
                .and_then(|fields| fields.get("proposalCount"))
                .cloned()
                .ok_or_else(|| contract("generation journal lacks proposal count"))?,
        ),
        (
            "candidateCount",
            population
                .as_object()
                .and_then(|fields| fields.get("candidateCount"))
                .cloned()
                .ok_or_else(|| contract("population lacks candidate count"))?,
        ),
        (
            "nextContinuationOrdinal",
            journal
                .as_object()
                .and_then(|fields| fields.get("nextImmigrantContinuationOrdinal"))
                .cloned()
                .ok_or_else(|| contract("generation journal lacks continuation ordinal"))?,
        ),
        (
            "g0Bootstrap",
            population
                .as_object()
                .and_then(|fields| fields.get("g0Bootstrap"))
                .cloned()
                .unwrap_or(Value::Null),
        ),
        ("pairGenerationResult", pair_generation_result),
    ])))
}

fn load_verified_segments(store: &ProposalJournal) -> Result<Vec<RecoveredSegment>> {
    let directory = store.root().join("internal/segments");
    let mut ordinals = fs::read_dir(&directory)?
        .map(|entry| {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| contract("segment filename is not UTF-8"))?;
            let ordinal = name
                .strip_suffix(".json")
                .ok_or_else(|| contract("segment filename is invalid"))?
                .parse::<u64>()
                .map_err(|_| contract("segment filename is invalid"))?;
            if name != format!("{ordinal:08}.json") {
                return Err(contract("segment filename is not canonical"));
            }
            Ok(ordinal)
        })
        .collect::<Result<Vec<_>>>()?;
    ordinals.sort_unstable();
    for (expected, ordinal) in ordinals.iter().enumerate() {
        if *ordinal != expected as u64 {
            return Err(contract(
                "proposal segment chain has a missing or interposed ordinal",
            ));
        }
    }
    let mut previous_segment_sha256 = None;
    ordinals
        .into_iter()
        .map(|ordinal| {
            let value = store.load_segment(ordinal)?;
            let fields = value
                .as_object()
                .ok_or_else(|| contract("proposal segment is invalid"))?;
            if fields.get("proposalOrdinal").and_then(Value::as_u64) != Some(ordinal) {
                return Err(contract(
                    "proposal segment filename does not bind its ordinal",
                ));
            }
            let previous = nullable_sha(fields, "previousSegmentSha256")?;
            if previous != previous_segment_sha256 {
                return Err(contract(
                    "proposal segment previous hash does not bind its chain",
                ));
            }
            let entry = fields
                .get("entry")
                .cloned()
                .ok_or_else(|| contract("proposal segment lacks entry"))?;
            verify_self_hash(
                &entry,
                "entrySha256",
                PROPOSAL_ENTRY_SCHEMA,
                "proposal entry",
            )?;
            let entry_sha256 = fields
                .get("entrySha256")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .ok_or_else(|| contract("proposal segment lacks entry SHA-256"))?;
            sha(&entry_sha256, "proposal segment entry SHA-256")?;
            if entry.get("entrySha256").and_then(Value::as_str) != Some(&entry_sha256) {
                return Err(contract(
                    "proposal segment entry identity mismatched its rich entry",
                ));
            }
            let entry_fields = entry
                .as_object()
                .ok_or_else(|| contract("proposal segment rich entry is invalid"))?;
            let origin_kind = entry_fields
                .get("originKind")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .ok_or_else(|| contract("proposal segment rich entry lacks origin kind"))?;
            let disposition = entry_fields
                .get("disposition")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .ok_or_else(|| contract("proposal segment rich entry lacks disposition"))?;
            let structural_parent_draws = structural_parent_draws(&entry)?;
            let ledger_delta = fields
                .get("ledgerDelta")
                .cloned()
                .filter(|value| !value.is_null())
                .ok_or_else(|| contract("proposal segment lacks ledger delta"))?;
            let accepted_reference = fields
                .get("acceptedReference")
                .cloned()
                .filter(|value| !value.is_null())
                .map(|value| AcceptedReference::from_value(&value))
                .transpose()?;
            let segment_sha256 = fields
                .get("segmentSha256")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .ok_or_else(|| contract("proposal segment lacks segment SHA-256"))?;
            sha(&segment_sha256, "proposal segment SHA-256")?;
            previous_segment_sha256 = Some(segment_sha256.clone());
            let recovered = RecoveredSegment {
                proposal_ordinal: ordinal,
                segment_sha256,
                entry_sha256,
                ledger_delta,
                accepted_reference,
                origin_kind,
                disposition,
                structural_parent_draws,
            };
            validate_recovered_acceptance(&recovered)?;
            Ok(recovered)
        })
        .collect()
}

fn validate_checkpoint_chain(
    store: &ProposalJournal,
    checkpoint_ordinals: &[u64],
    segments: &[RecoveredSegment],
    request_sha256: &str,
) -> Result<()> {
    let mut replayed_state = ProposalState::default();
    let mut replayed_references = Vec::new();
    for (expected, ordinal) in checkpoint_ordinals.iter().enumerate() {
        if *ordinal != expected as u64 {
            return Err(contract(
                "checkpoint chain has a missing or interposed ordinal",
            ));
        }
        if *ordinal as usize > segments.len() {
            return Err(contract(
                "checkpoint advances past the durable segment chain",
            ));
        }
        let checkpoint = store.load_checkpoint(*ordinal)?;
        let fields = checkpoint
            .as_object()
            .ok_or_else(|| contract("checkpoint is invalid"))?;
        if fields.get("requestSha256").and_then(Value::as_str) != Some(request_sha256) {
            return Err(contract(
                "checkpoint request identity mismatched the generation request",
            ));
        }
        if fields.get("nextProposalOrdinal").and_then(Value::as_u64) != Some(*ordinal) {
            return Err(contract(
                "checkpoint filename does not bind next proposal ordinal",
            ));
        }
        let expected_last = ordinal
            .checked_sub(1)
            .and_then(|index| segments.get(index as usize))
            .map(|segment| segment.segment_sha256.clone());
        if nullable_sha(fields, "lastSegmentSha256")? != expected_last {
            return Err(contract(
                "checkpoint does not bind the preceding segment chain",
            ));
        }
        let _ = nullable_sha(fields, "publicLedgerSha256")?;
        while replayed_state.next_proposal_ordinal < *ordinal {
            let segment = segments
                .get(replayed_state.next_proposal_ordinal as usize)
                .ok_or_else(|| contract("checkpoint replay lacks a sealed segment"))?;
            replayed_state.structural_parent_selections = replayed_state
                .structural_parent_selections
                .checked_add(segment.structural_parent_draws)
                .ok_or_else(|| contract("replayed parent selection count overflowed"))?;
            let accepted = segment
                .accepted_reference
                .as_ref()
                .map(accepted_proposal_from_reference);
            replayed_state.observe_recovered(
                segment.proposal_ordinal,
                &segment.origin_kind,
                &segment.disposition,
                &segment.entry_sha256,
                accepted.as_ref(),
            )?;
            if let Some(reference) = &segment.accepted_reference {
                replayed_references.push(reference.clone());
            }
        }
        let state = fields
            .get("proposalState")
            .ok_or_else(|| contract("checkpoint lacks proposal state"))?;
        if state != &replayed_state.compact_value() {
            return Err(contract(
                "checkpoint proposal state does not exactly replay sealed segments",
            ));
        }
        let references = fields
            .get("acceptedReferences")
            .ok_or_else(|| contract("checkpoint lacks accepted references"))?;
        let expected_references = Value::Array(
            replayed_references
                .iter()
                .map(AcceptedReference::value)
                .collect(),
        );
        if references != &expected_references {
            return Err(contract(
                "checkpoint accepted references do not exactly replay sealed segments",
            ));
        }
        validate_checkpoint_pending_public_entries(store, fields, *ordinal, segments)?;
        if !fields
            .get("parentSelectorState")
            .is_some_and(Value::is_object)
            || !fields.get("ledgerState").is_some_and(Value::is_object)
        {
            return Err(contract(
                "checkpoint selector and ledger state must be objects",
            ));
        }
    }
    Ok(())
}

/// Validate the one-entry crash-repair tail without cloning the rich entry out
/// of either document.  Checkpoints deliberately retain the tail so public
/// publication can be repaired after a crash, but recovery should never turn
/// that bounded receipt into a second all-segment in-memory inventory.
fn validate_checkpoint_pending_public_entries(
    store: &ProposalJournal,
    checkpoint: &Map<String, Value>,
    ordinal: u64,
    segments: &[RecoveredSegment],
) -> Result<()> {
    let pending = checkpoint
        .get("pendingPublicEntries")
        .and_then(Value::as_array)
        .ok_or_else(|| contract("checkpoint lacks pending public entries"))?;
    if ordinal == 0 {
        if pending.is_empty() {
            return Ok(());
        }
        return Err(contract(
            "ordinal-zero checkpoint must not retain pending public entries",
        ));
    }
    if pending.len() != 1 {
        return Err(contract(
            "checkpoint must retain exactly one pending public tail entry",
        ));
    }
    let row = pending[0]
        .as_object()
        .ok_or_else(|| contract("checkpoint pending public entry is invalid"))?;
    let expected_ordinal = ordinal - 1;
    if row.get("proposalOrdinal").and_then(Value::as_u64) != Some(expected_ordinal) {
        return Err(contract(
            "checkpoint pending public entry ordinal does not bind its sealed tail",
        ));
    }
    let checkpoint_entry = row
        .get("entry")
        .ok_or_else(|| contract("checkpoint pending public entry lacks rich entry"))?;
    let segment = segments
        .get(expected_ordinal as usize)
        .ok_or_else(|| contract("checkpoint pending entry has no durable segment"))?;
    let segment_value = store.load_segment(expected_ordinal)?;
    let segment_entry = segment_value
        .as_object()
        .and_then(|fields| fields.get("entry"))
        .ok_or_else(|| contract("durable segment lacks rich entry"))?;
    if checkpoint_entry != segment_entry {
        return Err(contract(
            "checkpoint pending public entries do not bind its sealed tail",
        ));
    }
    if segment_entry.get("entrySha256").and_then(Value::as_str)
        != Some(segment.entry_sha256.as_str())
    {
        return Err(contract(
            "checkpoint pending tail identity drifted from compact segment metadata",
        ));
    }
    Ok(())
}

fn accepted_proposal_from_reference(reference: &AcceptedReference) -> AcceptedProposal {
    AcceptedProposal {
        candidate_id: reference.candidate_id.clone(),
        candidate_identity_sha256: reference.candidate_identity_sha256.clone(),
        executable_semantic_sha256: reference.executable_semantic_sha256.clone(),
        descriptor_projection: reference.descriptor_projection.clone(),
    }
}

fn validate_recovered_acceptance(segment: &RecoveredSegment) -> Result<()> {
    if (segment.disposition == "accepted") != segment.accepted_reference.is_some() {
        return Err(contract(
            "recovered segment acceptance disposition is inconsistent",
        ));
    }
    Ok(())
}

/// Rebuild the planning cursor from the immutable proposal form.  A mutation
/// consumes one parent selection; same-side crossover consumes parent, mate,
/// and every recorded same-identity mate retry.
fn structural_parent_draws(entry: &Value) -> Result<u64> {
    let fields = entry
        .as_object()
        .ok_or_else(|| contract("proposal entry is invalid while replaying parent state"))?;
    if fields.get("originKind").and_then(Value::as_str) != Some("structural_offspring") {
        return Ok(0);
    }
    let proposal = fields
        .get("proposal")
        .and_then(Value::as_object)
        .ok_or_else(|| contract("structural proposal lacks proposal material"))?;
    if proposal.get("proposalKind").and_then(Value::as_str)
        != Some("temporal_qd_same_side_crossover_v1")
    {
        return Ok(1);
    }
    let retries = proposal
        .get("mateSelectionAttempts")
        .and_then(Value::as_array)
        .ok_or_else(|| contract("crossover proposal lacks mate selection retries"))?;
    2_u64
        .checked_add(retries.len() as u64)
        .ok_or_else(|| contract("crossover parent selection count overflowed"))
}

fn validate_public_inventory(
    store: &ProposalJournal,
    segments: &[RecoveredSegment],
    checkpoint_next_ordinal: u64,
) -> Result<()> {
    let directory = store.root().join("proposal-journal");
    let mut ordinals = fs::read_dir(&directory)?
        .map(|entry| {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| contract("public entry filename is not UTF-8"))?;
            let ordinal = name
                .strip_suffix(".json")
                .ok_or_else(|| contract("public entry filename is invalid"))?
                .parse::<u64>()
                .map_err(|_| contract("public entry filename is invalid"))?;
            if name != format!("{ordinal:08}.json") {
                return Err(contract("public entry filename is not canonical"));
            }
            Ok(ordinal)
        })
        .collect::<Result<Vec<_>>>()?;
    ordinals.sort_unstable();
    if ordinals
        .iter()
        .any(|ordinal| *ordinal as usize >= segments.len())
    {
        return Err(contract("public proposal entry has no durable segment"));
    }
    for ordinal in 0..checkpoint_next_ordinal.saturating_sub(1) {
        let entry = store.read_public_entry(ordinal)?;
        let segment = segments
            .get(ordinal as usize)
            .ok_or_else(|| contract("public entry has no durable segment"))?;
        let segment_value = store.load_segment(ordinal)?;
        let segment_entry = segment_value
            .as_object()
            .and_then(|fields| fields.get("entry"))
            .ok_or_else(|| contract("durable segment lacks rich entry"))?;
        if entry != *segment_entry
            || segment_entry.get("entrySha256").and_then(Value::as_str)
                != Some(segment.entry_sha256.as_str())
        {
            return Err(contract(
                "public entry bytes diverged from its durable segment",
            ));
        }
    }
    for ordinal in ordinals {
        let entry = store.read_public_entry(ordinal)?;
        let segment = segments
            .get(ordinal as usize)
            .ok_or_else(|| contract("public entry has no durable segment"))?;
        let segment_value = store.load_segment(ordinal)?;
        let segment_entry = segment_value
            .as_object()
            .and_then(|fields| fields.get("entry"))
            .ok_or_else(|| contract("durable segment lacks rich entry"))?;
        if entry != *segment_entry
            || segment_entry.get("entrySha256").and_then(Value::as_str)
                != Some(segment.entry_sha256.as_str())
        {
            return Err(contract(
                "public entry bytes diverged from its durable segment",
            ));
        }
    }
    Ok(())
}

fn apply_recovered_segment(
    store: &ProposalJournal,
    runtime: &mut RuntimeState,
    ledger: &mut dyn IdentityLedger,
    segment: &RecoveredSegment,
) -> Result<()> {
    if segment.proposal_ordinal != runtime.proposal_state.next_proposal_ordinal {
        return Err(contract(
            "recovered segment does not continue proposal state",
        ));
    }
    runtime.proposal_state.structural_parent_selections = runtime
        .proposal_state
        .structural_parent_selections
        .checked_add(segment.structural_parent_draws)
        .ok_or_else(|| contract("recovered parent selection count overflowed"))?;
    let accepted = segment
        .accepted_reference
        .as_ref()
        .map(accepted_proposal_from_reference);
    validate_recovered_acceptance(segment)?;
    ledger.commit_prepared_delta(&segment.ledger_delta)?;
    runtime.public_ledger_sha256 = synchronize_public_identity_ledger(store, ledger, false)?
        .map(public_identity_ledger_sha256)
        .transpose()?;
    runtime.proposal_state.observe_recovered(
        segment.proposal_ordinal,
        &segment.origin_kind,
        &segment.disposition,
        &segment.entry_sha256,
        accepted.as_ref(),
    )?;
    if let Some(reference) = segment.accepted_reference.clone() {
        runtime.accepted_references.push(reference);
    }
    runtime.last_segment_sha256 = Some(segment.segment_sha256.clone());
    Ok(())
}

/// Publish the Python-compatible mutable facade after every sealed ledger
/// delta. The segment remains the immutable receipt; the facade is atomically
/// replaced and then its exact identity is recorded in the following private
/// checkpoint for Rust-to-Python continuation.
fn synchronize_public_identity_ledger(
    store: &ProposalJournal,
    ledger: &dyn IdentityLedger,
    allow_initial_general_to_pair_upgrade: bool,
) -> Result<Option<Value>> {
    let Some(public_ledger) = ledger.public_ledger() else {
        return Ok(None);
    };
    let summary = public_identity_ledger_summary(&public_ledger)?;
    if store.root().join("identity-ledger.json").exists() {
        // Reject a corrupt old facade before replacing it; otherwise a failed
        // cross-authority handoff could be silently erased by a later write.
        let existing = store.read_public_identity_ledger()?;
        if let Err(summary_error) = public_identity_ledger_summary(&existing) {
            if !allow_initial_general_to_pair_upgrade
                || !is_exact_initial_general_to_pair_upgrade(&existing, &public_ledger)?
            {
                return Err(summary_error);
            }
        }
    }
    store.write_public_identity_ledger(&public_ledger)?;
    let persisted = store.read_public_identity_ledger()?;
    if !canonically_equal(&persisted, &public_ledger)? {
        return Err(contract(
            "public identity ledger bytes drifted after atomic replacement",
        ));
    }
    Ok(Some(summary))
}

/// The only permitted mutable-facade schema transition.  Python's generic
/// `_load_identity_ledger` can create a valid v3 general ledger before pair
/// generation restores a verified parent archive and initializes its additive
/// pair-semantic extension. At CP0, and only before a native checkpoint binds
/// the pair facade, replace that exact general predecessor with the pair
/// facade. Archive bootstrap may append verified historical records and pair
/// semantics; every other divergence, corruption, prior pair extension, or
/// restart remains rejected by the caller.
fn is_exact_initial_general_to_pair_upgrade(existing: &Value, pair: &Value) -> Result<bool> {
    public_identity_ledger_self_hash(existing)?;
    let existing_fields = existing
        .as_object()
        .ok_or_else(|| contract("public identity ledger must be an object"))?;
    if existing_fields.contains_key("pairExecutableSemantics")
        || existing_fields.contains_key("pairExecutableSemanticDuplicateRejections")
    {
        return Ok(false);
    }
    let pair_fields = pair
        .as_object()
        .ok_or_else(|| contract("public identity ledger must be an object"))?;
    if !matches!(
        pair_fields.get("pairExecutableSemantics"),
        Some(Value::Array(_))
    ) || pair_fields
        .get("pairExecutableSemanticDuplicateRejections")
        .and_then(Value::as_u64)
        .is_none()
    {
        return Ok(false);
    }
    let mut expected_general = pair.clone();
    let expected_fields = expected_general
        .as_object_mut()
        .expect("pair ledger checked as object");
    expected_fields.remove("ledgerSha256");
    expected_fields.remove("pairExecutableSemantics");
    expected_fields.remove("pairExecutableSemanticDuplicateRejections");
    match (existing_fields.get("records"), pair_fields.get("records")) {
        (None, None) => {}
        (Some(existing_records), Some(pair_records)) => {
            let existing_records = match existing_records.as_array() {
                Some(records) => records,
                None => return Ok(false),
            };
            let pair_records = match pair_records.as_array() {
                Some(records) => records,
                None => return Ok(false),
            };
            if existing_records.len() > pair_records.len()
                || pair_records[..existing_records.len()] != existing_records[..]
            {
                return Ok(false);
            }
            let Some(unique_counts) = legacy_general_unique_counts(existing_records) else {
                return Ok(false);
            };
            expected_fields.insert(
                "records".to_owned(),
                Value::Array(existing_records.to_vec()),
            );
            expected_fields.insert("uniqueCounts".to_owned(), unique_counts);
        }
        _ => return Ok(false),
    }
    let expected_sha256 = canonical_sha256(&expected_general)?;
    expected_general
        .as_object_mut()
        .expect("pair ledger checked as object")
        .insert("ledgerSha256".to_owned(), Value::String(expected_sha256));
    canonically_equal(existing, &expected_general)
}

/// Rebuild the only mutable count projection in a legacy general facade. The
/// runtime ledger has already validated these records before archive bootstrap;
/// this is solely the narrow CP0 predecessor check that proves the old public
/// facade is the prefix Python would extend.
fn legacy_general_unique_counts(records: &[Value]) -> Option<Value> {
    const FIELDS: [(&str, &str); 5] = [
        ("candidateIdentity", "candidateIdentitySha256"),
        ("program", "programSha256"),
        ("sourceProfile", "sourceProfileSha256"),
        ("profileSnapshot", "profileSnapshotSha256"),
        ("canonicalEvidence", "canonicalEvidenceIdentitySha256"),
    ];
    let mut counts = Map::new();
    for (count_name, record_name) in FIELDS {
        let mut identities = BTreeSet::new();
        for record in records {
            let identity = record.as_object()?.get(record_name)?.as_str()?;
            identities.insert(identity);
        }
        counts.insert(count_name.to_owned(), Value::from(identities.len() as u64));
    }
    Some(Value::Object(counts))
}

fn public_identity_ledger_sha256(summary: Value) -> Result<String> {
    summary
        .as_object()
        .and_then(|fields| fields.get("identityLedgerSha256"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| contract("public identity ledger summary is invalid"))
}

fn reconcile_checkpoint_public_identity_ledger(
    store: &ProposalJournal,
    ledger: &dyn IdentityLedger,
    expected_sha256: Option<&str>,
) -> Result<()> {
    match (expected_sha256, ledger.public_ledger()) {
        (None, None) => Ok(()),
        (None, Some(_)) => Err(contract(
            "checkpoint lacks the public identity ledger binding required by its runtime ledger",
        )),
        (Some(_), None) => Err(contract(
            "checkpoint requires a public identity ledger facade unavailable from the runtime ledger",
        )),
        (Some(expected), Some(public_ledger)) => {
            let summary = public_identity_ledger_summary(&public_ledger)?;
            if public_identity_ledger_sha256(summary)? != expected {
                return Err(contract(
                    "checkpoint public identity ledger identity diverged from restored ledger state",
                ));
            }
            let needs_repair = match store.read_public_identity_ledger() {
                Ok(persisted) => {
                    public_identity_ledger_summary(&persisted)?;
                    !canonically_equal(&persisted, &public_ledger)?
                }
                Err(crate::journal::JournalError::Io(error))
                    if error.kind() == std::io::ErrorKind::NotFound =>
                {
                    true
                }
                Err(error) => return Err(error.into()),
            };
            if needs_repair {
                // The immutable segment and checkpoint already bind this
                // exact public ledger identity, so restoring the mutable
                // facade is safe and idempotent after a crash window.
                store.write_public_identity_ledger(&public_ledger)?;
            }
            let persisted = store.read_public_identity_ledger()?;
            if !canonically_equal(&persisted, &public_ledger)? {
                return Err(contract(
                    "persisted public identity ledger diverged from checkpoint state",
                ));
            }
            Ok(())
        }
    }
}

fn current_public_identity_ledger_binding(
    store: &ProposalJournal,
    ledger: &dyn IdentityLedger,
    expected_sha256: Option<&str>,
) -> Result<Option<Value>> {
    reconcile_checkpoint_public_identity_ledger(store, ledger, expected_sha256)?;
    let Some(public_ledger) = ledger.public_ledger() else {
        return Ok(None);
    };
    let summary = public_identity_ledger_summary(&public_ledger)?;
    if Some(public_identity_ledger_sha256(summary.clone())?.as_str()) != expected_sha256 {
        return Err(contract(
            "completed generation public identity ledger drifted after its checkpoint",
        ));
    }
    Ok(Some(summary))
}

fn verify_public_identity_ledger(
    store: &ProposalJournal,
    ledger: &dyn IdentityLedger,
    generation_journal: &Value,
) -> Result<()> {
    let expected = generation_journal
        .as_object()
        .and_then(|fields| fields.get("globalIdentityLedger"));
    let Some(expected) = expected else {
        return Ok(());
    };
    let persisted = store.read_public_identity_ledger()?;
    let persisted_summary = public_identity_ledger_summary(&persisted)?;
    if &persisted_summary != expected {
        return Err(contract(
            "generation journal global identity ledger binding drifted from persisted facade",
        ));
    }
    let supplied = ledger.public_ledger().ok_or_else(|| {
        contract("completed generation requires the ledger public facade for restart verification")
    })?;
    if !canonically_equal(&supplied, &persisted)? {
        return Err(contract(
            "runtime identity ledger facade drifted from the persisted facade",
        ));
    }
    Ok(())
}

fn canonically_equal(left: &Value, right: &Value) -> Result<bool> {
    Ok(canonical_json_bytes(left)? == canonical_json_bytes(right)?)
}

fn public_identity_ledger_summary(ledger: &Value) -> Result<Value> {
    let ledger_sha256 = public_identity_ledger_self_hash(ledger)?;
    let fields = ledger
        .as_object()
        .ok_or_else(|| contract("public identity ledger must be an object"))?;
    let pair_executable_semantic_count = fields
        .get("pairExecutableSemantics")
        .and_then(Value::as_array)
        .ok_or_else(|| contract("public identity ledger lacks pair executable semantics"))?
        .len() as u64;
    let duplicate_rejections = fields
        .get("pairExecutableSemanticDuplicateRejections")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            contract("public identity ledger lacks pair executable semantic duplicate counter")
        })?;
    Ok(object([
        (
            "pairExecutableSemanticCount",
            Value::from(pair_executable_semantic_count),
        ),
        (
            "pairExecutableSemanticDuplicateRejections",
            Value::from(duplicate_rejections),
        ),
        ("identityLedgerSha256", Value::String(ledger_sha256)),
    ]))
}

fn public_identity_ledger_self_hash(ledger: &Value) -> Result<String> {
    let fields = ledger
        .as_object()
        .ok_or_else(|| contract("public identity ledger must be an object"))?;
    let ledger_sha256 = fields
        .get("ledgerSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| contract("public identity ledger lacks ledgerSha256"))?;
    sha(ledger_sha256, "public identity ledger SHA-256")?;
    let mut material = ledger.clone();
    material
        .as_object_mut()
        .expect("public identity ledger is object")
        .remove("ledgerSha256");
    if canonical_sha256(&material)? != ledger_sha256 {
        return Err(contract("public identity ledger self-hash is invalid"));
    }
    Ok(ledger_sha256.to_owned())
}

fn nullable_sha(fields: &Map<String, Value>, field: &str) -> Result<Option<String>> {
    match fields.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => {
            sha(value, field)?;
            Ok(Some(value.clone()))
        }
        _ => Err(contract(format!(
            "{field} must be a SHA-256 string or null"
        ))),
    }
}
