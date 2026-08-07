//! Concrete, frozen-input runtime for the Temporal QD front half.
//!
//! This crate owns no evaluator and never invokes AutoResearch for a proposal.
//! It reopens only exact frozen pair-run configuration and accepted-pair
//! artifacts, composes the native grammar/operator surfaces, and delegates
//! Dashboard validation and bidirectional compilation to the JSONL authority.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use sha2::{Digest, Sha256};
use temporal_qd_contract::{ContractError, Map, Value, canonical_sha256};
use temporal_qd_dashboard_jsonl::{
    CompileBidirectionalRequest, DashboardJsonlConfig, DashboardJsonlTransport,
    ValidateCandidateOutcome, ValidateCandidateRequest,
};
use temporal_qd_kernel::{
    construction::{
        ConstructionCatalog, GeneratorV3ConstructionRegistry, SCALAR_DYNAMIC_MANAGEMENT,
    },
    factory::{
        NativeConstructionContext, NativePairAuthority, NativeProposal, ParentReference,
        ProposalIntent,
    },
    genome::{
        CanonicalPairCompiler, FrozenModule, FrozenPair, HoldMutationPlan, IdentitySnapshot,
        NativeModuleValidator, SameSideCrossover, apply_pair_hold_mutation, canonical_hold,
        deterministic_same_side_crossover,
    },
    grammar::{GrammarContext, GrammarError, ModuleProgram, NativeValidator, TypedFragmentGrammar},
    identity::{
        PAIR_IMMIGRANT_BUILDER_VERSION, executable_pair_semantic_sha256, mutation_step_seed,
        proposal_side,
    },
    indicator::{
        IndicatorCatalog, IndicatorLearningRegistry, validate_entry_route_decision_indicator_cap,
    },
    proposal::ParentSelector,
    protection::{
        apply_immigrant_initial_protection, apply_initial_protection_plan,
        enumerate_initial_protection_plans, immigrant_initial_protection_selector,
    },
};
use time::{
    Date, Duration as TimeDuration, OffsetDateTime, Time, format_description::well_known::Rfc3339,
};

pub const RUNTIME_MANIFEST_SCHEMA: &str = "temporal_qd_runtime_manifest_v1";
pub const PAIR_RUN_CONFIG_SCHEMA: &str = "temporal_qd_bidirectional_pair_run_config_v2";

/// Production archive-parent projection and selection.
pub mod archive;
/// The campaign-global, Python-compatible identity ledger is intentionally
/// separate from proposal construction, preventing a candidate-only policy
/// from substituting for the frozen five-identity authority.
pub mod ledger;

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("canonical contract failure: {0}")]
    Canonical(#[from] ContractError),
    #[error("kernel contract failure: {0}")]
    Kernel(String),
    /// The frozen Dashboard authority completed the request and explicitly
    /// rejected the candidate as semantically invalid.  This is distinct from
    /// a transport/protocol failure: structural search records it as a normal
    /// rejected proposal and continues with the next deterministic ordinal.
    #[error("Dashboard semantic rejection: {0}")]
    SemanticRejection(String),
    /// A sealed local operator was well-formed but produced a candidate that
    /// violates an explicit search constraint.  This is journaled as an
    /// `operation_rejected` step with the matching Python exception label.
    #[error("candidate-local {exception_type} rejection: {message}")]
    OperatorRejected {
        exception_type: &'static str,
        message: String,
    },
    #[error("runtime manifest failure: {0}")]
    Manifest(String),
    #[error("runtime filesystem failure: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, RuntimeError>;

fn invalid(message: impl Into<String>) -> RuntimeError {
    RuntimeError::Manifest(message.into())
}

const DASHBOARD_SEMANTIC_REJECTION_MARKER: &str = "temporal_qd_dashboard_semantic_rejection:";

fn semantic_rejection_marker(message: impl AsRef<str>) -> String {
    format!("{DASHBOARD_SEMANTIC_REJECTION_MARKER}{}", message.as_ref())
}

fn semantic_rejection_from_marker(message: &str) -> Option<String> {
    message
        .strip_prefix(DASHBOARD_SEMANTIC_REJECTION_MARKER)
        .map(ToOwned::to_owned)
}

fn classify_grammar_materialization_error(error: GrammarError, context: &str) -> RuntimeError {
    match error {
        GrammarError::Invalid(message) => {
            if let Some(rejection) = semantic_rejection_from_marker(&message) {
                RuntimeError::SemanticRejection(rejection)
            } else {
                RuntimeError::Kernel(format!("{context}: {message}"))
            }
        }
        other => RuntimeError::Kernel(format!("{context}: {other}")),
    }
}

/// A grammar operation was selected from the sealed operation enumeration but
/// became structurally invalid when applied (for example, removing the last
/// exit branch).  The Python oracle records that as an `operation_rejected`
/// proposal step, rather than treating it as a Dashboard transport failure.
///
/// This is intentionally limited to `TypedFragmentGrammar::apply`: malformed
/// frozen inputs and materialization/authority failures retain their existing
/// fail-closed classifications.
fn classify_grammar_operator_error(error: GrammarError) -> RuntimeError {
    RuntimeError::OperatorRejected {
        exception_type: "GrammarError",
        message: error.to_string(),
    }
}

fn classify_frozen_module_error(
    error: temporal_qd_kernel::genome::GenomeError,
    context: &str,
) -> RuntimeError {
    match error {
        temporal_qd_kernel::genome::GenomeError::EntryRouteDecisionIndicatorCap => {
            RuntimeError::OperatorRejected {
                exception_type: "EntryRouteDecisionIndicatorCapError",
                message: "entry decision route exceeds the distinct decision-indicator cap".into(),
            }
        }
        temporal_qd_kernel::genome::GenomeError::Invalid(message)
            if semantic_rejection_from_marker(&message).is_some() =>
        {
            RuntimeError::SemanticRejection(
                semantic_rejection_from_marker(&message)
                    .expect("semantic rejection marker was checked"),
            )
        }
        other => RuntimeError::Kernel(format!("{context}: {other}")),
    }
}

fn operator_rejected_mutation_step(
    proposal_seed: String,
    side: &str,
    parent_pair: Value,
    parent_pair_identity_sha256: String,
    untouched_opposite_module_identity_sha256: String,
    operation: &Value,
    exception_type: &str,
) -> Result<Value> {
    let mut step = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_pair_proposal_v2".into()),
        ),
        ("proposalSeed", Value::String(proposal_seed)),
        ("originKind", Value::String("structural_offspring".into())),
        ("side", Value::String(side.into())),
        ("parentPair", parent_pair),
        (
            "parentPairIdentitySha256",
            Value::String(parent_pair_identity_sha256),
        ),
        (
            "untouchedOppositeModuleIdentitySha256",
            Value::String(untouched_opposite_module_identity_sha256),
        ),
        ("disposition", Value::String("operation_rejected".into())),
        ("operation", operation.clone()),
        (
            "rejection",
            object([
                (
                    "schemaVersion",
                    Value::String("temporal_qd_pair_rejection_audit_v1".into()),
                ),
                ("reasonCode", Value::String("operator_rejected".into())),
                ("exceptionType", Value::String(exception_type.into())),
                ("side", Value::String(side.into())),
                (
                    "operationSha256",
                    Value::String(canonical_sha256(operation)?),
                ),
            ]),
        ),
    ]);
    let step_sha = canonical_sha256(&step)?;
    step.as_object_mut()
        .expect("rejected proposal step")
        .insert("proposalSha256".into(), Value::String(step_sha));
    Ok(step)
}

fn typed_grammar_operation_audit(side: &str, plan: &Value) -> Value {
    // The accepted operator audit carries the full canonical plan, matching
    // Python's PairModuleOperator.apply_grammar lineage contract.  Its digest
    // is therefore part of module/pair identity, not redundant metadata.
    object([
        (
            "schemaVersion",
            Value::String("temporal_qd_typed_grammar_operation_audit_v1".into()),
        ),
        ("side", Value::String(side.into())),
        ("plan", plan.clone()),
    ])
}

/// Python's typed-pair mutation surface wraps the existing scalar dynamic
/// management construction plans so they compete with the static stop/target
/// grid as `initial_protection` mutations.  Keep the wrapper closed over the
/// exact construction enumeration: it is identity-bearing proposal material,
/// not a convenience selector.
fn dynamic_initial_protection_plans(catalog: &Value, profile: &Value) -> Result<Vec<Value>> {
    let catalog = ConstructionCatalog::new(catalog).map_err(|error| {
        RuntimeError::Kernel(format!("dynamic construction catalog is invalid: {error}"))
    })?;
    let registry = GeneratorV3ConstructionRegistry::new(catalog).map_err(|error| {
        RuntimeError::Kernel(format!("dynamic construction registry is invalid: {error}"))
    })?;
    let operator = registry.get(SCALAR_DYNAMIC_MANAGEMENT).map_err(|error| {
        RuntimeError::Kernel(format!(
            "dynamic construction operator is unavailable: {error}"
        ))
    })?;
    let mut rows = Vec::new();
    for construction_plan in operator.enumerate_plans(profile).map_err(|error| {
        RuntimeError::Kernel(format!("dynamic construction plans are invalid: {error}"))
    })? {
        let site = construction_plan
            .get("construction")
            .and_then(Value::as_object)
            .and_then(|construction| construction.get("site"))
            .and_then(Value::as_str);
        if !matches!(site, Some("initial_stop" | "initial_target")) {
            continue;
        }
        let mut wrapped = object([
            ("kind", Value::String("dynamic_construction".into())),
            ("constructionPlan", construction_plan),
            ("mutationClass", Value::String("kind_switch".into())),
        ]);
        let digest = canonical_sha256(&wrapped)?;
        wrapped
            .as_object_mut()
            .expect("dynamic construction wrapper")
            .insert("planSha256".into(), Value::String(digest));
        rows.push(wrapped);
    }
    rows.sort_by_key(|row| canonical_sha256(row).expect("finite dynamic protection plan"));
    Ok(rows)
}

fn dynamic_initial_protection_operator_plan(
    catalog: &Value,
    profile: &Value,
    requested: &Value,
) -> Result<(
    temporal_qd_kernel::construction::ConstructionOperator,
    Value,
)> {
    let canonical = dynamic_initial_protection_plans(catalog, profile)?
        .into_iter()
        .find(|plan| plan == requested)
        .ok_or_else(|| invalid("dynamic initial protection plan is not canonical"))?;
    let raw_plan = canonical
        .get("constructionPlan")
        .cloned()
        .ok_or_else(|| invalid("dynamic initial protection plan is incomplete"))?;
    let catalog = ConstructionCatalog::new(catalog).map_err(|error| {
        RuntimeError::Kernel(format!("dynamic construction catalog is invalid: {error}"))
    })?;
    let registry = GeneratorV3ConstructionRegistry::new(catalog).map_err(|error| {
        RuntimeError::Kernel(format!("dynamic construction registry is invalid: {error}"))
    })?;
    let operator = registry
        .get(SCALAR_DYNAMIC_MANAGEMENT)
        .map_err(|error| {
            RuntimeError::Kernel(format!(
                "dynamic construction operator is unavailable: {error}"
            ))
        })?
        .clone();
    Ok((operator, raw_plan))
}

fn object<'a>(entries: impl IntoIterator<Item = (&'a str, Value)>) -> Value {
    let mut output = Map::new();
    for (key, value) in entries {
        output.insert(key.to_owned(), value);
    }
    Value::Object(output)
}

fn require_object<'a>(value: &'a Value, name: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| invalid(format!("{name} must be an object")))
}

fn require_string(map: &Map<String, Value>, key: &str, name: &str) -> Result<String> {
    map.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| invalid(format!("{name} lacks nonempty {key}")))
}

fn require_sha256(map: &Map<String, Value>, key: &str, name: &str) -> Result<String> {
    let value = require_string(map, key, name)?;
    if value.len() != 71
        || !value.starts_with("sha256:")
        || !value.as_bytes()[7..]
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(invalid(format!(
            "{name} {key} must be a lowercase SHA-256 identity"
        )));
    }
    Ok(value)
}

fn exact_keys(map: &Map<String, Value>, expected: &[&str], name: &str) -> Result<()> {
    let actual = map.keys().map(String::as_str).collect::<Vec<_>>();
    let mut expected = expected.to_vec();
    expected.sort_unstable();
    if actual != expected {
        return Err(invalid(format!("{name} fields are not exact")));
    }
    Ok(())
}

fn value_without(value: &Value, key: &str) -> Result<Value> {
    let mut value = require_object(value, "self-hashed value")?.clone();
    value.remove(key);
    Ok(Value::Object(value))
}

/// Python's structural mutation candidate ID commits to the selected operation
/// JSON itself, not a hash string nested under the `operation` key.
fn mutation_candidate_id_for_choice(
    proposal_seed: &str,
    parent_identity_sha256: &str,
    choice: &Value,
) -> Result<String> {
    let hash = canonical_sha256(&object([
        ("seed", Value::String(proposal_seed.into())),
        ("parent", Value::String(parent_identity_sha256.into())),
        ("operation", choice.clone()),
    ]))?;
    Ok(format!("qd_pair_{}", &hash[7..35]))
}

fn parse_json(bytes: &[u8], name: &str) -> Result<Value> {
    serde_json::from_slice(bytes).map_err(|error| invalid(format!("{name} is not JSON: {error}")))
}

/// Reopen an exact accepted proposal journal entry.  The runtime deliberately
/// accepts the existing `proposal.factoryPair` / `proposal.pair` representation
/// rather than inventing a second parent-pair transport shape.
#[derive(Clone, Debug)]
pub struct AcceptedParentArtifact {
    pub entry: Value,
    pub pair: FrozenPair,
    pub candidate_id: String,
    pub candidate_identity_sha256: String,
}

impl AcceptedParentArtifact {
    pub fn from_entry(entry: &Value) -> Result<Self> {
        let map = require_object(entry, "accepted parent entry")?;
        if require_string(map, "schemaVersion", "accepted parent entry")?
            != "temporal_qd_proposal_entry_v3"
            || require_string(map, "disposition", "accepted parent entry")? != "accepted"
        {
            return Err(invalid(
                "parent artifact is not an accepted v3 proposal entry",
            ));
        }
        let candidate = require_object(
            map.get("candidate")
                .ok_or_else(|| invalid("accepted parent entry lacks candidate"))?,
            "accepted parent candidate",
        )?;
        let candidate_id = require_string(candidate, "candidateId", "accepted parent candidate")?;
        let candidate_identity_sha256 = require_sha256(
            candidate,
            "candidateIdentitySha256",
            "accepted parent candidate",
        )?;
        let proposal = require_object(
            map.get("proposal")
                .ok_or_else(|| invalid("accepted parent entry lacks proposal"))?,
            "accepted parent proposal",
        )?;
        if require_string(proposal, "disposition", "accepted parent proposal")? != "materialized" {
            return Err(invalid("accepted parent proposal is not materialized"));
        }
        let payload = proposal
            .get("pair")
            .or_else(|| proposal.get("factoryPair"))
            .ok_or_else(|| invalid("accepted parent proposal lacks frozen pair"))?;
        let pair = FrozenPair::from_payload(payload)
            .map_err(|error| invalid(format!("accepted parent pair is invalid: {error}")))?;
        let pair_identity = pair.identity_sha256().map_err(|error| {
            invalid(format!("accepted parent pair identity is invalid: {error}"))
        })?;
        if require_sha256(proposal, "pairIdentitySha256", "accepted parent proposal")?
            != pair_identity
        {
            return Err(invalid("accepted parent proposal pair identity drifted"));
        }
        if candidate
            .get("bidirectionalGenome")
            .and_then(Value::as_object)
            .is_some_and(|_| candidate.get("bidirectionalGenome") != Some(payload))
        {
            return Err(invalid(
                "accepted parent candidate genome diverged from proposal pair",
            ));
        }
        Ok(Self {
            entry: entry.clone(),
            pair,
            candidate_id,
            candidate_identity_sha256,
        })
    }

    pub fn parent_reference(&self) -> Result<ParentReference> {
        let pair_identity_sha256 = self.pair.identity_sha256().map_err(|error| {
            invalid(format!("accepted parent pair identity is invalid: {error}"))
        })?;
        let pair_payload = self.pair.canonical_payload().map_err(|error| {
            invalid(format!(
                "accepted parent pair cannot be serialized: {error}"
            ))
        })?;
        let selection_audit = self
            .entry
            .get("parentSelection")
            .cloned()
            .filter(|value| !value.is_null());
        let result = ParentReference {
            pair_identity_sha256,
            candidate_id: self.candidate_id.clone(),
            pair_payload,
            selection_audit,
        };
        result
            .validate()
            .map_err(|error| invalid(format!("accepted parent reference is invalid: {error}")))?;
        Ok(result)
    }
}

/// Internal, redesignable exact input bundle for the concrete native runtime.
/// `pairRunConfig` is the existing Python-frozen authority verbatim; production
/// parents are one signed QD archive, parsed exactly once before Dashboard.
#[derive(Clone, Debug)]
pub struct RuntimeManifest {
    pub pair_run_config: Value,
    pub pair_run_config_sha256: String,
    pub bidirectional_pair_policy: Value,
    pub bidirectional_pair_policy_sha256: String,
    pub evidence_identity_context: Option<Value>,
    pub evidence_identity_context_sha256: Option<String>,
    pub generation_index: u64,
    /// The pair-generation `configSha256` that seeds Python parent selection.
    /// It is distinct from the frozen pair-run configuration identity.
    pub pair_generation_config_sha256: String,
    /// Single validated production parent source. Selector construction and
    /// identity-ledger bootstrap both consume this projection.
    pub parent_archive: archive::VerifiedParentArchive,
    pub ledger: Value,
}

impl RuntimeManifest {
    pub fn from_value(value: &Value) -> Result<Self> {
        // Borrowed callers (fixtures and direct library consumers) retain the
        // historical API.  The production batch path consumes its assembled
        // envelope with `from_owned_value` so a multi-gigabyte archive is not
        // cloned merely to cross this validation seam.
        Self::from_owned_value(value.clone())
    }

    /// Validate an assembled runtime manifest while consuming its envelope.
    ///
    /// `qd-batch` owns the parsed archive and ledger, so this is the
    /// production constructor.  It performs the same strict checks as
    /// [`Self::from_value`] but releases the raw `parentArchive` immediately
    /// after constructing the compact verified projection.
    pub fn from_owned_value(value: Value) -> Result<Self> {
        let map = require_object(&value, "runtime manifest")?;
        exact_keys(
            map,
            &[
                "schemaVersion",
                "pairRunConfig",
                "pairRunConfigSha256",
                "bidirectionalPairPolicy",
                "bidirectionalPairPolicySha256",
                "evidenceIdentityContext",
                "evidenceIdentityContextSha256",
                "generationIndex",
                "pairGenerationConfigSha256",
                "parentArchive",
                "parentArchiveSha256",
                "identityLedger",
            ],
            "runtime manifest",
        )?;
        if require_string(map, "schemaVersion", "runtime manifest")? != RUNTIME_MANIFEST_SCHEMA {
            return Err(invalid("runtime manifest schema is incompatible"));
        }
        let pair_run_config = map
            .get("pairRunConfig")
            .cloned()
            .ok_or_else(|| invalid("runtime manifest lacks pair run config"))?;
        let pair_run_config_sha256 =
            require_sha256(map, "pairRunConfigSha256", "runtime manifest")?;
        if canonical_sha256(&value_without(&pair_run_config, "pairRunConfigSha256")?)?
            != pair_run_config_sha256
        {
            return Err(invalid(
                "runtime manifest pair-run configuration identity drifted",
            ));
        }
        validate_pair_run_config(&pair_run_config)?;
        let bidirectional_pair_policy = map
            .get("bidirectionalPairPolicy")
            .cloned()
            .ok_or_else(|| invalid("runtime manifest lacks pair policy"))?;
        require_object(&bidirectional_pair_policy, "runtime pair policy")?;
        let bidirectional_pair_policy_sha256 =
            require_sha256(map, "bidirectionalPairPolicySha256", "runtime manifest")?;
        if canonical_sha256(&bidirectional_pair_policy)? != bidirectional_pair_policy_sha256 {
            return Err(invalid("runtime manifest pair-policy identity drifted"));
        }
        let evidence_identity_context = map
            .get("evidenceIdentityContext")
            .cloned()
            .filter(|value| !value.is_null());
        let evidence_identity_context_sha256 = map
            .get("evidenceIdentityContextSha256")
            .filter(|value| !value.is_null())
            .map(|_| require_sha256(map, "evidenceIdentityContextSha256", "runtime manifest"))
            .transpose()?;
        match (
            &evidence_identity_context,
            &evidence_identity_context_sha256,
        ) {
            (None, None) => {}
            (Some(context), Some(identity))
                if canonical_sha256(&value_without(
                    context,
                    "predeclaredEvidenceContextSha256",
                )?)? == *identity => {}
            _ => {
                return Err(invalid(
                    "runtime evidence context and identity must be paired",
                ));
            }
        }
        let _generation_index = map
            .get("generationIndex")
            .and_then(Value::as_u64)
            .ok_or_else(|| invalid("runtime manifest generation index is invalid"))?;
        let _pair_generation_config_sha256 =
            require_sha256(map, "pairGenerationConfigSha256", "runtime manifest")?;
        let parent_archive = archive::VerifiedParentArchive::from_archive(
            map.get("parentArchive")
                .ok_or_else(|| invalid("runtime manifest lacks parent archive"))?,
        )
        .map_err(|error| invalid(format!("runtime parent archive is invalid: {error}")))?;
        if require_sha256(map, "parentArchiveSha256", "runtime manifest")?
            != parent_archive.archive_sha256()
        {
            return Err(invalid("runtime manifest parent archive identity drifted"));
        }
        // Archive members were evaluated under their source generation's frozen
        // context.  Python intentionally rebinds their evidence identities to
        // this manifest's current context during ledger bootstrap, so a
        // persisted historical identity is not a current-context assertion.
        // The verified archive hash, pair policy, and current proposal identity
        // checks remain authoritative.
        if evidence_identity_context.is_none() && parent_archive.members().next().is_some() {
            return Err(invalid(
                "runtime parent archive requires evidence identity context",
            ));
        }
        let ledger = map
            .get("identityLedger")
            .ok_or_else(|| invalid("runtime manifest lacks identity ledger"))?;
        validate_ledger(ledger)?;
        // The strict checks above operate only through immutable references.
        // Now move the retained authorities out and drop the source envelope,
        // including the raw parent archive, rather than retaining a second
        // canonical JSON tree for the duration of proposal generation.
        let mut fields = match value {
            Value::Object(fields) => fields,
            _ => unreachable!("runtime manifest was validated as an object"),
        };
        let take = |fields: &mut Map<String, Value>, key: &str| {
            fields
                .remove(key)
                .expect("runtime manifest exact-key validation retained required field")
        };
        let retained_pair_run_config = take(&mut fields, "pairRunConfig");
        let retained_pair_run_config_sha256 = take(&mut fields, "pairRunConfigSha256")
            .as_str()
            .expect("validated runtime pair-run SHA-256")
            .to_owned();
        let retained_pair_policy = take(&mut fields, "bidirectionalPairPolicy");
        let retained_pair_policy_sha256 = take(&mut fields, "bidirectionalPairPolicySha256")
            .as_str()
            .expect("validated runtime pair-policy SHA-256")
            .to_owned();
        let retained_evidence_context = take(&mut fields, "evidenceIdentityContext");
        let retained_evidence_context_sha256 = take(&mut fields, "evidenceIdentityContextSha256");
        let retained_generation_index = take(&mut fields, "generationIndex")
            .as_u64()
            .expect("validated runtime generation index");
        let retained_pair_generation_config_sha256 =
            take(&mut fields, "pairGenerationConfigSha256")
                .as_str()
                .expect("validated runtime pair-generation SHA-256")
                .to_owned();
        // Drop this exact raw archive rather than retaining it beside the
        // validated selection/ledger projection built above.
        let _ = take(&mut fields, "parentArchive");
        let _ = take(&mut fields, "parentArchiveSha256");
        let retained_ledger = take(&mut fields, "identityLedger");
        debug_assert!(fields.contains_key("schemaVersion"));
        Ok(Self {
            pair_run_config: retained_pair_run_config,
            pair_run_config_sha256: retained_pair_run_config_sha256,
            bidirectional_pair_policy: retained_pair_policy,
            bidirectional_pair_policy_sha256: retained_pair_policy_sha256,
            evidence_identity_context: (!retained_evidence_context.is_null())
                .then_some(retained_evidence_context),
            evidence_identity_context_sha256: (!retained_evidence_context_sha256.is_null()).then(
                || {
                    retained_evidence_context_sha256
                        .as_str()
                        .expect("validated runtime evidence SHA-256")
                        .to_owned()
                },
            ),
            generation_index: retained_generation_index,
            pair_generation_config_sha256: retained_pair_generation_config_sha256,
            parent_archive,
            ledger: retained_ledger,
        })
    }

    pub fn read_path(path: &Path) -> Result<Self> {
        let path = safe_manifest_path(path)?;
        Self::from_value(&parse_json(&fs::read(path)?, "runtime manifest")?)
    }

    /// Release the manifest envelope after its selector, authority, and
    /// archive-bootstrap inputs have been derived.  The returned ledger is
    /// already strictly validated by either manifest constructor.
    pub fn into_identity_ledger(self) -> Value {
        self.ledger
    }
}

fn validate_pair_run_config(value: &Value) -> Result<()> {
    let map = require_object(value, "frozen pair run config")?;
    let supplied = require_sha256(map, "pairRunConfigSha256", "frozen pair run config")?;
    if canonical_sha256(&value_without(value, "pairRunConfigSha256")?)? != supplied
        || require_string(map, "schemaVersion", "frozen pair run config")? != PAIR_RUN_CONFIG_SCHEMA
    {
        return Err(invalid("frozen pair-run config identity/schema mismatch"));
    }
    for key in [
        "longModule",
        "shortModule",
        "grammarRegistry",
        "holdOperatorPolicy",
        "initialProtectionOperatorPolicy",
        "immigrantConstructionPolicy",
        "nativeJsonlAuthority",
        "nativeAuthority",
        "pairCompilerAuthority",
        "operatorImplementation",
    ] {
        if !map.contains_key(key) {
            return Err(invalid(format!("frozen pair-run config lacks {key}")));
        }
    }
    for direction in ["long", "short"] {
        let side = require_object(
            map.get(&format!("{direction}Module")).expect("checked"),
            "frozen pair-run side",
        )?;
        for key in [
            "seedNames",
            "context",
            "catalog",
            "catalogSha256",
            "indicatorPolicy",
            "policy",
        ] {
            if !side.contains_key(key) {
                return Err(invalid(format!("frozen {direction} side lacks {key}")));
            }
        }
        let catalog = temporal_qd_kernel::indicator::IndicatorCatalog::new(
            side.get("catalog").expect("checked"),
        )
        .map_err(|error| invalid(format!("frozen {direction} catalog is invalid: {error}")))?;
        if side.get("catalogSha256").and_then(Value::as_str) != Some(catalog.catalog_sha256()) {
            return Err(invalid(format!(
                "frozen {direction} catalog identity drifted"
            )));
        }
        let registry = temporal_qd_kernel::indicator::IndicatorLearningRegistry::new(catalog)
            .map_err(|error| {
                invalid(format!(
                    "frozen {direction} indicator policy is invalid: {error}"
                ))
            })?;
        if side.get("indicatorPolicy") != Some(registry.policy()) {
            return Err(invalid(format!(
                "frozen {direction} indicator policy drifted"
            )));
        }
        let context = temporal_qd_kernel::grammar::GrammarContext::from_normalized(
            side.get("context").expect("checked"),
        )
        .map_err(|error| {
            invalid(format!(
                "frozen {direction} grammar context is invalid: {error}"
            ))
        })?;
        if context.normalized().map_err(|error| {
            invalid(format!(
                "frozen {direction} grammar context is invalid: {error}"
            ))
        })? != *side.get("context").expect("checked")
        {
            return Err(invalid(format!(
                "frozen {direction} grammar context drifted"
            )));
        }
    }
    temporal_qd_kernel::protection::validate_initial_protection_policy(
        map.get("initialProtectionOperatorPolicy").expect("checked"),
    )
    .map_err(|error| invalid(format!("frozen protection policy is invalid: {error}")))?;
    IdentitySnapshot::from_payload(
        map.get("nativeAuthority").expect("checked"),
        Some("nativeAuthority"),
    )
    .map_err(|error| invalid(format!("frozen native authority is invalid: {error}")))?;
    IdentitySnapshot::from_payload(
        map.get("pairCompilerAuthority").expect("checked"),
        Some("pairCompiler"),
    )
    .map_err(|error| {
        invalid(format!(
            "frozen pair compiler authority is invalid: {error}"
        ))
    })?;
    Ok(())
}

fn validate_ledger(value: &Value) -> Result<()> {
    ledger::GlobalIdentityLedger::from_public(value.clone())
        .map(|_| ())
        .map_err(|error| invalid(format!("global identity ledger is invalid: {error}")))
}

fn safe_manifest_path(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(invalid("runtime manifest path must be absolute"));
    }
    let mut current = PathBuf::new();
    let components = path.components().collect::<Vec<_>>();
    for (index, component) in components.iter().enumerate() {
        match component {
            Component::Prefix(_) => current.push(component.as_os_str()),
            Component::RootDir | Component::Normal(_) => current.push(component.as_os_str()),
            Component::CurDir | Component::ParentDir => {
                return Err(invalid("runtime manifest path contains unsafe component"));
            }
        }
        // Windows permits metadata queries on `C:\\` and `\\\\?\\C:\\`, but
        // rejects their prefix-only spellings (`C:` / `\\\\?\\C:`).  A prefix is
        // not a filesystem component, so defer checks until its root follows.
        if matches!(component, Component::Prefix(_)) {
            continue;
        }
        let metadata = fs::symlink_metadata(&current)?;
        if metadata.file_type().is_symlink() {
            return Err(invalid("runtime manifest path contains a symlink"));
        }
        #[cfg(windows)]
        let is_reparse_point = {
            use std::os::windows::fs::MetadataExt;
            metadata.file_attributes() & 0x0400 != 0
        };
        #[cfg(windows)]
        if is_reparse_point {
            return Err(invalid("runtime manifest path contains a reparse point"));
        }
        let final_component = index + 1 == components.len();
        if (!final_component && !metadata.is_dir()) || (final_component && !metadata.is_file()) {
            return Err(invalid("runtime manifest path is not a regular file"));
        }
    }
    Ok(path.to_path_buf())
}

/// Parent selection authority. `Archive` is production; the identity-sorted
/// ring is retained only for fixtures that explicitly supply parent artifacts.
#[derive(Clone, Debug)]
pub enum RuntimeParentSelector {
    Archive(archive::ArchiveParentSelector),
    ExplicitFixtureRing { parents: Vec<ParentReference> },
}

impl RuntimeParentSelector {
    pub fn from_verified_archive(
        verified_archive: &archive::VerifiedParentArchive,
        pair_generation_config_sha256: &str,
        allow_empty_quality_bootstrap: bool,
    ) -> Result<Self> {
        archive::ArchiveParentSelector::from_verified(
            verified_archive,
            pair_generation_config_sha256,
            allow_empty_quality_bootstrap,
        )
        .map(Self::Archive)
        .map_err(|error| invalid(format!("runtime parent archive is invalid: {error}")))
    }

    /// Open the production archive selector from the immutable QD archive.
    /// `pair_generation_config_sha256` is the freshly built generation
    /// configuration's `configSha256`, which seeds Python's proposal/parent
    /// RNG.  It is intentionally *not* the frozen `pairRunConfigSha256`.
    pub fn from_archive(
        verified_archive: &Value,
        pair_generation_config_sha256: &str,
        allow_empty_quality_bootstrap: bool,
    ) -> Result<Self> {
        archive::ArchiveParentSelector::from_archive(
            verified_archive,
            pair_generation_config_sha256,
            allow_empty_quality_bootstrap,
        )
        .map(Self::Archive)
        .map_err(|error| invalid(format!("runtime parent archive is invalid: {error}")))
    }

    /// Production constructor. It uses the exact archive projection validated
    /// by [`RuntimeManifest::from_value`], so parent selection does not reopen
    /// a raw file or diverge from identity-ledger archive bootstrap.
    pub fn from_manifest(
        manifest: &RuntimeManifest,
        allow_empty_quality_bootstrap: bool,
    ) -> Result<Self> {
        Self::from_verified_archive(
            &manifest.parent_archive,
            &manifest.pair_generation_config_sha256,
            allow_empty_quality_bootstrap,
        )
    }

    /// Fixture-only source. Production callers must use [`Self::from_manifest`]
    /// to bind selection order and checkpoint state to one verified archive.
    pub fn from_fixture_parent_artifacts(entries: &[Value]) -> Result<Self> {
        let mut parents = entries
            .iter()
            .map(AcceptedParentArtifact::from_entry)
            .collect::<Result<Vec<_>>>()?
            .iter()
            .map(AcceptedParentArtifact::parent_reference)
            .collect::<Result<Vec<_>>>()?;
        parents.sort_by_key(|parent| parent.pair_identity_sha256.clone());
        Ok(Self::ExplicitFixtureRing { parents })
    }
}

impl ParentSelector for RuntimeParentSelector {
    fn has_parents(&self) -> bool {
        match self {
            Self::Archive(selector) => selector.has_parents(),
            Self::ExplicitFixtureRing { parents } => !parents.is_empty(),
        }
    }

    fn eligible_parent_count(&self) -> usize {
        match self {
            Self::Archive(selector) => selector.eligible_parent_count(),
            Self::ExplicitFixtureRing { parents } => parents.len(),
        }
    }

    fn archive_cell_count(&self) -> usize {
        match self {
            Self::Archive(selector) => selector.archive_cell_count(),
            Self::ExplicitFixtureRing { .. } => 0,
        }
    }

    fn compact_state(&self) -> Value {
        match self {
            Self::Archive(selector) => selector.compact_state(),
            Self::ExplicitFixtureRing { .. } => object([(
                "schemaVersion",
                Value::String("temporal_qd_runtime_explicit_parent_ring_state_v1".into()),
            )]),
        }
    }

    fn restore_compact_state(&mut self, state: &Value) -> temporal_qd_kernel::proposal::Result<()> {
        match self {
            Self::Archive(selector) => selector.restore_compact_state(state),
            Self::ExplicitFixtureRing { .. } => {
                if state != &self.compact_state() {
                    return Err(temporal_qd_kernel::proposal::ProposalError::Contract(
                        "runtime explicit-parent selector checkpoint drifted".into(),
                    ));
                }
                Ok(())
            }
        }
    }

    fn select(
        &mut self,
        _label: &str,
        structural_selection_ordinal: u64,
    ) -> temporal_qd_kernel::proposal::Result<ParentReference> {
        match self {
            Self::Archive(selector) => selector.select(_label, structural_selection_ordinal),
            Self::ExplicitFixtureRing { parents } => {
                if parents.is_empty() {
                    return Err(
                        temporal_qd_kernel::proposal::ProposalError::ParentSelectorUnavailable,
                    );
                }
                parents
                    .get((structural_selection_ordinal % parents.len() as u64) as usize)
                    .cloned()
                    .ok_or(temporal_qd_kernel::proposal::ProposalError::ParentSelectorUnavailable)
            }
        }
    }
}

/// Reopen the sole campaign-global identity policy from the verified manifest.
/// The returned ledger implements `qd-kernel`'s transactional `IdentityLedger`
/// directly; no candidate-only wrapper exists in production.
pub fn global_identity_ledger_from_manifest(
    manifest: &RuntimeManifest,
) -> Result<ledger::GlobalIdentityLedger> {
    global_identity_ledger_from_public(manifest.ledger.clone())
}

/// Open an already-validated ledger by value.  The production batch path uses
/// this after extracting compact archive-bootstrap inputs, so it does not hold
/// two complete public-ledger trees while generating proposals.
pub fn global_identity_ledger_from_public(
    ledger_value: Value,
) -> Result<ledger::GlobalIdentityLedger> {
    let mut ledger = ledger::GlobalIdentityLedger::from_public(ledger_value)
        .map_err(|error| invalid(format!("global identity ledger is invalid: {error}")))?;
    // Python pair generation persists the executable-pair extension from CP0,
    // including after a first locally rejected proposal.
    ledger.enable_pair_mode().map_err(|error| {
        invalid(format!(
            "pair identity ledger extension is invalid: {error}"
        ))
    })?;
    Ok(ledger)
}

/// Recompute and insert every verified archive member into the pair-mode
/// ledger before generation. This traverses the exact projection already
/// validated by [`RuntimeManifest::from_value`]; it never reparses raw archive
/// JSON or reaches into selector-private state.
pub fn bootstrap_global_identity_ledger_from_manifest(
    ledger: &mut ledger::GlobalIdentityLedger,
    manifest: &RuntimeManifest,
) -> Result<()> {
    let inputs = archive_bootstrap_inputs_from_manifest(manifest)?;
    bootstrap_global_identity_ledger_inputs(ledger, inputs)
}

/// Reduce every verified archive member to its exact five-identity ledger
/// input.  No candidate graph or archive cell is retained in the result.
pub fn archive_bootstrap_inputs_from_manifest(
    manifest: &RuntimeManifest,
) -> Result<Vec<ledger::ArchiveBootstrapInput>> {
    let evidence_context = manifest.evidence_identity_context.as_ref().ok_or_else(|| {
        invalid("pair identity ledger archive bootstrap requires evidence identity context")
    })?;
    manifest
        .parent_archive
        .members()
        .map(|member| {
            let ledger_candidate = member
                .ledger_candidate()
                .map_err(|error| invalid(format!("runtime parent archive is invalid: {error}")))?;
            let (candidate, canonical_evidence_identity_sha256) =
                rebind_archive_candidate_evidence_identity(&ledger_candidate, evidence_context)?;
            let (long_profile_sha256, short_profile_sha256) = member.executable_profile_sha256s();
            let executable_semantic_sha256 =
                executable_pair_semantic_sha256(long_profile_sha256, short_profile_sha256);
            Ok(ledger::ArchiveBootstrapInput {
                candidate,
                canonical_evidence_identity_sha256,
                executable_semantic_sha256: Some(executable_semantic_sha256),
            })
        })
        .collect::<Result<Vec<_>>>()
}

/// Apply compact, already-verified parent identities to an open ledger.
pub fn bootstrap_global_identity_ledger_inputs(
    ledger: &mut ledger::GlobalIdentityLedger,
    inputs: Vec<ledger::ArchiveBootstrapInput>,
) -> Result<()> {
    ledger.bootstrap_archive(inputs).map_err(|error| {
        invalid(format!(
            "pair identity ledger archive bootstrap failed: {error}"
        ))
    })
}

/// The only capability the construction runtime needs from Dashboard.  It is a
/// deliberately small *validation/compile* port: it cannot evaluate a market
/// candidate, hydrate an archive, or invoke an AutoResearch proposal path.
/// Tests can provide a deterministic in-memory authority through this port;
/// production uses [`DashboardJsonlPort`].
pub trait DashboardPort {
    fn validate_v2(&self, profile: &Value, candidate_id: &str) -> Result<Value>;
    fn compile_bidirectional(
        &self,
        long_profile: &Value,
        short_profile: &Value,
        candidate_id: &str,
    ) -> Result<Value>;
}

/// Ordered, persistent Dashboard JSONL implementation of [`DashboardPort`].
/// The `RefCell` only serializes this single-threaded runtime's request stream;
/// `DashboardJsonlTransport` itself enforces one request/response at a time.
pub struct DashboardJsonlPort {
    transport: RefCell<DashboardJsonlTransport>,
}

impl DashboardJsonlPort {
    pub fn from_frozen_authority(authority: &Value) -> Result<Self> {
        let fields = require_object(authority, "frozen Dashboard JSONL authority")?;
        exact_keys(
            fields,
            &[
                "command",
                "timeoutSeconds",
                "persistentJsonl",
                "maxLineBytes",
                "stderrLimitBytes",
                "interpreterPath",
                "validatorScriptPath",
                "dashboardSourceRoot",
                "environment",
                "authorityContent",
            ],
            "frozen Dashboard JSONL authority",
        )?;
        if fields.get("persistentJsonl") != Some(&Value::Bool(true)) {
            return Err(invalid(
                "frozen Dashboard authority is not persistent JSONL",
            ));
        }
        let command = fields
            .get("command")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("frozen Dashboard command must be an array"))?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .filter(|item| !item.trim().is_empty())
                    .map(ToOwned::to_owned)
                    .ok_or_else(|| invalid("frozen Dashboard command item is invalid"))
            })
            .collect::<Result<Vec<_>>>()?;
        if command.len() < 2 {
            return Err(invalid(
                "frozen Dashboard command must bind interpreter and script",
            ));
        }
        let interpreter = absolute_regular_path(
            Path::new(&require_string(
                fields,
                "interpreterPath",
                "frozen Dashboard authority",
            )?),
            "Dashboard interpreter",
        )?;
        let script = absolute_regular_path(
            Path::new(&require_string(
                fields,
                "validatorScriptPath",
                "frozen Dashboard authority",
            )?),
            "Dashboard validator script",
        )?;
        let source_root = absolute_directory_path(
            Path::new(&require_string(
                fields,
                "dashboardSourceRoot",
                "frozen Dashboard authority",
            )?),
            "Dashboard source root",
        )?;
        validate_frozen_command_paths(&command, &interpreter, &script)?;
        validate_authority_content(
            fields.get("authorityContent").expect("checked"),
            &interpreter,
            &script,
            &source_root,
            fields.get("environment").expect("checked"),
        )?;
        let timeout_seconds = fields
            .get("timeoutSeconds")
            .and_then(Value::as_u64)
            .ok_or_else(|| invalid("frozen Dashboard timeoutSeconds must be an integer"))?;
        let max_line_bytes = fields
            .get("maxLineBytes")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| invalid("frozen Dashboard maxLineBytes must be an integer"))?;
        let stderr_limit_bytes = fields
            .get("stderrLimitBytes")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .ok_or_else(|| invalid("frozen Dashboard stderrLimitBytes must be an integer"))?;
        let environment = require_object(
            fields.get("environment").expect("checked"),
            "frozen Dashboard environment",
        )?;
        exact_keys(environment, &["PYTHONPATH"], "frozen Dashboard environment")?;
        let python_path = environment
            .get("PYTHONPATH")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("frozen Dashboard PYTHONPATH must be an array"))?
            .iter()
            .map(|value| {
                absolute_directory_path(
                    Path::new(
                        value
                            .as_str()
                            .ok_or_else(|| invalid("frozen PYTHONPATH item is invalid"))?,
                    ),
                    "Dashboard PYTHONPATH item",
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let joined_python_path = std::env::join_paths(&python_path)
            .map_err(|_| invalid("frozen Dashboard PYTHONPATH cannot be joined"))?
            .into_string()
            .map_err(|_| invalid("frozen Dashboard PYTHONPATH is not Unicode"))?;
        let config = DashboardJsonlConfig::new(command)
            .map_err(|error| invalid(format!("frozen Dashboard config is invalid: {error}")))?
            .with_timeout(Duration::from_secs(timeout_seconds))
            .map_err(|error| invalid(format!("frozen Dashboard timeout is invalid: {error}")))?
            .with_max_line_bytes(max_line_bytes)
            .map_err(|error| invalid(format!("frozen Dashboard line limit is invalid: {error}")))?
            .with_stderr_limit_bytes(stderr_limit_bytes)
            .map_err(|error| invalid(format!("frozen Dashboard stderr limit is invalid: {error}")))?
            .with_current_dir(source_root)
            .with_environment("PYTHONPATH", joined_python_path)
            .map_err(|error| {
                invalid(format!("frozen Dashboard environment is invalid: {error}"))
            })?;
        let transport = DashboardJsonlTransport::spawn(config).map_err(|error| {
            RuntimeError::Kernel(format!(
                "could not start frozen Dashboard authority: {error}"
            ))
        })?;
        Ok(Self {
            transport: RefCell::new(transport),
        })
    }
}

impl DashboardPort for DashboardJsonlPort {
    fn validate_v2(&self, profile: &Value, candidate_id: &str) -> Result<Value> {
        let source_profile = require_object(profile, "Dashboard v2 source profile")?.clone();
        let expected_raw_source_profile_sha256 = canonical_sha256(profile)?;
        let response = self
            .transport
            .borrow_mut()
            .validate_candidate(ValidateCandidateRequest {
                candidate_id: candidate_id.to_owned(),
                expected_raw_source_profile_sha256: Some(expected_raw_source_profile_sha256),
                source_profile,
            })
            .map_err(|error| {
                RuntimeError::Kernel(format!("Dashboard v2 validation failed: {error}"))
            })?;
        if response.outcome != ValidateCandidateOutcome::Accepted {
            return Err(RuntimeError::SemanticRejection(
                "Dashboard rejected v2 source profile".to_owned(),
            ));
        }
        Ok(Value::Object(response.report))
    }

    fn compile_bidirectional(
        &self,
        long_profile: &Value,
        short_profile: &Value,
        candidate_id: &str,
    ) -> Result<Value> {
        let long_profile =
            require_object(long_profile, "Dashboard long v2 source profile")?.clone();
        let short_profile =
            require_object(short_profile, "Dashboard short v2 source profile")?.clone();
        let response = self
            .transport
            .borrow_mut()
            .compile_bidirectional(CompileBidirectionalRequest {
                candidate_id: candidate_id.to_owned(),
                expected_long_raw_source_profile_sha256: canonical_sha256(&Value::Object(
                    long_profile.clone(),
                ))?,
                expected_short_raw_source_profile_sha256: canonical_sha256(&Value::Object(
                    short_profile.clone(),
                ))?,
                long_profile,
                short_profile,
            })
            .map_err(|error| {
                RuntimeError::Kernel(format!(
                    "Dashboard bidirectional compilation failed: {error}"
                ))
            })?;
        Ok(object([
            ("profile", Value::Object(response.result.profile)),
            ("validation", Value::Object(response.result.report)),
        ]))
    }
}

fn sha256_file(path: &Path, _label: &str) -> Result<String> {
    let bytes = fs::read(path).map_err(RuntimeError::Io)?;
    let mut digest = Sha256::new();
    digest.update(bytes);
    Ok(format!("sha256:{:x}", digest.finalize()))
}

fn absolute_regular_path(path: &Path, label: &str) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(invalid(format!("{label} path must be absolute")));
    }
    let path = safe_manifest_path(path)?;
    Ok(path.canonicalize()?)
}

/// Compare the executable portion of a frozen Dashboard command by the same
/// canonical, regular-file path policy used for the authoritative paths.
/// This treats Windows drive paths and their `\\\\?\\` spelling as the same
/// file while still rejecting symlink/reparse traversal and substitution.
fn validate_frozen_command_paths(
    command: &[String],
    interpreter: &Path,
    script: &Path,
) -> Result<()> {
    let command_interpreter = absolute_regular_path(
        Path::new(
            command
                .first()
                .ok_or_else(|| invalid("frozen Dashboard command lacks interpreter"))?,
        ),
        "Dashboard command interpreter",
    )?;
    let command_script = absolute_regular_path(
        Path::new(
            command
                .get(1)
                .ok_or_else(|| invalid("frozen Dashboard command lacks validator script"))?,
        ),
        "Dashboard command validator script",
    )?;
    if command_interpreter != interpreter || command_script != script {
        return Err(invalid(
            "frozen Dashboard command does not bind frozen executable paths",
        ));
    }
    Ok(())
}

fn absolute_directory_path(path: &Path, label: &str) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(invalid(format!("{label} path must be absolute")));
    }
    let mut current = PathBuf::new();
    let components = path.components().collect::<Vec<_>>();
    for component in &components {
        match component {
            Component::Prefix(_) => current.push(component.as_os_str()),
            Component::RootDir | Component::Normal(_) => current.push(component.as_os_str()),
            Component::CurDir | Component::ParentDir => {
                return Err(invalid(format!("{label} path contains unsafe component")));
            }
        }
        // See `safe_manifest_path`: an extended Windows prefix has no
        // queryable metadata until its root component is present.
        if matches!(component, Component::Prefix(_)) {
            continue;
        }
        let metadata = fs::symlink_metadata(&current)?;
        if metadata.file_type().is_symlink() {
            return Err(invalid(format!("{label} path contains a symlink")));
        }
        #[cfg(windows)]
        let is_reparse_point = {
            use std::os::windows::fs::MetadataExt;
            metadata.file_attributes() & 0x0400 != 0
        };
        #[cfg(windows)]
        if is_reparse_point {
            return Err(invalid(format!("{label} path contains a reparse point")));
        }
        if !metadata.is_dir() {
            return Err(invalid(format!(
                "{label} path component is not a directory"
            )));
        }
    }
    Ok(path.canonicalize()?)
}

fn validate_authority_content(
    value: &Value,
    interpreter: &Path,
    script: &Path,
    source_root: &Path,
    environment: &Value,
) -> Result<()> {
    let content = require_object(value, "frozen Dashboard authority content")?;
    let required = [
        "schemaVersion",
        "interpreterPath",
        "interpreterSha256",
        "interpreterVersion",
        "validatorScriptPath",
        "validatorScriptSha256",
        "dashboardSourceRoot",
        "dashboardSourceGitCommit",
        "dashboardSourceDirtyProvenance",
        "dashboardTemporalGraphContentManifest",
        "dashboardTemporalGraphContentSha256",
        "environment",
        "jsonlProtocol",
        "validateOperation",
        "compileOperation",
        "validateRequestSchema",
        "compileRequestSchema",
        "compileResponseSchema",
    ];
    exact_keys(content, &required, "frozen Dashboard authority content")?;
    if require_string(
        content,
        "schemaVersion",
        "frozen Dashboard authority content",
    )? != "temporal_qd_pair_native_authority_content_v1"
    {
        return Err(invalid(
            "frozen Dashboard authority content bindings drifted",
        ));
    }
    validate_frozen_authority_content_paths(
        &require_string(
            content,
            "interpreterPath",
            "frozen Dashboard authority content",
        )?,
        &require_string(
            content,
            "validatorScriptPath",
            "frozen Dashboard authority content",
        )?,
        &require_string(
            content,
            "dashboardSourceRoot",
            "frozen Dashboard authority content",
        )?,
        interpreter,
        script,
        source_root,
    )?;
    if content.get("environment") != Some(environment) {
        return Err(invalid(
            "frozen Dashboard authority content bindings drifted",
        ));
    }
    if require_sha256(
        content,
        "interpreterSha256",
        "frozen Dashboard authority content",
    )? != sha256_file(interpreter, "Dashboard interpreter")?
        || require_sha256(
            content,
            "validatorScriptSha256",
            "frozen Dashboard authority content",
        )? != sha256_file(script, "Dashboard validator script")?
    {
        return Err(invalid("frozen Dashboard authority file identity drifted"));
    }
    for (key, expected) in [
        (
            "jsonlProtocol",
            "temporal_search_candidate_validation_jsonl_v1",
        ),
        ("validateOperation", "validate_candidate"),
        ("compileOperation", "compile_bidirectional"),
        (
            "validateRequestSchema",
            "temporal_search_candidate_validation_jsonl_request_v1",
        ),
        (
            "compileRequestSchema",
            "temporal_search_bidirectional_compile_jsonl_request_v1",
        ),
        (
            "compileResponseSchema",
            "temporal_search_bidirectional_compile_jsonl_response_v1",
        ),
    ] {
        if require_string(content, key, "frozen Dashboard authority content")? != expected {
            return Err(invalid("frozen Dashboard authority protocol drifted"));
        }
    }
    let manifest = content
        .get("dashboardTemporalGraphContentManifest")
        .ok_or_else(|| invalid("frozen Dashboard authority lacks source manifest"))?;
    let manifest_fields = require_object(manifest, "frozen Dashboard temporal graph manifest")?;
    exact_keys(
        manifest_fields,
        &["schemaVersion", "files"],
        "frozen Dashboard temporal graph manifest",
    )?;
    if require_string(
        manifest_fields,
        "schemaVersion",
        "frozen Dashboard temporal graph manifest",
    )? != "temporal_qd_pair_dashboard_source_manifest_v1"
        || canonical_sha256(manifest)?
            != require_sha256(
                content,
                "dashboardTemporalGraphContentSha256",
                "frozen Dashboard authority content",
            )?
    {
        return Err(invalid(
            "frozen Dashboard temporal graph manifest identity drifted",
        ));
    }
    let files = manifest_fields
        .get("files")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("frozen Dashboard source manifest files must be an array"))?;
    let mut observed = BTreeMap::new();
    for row in files {
        let row = require_object(row, "frozen Dashboard source manifest file")?;
        exact_keys(
            row,
            &["path", "sha256"],
            "frozen Dashboard source manifest file",
        )?;
        let relative = require_string(row, "path", "frozen Dashboard source manifest file")?;
        let path = resolve_frozen_source_manifest_file(source_root, &relative)?;
        let supplied = require_sha256(row, "sha256", "frozen Dashboard source manifest file")?;
        if sha256_file(&path, "Dashboard source manifest file")? != supplied
            || observed.insert(path, ()).is_some()
        {
            return Err(invalid(
                "frozen Dashboard source manifest file identity drifted",
            ));
        }
    }
    Ok(())
}

/// Resolve a frozen source-manifest filename without changing the raw string
/// whose enclosing manifest is identity-hashed.  Python can freeze Windows
/// paths using backslashes; we normalize only for filesystem lookup and admit
/// no platform-specific path syntax beyond ordinary relative components.
fn resolve_frozen_source_manifest_file(source_root: &Path, raw_relative: &str) -> Result<PathBuf> {
    if raw_relative.contains(':') {
        return Err(invalid("frozen Dashboard source manifest path is unsafe"));
    }
    let parts = raw_relative.split(['/', '\\']).collect::<Vec<_>>();
    if parts.is_empty()
        || parts
            .iter()
            .any(|part| part.is_empty() || matches!(*part, "." | ".."))
    {
        return Err(invalid("frozen Dashboard source manifest path is unsafe"));
    }
    let relative = parts.iter().fold(PathBuf::new(), |mut path, part| {
        path.push(part);
        path
    });
    let path = absolute_regular_path(&source_root.join(relative), "Dashboard manifest file")?;
    if !path.starts_with(source_root) {
        return Err(invalid("frozen Dashboard source manifest escaped root"));
    }
    Ok(path)
}

/// Bind the path fields repeated inside `authorityContent` to the already
/// verified outer authority paths.  Every comparison is between canonical
/// paths after the same symlink/reparse-safe resolution used on the outer
/// fields, so alternate Windows spellings cannot cause false drift.
fn validate_frozen_authority_content_paths(
    content_interpreter: &str,
    content_script: &str,
    content_source_root: &str,
    interpreter: &Path,
    script: &Path,
    source_root: &Path,
) -> Result<()> {
    let content_interpreter = absolute_regular_path(
        Path::new(content_interpreter),
        "Dashboard authority content interpreter",
    )?;
    let content_script = absolute_regular_path(
        Path::new(content_script),
        "Dashboard authority content validator script",
    )?;
    let content_source_root = absolute_directory_path(
        Path::new(content_source_root),
        "Dashboard authority content source root",
    )?;
    if content_interpreter != interpreter
        || content_script != script
        || content_source_root != source_root
    {
        return Err(invalid(
            "frozen Dashboard authority content bindings drifted",
        ));
    }
    Ok(())
}

struct NativeDashboardAdapter<'a, D>(&'a D);

impl<D: DashboardPort> NativeValidator for NativeDashboardAdapter<'_, D> {
    fn validate_v2(
        &self,
        profile: &Value,
        candidate_id: &str,
    ) -> std::result::Result<Value, GrammarError> {
        self.0
            .validate_v2(profile, candidate_id)
            .map_err(|error| match error {
                RuntimeError::SemanticRejection(message) => {
                    GrammarError::Invalid(semantic_rejection_marker(message))
                }
                other => GrammarError::Invalid(other.to_string()),
            })
    }
}

impl<D: DashboardPort> NativeModuleValidator for NativeDashboardAdapter<'_, D> {
    fn validate_v2(
        &self,
        profile: &Value,
        candidate_id: &str,
    ) -> std::result::Result<Value, temporal_qd_kernel::genome::GenomeError> {
        self.0
            .validate_v2(profile, candidate_id)
            .map_err(|error| match error {
                RuntimeError::SemanticRejection(message) => {
                    temporal_qd_kernel::genome::GenomeError::Invalid(semantic_rejection_marker(
                        message,
                    ))
                }
                other => temporal_qd_kernel::genome::GenomeError::Invalid(other.to_string()),
            })
    }
}

impl<D: DashboardPort> CanonicalPairCompiler for NativeDashboardAdapter<'_, D> {
    fn compile_pair(
        &self,
        long_profile: &Value,
        short_profile: &Value,
        candidate_id: &str,
    ) -> std::result::Result<Value, temporal_qd_kernel::genome::GenomeError> {
        self.0
            .compile_bidirectional(long_profile, short_profile, candidate_id)
            .map_err(|error| temporal_qd_kernel::genome::GenomeError::Invalid(error.to_string()))
    }
}

#[derive(Clone, Debug)]
struct FrozenSideRuntime {
    direction: &'static str,
    seed_names: Vec<String>,
    context: Value,
    catalog: Value,
    /// The indicator catalog and its eight frozen operators are immutable for
    /// the entire runtime.  Building a registry clones the catalog into each
    /// operator, so constructing one per proposal caused substantial transient
    /// allocation and allocator-retained RSS.  Keep one provenance-checked
    /// registry per frozen side and borrow it through every construction and
    /// mutation path instead.
    indicator_registry: Arc<IndicatorLearningRegistry>,
    grammar_context: IdentitySnapshot,
    catalog_snapshot: IdentitySnapshot,
    immigrant_policy: IdentitySnapshot,
}

#[derive(Clone, Debug)]
struct FrozenRuntimeInputs {
    native_authority_identity: Value,
    pair_compiler_identity: IdentitySnapshot,
    hold_policy: Value,
    initial_protection_policy: Value,
    immigrant_construction_policy: Value,
    long: FrozenSideRuntime,
    short: FrozenSideRuntime,
}

impl FrozenRuntimeInputs {
    fn from_manifest(manifest: &RuntimeManifest) -> Result<Self> {
        let config = require_object(&manifest.pair_run_config, "frozen pair run config")?;
        let hold_policy = config.get("holdOperatorPolicy").expect("validated").clone();
        let initial_protection_policy = config
            .get("initialProtectionOperatorPolicy")
            .expect("validated")
            .clone();
        let immigrant_construction_policy = config
            .get("immigrantConstructionPolicy")
            .expect("validated")
            .clone();
        let native_snapshot = IdentitySnapshot::from_payload(
            config.get("nativeAuthority").expect("validated"),
            Some("nativeAuthority"),
        )
        .map_err(|error| {
            invalid(format!(
                "frozen native authority snapshot is invalid: {error}"
            ))
        })?;
        let pair_compiler_identity = IdentitySnapshot::from_payload(
            config.get("pairCompilerAuthority").expect("validated"),
            Some("pairCompiler"),
        )
        .map_err(|error| invalid(format!("frozen pair compiler snapshot is invalid: {error}")))?;
        let make_side = |direction: &'static str| -> Result<FrozenSideRuntime> {
            let side = require_object(
                config
                    .get(&format!("{direction}Module"))
                    .expect("validated"),
                "frozen pair side",
            )?;
            let seed_names = side
                .get("seedNames")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid(format!("frozen {direction} seed names are invalid")))?
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .filter(|name| matches!(*name, "mean_reversion" | "breakout" | "trend"))
                        .map(ToOwned::to_owned)
                        .ok_or_else(|| invalid(format!("frozen {direction} seed name is invalid")))
                })
                .collect::<Result<Vec<_>>>()?;
            let context = side.get("context").expect("validated").clone();
            let catalog = side.get("catalog").expect("validated").clone();
            let grammar_context = IdentitySnapshot::create(
                "grammarContext",
                "temporal_typed_grammar_context_v1",
                &context,
            )
            .map_err(|error| {
                invalid(format!(
                    "frozen {direction} grammar context snapshot is invalid: {error}"
                ))
            })?;
            let catalog_snapshot = IdentitySnapshot::create(
                "catalog",
                "temporal_indicator_learning_catalog_v1",
                &object([
                    ("catalog", catalog.clone()),
                    (
                        "catalogSha256",
                        side.get("catalogSha256").expect("validated").clone(),
                    ),
                ]),
            )
            .map_err(|error| {
                invalid(format!(
                    "frozen {direction} catalog snapshot is invalid: {error}"
                ))
            })?;
            let indicator_registry = Arc::new(
                IndicatorLearningRegistry::new(IndicatorCatalog::new(&catalog).map_err(
                    |error| invalid(format!("frozen {direction} catalog is invalid: {error}")),
                )?)
                .map_err(|error| {
                    invalid(format!(
                        "frozen {direction} indicator policy is invalid: {error}"
                    ))
                })?,
            );
            if side.get("indicatorPolicy") != Some(indicator_registry.policy()) {
                return Err(invalid(format!(
                    "frozen {direction} indicator policy drifted"
                )));
            }
            let immigrant_policy = IdentitySnapshot::create(
                "policy",
                "temporal_qd_pair_module_policy_v2",
                &object([
                    (
                        "modulePolicy",
                        side.get("policy").expect("validated").clone(),
                    ),
                    (
                        "indicatorPolicy",
                        side.get("indicatorPolicy").expect("validated").clone(),
                    ),
                    ("holdOperatorPolicy", hold_policy.clone()),
                    (
                        "initialProtectionOperatorPolicy",
                        initial_protection_policy.clone(),
                    ),
                    (
                        "immigrantConstructionPolicy",
                        immigrant_construction_policy.clone(),
                    ),
                ]),
            )
            .map_err(|error| {
                invalid(format!(
                    "frozen {direction} module policy snapshot is invalid: {error}"
                ))
            })?;
            Ok(FrozenSideRuntime {
                direction,
                seed_names,
                context,
                catalog,
                indicator_registry,
                grammar_context,
                catalog_snapshot,
                immigrant_policy,
            })
        };
        let long = make_side("long")?;
        let short = make_side("short")?;
        Ok(Self {
            native_authority_identity: native_snapshot.canonical_payload(),
            pair_compiler_identity,
            hold_policy,
            initial_protection_policy,
            immigrant_construction_policy,
            long,
            short,
        })
    }

    fn side(&self, direction: &str) -> Result<&FrozenSideRuntime> {
        match direction {
            "long" => Ok(&self.long),
            "short" => Ok(&self.short),
            _ => Err(invalid("frozen runtime direction is unknown")),
        }
    }
}

fn selector_index(seed: &str, axis: &str, size: usize) -> Result<usize> {
    if size == 0 {
        return Err(invalid(format!(
            "rich immigrant selector axis has no values: {axis}"
        )));
    }
    let upper = num_bigint::BigUint::from(1_u8) << 256_usize;
    let limit = &upper - (&upper % size);
    for attempt in 0_u64.. {
        let seed_bytes = seed.as_bytes();
        let axis_bytes = axis.as_bytes();
        let mut material = Vec::with_capacity(seed_bytes.len() + axis_bytes.len() + 16);
        material.extend_from_slice(&(seed_bytes.len() as u32).to_be_bytes());
        material.extend_from_slice(seed_bytes);
        material.extend_from_slice(&(axis_bytes.len() as u32).to_be_bytes());
        material.extend_from_slice(axis_bytes);
        material.extend_from_slice(&attempt.to_be_bytes());
        let draw = num_bigint::BigUint::from_bytes_be(&Sha256::digest(material));
        if draw < limit {
            return Ok((&draw % size)
                .try_into()
                .expect("selector remainder fits usize"));
        }
    }
    unreachable!("unbounded selector iterator cannot terminate")
}

fn selector_value<'a>(seed: &str, axis: &str, values: &'a [Value]) -> Result<&'a Value> {
    values
        .get(selector_index(seed, axis, values.len())?)
        .ok_or_else(|| invalid("rich immigrant selector index escaped finite axis"))
}

fn unbiased_choice(seed: &str, size: usize) -> Result<usize> {
    if size == 0 {
        return Err(invalid("pair selection bucket size must be positive"));
    }
    let upper = num_bigint::BigUint::from(1_u8) << 256_usize;
    let limit = &upper - (&upper % size);
    for attempt in 0_u64.. {
        let identity = canonical_sha256(&object([
            ("seed", Value::String(seed.to_owned())),
            ("attempt", Value::from(attempt)),
        ]))?;
        let draw = num_bigint::BigUint::parse_bytes(&identity.as_bytes()[7..], 16)
            .expect("canonical SHA-256 is lowercase hex");
        if draw < limit {
            return Ok((&draw % size)
                .try_into()
                .expect("selector remainder fits usize"));
        }
    }
    unreachable!("unbounded selector iterator cannot terminate")
}

fn seeded_order(values: Vec<Value>, seed: &str, axis: &str) -> Result<Vec<Value>> {
    let mut rows = values
        .into_iter()
        .map(|value| {
            Ok((
                canonical_sha256(&object([
                    (
                        "schemaVersion",
                        Value::String("temporal_qd_immigrant_seeded_order_v1".into()),
                    ),
                    ("seed", Value::String(seed.to_owned())),
                    ("axis", Value::String(axis.to_owned())),
                    ("value", value.clone()),
                ]))?,
                value,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(rows.into_iter().map(|(_, value)| value).collect())
}

fn side_seed(proposal_seed: &str, side: &str) -> Result<String> {
    Ok(canonical_sha256(&object([
        (
            "schemaVersion",
            Value::String(PAIR_IMMIGRANT_BUILDER_VERSION.into()),
        ),
        ("proposalSeed", Value::String(proposal_seed.to_owned())),
        ("side", Value::String(side.to_owned())),
    ]))?)
}

fn candidate_suffix(identity: &str) -> Result<String> {
    identity
        .strip_prefix("sha256:")
        .and_then(|value| value.get(..28))
        .map(ToOwned::to_owned)
        .ok_or_else(|| invalid("candidate identity cannot form a 28-hex identifier"))
}

/// The construction audit records the shape of evidence groups, not their
/// graph order. Keep the multiset canonical so equivalent profiles have the
/// same construction identity when their groups are declared in a different
/// order.
fn sorted_evidence_group_member_counts(groups: &[Value]) -> Vec<Value> {
    let mut counts = groups
        .iter()
        .filter_map(|group| {
            group
                .get("indicatorInstanceIds")
                .and_then(Value::as_array)
                .map(|items| items.len() as u64)
        })
        .collect::<Vec<_>>();
    counts.sort_unstable();
    counts.into_iter().map(Value::from).collect()
}

fn construction_evidence_scope() -> Result<Value> {
    let mut result = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_construction_evidence_scope_v1".into()),
        ),
        ("evidencePlanRotationRequired", Value::Bool(false)),
        ("lakeScopeRegenerationRequired", Value::Bool(false)),
        ("reasons", Value::Array(Vec::new())),
        ("timeframeMutationTraceSha256s", Value::Array(Vec::new())),
    ]);
    let hash = canonical_sha256(&result)?;
    result
        .as_object_mut()
        .expect("constructed evidence scope is an object")
        .insert("evidenceScopeSha256".into(), Value::String(hash));
    Ok(result)
}

fn bidirectional_pair_policy(pair_policy: &Value) -> Result<(Value, String)> {
    let policy = require_object(pair_policy, "bidirectional pair policy")?;
    exact_keys(
        policy,
        &["schemaVersion", "enabled", "compilerAuthority"],
        "bidirectional pair policy",
    )?;
    if require_string(policy, "schemaVersion", "bidirectional pair policy")?
        != "temporal_qd_bidirectional_pair_policy_v1"
        || policy.get("enabled") != Some(&Value::Bool(true))
    {
        return Err(invalid("bidirectional pair policy is not enabled v1"));
    }
    let compiler = IdentitySnapshot::from_payload(
        policy.get("compilerAuthority").expect("checked"),
        Some("pairCompiler"),
    )
    .map_err(|error| {
        invalid(format!(
            "bidirectional pair policy compiler is invalid: {error}"
        ))
    })?;
    let normalized = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_bidirectional_pair_policy_v1".into()),
        ),
        ("enabled", Value::Bool(true)),
        ("compilerAuthority", compiler.canonical_payload()),
    ]);
    Ok((normalized.clone(), canonical_sha256(&normalized)?))
}

/// Python-compatible `qd_canonical_evidence_identity`.  This identity is
/// deliberately derived from the frozen evaluation authority and executable
/// candidate material; it is never guessed from a proposal slot.
fn canonical_evidence_identity(candidate: &Value, evidence_context: &Value) -> Result<String> {
    let mut context = require_object(evidence_context, "predeclared evidence context")?.clone();
    if let Some(supplied) = context.remove("predeclaredEvidenceContextSha256") {
        let wrapper = object([("predeclaredEvidenceContextSha256", supplied)]);
        let supplied = require_sha256(
            require_object(&wrapper, "predeclared evidence context identity")?,
            "predeclaredEvidenceContextSha256",
            "predeclared evidence context identity",
        )?;
        if supplied != canonical_sha256(&Value::Object(context.clone()))? {
            return Err(invalid("QD predeclared evidence context diverged"));
        }
    }
    let candidate = require_object(candidate, "candidate evidence material")?;
    let program_sha256 = require_sha256(candidate, "programSha256", "candidate evidence material")?;
    let source_profile_sha256 = require_sha256(
        candidate,
        "sourceProfileSha256",
        "candidate evidence material",
    )?;
    let profile_snapshot_sha256 = match candidate.get("profileSnapshotSha256") {
        Some(Value::String(_)) => require_sha256(
            candidate,
            "profileSnapshotSha256",
            "candidate evidence material",
        )?,
        Some(_) => {
            return Err(invalid(
                "candidate evidence profileSnapshotSha256 is invalid",
            ));
        }
        None => source_profile_sha256.clone(),
    };
    let source_profile = candidate
        .get("sourceProfile")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("QD evidence source profile must be an object"))?;
    let execution_config = source_profile
        .get("executionConfig")
        .cloned()
        .unwrap_or_else(|| Value::Object(Map::new()));
    Ok(canonical_sha256(&object([
        (
            "schemaVersion",
            Value::String("temporal_qd_canonical_evidence_identity_v3".into()),
        ),
        ("programSha256", Value::String(program_sha256)),
        ("sourceProfileSha256", Value::String(source_profile_sha256)),
        (
            "profileSnapshotSha256",
            Value::String(profile_snapshot_sha256),
        ),
        (
            "orderedWindowPlanSemantic",
            context
                .get("orderedWindowPlanSemantic")
                .cloned()
                .unwrap_or(Value::Null),
        ),
        (
            "costViews",
            context.get("costViews").cloned().unwrap_or(Value::Null),
        ),
        (
            "workerContractSha256",
            context
                .get("workerContractSha256")
                .cloned()
                .unwrap_or(Value::Null),
        ),
        (
            "executionConfigSha256",
            Value::String(canonical_sha256(&execution_config)?),
        ),
    ]))?)
}

/// Rebind a verified historical archive candidate to the current frozen
/// evaluation context before it enters the runtime identity ledger.  The
/// archive's persisted identity may be valid for its source generation while
/// necessarily differing from this generation's context-bound identity.
fn rebind_archive_candidate_evidence_identity(
    candidate: &Value,
    evidence_context: &Value,
) -> Result<(Value, String)> {
    let canonical_evidence_identity_sha256 =
        canonical_evidence_identity(candidate, evidence_context)?;
    let mut rebound = require_object(candidate, "runtime parent candidate")?.clone();
    rebound.insert(
        "canonicalEvidenceIdentitySha256".into(),
        Value::String(canonical_evidence_identity_sha256.clone()),
    );
    Ok((Value::Object(rebound), canonical_evidence_identity_sha256))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LakeRequest {
    pairs: Vec<String>,
    timeframes: Vec<String>,
    data_start: String,
    data_end: String,
    coverage_policy: String,
}

fn lake_string_list(value: Option<&Value>, label: &str) -> std::result::Result<Vec<String>, ()> {
    let values = value.and_then(Value::as_array).ok_or(())?;
    let rows = values
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(str::to_ascii_uppercase)
                .ok_or(())
        })
        .collect::<std::result::Result<BTreeSet<_>, _>>()?;
    if rows.is_empty() {
        return Err(());
    }
    let _ = label;
    Ok(rows.into_iter().collect())
}

fn parse_scope_timestamp(value: &str) -> std::result::Result<OffsetDateTime, ()> {
    OffsetDateTime::parse(value, &Rfc3339)
        .map(|value| value.to_offset(time::UtcOffset::UTC))
        .map_err(|_| ())
}

fn canonical_scope_timestamp(value: OffsetDateTime) -> std::result::Result<String, ()> {
    value
        .to_offset(time::UtcOffset::UTC)
        .format(&Rfc3339)
        .map_err(|_| ())
}

fn parse_lake_request(value: &Value) -> std::result::Result<LakeRequest, ()> {
    let fields = value.as_object().ok_or(())?;
    if fields.keys().any(|key| {
        !matches!(
            key.as_str(),
            "schema_version"
                | "dataset"
                | "pairs"
                | "timeframes"
                | "data_start"
                | "data_end"
                | "coverage_policy"
        )
    }) {
        return Err(());
    }
    if fields
        .get("schema_version")
        .is_some_and(|value| value.as_str() != Some("fuzzfolio.market-data-window-request.v1"))
        || fields
            .get("dataset")
            .is_some_and(|value| value.as_str() != Some("bars"))
    {
        return Err(());
    }
    let coverage_policy = fields
        .get("coverage_policy")
        .and_then(Value::as_str)
        .unwrap_or("require_complete");
    if !matches!(coverage_policy, "require_complete" | "allow_truncated") {
        return Err(());
    }
    let pairs = lake_string_list(fields.get("pairs"), "lake request pairs")?;
    let timeframes = lake_string_list(fields.get("timeframes"), "lake request timeframes")?;
    let start = fields.get("data_start").and_then(Value::as_str).ok_or(())?;
    let end = fields.get("data_end").and_then(Value::as_str).ok_or(())?;
    let start = parse_scope_timestamp(start)?;
    let end = parse_scope_timestamp(end)?;
    if end.time() != Time::MIDNIGHT || end.microsecond() != 0 || start >= end {
        return Err(());
    }
    Ok(LakeRequest {
        pairs,
        timeframes,
        data_start: canonical_scope_timestamp(start)?,
        data_end: canonical_scope_timestamp(end)?,
        coverage_policy: coverage_policy.to_owned(),
    })
}

fn lake_request_payload(request: &LakeRequest) -> Value {
    object([
        (
            "schema_version",
            Value::String("fuzzfolio.market-data-window-request.v1".into()),
        ),
        ("dataset", Value::String("bars".into())),
        (
            "pairs",
            Value::Array(request.pairs.iter().cloned().map(Value::String).collect()),
        ),
        (
            "timeframes",
            Value::Array(
                request
                    .timeframes
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        ),
        ("data_start", Value::String(request.data_start.clone())),
        ("data_end", Value::String(request.data_end.clone())),
        (
            "coverage_policy",
            Value::String(request.coverage_policy.clone()),
        ),
    ])
}

fn lake_request_contains(frozen: &LakeRequest, derived: &LakeRequest) -> bool {
    frozen.pairs == derived.pairs
        && frozen.coverage_policy == derived.coverage_policy
        && frozen.data_end == derived.data_end
        && derived
            .timeframes
            .iter()
            .all(|timeframe| frozen.timeframes.contains(timeframe))
        && matches!(
            (
                parse_scope_timestamp(&frozen.data_start),
                parse_scope_timestamp(&derived.data_start),
            ),
            (Ok(frozen_start), Ok(derived_start)) if frozen_start <= derived_start
        )
}

fn deep_merge_catalog_config(base: &Value, overlay: &Value) -> Value {
    match (base.as_object(), overlay.as_object()) {
        (Some(base), Some(overlay)) => {
            let mut merged = base.clone();
            for (key, value) in overlay {
                let next = merged
                    .get(key)
                    .map(|current| deep_merge_catalog_config(current, value))
                    .unwrap_or_else(|| value.clone());
                merged.insert(key.clone(), next);
            }
            Value::Object(merged)
        }
        _ => overlay.clone(),
    }
}

fn integer_like(value: Option<&Value>) -> std::result::Result<i64, ()> {
    match value {
        Some(value) if value.as_i64().is_some() => Ok(value.as_i64().expect("checked")),
        Some(value) if value.as_u64().is_some() => value
            .as_u64()
            .and_then(|value| value.try_into().ok())
            .ok_or(()),
        Some(value) if value.as_f64().is_some() => Ok(value.as_f64().expect("checked") as i64),
        Some(Value::String(value)) => value.trim().parse::<i64>().map_err(|_| ()),
        _ => Err(()),
    }
}

fn timeframe_minutes(timeframe: &str) -> i64 {
    let timeframe = timeframe.trim().to_ascii_uppercase();
    let numeric = |suffix: &str, fallback: i64| {
        timeframe
            .strip_prefix(suffix)
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(fallback)
    };
    if timeframe.starts_with('M') {
        numeric("M", 5)
    } else if timeframe.starts_with('H') {
        numeric("H", 5) * 60
    } else if timeframe.starts_with('D') {
        numeric("D", 1) * 1440
    } else {
        5
    }
}

fn hydrated_scope_profile(
    profile: &Value,
    catalog: Option<&Value>,
) -> std::result::Result<Value, ()> {
    let mut profile = profile.as_object().ok_or(())?.clone();
    let Some(indicators) = profile.get("indicators").cloned() else {
        return Ok(Value::Object(profile));
    };
    let indicators = indicators.as_array().ok_or(())?;
    if indicators.is_empty() {
        return Ok(Value::Object(profile));
    }
    let catalog = catalog.ok_or(())?.as_object().ok_or(())?;
    let catalog_timeframes = catalog
        .get("timeframes")
        .and_then(Value::as_object)
        .ok_or(())?
        .keys()
        .map(|value| value.trim().to_ascii_uppercase())
        .filter(|value| !value.is_empty())
        .collect::<BTreeSet<_>>();
    if catalog_timeframes.is_empty() {
        return Err(());
    }
    let mut catalog_items = BTreeMap::new();
    for item in catalog
        .get("indicators")
        .and_then(Value::as_array)
        .ok_or(())?
    {
        let item = item.as_object().ok_or(())?;
        let meta = item.get("meta").and_then(Value::as_object).ok_or(())?;
        let config = item.get("config").and_then(Value::as_object).ok_or(())?;
        let id = meta
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .ok_or(())?;
        if catalog_items
            .insert(
                id.to_owned(),
                (Value::Object(meta.clone()), Value::Object(config.clone())),
            )
            .is_some()
        {
            return Err(());
        }
    }
    if catalog_items.is_empty() {
        return Err(());
    }
    let mut resolved = Vec::new();
    for indicator in indicators {
        let indicator = indicator.as_object().ok_or(())?;
        let authored_meta = indicator.get("meta").and_then(Value::as_object).ok_or(())?;
        let authored_config = indicator
            .get("config")
            .and_then(Value::as_object)
            .ok_or(())?;
        let id = authored_meta
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .ok_or(())?;
        let (catalog_meta, catalog_config) = catalog_items.get(id).ok_or(())?;
        let catalog_meta_map = catalog_meta.as_object().expect("catalog meta object");
        for (key, value) in authored_meta {
            if matches!(key.as_str(), "instanceId" | "instance_id") {
                continue;
            }
            if catalog_meta_map.get(key) != Some(value) {
                return Err(());
            }
        }
        let required_padding = integer_like(catalog_meta_map.get("requiredPaddingBars"))?;
        if required_padding < 0 {
            return Err(());
        }
        let mut config =
            deep_merge_catalog_config(catalog_config, &Value::Object(authored_config.clone()));
        config
            .as_object_mut()
            .expect("merged config object")
            .insert("isActive".into(), Value::Bool(true));
        let timeframe = config
            .get("timeframe")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or(())?
            .to_ascii_uppercase();
        if !catalog_timeframes.contains(&timeframe)
            || integer_like(config.get("lookbackBars")).is_err()
            || integer_like(config.get("lookbackBars"))? < 0
        {
            return Err(());
        }
        let mut meta = catalog_meta_map.clone();
        meta.insert("requiredPaddingBars".into(), Value::from(required_padding));
        if let Some(instance_id) = authored_meta
            .get("instanceId")
            .or_else(|| authored_meta.get("instance_id"))
        {
            meta.insert("instanceId".into(), instance_id.clone());
        }
        resolved.push(object([("meta", Value::Object(meta)), ("config", config)]));
    }
    profile.insert("indicators".into(), Value::Array(resolved));
    Ok(Value::Object(profile))
}

fn required_lake_request(
    profile: &Value,
    base_timeframe: &str,
    analysis_window_start: &str,
    analysis_window_end: &str,
    catalog: Option<&Value>,
) -> std::result::Result<LakeRequest, ()> {
    let base_timeframe = base_timeframe.trim().to_ascii_uppercase();
    if base_timeframe.is_empty() {
        return Err(());
    }
    let profile = if catalog.is_some() {
        hydrated_scope_profile(profile, catalog)?
    } else {
        profile.clone()
    };
    let profile_map = profile.as_object().ok_or(())?;
    let pairs = lake_string_list(profile_map.get("instruments"), "profile instruments")?;
    let indicators = profile_map
        .get("indicators")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut timeframes = vec![base_timeframe.clone()];
    let mut warmup_minutes = 0_i64;
    for indicator in indicators {
        let indicator = indicator.as_object().ok_or(())?;
        let empty_config = Map::new();
        let empty_meta = Map::new();
        let config = indicator
            .get("config")
            .and_then(Value::as_object)
            .unwrap_or(&empty_config);
        let meta = indicator
            .get("meta")
            .and_then(Value::as_object)
            .unwrap_or(&empty_meta);
        if config.get("isActive").and_then(Value::as_bool) == Some(false) {
            continue;
        }
        let timeframe = config
            .get("timeframe")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("")
            .to_ascii_uppercase();
        if timeframe.is_empty() {
            continue;
        }
        timeframes.push(timeframe.clone());
        let padding = match meta.get("requiredPaddingBars") {
            Some(value) => integer_like(Some(value))?,
            None => 0,
        };
        let lookback = match config.get("lookbackBars") {
            Some(value) => integer_like(Some(value))?,
            None => 1,
        };
        warmup_minutes =
            warmup_minutes.max((padding + lookback + 10).max(1) * timeframe_minutes(&timeframe));
    }
    let start = parse_scope_timestamp(analysis_window_start)?;
    let end = parse_scope_timestamp(analysis_window_end)?;
    if end.time() != Time::MIDNIGHT || end.microsecond() != 0 {
        return Err(());
    }
    let raw_start = start - TimeDuration::minutes(warmup_minutes);
    let data_start = Date::from_calendar_date(raw_start.year(), raw_start.month(), raw_start.day())
        .map_err(|_| ())?
        .with_time(Time::MIDNIGHT)
        .assume_utc();
    let mut timeframes = timeframes
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if timeframes.is_empty() {
        return Err(());
    }
    Ok(LakeRequest {
        pairs,
        timeframes: std::mem::take(&mut timeframes),
        data_start: canonical_scope_timestamp(data_start)?,
        data_end: canonical_scope_timestamp(end)?,
        coverage_policy: "require_complete".into(),
    })
}

/// Exact pair-generation admission report for pre-attested evidence scope.
/// It verifies containment only; it never creates a lake identity or calls an
/// evaluator.
fn pair_predeclared_lake_scope_report(
    candidate: &Value,
    evidence_context: Option<&Value>,
    frozen_construction_catalog: Option<&Value>,
) -> Result<Option<Value>> {
    let Some(context) = evidence_context.and_then(Value::as_object) else {
        return Ok(None);
    };
    let Some(windows) = context
        .get("orderedWindowPlanSemantic")
        .and_then(Value::as_array)
    else {
        return Ok(None);
    };
    if windows.is_empty() {
        return Ok(None);
    }
    let base_timeframe = context
        .get("baseDecisionTimeframe")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_ascii_uppercase();
    let profile = candidate.get("sourceProfile").and_then(Value::as_object);
    if base_timeframe.is_empty()
        || profile.is_none()
        || profile
            .and_then(|profile| profile.get("instruments"))
            .and_then(Value::as_array)
            .is_none_or(Vec::is_empty)
    {
        return Ok(Some(object([
            ("acceptable", Value::Bool(false)),
            (
                "reason",
                Value::String("predeclared_lake_scope_context_incomplete".into()),
            ),
            ("windows", Value::Array(Vec::new())),
        ])));
    }
    let profile = candidate.get("sourceProfile").expect("checked profile");
    let mut reports = Vec::new();
    for raw_window in windows {
        let malformed = || {
            Ok(Some(object([
                ("acceptable", Value::Bool(false)),
                (
                    "reason",
                    Value::String("predeclared_lake_scope_context_malformed".into()),
                ),
                ("windows", Value::Array(reports.clone())),
            ])))
        };
        let Some(raw_window) = raw_window.as_object() else {
            return malformed();
        };
        let Some(window_id) = raw_window
            .get("windowId")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        else {
            return malformed();
        };
        let Some(window) = raw_window.get("window").and_then(Value::as_object) else {
            return malformed();
        };
        let Some(plan) = raw_window
            .get("evidencePlanSemantic")
            .and_then(Value::as_object)
        else {
            return malformed();
        };
        let Some(binding) = plan.get("lake_window_binding").and_then(Value::as_object) else {
            return malformed();
        };
        if binding.keys().any(|key| {
            !matches!(
                key.as_str(),
                "schema_version"
                    | "request"
                    | "window_semantic_sha256"
                    | "semantic_contract_id"
                    | "attestation_sha256"
                    | "creation_global_coverage_sha256"
                    | "creation_source_coverage_sha256"
                    | "legacy_selection_manifest_sha256"
            )
        }) || binding
            .get("schema_version")
            .is_some_and(|value| value.as_str() != Some("fuzzfolio.market-data-window-binding.v1"))
            || binding.get("semantic_contract_id").is_some_and(|value| {
                value.as_str() != Some("fuzzfolio.canonical-bars.semantic-digest.v2")
            })
        {
            return malformed();
        }
        for key in [
            "attestation_sha256",
            "creation_global_coverage_sha256",
            "creation_source_coverage_sha256",
            "legacy_selection_manifest_sha256",
        ] {
            let Some(value) = binding.get(key) else {
                continue;
            };
            if value.is_null() {
                continue;
            }
            let Some(value) = value.as_str() else {
                return malformed();
            };
            if value.trim().is_empty() {
                continue;
            }
            let wrapper = object([("hash", Value::String(value.to_owned()))]);
            if require_sha256(
                require_object(&wrapper, "lake binding hash")?,
                "hash",
                "lake binding hash",
            )
            .is_err()
            {
                return malformed();
            }
        }
        let Some(request) = binding.get("request") else {
            return malformed();
        };
        let Some(window_semantic) = binding
            .get("window_semantic_sha256")
            .and_then(Value::as_str)
        else {
            return malformed();
        };
        let wrapper = object([("hash", Value::String(window_semantic.to_owned()))]);
        if require_sha256(
            require_object(&wrapper, "window semantic")?,
            "hash",
            "window semantic",
        )
        .is_err()
        {
            return malformed();
        }
        let Some(start) = window.get("analysisWindowStart").and_then(Value::as_str) else {
            return malformed();
        };
        let Some(end) = window.get("analysisWindowEnd").and_then(Value::as_str) else {
            return malformed();
        };
        let Ok(frozen_request) = parse_lake_request(request) else {
            return malformed();
        };
        let Ok(required) = required_lake_request(
            profile,
            &base_timeframe,
            start,
            end,
            frozen_construction_catalog,
        ) else {
            return malformed();
        };
        let contained = lake_request_contains(&frozen_request, &required);
        reports.push(object([
            ("windowId", Value::String(window_id.to_owned())),
            ("contained", Value::Bool(contained)),
            ("requiredRequest", lake_request_payload(&required)),
            (
                "frozenWindowSemanticSha256",
                Value::String(window_semantic.to_owned()),
            ),
        ]));
        if !contained {
            return Ok(Some(object([
                ("acceptable", Value::Bool(false)),
                (
                    "reason",
                    Value::String("candidate_derived_request_outside_pre_attested_scope".into()),
                ),
                ("windows", Value::Array(reports)),
            ])));
        }
    }
    Ok(Some(object([
        ("acceptable", Value::Bool(true)),
        ("reason", Value::Null),
        ("windows", Value::Array(reports)),
    ])))
}

const RICH_IMMIGRANT_MODULE_CONSTRUCTION_SCHEMA: &str =
    "temporal_qd_rich_immigrant_module_construction_v1";
const RICH_IMMIGRANT_PAIR_CONSTRUCTION_SCHEMA: &str =
    "temporal_qd_rich_immigrant_pair_construction_v1";

/// Reconstruct the exact construction-audit sides which Python records on a
/// materialized rich-immigrant proposal.  The audits are already frozen into
/// each module's lineage; this projection makes them proposal material before
/// its identity is calculated instead of creating a second source of truth.
fn rich_immigrant_construction_sides(pair: &FrozenPair) -> Result<Value> {
    let mut sides = Map::new();
    for module in [&pair.long, &pair.short] {
        let direction = module.direction.as_str();
        if !matches!(direction, "long" | "short") || sides.contains_key(direction) {
            return Err(invalid("rich immigrant pair has invalid module sides"));
        }
        let construction = module
            .lineage
            .iter()
            .rev()
            .find(|row| {
                row.get("operation").and_then(Value::as_str) == Some("rich_immigrant_construction")
            })
            .and_then(|row| row.get("audit"))
            .ok_or_else(|| invalid("rich immigrant pair lacks a module construction audit"))?;
        let construction_fields =
            require_object(construction, "rich immigrant module construction audit")?;
        if require_string(
            construction_fields,
            "schemaVersion",
            "rich immigrant module construction audit",
        )? != RICH_IMMIGRANT_MODULE_CONSTRUCTION_SCHEMA
        {
            return Err(invalid(
                "rich immigrant module construction audit schema drifted",
            ));
        }
        if require_string(
            construction_fields,
            "side",
            "rich immigrant module construction audit",
        )? != direction
        {
            return Err(invalid(
                "rich immigrant module construction audit side drifted",
            ));
        }
        let audit_sha = require_sha256(
            construction_fields,
            "auditSha256",
            "rich immigrant module construction audit",
        )?;
        if canonical_sha256(&value_without(construction, "auditSha256")?)? != audit_sha {
            return Err(invalid(
                "rich immigrant module construction audit identity drifted",
            ));
        }
        sides.insert(direction.to_owned(), construction.clone());
    }
    if sides.len() != 2 || !sides.contains_key("long") || !sides.contains_key("short") {
        return Err(invalid(
            "rich immigrant pair must have long and short construction audits",
        ));
    }
    Ok(Value::Object(sides))
}

fn rich_immigrant_factory_construction_audit(pair: &FrozenPair) -> Result<Value> {
    let pair_identity = pair
        .identity_sha256()
        .map_err(|error| invalid(error.to_string()))?;
    let mut audit = object([
        (
            "schemaVersion",
            Value::String(RICH_IMMIGRANT_PAIR_CONSTRUCTION_SCHEMA.into()),
        ),
        ("pairIdentitySha256", Value::String(pair_identity)),
        ("sides", rich_immigrant_construction_sides(pair)?),
    ]);
    let audit_sha = canonical_sha256(&audit)?;
    audit
        .as_object_mut()
        .expect("factory construction audit object")
        .insert("auditSha256".into(), Value::String(audit_sha));
    Ok(audit)
}

fn validate_rich_immigrant_factory_construction_audit(
    audit: &Value,
    pair: &FrozenPair,
) -> Result<()> {
    let fields = require_object(audit, "rich immigrant pair construction audit")?;
    exact_keys(
        fields,
        &[
            "auditSha256",
            "pairIdentitySha256",
            "schemaVersion",
            "sides",
        ],
        "rich immigrant pair construction audit",
    )?;
    if require_string(
        fields,
        "schemaVersion",
        "rich immigrant pair construction audit",
    )? != RICH_IMMIGRANT_PAIR_CONSTRUCTION_SCHEMA
    {
        return Err(invalid(
            "rich immigrant pair construction audit schema drifted",
        ));
    }
    let pair_identity = pair
        .identity_sha256()
        .map_err(|error| invalid(error.to_string()))?;
    if require_sha256(
        fields,
        "pairIdentitySha256",
        "rich immigrant pair construction audit",
    )? != pair_identity
    {
        return Err(invalid(
            "rich immigrant pair construction audit pair identity drifted",
        ));
    }
    let audit_sha = require_sha256(
        fields,
        "auditSha256",
        "rich immigrant pair construction audit",
    )?;
    if canonical_sha256(&value_without(audit, "auditSha256")?)? != audit_sha {
        return Err(invalid(
            "rich immigrant pair construction audit identity drifted",
        ));
    }
    if fields.get("sides") != Some(&rich_immigrant_construction_sides(pair)?) {
        return Err(invalid(
            "rich immigrant pair construction audit sides drifted from module lineage",
        ));
    }
    Ok(())
}

fn materialize_pair_candidate(
    pair: &FrozenPair,
    proposal: &Value,
    context: &NativeConstructionContext,
) -> Result<Value> {
    let proposal_fields = require_object(proposal, "materialized pair proposal")?;
    let proposal_sha = require_sha256(
        proposal_fields,
        "proposalSha256",
        "materialized pair proposal",
    )?;
    if canonical_sha256(&value_without(proposal, "proposalSha256")?)? != proposal_sha
        || require_string(proposal_fields, "disposition", "materialized pair proposal")?
            != "materialized"
    {
        return Err(invalid(
            "only an exact materialized pair proposal can enter QD",
        ));
    }
    let supplied_pair_payload = proposal_fields
        .get("pair")
        .or_else(|| proposal_fields.get("factoryPair"))
        .ok_or_else(|| invalid("materialized pair proposal lacks frozen pair"))?;
    // A materialized proposal and its candidate intentionally publish two
    // independent copies of the frozen pair.  That duplication is part of
    // the Python compatibility contract, but reconstructing a third
    // `FrozenPair` solely to validate the proposal was not.  In particular,
    // `FrozenPair::from_payload` deep-cloned every profile, report, and
    // lineage object before we immediately serialized the authoritative pair
    // again below.  Produce that one canonical payload once, compare it
    // structurally (a stronger check than matching only the identity), then
    // move its required public copy into the candidate.
    let canonical_pair_payload = pair
        .canonical_payload()
        .map_err(|error| invalid(error.to_string()))?;
    if supplied_pair_payload != &canonical_pair_payload {
        return Err(invalid("materialized pair proposal payload drifted"));
    }
    let canonical_pair_fields = require_object(&canonical_pair_payload, "canonical frozen pair")?;
    let compiled_identities = require_object(
        canonical_pair_fields
            .get("identities")
            .ok_or_else(|| invalid("canonical frozen pair lacks identities"))?,
        "canonical frozen pair identities",
    )?;
    // `canonical_payload` has just generated and self-bound these values from
    // the authoritative pair. Reusing them avoids rebuilding the same nested
    // module identity materials (which include lineage) three more times.
    let pair_identity = require_sha256(
        compiled_identities,
        "pairIdentitySha256",
        "canonical frozen pair identities",
    )?;
    let module_identity = |side: &str| -> Result<String> {
        let module = require_object(
            canonical_pair_fields
                .get(side)
                .ok_or_else(|| invalid(format!("canonical frozen pair lacks {side} module")))?,
            "canonical frozen pair module",
        )?;
        let identities = require_object(
            module
                .get("identities")
                .ok_or_else(|| invalid("canonical frozen module lacks identities"))?,
            "canonical frozen module identities",
        )?;
        require_sha256(
            identities,
            "moduleIdentitySha256",
            "canonical frozen module identities",
        )
    };
    let long_module_identity = module_identity("long")?;
    let short_module_identity = module_identity("short")?;
    if require_sha256(
        proposal_fields,
        "pairIdentitySha256",
        "materialized pair proposal",
    )? != pair_identity
    {
        return Err(invalid("materialized pair proposal identity drifted"));
    }
    let (policy, policy_sha256) = bidirectional_pair_policy(&context.pair_policy)?;
    if pair.pair_compiler.canonical_payload() != policy["compilerAuthority"] {
        return Err(invalid(
            "bidirectional pair compiler authority does not match policy",
        ));
    }
    let origin_kind = require_string(proposal_fields, "originKind", "materialized pair proposal")?;
    if !matches!(
        origin_kind.as_str(),
        "random_immigrant" | "structural_offspring"
    ) {
        return Err(invalid("bidirectional QD origin kind is unknown"));
    }
    if let Some(audit) = proposal_fields.get("factoryConstructionAudit") {
        validate_rich_immigrant_factory_construction_audit(audit, pair)?;
    }
    // The pair has already validated this lineage while compiling.  Borrow
    // the authoritative vector rather than walking the freshly-cloned public
    // payload to recover it.
    let lineage = pair.side_targeted_lineage.clone();
    let material = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_bidirectional_candidate_identity_v1".into()),
        ),
        (
            "qdEngineVersion",
            Value::String("temporal_qd_evolution_v3".into()),
        ),
        ("originKind", Value::String(origin_kind.clone())),
        (
            "bidirectionalGenomeIdentitySha256",
            Value::String(pair_identity.clone()),
        ),
        ("pairPolicySha256", Value::String(policy_sha256)),
        (
            "longModuleIdentitySha256",
            Value::String(long_module_identity),
        ),
        (
            "shortModuleIdentitySha256",
            Value::String(short_module_identity),
        ),
        (
            "longGrammarContextSha256",
            Value::String(pair.long.grammar_context.sha256.clone()),
        ),
        (
            "shortGrammarContextSha256",
            Value::String(pair.short.grammar_context.sha256.clone()),
        ),
        (
            "longCatalogSha256",
            Value::String(pair.long.catalog.sha256.clone()),
        ),
        (
            "shortCatalogSha256",
            Value::String(pair.short.catalog.sha256.clone()),
        ),
        (
            "longPolicySha256",
            Value::String(pair.long.policy.sha256.clone()),
        ),
        (
            "shortPolicySha256",
            Value::String(pair.short.policy.sha256.clone()),
        ),
        (
            "longNativeAuthoritySha256",
            Value::String(pair.long.native_authority.sha256.clone()),
        ),
        (
            "shortNativeAuthoritySha256",
            Value::String(pair.short.native_authority.sha256.clone()),
        ),
        (
            "pairCompilerAuthoritySha256",
            Value::String(pair.pair_compiler.sha256.clone()),
        ),
        (
            "compiledRawPairSha256",
            Value::String(pair.raw_pair_sha256.clone()),
        ),
        (
            "compiledProfileSha256",
            Value::String(pair.profile_sha256.clone()),
        ),
        (
            "compiledProgramSha256",
            Value::String(pair.native_program_sha256.clone()),
        ),
        (
            "compiledValidationReportSha256",
            Value::String(pair.native_validation_report_sha256.clone()),
        ),
        ("orderedSideLineage", Value::Array(lineage.clone())),
        (
            "materializedPairProposalSha256",
            Value::String(proposal_sha.clone()),
        ),
    ]);
    let candidate_identity = canonical_sha256(&material)?;
    let candidate_id = format!("qd_{}", candidate_suffix(&candidate_identity)?);
    let mut candidate = object([
        ("candidateId", Value::String(candidate_id.clone())),
        (
            "sourceMode",
            Value::String(format!("qd_{origin_kind}_bidirectional_pair")),
        ),
        ("seedId", Value::String("bidirectional_pair".into())),
        ("generationIndex", Value::from(context.generation_index)),
        ("birthOrdinal", Value::from(context.birth_ordinal)),
        ("proposalOrdinal", Value::from(context.proposal_ordinal)),
        ("sourceProfile", pair.profile.clone()),
        (
            "sourceProfileSha256",
            Value::String(pair.raw_pair_sha256.clone()),
        ),
        (
            "profileSnapshotSha256",
            Value::String(pair.profile_sha256.clone()),
        ),
        (
            "programSha256",
            Value::String(pair.native_program_sha256.clone()),
        ),
        (
            "validationReportSha256",
            Value::String(pair.native_validation_report_sha256.clone()),
        ),
        ("candidateIdentityMaterial", material),
        (
            "candidateIdentitySha256",
            Value::String(candidate_identity.clone()),
        ),
        ("structuralDepth", Value::from(lineage.len() as u64)),
        ("structuralOperatorHistory", Value::Array(lineage.clone())),
        ("mutationTrace", Value::Array(Vec::new())),
        ("activationAwareRepairs", Value::Array(Vec::new())),
        ("constructionEvidenceScope", construction_evidence_scope()?),
        ("bidirectionalGenome", canonical_pair_payload),
        (
            "lineage",
            object([
                (
                    "schemaVersion",
                    Value::String("temporal_qd_bidirectional_candidate_lineage_v1".into()),
                ),
                ("candidateId", Value::String(candidate_id)),
                ("candidateIdentitySha256", Value::String(candidate_identity)),
                ("pairIdentitySha256", Value::String(pair_identity)),
                ("orderedSideLineage", Value::Array(lineage)),
            ]),
        ),
        ("pairProposal", proposal.clone()),
        ("pairProposalSha256", Value::String(proposal_sha)),
    ]);
    if let Some(evidence_context) = &context.evidence_identity_context {
        let identity = canonical_evidence_identity(&candidate, evidence_context)?;
        candidate.as_object_mut().expect("candidate object").insert(
            "canonicalEvidenceIdentitySha256".into(),
            Value::String(identity),
        );
    }
    Ok(candidate)
}

/// Concrete grammar/operator composition around one frozen pair-run input.
pub struct RuntimePairAuthority<D> {
    inputs: FrozenRuntimeInputs,
    dashboard: D,
}

pub type DashboardPairAuthority = RuntimePairAuthority<DashboardJsonlPort>;

impl DashboardPairAuthority {
    pub fn from_manifest(manifest: &RuntimeManifest) -> Result<Self> {
        let config = require_object(&manifest.pair_run_config, "frozen pair run config")?;
        let dashboard = DashboardJsonlPort::from_frozen_authority(
            config
                .get("nativeJsonlAuthority")
                .expect("validated pair config"),
        )?;
        Self::new(manifest, dashboard)
    }
}

impl<D: DashboardPort> RuntimePairAuthority<D> {
    pub fn new(manifest: &RuntimeManifest, dashboard: D) -> Result<Self> {
        Ok(Self {
            inputs: FrozenRuntimeInputs::from_manifest(manifest)?,
            dashboard,
        })
    }

    fn native_adapter(&self) -> NativeDashboardAdapter<'_, D> {
        NativeDashboardAdapter(&self.dashboard)
    }

    fn grammar_context(&self, side: &FrozenSideRuntime) -> Result<GrammarContext> {
        GrammarContext::from_normalized(&side.context).map_err(|error| {
            invalid(format!(
                "frozen {} grammar context is invalid: {error}",
                side.direction
            ))
        })
    }

    fn freeze_module(
        &self,
        template: &FrozenModule,
        program: &Value,
        profile: &Value,
        candidate_id: &str,
        lineage: &[Value],
    ) -> Result<FrozenModule> {
        match FrozenModule::validate_native(
            program,
            profile,
            &template.grammar_context,
            &template.catalog,
            &template.policy,
            &template.native_authority,
            &self.native_adapter(),
            candidate_id,
            lineage,
        ) {
            Ok(module) => Ok(module),
            Err(error) => Err(classify_frozen_module_error(
                error,
                "Dashboard module construction rejected",
            )),
        }
    }

    fn freeze_module_with_report(
        &self,
        template: &FrozenModule,
        program: &Value,
        profile: &Value,
        report: &Value,
        lineage: &[Value],
    ) -> Result<FrozenModule> {
        FrozenModule::freeze(
            program,
            profile,
            &template.grammar_context,
            &template.catalog,
            &template.policy,
            &template.native_authority,
            report,
            lineage,
        )
        .map_err(|error| {
            classify_frozen_module_error(error, "Dashboard report could not freeze module")
        })
    }

    fn compile_pair(
        &self,
        long: FrozenModule,
        short: FrozenModule,
        candidate_id: &str,
        lineage: &[Value],
    ) -> Result<FrozenPair> {
        FrozenPair::compile(
            long,
            short,
            &self.inputs.pair_compiler_identity,
            &self.native_adapter(),
            candidate_id,
            lineage,
        )
        .map_err(|error| {
            RuntimeError::Kernel(format!("Dashboard pair compilation rejected: {error}"))
        })
    }

    fn rich_selector(&self, proposal_seed: &str, side: &FrozenSideRuntime) -> Result<Value> {
        let seed = side_seed(proposal_seed, side.direction)?;
        let context = require_object(&side.context, "frozen grammar context")?;
        let axis = |name: &str| -> Result<Vec<Value>> {
            context
                .get(name)
                .and_then(Value::as_array)
                .cloned()
                .ok_or_else(|| invalid(format!("frozen {} context lacks {name}", side.direction)))
        };
        let seed_names = side
            .seed_names
            .iter()
            .cloned()
            .map(Value::String)
            .collect::<Vec<_>>();
        let group = selector_value(&seed, "evidence_group", &axis("groups")?)?
            .get("id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| invalid("rich immigrant evidence-group axis is invalid"))?;
        let event = selector_value(&seed, "event_binding", &axis("events")?)?
            .get("id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| invalid("rich immigrant event axis is invalid"))?;
        let plan = selector_value(&seed, "management_plan", &axis("plans")?)?
            .as_str()
            .map(ToOwned::to_owned)
            .ok_or_else(|| invalid("rich immigrant management-plan axis is invalid"))?;
        let hold_values = require_object(&self.inputs.hold_policy, "frozen hold operator policy")?
            .get("choices")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("frozen hold operator policy lacks choices"))?;
        let protection = immigrant_initial_protection_selector(
            &self.inputs.initial_protection_policy,
            &seed,
            |selected_seed, name, values| {
                selector_value(selected_seed, name, values)
                    .cloned()
                    .map_err(|error| {
                        temporal_qd_kernel::protection::ProtectionError::Invalid(error.to_string())
                    })
            },
        )
        .map_err(|error| {
            RuntimeError::Kernel(format!(
                "rich immigrant protection selection failed: {error}"
            ))
        })?;
        Ok(object([
            (
                "seedName",
                selector_value(&seed, "seed_name", &seed_names)?.clone(),
            ),
            ("groupId", Value::String(group)),
            ("eventId", Value::String(event)),
            ("planId", Value::String(plan)),
            (
                "hold",
                selector_value(&seed, "hold_policy", hold_values)?.clone(),
            ),
            ("initialProtection", protection),
        ]))
    }

    fn apply_hold_profile(&self, profile: &Value, plan_id: &str, hold: &Value) -> Result<Value> {
        let canonical = canonical_hold(Some(hold)).map_err(|error| {
            RuntimeError::Kernel(format!("rich immigrant hold is invalid: {error}"))
        })?;
        let mut child = profile.clone();
        let plans = child
            .as_object_mut()
            .and_then(|root| root.get_mut("executionConfig"))
            .and_then(Value::as_object_mut)
            .and_then(|config| config.get_mut("managementLibrary"))
            .and_then(Value::as_object_mut)
            .and_then(|library| library.get_mut("plans"))
            .and_then(Value::as_array_mut)
            .ok_or_else(|| invalid("rich immigrant profile lacks management plans"))?;
        let mut selected = plans
            .iter_mut()
            .filter(|plan| plan.get("id").and_then(Value::as_str) == Some(plan_id))
            .collect::<Vec<_>>();
        if selected.len() != 1 {
            return Err(invalid(
                "rich immigrant hold selector did not resolve one management plan",
            ));
        }
        let selected = selected[0]
            .as_object_mut()
            .ok_or_else(|| invalid("rich immigrant management plan is invalid"))?;
        if canonical.get("kind").and_then(Value::as_str) == Some("none") {
            selected.remove("holdPolicy");
        } else {
            selected.insert("holdPolicy".into(), canonical);
        }
        Ok(child)
    }

    fn apply_grammar_steps(
        &self,
        grammar: &TypedFragmentGrammar<'_>,
        mut program: ModuleProgram,
        side_seed: &str,
    ) -> Result<(ModuleProgram, Vec<Value>, u64)> {
        let buckets = require_object(
            &self.inputs.immigrant_construction_policy,
            "rich immigrant construction policy",
        )?
        .get("grammarMutationDepthBuckets")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("rich immigrant construction policy lacks grammar depth buckets"))?;
        let depth = selector_value(side_seed, "grammar_mutation_depth", buckets)?
            .as_u64()
            .ok_or_else(|| invalid("rich immigrant grammar depth is invalid"))?;
        let mut trace = Vec::new();
        let mut seen = BTreeMap::from([(canonical_sha256(&program.canonical())?, ())]);
        for step in 0..depth {
            let plans = grammar.enumerate_operations(&program).map_err(|error| {
                RuntimeError::Kernel(format!("rich immigrant grammar plans are invalid: {error}"))
            })?;
            let mut family_names = plans
                .iter()
                .filter_map(|plan| plan.get("operation").and_then(Value::as_str))
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>();
            family_names.sort();
            family_names.dedup();
            let mut applied = None;
            for family in seeded_order(
                family_names.into_iter().map(Value::String).collect(),
                side_seed,
                &format!("grammar_family_{step}"),
            )? {
                let family = family.as_str().expect("family name");
                let family_plans = plans
                    .iter()
                    .filter(|plan| plan.get("operation").and_then(Value::as_str) == Some(family))
                    .cloned()
                    .collect();
                for plan in seeded_order(
                    family_plans,
                    side_seed,
                    &format!("grammar_plan_{step}_{family}"),
                )? {
                    let Ok(child) = grammar.apply(&program, &plan) else {
                        continue;
                    };
                    let child_sha = canonical_sha256(&child.canonical())?;
                    if seen.insert(child_sha.clone(), ()).is_none() {
                        applied = Some((
                            child,
                            object([
                                ("step", Value::from(step)),
                                ("operationFamily", Value::String(family.to_owned())),
                                ("plan", plan.clone()),
                                ("planSha256", Value::String(canonical_sha256(&plan)?)),
                                ("childProgramSha256", Value::String(child_sha)),
                            ]),
                        ));
                        break;
                    }
                }
                if applied.is_some() {
                    break;
                }
            }
            let Some((child, audit)) = applied else { break };
            program = child;
            trace.push(audit);
        }
        Ok((program, trace, depth))
    }

    fn apply_indicator_steps(
        &self,
        registry: &IndicatorLearningRegistry,
        mut profile: Value,
        side_seed: &str,
    ) -> Result<(Value, Vec<Value>, u64, Value)> {
        let buckets = require_object(
            &self.inputs.immigrant_construction_policy,
            "rich immigrant construction policy",
        )?
        .get("indicatorMutationDepthBuckets")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            invalid("rich immigrant construction policy lacks indicator depth buckets")
        })?;
        let depth = selector_value(side_seed, "indicator_mutation_depth", buckets)?
            .as_u64()
            .ok_or_else(|| invalid("rich immigrant indicator depth is invalid"))?;
        let mut trace = Vec::new();
        let mut seen = BTreeMap::from([(canonical_sha256(&profile)?, ())]);
        let mut cap_rejected = Vec::new();
        for step in 0..depth {
            let plans = registry.enumerate_plans(&profile).map_err(|error| {
                RuntimeError::Kernel(format!(
                    "rich immigrant indicator plans are invalid: {error}"
                ))
            })?;
            let mut operator_ids = plans
                .iter()
                .filter_map(|plan| plan.get("operatorId").and_then(Value::as_str))
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>();
            operator_ids.sort();
            operator_ids.dedup();
            let mut applied = None;
            for operator_id in seeded_order(
                operator_ids.into_iter().map(Value::String).collect(),
                side_seed,
                &format!("indicator_operator_{step}"),
            )? {
                let operator_id = operator_id.as_str().expect("operator id");
                let candidates = plans
                    .iter()
                    .filter(|plan| {
                        plan.get("operatorId").and_then(Value::as_str) == Some(operator_id)
                    })
                    .cloned()
                    .collect();
                let operator = registry.get(operator_id).map_err(|error| {
                    RuntimeError::Kernel(format!(
                        "rich immigrant indicator operator is invalid: {error}"
                    ))
                })?;
                for plan in seeded_order(
                    candidates,
                    side_seed,
                    &format!("indicator_plan_{step}_{operator_id}"),
                )? {
                    let Ok(child) = operator.preview(&profile, &plan) else {
                        continue;
                    };
                    if validate_entry_route_decision_indicator_cap(&child).is_err() {
                        cap_rejected.push(object([
                            ("step", Value::from(step)),
                            ("operatorId", Value::String(operator_id.to_owned())),
                            (
                                "planSha256",
                                plan.get("planSha256").cloned().unwrap_or(Value::Null),
                            ),
                        ]));
                        continue;
                    }
                    let child_sha = canonical_sha256(&child)?;
                    if seen.insert(child_sha.clone(), ()).is_none() {
                        applied = Some((
                            child,
                            object([
                                ("step", Value::from(step)),
                                ("operatorId", Value::String(operator_id.to_owned())),
                                (
                                    "constructionKind",
                                    plan.get("construction")
                                        .and_then(Value::as_object)
                                        .and_then(|item| item.get("kind"))
                                        .cloned()
                                        .unwrap_or(Value::Null),
                                ),
                                (
                                    "planSha256",
                                    plan.get("planSha256").cloned().unwrap_or(Value::Null),
                                ),
                                ("childProfileSha256", Value::String(child_sha)),
                            ]),
                        ));
                        break;
                    }
                }
                if applied.is_some() {
                    break;
                }
            }
            let Some((child, audit)) = applied else { break };
            profile = child;
            trace.push(audit);
        }
        let cap_rejections = object([
            ("count", Value::from(cap_rejected.len() as u64)),
            (
                "rowsSha256",
                Value::String(canonical_sha256(&Value::Array(cap_rejected))?),
            ),
        ]);
        Ok((profile, trace, depth, cap_rejections))
    }

    /// Port of Python's `_apply_dynamic_initial_protection`.  This is a
    /// construction transaction, not a runtime/evaluator fallback: no eligible
    /// catalog scalar produces the source-authorized static grid fallback and
    /// records that disposition explicitly.
    fn apply_dynamic_initial_protection(
        &self,
        profile: &Value,
        side: &FrozenSideRuntime,
        side_seed: &str,
        selector: &Value,
    ) -> Result<(Value, Value, Option<Value>)> {
        let desired_site = require_string(
            require_object(selector, "dynamic initial protection selector")?,
            "dynamicSite",
            "dynamic initial protection selector",
        )?;
        if !matches!(desired_site.as_str(), "initial_stop" | "initial_target") {
            return Err(invalid("dynamic initial protection site is invalid"));
        }
        let catalog = ConstructionCatalog::new(&side.catalog).map_err(|error| {
            RuntimeError::Kernel(format!("dynamic construction catalog is invalid: {error}"))
        })?;
        let registry = GeneratorV3ConstructionRegistry::new(catalog).map_err(|error| {
            RuntimeError::Kernel(format!("dynamic construction registry is invalid: {error}"))
        })?;
        let operator = registry.get(SCALAR_DYNAMIC_MANAGEMENT).map_err(|error| {
            RuntimeError::Kernel(format!(
                "dynamic construction operator is unavailable: {error}"
            ))
        })?;
        let mut plans = operator
            .enumerate_plans(profile)
            .map_err(|error| {
                RuntimeError::Kernel(format!("dynamic construction plans are invalid: {error}"))
            })?
            .into_iter()
            .filter(|plan| {
                plan.get("construction")
                    .and_then(Value::as_object)
                    .and_then(|construction| construction.get("site"))
                    .and_then(Value::as_str)
                    == Some(desired_site.as_str())
            })
            .collect::<Vec<_>>();
        plans.sort_by_key(|plan| canonical_sha256(plan).expect("canonical construction plan"));

        let plan_id = require_string(
            require_object(selector, "dynamic initial protection selector")?,
            "planId",
            "dynamic initial protection selector",
        )?;
        if plans.is_empty() {
            let policy = require_object(
                &self.inputs.initial_protection_policy,
                "frozen initial protection policy",
            )?;
            let stop_choices = policy
                .get("stopPercentChoices")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    invalid("frozen initial protection policy lacks stopPercentChoices")
                })?;
            let reward_choices = policy
                .get("rewardMultipleChoices")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    invalid("frozen initial protection policy lacks rewardMultipleChoices")
                })?;
            let fallback = object([
                ("mode", Value::String("coupled_reward_multiple".into())),
                (
                    "stopPercent",
                    selector_value(
                        side_seed,
                        "initial_protection_dynamic_fallback_stop_percent",
                        stop_choices,
                    )?
                    .clone(),
                ),
                (
                    "rewardMultiple",
                    selector_value(
                        side_seed,
                        "initial_protection_dynamic_fallback_reward_multiple",
                        reward_choices,
                    )?
                    .clone(),
                ),
            ]);
            let (child, mut audit) = apply_immigrant_initial_protection(
                profile,
                &plan_id,
                &fallback,
                &self.inputs.initial_protection_policy,
            )
            .map_err(|error| {
                RuntimeError::Kernel(format!(
                    "dynamic initial protection fallback failed: {error}"
                ))
            })?;
            let audit_map = audit
                .as_object_mut()
                .expect("initial protection audit is object");
            audit_map.insert(
                "dynamicDisposition".into(),
                Value::String("deferred_no_catalog_authorized_completed_bar_scalar".into()),
            );
            audit_map.insert("requestedDynamicSite".into(), Value::String(desired_site));
            audit_map.remove("applicationSha256");
            let application_sha = canonical_sha256(&audit)?;
            audit
                .as_object_mut()
                .expect("initial protection audit is object")
                .insert("applicationSha256".into(), Value::String(application_sha));
            return Ok((child, audit, None));
        }

        let selected = selector_value(
            side_seed,
            "initial_protection_dynamic_construction_plan",
            &plans,
        )?
        .clone();
        let parent_report = self.dashboard.validate_v2(
            profile,
            &format!("qd_rich_dynamic_parent_{}", &side_seed[7..35]),
        )?;
        let preview = operator.preview(profile, &selected).map_err(|error| {
            RuntimeError::Kernel(format!("dynamic construction preview failed: {error}"))
        })?;
        let mut child_report = self.dashboard.validate_v2(
            &preview,
            &format!("qd_rich_dynamic_child_{}", &side_seed[7..35]),
        )?;
        let parent_program = require_string(
            require_object(&parent_report, "dynamic construction parent validation")?,
            "programSha256",
            "dynamic construction parent validation",
        )?;
        let child_program = require_string(
            require_object(&child_report, "dynamic construction child validation")?,
            "programSha256",
            "dynamic construction child validation",
        )?;
        let (mut child, application) = operator
            .apply(profile, &selected, &parent_program, &child_program)
            .map_err(|error| {
                RuntimeError::Kernel(format!("dynamic construction application failed: {error}"))
            })?;
        if child != preview {
            return Err(invalid(
                "dynamic initial protection construction preview/application diverged",
            ));
        }

        let construction = selected
            .get("construction")
            .and_then(Value::as_object)
            .ok_or_else(|| invalid("dynamic construction plan lacks construction"))?;
        let construction_plan_id = construction
            .get("planId")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("dynamic construction plan lacks plan id"))?;
        let locator_site = if desired_site == "initial_stop" {
            "stop"
        } else {
            "target"
        };
        let locator_key = if locator_site == "stop" {
            "initialStop"
        } else {
            "initialTarget"
        };
        let locator = child
            .get("executionConfig")
            .and_then(Value::as_object)
            .and_then(|config| config.get("managementLibrary"))
            .and_then(Value::as_object)
            .and_then(|library| library.get("plans"))
            .and_then(Value::as_array)
            .and_then(|plans| {
                plans.iter().find(|plan| {
                    plan.get("id").and_then(Value::as_str) == Some(construction_plan_id)
                })
            })
            .and_then(Value::as_object)
            .and_then(|plan| plan.get(locator_key))
            .cloned()
            .ok_or_else(|| invalid("dynamic construction result lacks selected locator"))?;
        let mut multiplier_audit = None;
        if locator.get("kind").and_then(Value::as_str) == Some("indicator_distance_multiple") {
            let policy = require_object(
                &self.inputs.initial_protection_policy,
                "frozen initial protection policy",
            )?;
            let choices = policy
                .get("distanceMultipleChoices")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    invalid("frozen initial protection policy lacks distanceMultipleChoices")
                })?;
            let desired_multiple = selector_value(
                side_seed,
                "initial_protection_dynamic_distance_multiple",
                choices,
            )?
            .clone();
            let replacement = object([
                ("kind", Value::String("indicator_distance_multiple".into())),
                (
                    "bindingId",
                    locator
                        .get("bindingId")
                        .cloned()
                        .ok_or_else(|| invalid("dynamic distance locator lacks bindingId"))?,
                ),
                ("multiple", desired_multiple.clone()),
            ]);
            if replacement != locator {
                let adjustment = enumerate_initial_protection_plans(
                    &child,
                    &self.inputs.initial_protection_policy,
                )
                .map_err(|error| {
                    RuntimeError::Kernel(format!(
                        "dynamic protection grid enumeration failed: {error}"
                    ))
                })?
                .into_iter()
                .find(|plan| {
                    plan.get("planId").and_then(Value::as_str) == Some(construction_plan_id)
                        && plan.get("site").and_then(Value::as_str) == Some(locator_site)
                        && plan.get("replacement") == Some(&replacement)
                })
                .ok_or_else(|| {
                    invalid("dynamic distance multiplier adjustment is not canonical")
                })?;
                let (adjusted, audit) = apply_initial_protection_plan(
                    &child,
                    &adjustment,
                    &self.inputs.initial_protection_policy,
                )
                .map_err(|error| {
                    RuntimeError::Kernel(format!(
                        "dynamic distance multiplier adjustment failed: {error}"
                    ))
                })?;
                child = adjusted;
                multiplier_audit = Some(audit);
                child_report = self.dashboard.validate_v2(
                    &child,
                    &format!("qd_rich_dynamic_adjusted_{}", &side_seed[7..35]),
                )?;
            } else {
                multiplier_audit = Some(object([
                    (
                        "schemaVersion",
                        Value::String("temporal_qd_initial_protection_dynamic_grid_v1".into()),
                    ),
                    ("selectedMultiple", desired_multiple),
                    (
                        "disposition",
                        Value::String("already_selected_by_construction".into()),
                    ),
                ]));
            }
        }
        let mut audit = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_initial_protection_immigrant_dynamic_v1".into()),
            ),
            ("requestedDynamicSite", Value::String(desired_site)),
            ("dynamicDisposition", Value::String("materialized".into())),
            (
                "constructionPlanSha256",
                selected
                    .get("planSha256")
                    .cloned()
                    .ok_or_else(|| invalid("dynamic construction plan lacks identity"))?,
            ),
            ("application", application),
        ]);
        if let Some(multiplier_audit) = multiplier_audit {
            audit
                .as_object_mut()
                .expect("dynamic audit object")
                .insert("distanceMultipleApplication".into(), multiplier_audit);
        }
        let application_sha = canonical_sha256(&audit)?;
        audit
            .as_object_mut()
            .expect("dynamic audit object")
            .insert("applicationSha256".into(), Value::String(application_sha));
        Ok((child, audit, Some(child_report)))
    }

    fn construct_rich_module(
        &self,
        proposal_seed: &str,
        side: &FrozenSideRuntime,
    ) -> Result<FrozenModule> {
        let selector = self.rich_selector(proposal_seed, side)?;
        let selector_fields = require_object(&selector, "rich immigrant selector")?;
        let seed = side_seed(proposal_seed, side.direction)?;
        let grammar_context = self.grammar_context(side)?;
        let native = self.native_adapter();
        let grammar = TypedFragmentGrammar::new(grammar_context, &native).map_err(|error| {
            RuntimeError::Kernel(format!("rich immigrant grammar cannot initialize: {error}"))
        })?;
        let program = grammar
            .seed(
                side.direction,
                &require_string(selector_fields, "seedName", "rich immigrant selector")?,
                Some(&require_string(
                    selector_fields,
                    "groupId",
                    "rich immigrant selector",
                )?),
                Some(&require_string(
                    selector_fields,
                    "eventId",
                    "rich immigrant selector",
                )?),
                Some(&require_string(
                    selector_fields,
                    "planId",
                    "rich immigrant selector",
                )?),
            )
            .map_err(|error| {
                RuntimeError::Kernel(format!("rich immigrant grammar seed rejected: {error}"))
            })?;
        let (program, grammar_trace, grammar_depth) =
            self.apply_grammar_steps(&grammar, program, &seed)?;
        let mut profile = grammar.materialize_profile(&program).map_err(|error| {
            RuntimeError::Kernel(format!("rich immigrant grammar profile failed: {error}"))
        })?;
        let (profile_after_indicator, indicator_trace, indicator_depth, cap_rejections) =
            self.apply_indicator_steps(side.indicator_registry.as_ref(), profile, &seed)?;
        profile = self.apply_hold_profile(
            &profile_after_indicator,
            &require_string(selector_fields, "planId", "rich immigrant selector")?,
            selector_fields.get("hold").expect("selector hold"),
        )?;
        let mut protection = selector_fields
            .get("initialProtection")
            .expect("selector protection")
            .clone();
        protection
            .as_object_mut()
            .expect("selector protection object")
            .insert(
                "planId".into(),
                selector_fields
                    .get("planId")
                    .expect("selector plan")
                    .clone(),
            );
        let (profile, protection_audit, final_native_report) = if protection
            .get("mode")
            .and_then(Value::as_str)
            == Some("dynamic_catalog_authorized")
        {
            self.apply_dynamic_initial_protection(&profile, side, &seed, &protection)?
        } else {
            let (profile, audit) = apply_immigrant_initial_protection(
                &profile,
                &require_string(selector_fields, "planId", "rich immigrant selector")?,
                &protection,
                &self.inputs.initial_protection_policy,
            )
            .map_err(|error| {
                RuntimeError::Kernel(format!("rich immigrant initial protection failed: {error}"))
            })?;
            (profile, audit, None)
        };
        let entry_cap = validate_entry_route_decision_indicator_cap(&profile).map_err(|error| {
            RuntimeError::Kernel(format!("rich immigrant entry-route cap failed: {error}"))
        })?;
        let graph_groups = profile
            .get("graph")
            .and_then(Value::as_object)
            .and_then(|graph| graph.get("evidenceGroups"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let profile_shape = object([
            ("fragmentCount", Value::from(program.fragments.len() as u64)),
            (
                "indicatorCount",
                Value::from(
                    profile
                        .get("indicators")
                        .and_then(Value::as_array)
                        .map_or(0, Vec::len) as u64,
                ),
            ),
            (
                "evidenceGroupMemberCounts",
                Value::Array(sorted_evidence_group_member_counts(&graph_groups)),
            ),
            (
                "holdKind",
                selector_fields
                    .get("hold")
                    .and_then(Value::as_object)
                    .and_then(|hold| hold.get("kind"))
                    .cloned()
                    .unwrap_or(Value::Null),
            ),
            (
                "initialProtectionMode",
                protection.get("mode").cloned().unwrap_or(Value::Null),
            ),
        ]);
        let mut audit = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_rich_immigrant_module_construction_v1".into()),
            ),
            (
                "builderVersion",
                Value::String(PAIR_IMMIGRANT_BUILDER_VERSION.into()),
            ),
            ("side", Value::String(side.direction.into())),
            ("proposalSeed", Value::String(proposal_seed.into())),
            ("selector", selector.clone()),
            (
                "grammar",
                object([
                    ("plannedDepth", Value::from(grammar_depth)),
                    ("appliedDepth", Value::from(grammar_trace.len() as u64)),
                    ("steps", Value::Array(grammar_trace)),
                ]),
            ),
            (
                "indicator",
                object([
                    ("plannedDepth", Value::from(indicator_depth)),
                    ("appliedDepth", Value::from(indicator_trace.len() as u64)),
                    ("steps", Value::Array(indicator_trace)),
                    (
                        "entryRouteCapRejectedPlanCount",
                        cap_rejections["count"].clone(),
                    ),
                    (
                        "entryRouteCapRejectedPlanRowsSha256",
                        cap_rejections["rowsSha256"].clone(),
                    ),
                ]),
            ),
            ("initialProtection", protection_audit),
            (
                "entryRouteDecisionIndicatorReportSha256",
                Value::String(canonical_sha256(&entry_cap)?),
            ),
            ("profileShape", profile_shape),
        ]);
        let audit_sha = canonical_sha256(&audit)?;
        audit
            .as_object_mut()
            .expect("audit object")
            .insert("auditSha256".into(), Value::String(audit_sha));
        let lineage = vec![
            object([
                ("operation", Value::String("typed_seed".into())),
                ("side", Value::String(side.direction.into())),
                (
                    "seedName",
                    selector_fields.get("seedName").expect("selector").clone(),
                ),
                (
                    "groupId",
                    selector_fields.get("groupId").expect("selector").clone(),
                ),
                (
                    "eventId",
                    selector_fields.get("eventId").expect("selector").clone(),
                ),
                (
                    "planId",
                    selector_fields.get("planId").expect("selector").clone(),
                ),
                ("proposalSeed", Value::String(proposal_seed.into())),
            ]),
            object([
                (
                    "operation",
                    Value::String("rich_immigrant_construction".into()),
                ),
                ("side", Value::String(side.direction.into())),
                ("audit", audit),
            ]),
        ];
        let native_authority = IdentitySnapshot::from_payload(
            &self.inputs.native_authority_identity,
            Some("nativeAuthority"),
        )
        .map_err(|error| invalid(error.to_string()))?;
        match final_native_report {
            Some(report) => FrozenModule::freeze(
                &program.canonical(),
                &profile,
                &side.grammar_context,
                &side.catalog_snapshot,
                &side.immigrant_policy,
                &native_authority,
                &report,
                &lineage,
            )
            .map_err(|error| {
                RuntimeError::Kernel(format!("rich immigrant dynamic module rejected: {error}"))
            }),
            None => FrozenModule::validate_native(
                &program.canonical(),
                &profile,
                &side.grammar_context,
                &side.catalog_snapshot,
                &side.immigrant_policy,
                &native_authority,
                &native,
                &format!("qd_rich_module_{}", &seed[7..35]),
                &lineage,
            )
            .map_err(|error| {
                RuntimeError::Kernel(format!("rich immigrant native module rejected: {error}"))
            }),
        }
    }

    fn rich_immigrant(
        &self,
        proposal_seed: &str,
        context: &NativeConstructionContext,
    ) -> Result<NativeProposal> {
        let long = self.construct_rich_module(proposal_seed, &self.inputs.long)?;
        let short = self.construct_rich_module(proposal_seed, &self.inputs.short)?;
        // Build the compact pair audit while the two frozen modules are still
        // borrowed, then move them directly into the compiler.  Cloning here
        // temporarily duplicated both rich profiles/reports/lineages for
        // every immigrant despite the pair becoming their sole owner.
        let pair_lineage = [&long, &short]
            .iter()
            .map(|module| {
                let construction = module
                    .lineage
                    .iter()
                    .rev()
                    .find(|row| {
                        row.get("operation").and_then(Value::as_str)
                            == Some("rich_immigrant_construction")
                    })
                    .and_then(|row| row.get("audit"))
                    .expect("rich module was constructed");
                object([
                    (
                        "operation",
                        Value::String("rich_immigrant_construction".into()),
                    ),
                    ("side", Value::String(module.direction.clone())),
                    ("proposalSeed", Value::String(proposal_seed.into())),
                    (
                        "constructionAuditSha256",
                        construction
                            .get("auditSha256")
                            .expect("construction audit hash")
                            .clone(),
                    ),
                ])
            })
            .collect::<Vec<_>>();
        let pair = self.compile_pair(
            long,
            short,
            &format!(
                "qd_rich_pair_{}",
                &canonical_sha256(&object([("seed", Value::String(proposal_seed.into()))]))?[7..35]
            ),
            &pair_lineage,
        )?;
        let pair_identity = pair
            .identity_sha256()
            .map_err(|error| invalid(error.to_string()))?;
        let factory_construction_audit = rich_immigrant_factory_construction_audit(&pair)?;
        validate_rich_immigrant_factory_construction_audit(&factory_construction_audit, &pair)?;
        let mut proposal = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_pair_proposal_v2".into()),
            ),
            ("proposalSeed", Value::String(proposal_seed.into())),
            ("originKind", Value::String("random_immigrant".into())),
            // A bidirectional immigrant has no single mutated side, but its
            // public proposal metadata still commits to Python's pure
            // `proposal_side(seed)` identity calculation.  This consumes no
            // selector draw and must not be replaced by a fixed label.
            (
                "side",
                Value::String(
                    proposal_side(proposal_seed)
                        .map_err(|error| invalid(error.to_string()))?
                        .as_str()
                        .into(),
                ),
            ),
            (
                "factoryPair",
                pair.canonical_payload()
                    .map_err(|error| invalid(error.to_string()))?,
            ),
            ("pairIdentitySha256", Value::String(pair_identity)),
            ("factoryConstructionAudit", factory_construction_audit),
            ("disposition", Value::String("materialized".into())),
        ]);
        let proposal_sha = canonical_sha256(&proposal)?;
        proposal
            .as_object_mut()
            .expect("proposal object")
            .insert("proposalSha256".into(), Value::String(proposal_sha));
        self.materialized_native(pair, proposal, context)
    }

    fn materialized_native(
        &self,
        pair: FrozenPair,
        proposal: Value,
        context: &NativeConstructionContext,
    ) -> Result<NativeProposal> {
        let candidate = materialize_pair_candidate(&pair, &proposal, context)?;
        let predeclared_lake_scope = pair_predeclared_lake_scope_report(
            &candidate,
            context.evidence_identity_context.as_ref(),
            context.frozen_construction_catalog.as_ref(),
        )?;
        let semantic =
            executable_pair_semantic_sha256(&pair.long.profile_sha256, &pair.short.profile_sha256);
        Ok(NativeProposal {
            proposal,
            candidate: Some(candidate),
            executable_semantic_sha256: Some(semantic),
            predeclared_lake_scope,
            funnel_material: Some(temporal_qd_kernel::factory::NativeFunnelMaterial {
                raw_source_profile_sha256: pair.raw_pair_sha256,
                resolved_profile_sha256: pair.profile_sha256,
                program_sha256: pair.native_program_sha256,
                validation_report_sha256: pair.native_validation_report_sha256,
            }),
        })
    }
}

fn finalize_crossover_proposal(
    mut proposal: Value,
    crossover_audit: Value,
    pair_payload: Value,
    pair_identity: String,
    parent_semantic: &str,
    child_semantic: &str,
) -> Result<(Value, bool)> {
    proposal
        .as_object_mut()
        .expect("crossover proposal")
        .extend([
            ("disposition".into(), Value::String("materialized".into())),
            ("crossoverAudit".into(), crossover_audit),
            ("pair".into(), pair_payload),
            ("pairIdentitySha256".into(), Value::String(pair_identity)),
        ]);
    let no_op = child_semantic == parent_semantic;
    if no_op {
        let fields = proposal.as_object_mut().expect("crossover proposal");
        fields.insert("disposition".into(), Value::String("no_op_proposal".into()));
        fields.remove("pair");
        fields.remove("pairIdentitySha256");
    }
    let proposal_sha = canonical_sha256(&proposal)?;
    proposal
        .as_object_mut()
        .expect("proposal")
        .insert("proposalSha256".into(), Value::String(proposal_sha));
    Ok((proposal, no_op))
}

// This internal test module intentionally lives beside the private helpers it
// exercises; keep this exception scoped to the module rather than the crate.
#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;

    #[cfg(windows)]
    #[test]
    fn frozen_dashboard_paths_canonicalize_equivalent_windows_spellings_and_reject_substitution_or_reparse()
     {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "temporal_qd_runtime_command_binding_{}_{}",
            std::process::id(),
            unique
        ));
        fs::create_dir_all(&root).expect("temporary authority directory");
        let interpreter = root.join("python.exe");
        let script = root.join("validate.py");
        let substitute = root.join("substitute.py");
        let source_root = root.join("dashboard");
        let substitute_source_root = root.join("substitute-dashboard");
        fs::write(&interpreter, b"interpreter").expect("temporary interpreter");
        fs::write(&script, b"script").expect("temporary validator script");
        fs::write(&substitute, b"substitute").expect("temporary substitute script");
        fs::create_dir(&source_root).expect("temporary Dashboard source root");
        fs::create_dir(&substitute_source_root).expect("temporary substitute source root");

        let verbatim = |path: &Path| PathBuf::from(format!(r"\\?\{}", path.display()));
        let equivalent_command = vec![
            verbatim(&interpreter).to_string_lossy().into_owned(),
            verbatim(&script).to_string_lossy().into_owned(),
        ];
        assert_eq!(
            absolute_regular_path(Path::new(&equivalent_command[0]), "command interpreter")
                .expect("canonical command interpreter"),
            absolute_regular_path(&interpreter, "authority interpreter")
                .expect("canonical authority interpreter")
        );
        assert_eq!(
            absolute_regular_path(Path::new(&equivalent_command[1]), "command script")
                .expect("canonical command script"),
            absolute_regular_path(&script, "authority script").expect("canonical authority script")
        );
        let frozen_interpreter = absolute_regular_path(&interpreter, "authority interpreter")
            .expect("canonical authority interpreter");
        let frozen_script =
            absolute_regular_path(&script, "authority script").expect("canonical authority script");
        let frozen_source_root = absolute_directory_path(&source_root, "authority source root")
            .expect("canonical authority source root");
        validate_frozen_command_paths(&equivalent_command, &frozen_interpreter, &frozen_script)
            .expect("extended and drive paths must resolve to the same authority files");
        validate_frozen_authority_content_paths(
            &verbatim(&interpreter).to_string_lossy(),
            &verbatim(&script).to_string_lossy(),
            &verbatim(&source_root).to_string_lossy(),
            &frozen_interpreter,
            &frozen_script,
            &frozen_source_root,
        )
        .expect("extended authority content paths must bind their frozen files and root");

        let nested_source_directory = source_root.join("shared").join("python");
        fs::create_dir_all(&nested_source_directory).expect("nested source directory");
        let source_file = nested_source_directory.join("validator.py");
        fs::write(&source_file, b"source file").expect("temporary source file");
        assert_eq!(
            resolve_frozen_source_manifest_file(&frozen_source_root, r"shared\python\validator.py")
                .expect("Windows separator manifest path"),
            absolute_regular_path(&source_file, "expected source file")
                .expect("canonical expected source file")
        );
        for unsafe_path in [
            r"shared\..\validator.py",
            r"\\server\share\validator.py",
            r"C:\\validator.py",
        ] {
            let error = resolve_frozen_source_manifest_file(&frozen_source_root, unsafe_path)
                .expect_err("traversal, UNC, and drive source-manifest paths must fail");
            assert!(error.to_string().contains("source manifest path is unsafe"));
        }

        let substituted_command = vec![
            interpreter.to_string_lossy().into_owned(),
            substitute.to_string_lossy().into_owned(),
        ];
        let error = validate_frozen_command_paths(
            &substituted_command,
            &frozen_interpreter,
            &frozen_script,
        )
        .expect_err("a different regular script must not substitute for the frozen authority");
        assert!(
            error
                .to_string()
                .contains("does not bind frozen executable paths")
        );

        let error = validate_frozen_authority_content_paths(
            &interpreter.to_string_lossy(),
            &substitute.to_string_lossy(),
            &substitute_source_root.to_string_lossy(),
            &frozen_interpreter,
            &frozen_script,
            &frozen_source_root,
        )
        .expect_err("content paths must not substitute a script or source root");
        assert!(
            error
                .to_string()
                .contains("authority content bindings drifted")
        );

        let reparse_script = root.join("validator-link.py");
        std::os::windows::fs::symlink_file(&script, &reparse_script)
            .expect("Windows test setup requires file symlink support");
        let error = validate_frozen_authority_content_paths(
            &interpreter.to_string_lossy(),
            &reparse_script.to_string_lossy(),
            &source_root.to_string_lossy(),
            &frozen_interpreter,
            &frozen_script,
            &frozen_source_root,
        )
        .expect_err("authority content must fail closed on a reparse path");
        assert!(
            error.to_string().contains("symlink") || error.to_string().contains("reparse point")
        );

        let source_substitution = source_root.join("substituted.py");
        std::os::windows::fs::symlink_file(&substitute, &source_substitution)
            .expect("Windows test setup requires file symlink support");
        let error = resolve_frozen_source_manifest_file(&frozen_source_root, "substituted.py")
            .expect_err("a reparse file cannot substitute for a frozen source-manifest file");
        assert!(
            error.to_string().contains("symlink") || error.to_string().contains("reparse point")
        );

        fs::remove_dir_all(&root).expect("remove temporary authority directory");
    }

    #[test]
    fn construction_audit_canonicalizes_evidence_group_member_counts() {
        let groups = vec![
            object([(
                "indicatorInstanceIds",
                Value::Array(vec![Value::from("first"), Value::from("second")]),
            )]),
            object([(
                "indicatorInstanceIds",
                Value::Array(vec![Value::from("third")]),
            )]),
        ];

        assert_eq!(
            sorted_evidence_group_member_counts(&groups),
            vec![Value::from(1_u64), Value::from(2_u64)]
        );
    }

    #[test]
    fn python_canonical_evidence_identity_golden_is_exact() {
        let candidate = object([
            ("candidateId", Value::String("qd_fixture".into())),
            (
                "programSha256",
                Value::String(format!("sha256:{}", "1".repeat(64))),
            ),
            (
                "sourceProfileSha256",
                Value::String(format!("sha256:{}", "2".repeat(64))),
            ),
            (
                "profileSnapshotSha256",
                Value::String(format!("sha256:{}", "3".repeat(64))),
            ),
            (
                "sourceProfile",
                object([(
                    "executionConfig",
                    object([(
                        "managementLibrary",
                        object([("plans", Value::Array(vec![]))]),
                    )]),
                )]),
            ),
        ]);
        let mut context = object([
            (
                "orderedWindowPlanSemantic",
                Value::Array(vec![object([
                    ("windowId", Value::String("fixture".into())),
                    ("start", Value::String("2024-01-01".into())),
                    ("end", Value::String("2024-01-02".into())),
                ])]),
            ),
            (
                "costViews",
                object([("none", object([("spreadBps", Value::from(0.0))]))]),
            ),
            (
                "workerContractSha256",
                Value::String(format!("sha256:{}", "4".repeat(64))),
            ),
        ]);
        let context_sha = canonical_sha256(&context).unwrap();
        context.as_object_mut().unwrap().insert(
            "predeclaredEvidenceContextSha256".into(),
            Value::String(context_sha),
        );

        assert_eq!(
            canonical_evidence_identity(&candidate, &context).unwrap(),
            "sha256:d531f1ba89171b086db7f4a24f30f96ab097b7e9e08dcd496a3363b6ad967493",
        );
    }

    #[test]
    fn archive_bootstrap_rebinds_historical_evidence_identity() {
        let candidate = object([
            ("candidateId", Value::String("historical_parent".into())),
            (
                "programSha256",
                Value::String(format!("sha256:{}", "1".repeat(64))),
            ),
            (
                "sourceProfileSha256",
                Value::String(format!("sha256:{}", "2".repeat(64))),
            ),
            (
                "profileSnapshotSha256",
                Value::String(format!("sha256:{}", "3".repeat(64))),
            ),
            (
                "canonicalEvidenceIdentitySha256",
                Value::String(format!("sha256:{}", "f".repeat(64))),
            ),
            (
                "sourceProfile",
                object([(
                    "executionConfig",
                    object([("entryMode", Value::String("next_open".into()))]),
                )]),
            ),
        ]);
        let context = object([(
            "orderedWindowPlanSemantic",
            Value::Array(vec![object([(
                "windowId",
                Value::String("current".into()),
            )])]),
        )]);

        let (rebound, current_identity) =
            rebind_archive_candidate_evidence_identity(&candidate, &context)
                .expect("a historical candidate can bind to the current context");
        assert_ne!(
            candidate["canonicalEvidenceIdentitySha256"],
            Value::String(current_identity.clone()),
            "the archived value deliberately represents a different source context"
        );
        assert_eq!(
            rebound["canonicalEvidenceIdentitySha256"],
            Value::String(current_identity),
            "ledger bootstrap must use the current context-bound identity"
        );

        let mut original_material = candidate;
        original_material
            .as_object_mut()
            .expect("candidate object")
            .remove("canonicalEvidenceIdentitySha256");
        let mut rebound_material = rebound;
        rebound_material
            .as_object_mut()
            .expect("candidate object")
            .remove("canonicalEvidenceIdentitySha256");
        assert_eq!(
            rebound_material, original_material,
            "rebinding must not alter executable historical candidate material"
        );
    }

    #[test]
    fn python_structural_mutation_candidate_id_commits_to_the_operation_value() {
        let choice = object([
            ("kind", Value::String("initial_protection".into())),
            (
                "plan",
                object([
                    ("kind", Value::String("initial_protection".into())),
                    ("mutationClass", Value::String("jump".into())),
                    ("planId", Value::String("base".into())),
                    (
                        "planSha256",
                        Value::String(
                            "sha256:9a162b7bd20a1e133f08fb70e28d60d7dbc1e0f2395c1b0ea660adf722cb4147"
                                .into(),
                        ),
                    ),
                    (
                        "replacement",
                        object([
                            ("kind", Value::String("reward_multiple".into())),
                            ("multiple", Value::from(0.25)),
                        ]),
                    ),
                    ("site", Value::String("target".into())),
                ]),
            ),
        ]);
        assert_eq!(
            canonical_sha256(&choice).unwrap(),
            "sha256:085db9e0b12132fbc596c0f830f1b2d83e813aad4b3fd9d3a605a211c4d15e37"
        );
        assert_eq!(
            mutation_candidate_id_for_choice(
                "sha256:8f301adf2d0edb792ee85718d6d7ec6e8c2ef5b1e9f37d3219c6ed04a9fba6f0",
                "sha256:f23d1888f85891e99b3c551f155c88bf496e27e4d89c56677589fd214c1d0e09",
                &choice,
            )
            .unwrap(),
            "qd_pair_23562e65790af58ae735ba9a74a3"
        );
    }

    #[test]
    fn crossover_no_op_finalization_drops_pair_material_and_is_self_hashed() {
        let base = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_pair_proposal_v2".into()),
            ),
            (
                "proposalKind",
                Value::String("temporal_qd_same_side_crossover_v1".into()),
            ),
            ("proposalSeed", Value::String("no-op-fixture".into())),
            ("originKind", Value::String("structural_offspring".into())),
            ("side", Value::String("long".into())),
            ("parentSelection", Value::Null),
            ("mateSelection", Value::Null),
            ("mateSelectionAttempts", Value::Array(Vec::new())),
        ]);
        let (proposal, no_op) = finalize_crossover_proposal(
            base,
            object([("schemaVersion", Value::String("audit-v1".into()))]),
            object([("schemaVersion", Value::String("pair-v1".into()))]),
            format!("sha256:{}", "a".repeat(64)),
            &format!("sha256:{}", "b".repeat(64)),
            &format!("sha256:{}", "b".repeat(64)),
        )
        .unwrap();

        assert!(no_op);
        assert_eq!(proposal["disposition"], "no_op_proposal");
        assert!(proposal.get("pair").is_none());
        assert!(proposal.get("pairIdentitySha256").is_none());
        assert_eq!(
            proposal["proposalSha256"],
            canonical_sha256(&value_without(&proposal, "proposalSha256").unwrap()).unwrap()
        );
    }

    fn scope_fixture(timeframe: &str) -> (Value, Value, Value) {
        let catalog = object([
            (
                "timeframes",
                object([
                    ("M1", object([])),
                    ("M5", object([])),
                    ("M15", object([])),
                    ("H1", object([])),
                ]),
            ),
            (
                "indicators",
                Value::Array(vec![object([
                    (
                        "meta",
                        object([
                            ("id", Value::String("FIXTURE_INDICATOR".into())),
                            ("requiredPaddingBars", Value::from(260_u64)),
                        ]),
                    ),
                    (
                        "config",
                        object([
                            ("isActive", Value::Bool(true)),
                            ("timeframe", Value::String("M5".into())),
                            ("lookbackBars", Value::from(14_u64)),
                        ]),
                    ),
                ])]),
            ),
        ]);
        let candidate = object([(
            "sourceProfile",
            object([
                ("version", Value::String("v3".into())),
                ("directionMode", Value::String("both".into())),
                (
                    "instruments",
                    Value::Array(vec![Value::String("EURUSD".into())]),
                ),
                (
                    "indicators",
                    Value::Array(vec![object([
                        (
                            "meta",
                            object([
                                ("id", Value::String("FIXTURE_INDICATOR".into())),
                                ("instanceId", Value::String("sig".into())),
                            ]),
                        ),
                        (
                            "config",
                            object([
                                ("isActive", Value::Bool(true)),
                                ("useFormingBar", Value::Bool(false)),
                                ("timeframe", Value::String(timeframe.into())),
                            ]),
                        ),
                    ])]),
                ),
            ]),
        )]);
        let context = object([
            ("baseDecisionTimeframe", Value::String("M5".into())),
            (
                "orderedWindowPlanSemantic",
                Value::Array(vec![object([
                    ("windowId", Value::String("development".into())),
                    (
                        "window",
                        object([
                            (
                                "analysisWindowStart",
                                Value::String("2024-02-01T00:00:00Z".into()),
                            ),
                            (
                                "analysisWindowEnd",
                                Value::String("2024-03-01T00:00:00Z".into()),
                            ),
                        ]),
                    ),
                    (
                        "evidencePlanSemantic",
                        object([(
                            "lake_window_binding",
                            object([
                                (
                                    "window_semantic_sha256",
                                    Value::String(format!("sha256:{}", "e".repeat(64))),
                                ),
                                (
                                    "request",
                                    object([
                                        (
                                            "pairs",
                                            Value::Array(vec![Value::String("EURUSD".into())]),
                                        ),
                                        (
                                            "timeframes",
                                            Value::Array(vec![
                                                Value::String("M5".into()),
                                                Value::String("M15".into()),
                                                Value::String("H1".into()),
                                            ]),
                                        ),
                                        (
                                            "data_start",
                                            Value::String("2024-01-01T00:00:00Z".into()),
                                        ),
                                        ("data_end", Value::String("2024-03-01T00:00:00Z".into())),
                                    ]),
                                ),
                            ]),
                        )]),
                    ),
                ])]),
            ),
        ]);
        (candidate, context, catalog)
    }

    #[test]
    fn python_predeclared_lake_scope_golden_covers_in_and_out_of_scope_requests() {
        let (candidate, context, catalog) = scope_fixture("M15");
        let accepted =
            pair_predeclared_lake_scope_report(&candidate, Some(&context), Some(&catalog))
                .unwrap()
                .unwrap();
        assert_eq!(accepted["acceptable"], Value::Bool(true));
        assert_eq!(accepted["reason"], Value::Null);
        assert_eq!(
            accepted["windows"][0]["requiredRequest"]["data_start"],
            Value::String("2024-01-29T00:00:00Z".into())
        );
        assert_eq!(
            accepted["windows"][0]["requiredRequest"]["timeframes"],
            Value::Array(vec![
                Value::String("M15".into()),
                Value::String("M5".into())
            ])
        );

        let (candidate, context, catalog) = scope_fixture("M1");
        let rejected =
            pair_predeclared_lake_scope_report(&candidate, Some(&context), Some(&catalog))
                .unwrap()
                .unwrap();
        assert_eq!(rejected["acceptable"], Value::Bool(false));
        assert_eq!(
            rejected["reason"],
            Value::String("candidate_derived_request_outside_pre_attested_scope".into())
        );
        assert_eq!(
            rejected["windows"][0]["requiredRequest"]["data_start"],
            Value::String("2024-01-31T00:00:00Z".into())
        );
    }

    #[test]
    fn runtime_oracle_scope_fixture_covers_exact_in_and_out_of_scope_requests() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../../tests/fixtures/temporal_qd_runtime_oracle");
        let fixture: Value = serde_json::from_slice(
            &fs::read(root.join("fixture.json")).expect("runtime oracle fixture"),
        )
        .expect("runtime oracle fixture JSON");
        let manifest: Value = serde_json::from_slice(
            &fs::read(root.join("runtime-manifest.json")).expect("runtime oracle manifest"),
        )
        .expect("runtime oracle manifest JSON");
        let transcript: Value = serde_json::from_slice(
            &fs::read(root.join("dashboard-jsonl-transcript.json"))
                .expect("runtime oracle transcript"),
        )
        .expect("runtime oracle transcript JSON");

        let context = fixture["predeclaredScope"]["context"].clone();
        let catalog = manifest["pairRunConfig"]["longModule"]["catalog"].clone();
        let mut candidate = object([(
            "sourceProfile",
            transcript["records"][0]["request"]["sourceProfile"].clone(),
        )]);
        candidate["sourceProfile"]["indicators"][0]["config"]["timeframe"] =
            Value::String("M15".into());
        let in_scope =
            pair_predeclared_lake_scope_report(&candidate, Some(&context), Some(&catalog))
                .expect("in-scope report")
                .expect("in-scope report is present");
        assert_eq!(in_scope, fixture["predeclaredScope"]["inScope"]);

        candidate["sourceProfile"]["indicators"][0]["config"]["timeframe"] =
            Value::String("M1".into());
        let out_of_scope =
            pair_predeclared_lake_scope_report(&candidate, Some(&context), Some(&catalog))
                .expect("out-of-scope report")
                .expect("out-of-scope report is present");
        assert_eq!(out_of_scope, fixture["predeclaredScope"]["outOfScope"]);
    }

    #[test]
    fn frozen_side_indicator_registry_cache_matches_uncached_fixture_registry() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../../tests/fixtures/temporal_qd_runtime_oracle");
        let manifest_value: Value = serde_json::from_slice(
            &fs::read(root.join("runtime-manifest.json")).expect("runtime oracle manifest"),
        )
        .expect("runtime oracle manifest JSON");
        let manifest =
            RuntimeManifest::from_value(&manifest_value).expect("valid fixture manifest");
        let inputs = FrozenRuntimeInputs::from_manifest(&manifest).expect("valid frozen inputs");

        for side in [&inputs.long, &inputs.short] {
            // Cloning the cache handle must never reconstruct the catalog or
            // the operator map. This is the ownership invariant that keeps
            // construction/mutation paths allocation-bounded.
            let cache_handle = side.indicator_registry.clone();
            assert!(std::sync::Arc::ptr_eq(
                &cache_handle,
                &side.indicator_registry
            ));

            // Rebuilding from the exact fixture catalog remains the semantic
            // oracle for the cached immutable registry: every exported policy
            // and operator contract must be byte-identical.
            let uncached = IndicatorLearningRegistry::new(
                IndicatorCatalog::new(&side.catalog).expect("valid fixture catalog"),
            )
            .expect("valid fixture indicator registry");
            assert_eq!(
                side.indicator_registry.catalog().payload(),
                uncached.catalog().payload()
            );
            assert_eq!(
                side.indicator_registry.catalog().catalog_sha256(),
                uncached.catalog().catalog_sha256()
            );
            assert_eq!(side.indicator_registry.policy(), uncached.policy());
            assert_eq!(
                side.indicator_registry.operator_ids(),
                uncached.operator_ids()
            );
            for operator_id in side.indicator_registry.operator_ids() {
                assert_eq!(
                    side.indicator_registry
                        .get(&operator_id)
                        .expect("cached operator")
                        .specification(),
                    uncached
                        .get(&operator_id)
                        .expect("uncached operator")
                        .specification(),
                );
            }
        }
    }

    #[test]
    fn owned_runtime_manifest_matches_borrowed_parent_selection_oracle() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../../tests/fixtures/temporal_qd_runtime_oracle");
        let manifest_value: Value = serde_json::from_slice(
            &fs::read(root.join("runtime-manifest.json")).expect("runtime oracle manifest"),
        )
        .expect("runtime oracle manifest JSON");
        let borrowed = RuntimeManifest::from_value(&manifest_value)
            .expect("borrowed oracle manifest must validate");
        let owned = RuntimeManifest::from_owned_value(manifest_value)
            .expect("owned oracle manifest must validate");
        assert_eq!(
            borrowed.pair_run_config_sha256,
            owned.pair_run_config_sha256
        );
        assert_eq!(
            borrowed.parent_archive.archive_sha256(),
            owned.parent_archive.archive_sha256()
        );
        assert_eq!(
            borrowed.parent_archive.members().count(),
            owned.parent_archive.members().count()
        );
        let borrowed_selector =
            RuntimeParentSelector::from_manifest(&borrowed, true).expect("borrowed selector");
        let owned_selector =
            RuntimeParentSelector::from_manifest(&owned, true).expect("owned selector");
        assert_eq!(
            borrowed_selector.has_parents(),
            owned_selector.has_parents()
        );
        assert_eq!(
            borrowed_selector.eligible_parent_count(),
            owned_selector.eligible_parent_count()
        );
        assert_eq!(
            borrowed_selector.compact_state(),
            owned_selector.compact_state()
        );
    }

    #[test]
    fn dashboard_semantic_marker_remains_distinct_from_infrastructure_failure() {
        let rejected = classify_grammar_materialization_error(
            GrammarError::Invalid(semantic_rejection_marker(
                "module requires exactly one arm and entry plus at least one exit",
            )),
            "test materialization",
        );
        assert!(matches!(
            rejected,
            RuntimeError::SemanticRejection(message)
                if message == "module requires exactly one arm and entry plus at least one exit"
        ));

        let infrastructure = classify_grammar_materialization_error(
            GrammarError::Invalid("persistent validator transport closed".into()),
            "test materialization",
        );
        assert!(matches!(infrastructure, RuntimeError::Kernel(_)));
    }

    #[test]
    fn local_grammar_operator_failure_is_candidate_local() {
        let error = classify_grammar_operator_error(GrammarError::Invalid(
            "module requires exactly one arm and entry plus at least one exit".into(),
        ));
        assert!(
            matches!(error, RuntimeError::OperatorRejected { exception_type: "GrammarError", message } if message.contains("exactly one arm"))
        );
    }

    #[test]
    fn entry_route_indicator_cap_is_a_typed_candidate_local_rejection() {
        let error = classify_frozen_module_error(
            temporal_qd_kernel::genome::GenomeError::EntryRouteDecisionIndicatorCap,
            "test frozen module",
        );
        assert!(matches!(
            error,
            RuntimeError::OperatorRejected {
                exception_type: "EntryRouteDecisionIndicatorCapError",
                message,
            } if message == "entry decision route exceeds the distinct decision-indicator cap"
        ));
    }

    #[test]
    fn semantic_grammar_rejection_step_matches_python_journal_shape() {
        let operation = serde_json::json!({
            "kind": "typed_grammar",
            "plan": {"index": 4, "operation": "remove_branch"},
        });
        let step = operator_rejected_mutation_step(
            format!("sha256:{}", "1".repeat(64)),
            "long",
            serde_json::json!({"fixture": "real-invalid-module-parent"}),
            format!("sha256:{}", "2".repeat(64)),
            format!("sha256:{}", "3".repeat(64)),
            &operation,
            "GrammarError",
        )
        .expect("journalable semantic rejection");
        assert_eq!(step["disposition"], "operation_rejected");
        assert_eq!(step["operation"], operation);
        assert_eq!(step["rejection"]["reasonCode"], "operator_rejected");
        assert_eq!(step["rejection"]["exceptionType"], "GrammarError");
        assert_eq!(
            step["proposalSha256"],
            "sha256:e524b7e0b056e972cfef7f4969776db100157d1800566365a85c29a24c413274"
        );
        let mut material = step.clone();
        let identity = material
            .as_object_mut()
            .expect("step is object")
            .remove("proposalSha256")
            .expect("step has identity");
        assert_eq!(
            identity,
            Value::String(canonical_sha256(&material).unwrap())
        );
    }

    #[test]
    fn indicator_cap_rejection_step_matches_python_exception_label() {
        let operation = serde_json::json!({
            "kind": "indicator_learning",
            "plan": {
                "operatorId": "indicator_instance_structure_v1",
                "planId": "add_entry_indicator",
            },
        });
        let step = operator_rejected_mutation_step(
            format!("sha256:{}", "4".repeat(64)),
            "short",
            serde_json::json!({"fixture": "indicator-cap-parent"}),
            format!("sha256:{}", "5".repeat(64)),
            format!("sha256:{}", "6".repeat(64)),
            &operation,
            "EntryRouteDecisionIndicatorCapError",
        )
        .expect("journalable indicator-cap rejection");
        assert_eq!(step["disposition"], "operation_rejected");
        assert_eq!(
            step["rejection"]["exceptionType"],
            "EntryRouteDecisionIndicatorCapError"
        );
        assert_eq!(
            step["proposalSha256"],
            "sha256:86a314a75b16d6e588551ef025c63c9b93e5b0c7cfb743bbfbd11c8f0a935b8f",
            "the exception label is identity-bound in the rejection journal"
        );
    }

    #[test]
    fn accepted_typed_grammar_audit_keeps_the_full_plan_for_identity_parity() {
        let plan = serde_json::json!({
            "index": 4,
            "operation": "substitute",
            "productionId": "exit_on_signal",
        });
        let audit = typed_grammar_operation_audit("long", &plan);
        assert_eq!(audit["side"], "long");
        assert_eq!(audit["plan"], plan);
        assert!(audit.get("planSha256").is_none());
        assert_eq!(
            canonical_sha256(&audit).expect("audit identity"),
            "sha256:16066a9d682a735d54a83044df3c2f92074f3b659705c25f85006677d34bf12c"
        );
    }

    #[test]
    fn dynamic_initial_protection_rows_are_closed_selected_and_applied_like_python() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../qd-kernel/tests/fixtures");
        let catalog: Value = serde_json::from_slice(
            &fs::read(root.join("construction_catalog.json")).expect("construction catalog"),
        )
        .expect("construction catalog JSON");
        let profile: Value = serde_json::from_slice(
            &fs::read(root.join("construction_scalar_timeframe.json"))
                .expect("scalar construction profile"),
        )
        .expect("scalar construction profile JSON");

        let plans = dynamic_initial_protection_plans(&catalog, &profile)
            .expect("dynamic protection plan enumeration");
        assert!(
            !plans.is_empty(),
            "fixture must expose dynamic protection rows"
        );
        assert!(plans.iter().all(|plan| {
            plan.get("kind").and_then(Value::as_str) == Some("dynamic_construction")
                && plan.get("mutationClass").and_then(Value::as_str) == Some("kind_switch")
                && matches!(
                    plan.get("constructionPlan")
                        .and_then(Value::as_object)
                        .and_then(|row| row.get("construction"))
                        .and_then(Value::as_object)
                        .and_then(|row| row.get("site"))
                        .and_then(Value::as_str),
                    Some("initial_stop" | "initial_target")
                )
        }));
        let selected = plans[0].clone();
        let mut material = selected.clone();
        let identity = material
            .as_object_mut()
            .expect("dynamic wrapper")
            .remove("planSha256")
            .expect("dynamic wrapper identity");
        assert_eq!(
            identity,
            Value::String(canonical_sha256(&material).unwrap())
        );

        let (operator, raw_plan) =
            dynamic_initial_protection_operator_plan(&catalog, &profile, &selected)
                .expect("selected dynamic plan remains canonical");
        let preview = operator
            .preview(&profile, &raw_plan)
            .expect("dynamic preview");
        let (applied, application) = operator
            .apply(
                &profile,
                &raw_plan,
                &format!("sha256:{}", "a".repeat(64)),
                &format!("sha256:{}", "b".repeat(64)),
            )
            .expect("dynamic application");
        assert_eq!(applied, preview, "dynamic application must equal preview");
        assert!(application.get("applicationSha256").is_some());

        let outer_choice = object([
            ("kind", Value::String("initial_protection".into())),
            ("plan", selected),
        ]);
        assert_eq!(
            canonical_sha256(&outer_choice).expect("outer choice identity"),
            "sha256:eb200267db1633ce9086328456f356f14947da0abc7714373768f079a3ffafb5"
        );
    }
}

impl<D: DashboardPort> NativePairAuthority for RuntimePairAuthority<D> {
    fn authority_identity(&self) -> &Value {
        &self.inputs.native_authority_identity
    }

    fn execute(
        &mut self,
        intent: &ProposalIntent,
        context: &NativeConstructionContext,
    ) -> temporal_qd_kernel::factory::Result<NativeProposal> {
        let result = match intent {
            ProposalIntent::RichImmigrant { proposal_seed, .. } => {
                self.rich_immigrant(proposal_seed, context)
            }
            ProposalIntent::StructuralMutation {
                proposal_seed,
                parent,
                mutation_depth,
            } => {
                let pair = FrozenPair::from_payload(&parent.pair_payload).map_err(|error| {
                    temporal_qd_kernel::factory::FactoryError::Authority(format!(
                        "structural parent is invalid: {error}"
                    ))
                })?;
                if pair.identity_sha256().map_err(|error| {
                    temporal_qd_kernel::factory::FactoryError::Authority(error.to_string())
                })? != parent.pair_identity_sha256
                {
                    return Err(temporal_qd_kernel::factory::FactoryError::Contract(
                        "structural parent identity drifted".into(),
                    ));
                }
                self.structural_mutation(
                    proposal_seed,
                    pair,
                    *mutation_depth,
                    parent.selection_audit.clone(),
                    context,
                )
            }
            ProposalIntent::SameSideCrossover {
                proposal_seed,
                parent,
                mate,
                mate_selection_attempts,
                ..
            } => {
                let parent_pair =
                    FrozenPair::from_payload(&parent.pair_payload).map_err(|error| {
                        temporal_qd_kernel::factory::FactoryError::Authority(format!(
                            "crossover parent is invalid: {error}"
                        ))
                    })?;
                let mate_pair = FrozenPair::from_payload(&mate.pair_payload).map_err(|error| {
                    temporal_qd_kernel::factory::FactoryError::Authority(format!(
                        "crossover mate is invalid: {error}"
                    ))
                })?;
                self.crossover(
                    proposal_seed,
                    parent_pair,
                    mate_pair,
                    parent.selection_audit.clone(),
                    mate.selection_audit.clone(),
                    mate_selection_attempts.clone(),
                    context,
                )
            }
        };
        result.map_err(|error| {
            temporal_qd_kernel::factory::FactoryError::Authority(error.to_string())
        })
    }
}

struct RuntimeCrossover<'a, D> {
    runtime: &'a RuntimePairAuthority<D>,
}

impl<D: DashboardPort> SameSideCrossover for RuntimeCrossover<'_, D> {
    #[allow(clippy::too_many_arguments)]
    fn crossover(
        &self,
        left: &Value,
        right: &Value,
        direction: &str,
        _seed: &str,
    ) -> std::result::Result<Value, temporal_qd_kernel::genome::GenomeError> {
        let side =
            self.runtime.inputs.side(direction).map_err(|error| {
                temporal_qd_kernel::genome::GenomeError::Invalid(error.to_string())
            })?;
        let native = self.runtime.native_adapter();
        let grammar = TypedFragmentGrammar::new(
            self.runtime.grammar_context(side).map_err(|error| {
                temporal_qd_kernel::genome::GenomeError::Invalid(error.to_string())
            })?,
            &native,
        )
        .map_err(|error| temporal_qd_kernel::genome::GenomeError::Invalid(error.to_string()))?;
        let left = ModuleProgram::from_canonical(left)
            .map_err(|error| temporal_qd_kernel::genome::GenomeError::Invalid(error.to_string()))?;
        let right = ModuleProgram::from_canonical(right)
            .map_err(|error| temporal_qd_kernel::genome::GenomeError::Invalid(error.to_string()))?;
        Ok(grammar
            .crossover(&left, &right, direction)
            .map_err(|error| temporal_qd_kernel::genome::GenomeError::Invalid(error.to_string()))?
            .canonical())
    }
}

impl<D: DashboardPort> RuntimePairAuthority<D> {
    #[allow(clippy::too_many_arguments)]
    fn crossover(
        &self,
        seed: &str,
        parent: FrozenPair,
        mate: FrozenPair,
        parent_selection: Option<Value>,
        mate_selection: Option<Value>,
        attempts: Vec<Value>,
        context: &NativeConstructionContext,
    ) -> Result<NativeProposal> {
        let side = proposal_side(seed)
            .map_err(|error| invalid(error.to_string()))?
            .as_str();
        let parent_payload = parent
            .canonical_payload()
            .map_err(|error| invalid(error.to_string()))?;
        let parent_identity = parent
            .identity_sha256()
            .map_err(|error| invalid(error.to_string()))?;
        let mate_payload = mate
            .canonical_payload()
            .map_err(|error| invalid(error.to_string()))?;
        let mate_identity = mate
            .identity_sha256()
            .map_err(|error| invalid(error.to_string()))?;
        let proposal_base = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_pair_proposal_v2".into()),
            ),
            (
                "proposalKind",
                Value::String("temporal_qd_same_side_crossover_v1".into()),
            ),
            ("proposalSeed", Value::String(seed.into())),
            ("originKind", Value::String("structural_offspring".into())),
            ("side", Value::String(side.into())),
            ("parentPair", parent_payload),
            ("parentPairIdentitySha256", Value::String(parent_identity)),
            ("matePair", mate_payload),
            ("matePairIdentitySha256", Value::String(mate_identity)),
            (
                "parentSelection",
                parent_selection.clone().unwrap_or(Value::Null),
            ),
            (
                "mateSelection",
                mate_selection.clone().unwrap_or(Value::Null),
            ),
            ("mateSelectionAttempts", Value::Array(attempts.clone())),
        ]);
        let target = if side == "long" {
            &parent.long
        } else {
            &parent.short
        };
        let mate_target = if side == "long" {
            &mate.long
        } else {
            &mate.short
        };
        let (program, record) = match deterministic_same_side_crossover(
            target,
            mate_target,
            seed,
            &RuntimeCrossover { runtime: self },
        ) {
            Ok(result) => result,
            Err(_) => {
                let mut proposal = proposal_base;
                proposal
                    .as_object_mut()
                    .expect("crossover proposal")
                    .extend([
                        (
                            "disposition".into(),
                            Value::String("operation_rejected".into()),
                        ),
                        (
                            "rejection".into(),
                            object([
                                (
                                    "schemaVersion",
                                    Value::String("temporal_qd_pair_rejection_audit_v1".into()),
                                ),
                                ("reasonCode", Value::String("crossover_rejected".into())),
                                (
                                    "exceptionType",
                                    Value::String("BidirectionalGenomeError".into()),
                                ),
                            ]),
                        ),
                    ]);
                let proposal_sha = canonical_sha256(&proposal)?;
                proposal
                    .as_object_mut()
                    .expect("crossover proposal")
                    .insert("proposalSha256".into(), Value::String(proposal_sha));
                return Ok(NativeProposal::rejected(proposal));
            }
        };
        let native = self.native_adapter();
        let grammar =
            TypedFragmentGrammar::new(self.grammar_context(self.inputs.side(side)?)?, &native)
                .map_err(|error| RuntimeError::Kernel(error.to_string()))?;
        let child_program = ModuleProgram::from_canonical(&program)
            .map_err(|error| RuntimeError::Kernel(error.to_string()))?;
        let profile = grammar
            .materialize_profile(&child_program)
            .map_err(|error| RuntimeError::Kernel(error.to_string()))?;
        let child_hash = canonical_sha256(&object([
            ("seed", Value::String(seed.into())),
            ("side", Value::String(side.into())),
        ]))?;
        let changed = self.freeze_module(
            target,
            &program,
            &profile,
            &format!("qd_pair_cross_{}", &child_hash[7..31]),
            &[
                target.lineage.clone(),
                vec![object([
                    ("operation", Value::String("same_side_crossover".into())),
                    ("side", Value::String(side.into())),
                    (
                        "childProgramSha256",
                        Value::String(canonical_sha256(&program)?),
                    ),
                ])],
            ]
            .concat(),
        )?;
        let mut side_record = record;
        side_record
            .as_object_mut()
            .expect("record")
            .insert("side".into(), Value::String(side.into()));
        let lineage = [
            parent.side_targeted_lineage.clone(),
            vec![side_record.clone()],
        ]
        .concat();
        let pair_hash = canonical_sha256(&object([
            ("seed", Value::String(seed.into())),
            (
                "parent",
                Value::String(
                    parent
                        .identity_sha256()
                        .map_err(|error| invalid(error.to_string()))?,
                ),
            ),
            (
                "mate",
                Value::String(
                    mate.identity_sha256()
                        .map_err(|error| invalid(error.to_string()))?,
                ),
            ),
        ]))?;
        let pair = self.compile_pair(
            if side == "long" {
                changed.clone()
            } else {
                parent.long.clone()
            },
            if side == "long" {
                parent.short.clone()
            } else {
                changed
            },
            &format!("qd_pair_cross_{}", &pair_hash[7..31]),
            &lineage,
        )?;
        let mut audit = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_pair_crossover_audit_v1".into()),
            ),
            ("side", Value::String(side.into())),
            ("sameSide", Value::Bool(true)),
            ("operation", side_record),
            (
                "pairIdentitySha256",
                Value::String(
                    pair.identity_sha256()
                        .map_err(|error| invalid(error.to_string()))?,
                ),
            ),
        ]);
        let audit_sha = canonical_sha256(&audit)?;
        audit
            .as_object_mut()
            .expect("audit")
            .insert("auditSha256".into(), Value::String(audit_sha));
        let parent_semantic = executable_pair_semantic_sha256(
            &parent.long.profile_sha256,
            &parent.short.profile_sha256,
        );
        let child_semantic =
            executable_pair_semantic_sha256(&pair.long.profile_sha256, &pair.short.profile_sha256);
        let (proposal, no_op) = finalize_crossover_proposal(
            proposal_base,
            audit,
            pair.canonical_payload()
                .map_err(|error| invalid(error.to_string()))?,
            pair.identity_sha256()
                .map_err(|error| invalid(error.to_string()))?,
            &parent_semantic,
            &child_semantic,
        )?;
        if no_op {
            Ok(NativeProposal::rejected(proposal))
        } else {
            self.materialized_native(pair, proposal, context)
        }
    }
}

impl<D: DashboardPort> RuntimePairAuthority<D> {
    fn mutation_choices(&self, module: &FrozenModule) -> Result<Vec<Value>> {
        let side = self.inputs.side(&module.direction)?;
        let native = self.native_adapter();
        let grammar =
            TypedFragmentGrammar::new(self.grammar_context(side)?, &native).map_err(|error| {
                RuntimeError::Kernel(format!("mutation grammar cannot initialize: {error}"))
            })?;
        let program = ModuleProgram::from_canonical(&module.program).map_err(|error| {
            RuntimeError::Kernel(format!("mutation parent program is invalid: {error}"))
        })?;
        let mut choices = grammar
            .enumerate_operations(&program)
            .map_err(|error| {
                RuntimeError::Kernel(format!("mutation grammar plans are invalid: {error}"))
            })?
            .into_iter()
            .map(|plan| {
                object([
                    ("kind", Value::String("typed_grammar".into())),
                    ("plan", plan),
                ])
            })
            .collect::<Vec<_>>();
        choices.extend(
            side.indicator_registry
                .enumerate_plans(&module.profile)
                .map_err(|error| RuntimeError::Kernel(error.to_string()))?
                .into_iter()
                // A plan is eligible only when the same frozen operator can
                // preview it against the exact current module. This closes a
                // catalog/fragment-shape edge where enumeration advertised a
                // construction whose recorded parent fields could not apply.
                .filter(|plan| {
                    plan.get("operatorId")
                        .and_then(Value::as_str)
                        .and_then(|operator_id| side.indicator_registry.get(operator_id).ok())
                        .is_some_and(|operator| operator.preview(&module.profile, plan).is_ok())
                })
                .map(|plan| {
                    object([
                        ("kind", Value::String("indicator_learning".into())),
                        ("plan", plan),
                    ])
                }),
        );
        let holds = require_object(&self.inputs.hold_policy, "frozen hold policy")?
            .get("choices")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("frozen hold choices are invalid"))?;
        let plans = module
            .profile
            .get("executionConfig")
            .and_then(Value::as_object)
            .and_then(|v| v.get("managementLibrary"))
            .and_then(Value::as_object)
            .and_then(|v| v.get("plans"))
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("mutation profile lacks management plans"))?;
        for plan in plans {
            let plan_id = plan
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("mutation management plan is invalid"))?;
            let old = canonical_hold(plan.get("holdPolicy"))
                .map_err(|error| RuntimeError::Kernel(error.to_string()))?;
            for new_hold in holds {
                if canonical_hold(Some(new_hold))
                    .map_err(|error| RuntimeError::Kernel(error.to_string()))?
                    != old
                {
                    choices.push(object([
                        ("kind", Value::String("hold".into())),
                        ("planId", Value::String(plan_id.into())),
                        ("newHold", new_hold.clone()),
                    ]));
                }
            }
        }
        choices.extend(
            enumerate_initial_protection_plans(
                &module.profile,
                &self.inputs.initial_protection_policy,
            )
            .map_err(|error| RuntimeError::Kernel(error.to_string()))?
            .into_iter()
            .map(|plan| {
                object([
                    ("kind", Value::String("initial_protection".into())),
                    ("plan", plan),
                ])
            }),
        );
        choices.extend(
            dynamic_initial_protection_plans(&side.catalog, &module.profile)?
                .into_iter()
                .map(|plan| {
                    object([
                        ("kind", Value::String("initial_protection".into())),
                        ("plan", plan),
                    ])
                }),
        );
        choices.sort_by_key(|choice| canonical_sha256(choice).expect("finite mutation choice"));
        Ok(choices)
    }

    fn select_mutation_choice(&self, seed: &str, parent: &str, choices: &[Value]) -> Result<Value> {
        if choices.is_empty() {
            return Err(invalid("mutation has no eligible operation"));
        }
        let mut kinds = choices
            .iter()
            .filter_map(|row| row.get("kind").and_then(Value::as_str))
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        kinds.sort();
        kinds.dedup();
        let family_seed = canonical_sha256(&object([
            ("seed", Value::String(seed.into())),
            ("parent", Value::String(parent.into())),
            ("draw", Value::String("family".into())),
        ]))?;
        let family = &kinds[unbiased_choice(&family_seed, kinds.len())?];
        let rows = choices
            .iter()
            .filter(|row| row.get("kind").and_then(Value::as_str) == Some(family))
            .cloned()
            .collect::<Vec<_>>();
        if family != "initial_protection" {
            let plan_seed = canonical_sha256(&object([
                ("seed", Value::String(seed.into())),
                ("parent", Value::String(parent.into())),
                ("draw", Value::String("plan".into())),
                ("family", Value::String(family.clone())),
            ]))?;
            return Ok(rows[unbiased_choice(&plan_seed, rows.len())?].clone());
        }
        let weights = BTreeMap::from([
            ("adjacent", 70_usize),
            ("jump", 25_usize),
            ("kind_switch", 5_usize),
        ]);
        let mut groups = BTreeMap::<String, Vec<Value>>::new();
        for row in rows {
            let class = row
                .get("plan")
                .and_then(Value::as_object)
                .and_then(|plan| plan.get("mutationClass"))
                .and_then(Value::as_str)
                .filter(|class| weights.contains_key(*class))
                .ok_or_else(|| invalid("initial protection choice has no admitted class"))?;
            groups.entry(class.to_owned()).or_default().push(row);
        }
        let total = groups.keys().map(|class| weights[class.as_str()]).sum();
        let class_seed = canonical_sha256(&object([
            ("seed", Value::String(seed.into())),
            ("parent", Value::String(parent.into())),
            ("draw", Value::String("protection_class".into())),
        ]))?;
        let bucket = unbiased_choice(&class_seed, total)?;
        let mut selected = groups.keys().last().expect("classes nonempty").clone();
        let mut cursor = 0;
        for class in groups.keys() {
            cursor += weights[class.as_str()];
            if bucket < cursor {
                selected = class.clone();
                break;
            }
        }
        let rows = &groups[&selected];
        let plan_seed = canonical_sha256(&object([
            ("seed", Value::String(seed.into())),
            ("parent", Value::String(parent.into())),
            ("draw", Value::String("protection_plan".into())),
            ("class", Value::String(selected)),
        ]))?;
        Ok(rows[unbiased_choice(&plan_seed, rows.len())?].clone())
    }

    fn apply_mutation_choice(
        &self,
        parent: &FrozenPair,
        seed: &str,
        choice: &Value,
    ) -> Result<(FrozenPair, Value)> {
        let side_name = proposal_side(seed)
            .map_err(|error| invalid(error.to_string()))?
            .as_str();
        let (target, other) = if side_name == "long" {
            (&parent.long, &parent.short)
        } else {
            (&parent.short, &parent.long)
        };
        let parent_id = parent
            .identity_sha256()
            .map_err(|error| invalid(error.to_string()))?;
        let candidate_id = mutation_candidate_id_for_choice(seed, &parent_id, choice)?;
        let kind = choice
            .get("kind")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("mutation choice lacks kind"))?;
        if kind == "hold" {
            let plan = HoldMutationPlan::create(
                target,
                choice
                    .get("planId")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid("hold choice lacks plan id"))?,
                choice.get("newHold"),
            )
            .map_err(|error| RuntimeError::Kernel(error.to_string()))?;
            let pair = apply_pair_hold_mutation(
                parent,
                &plan,
                &self.native_adapter(),
                &self.native_adapter(),
                &candidate_id,
            )
            .map_err(|error| RuntimeError::Kernel(error.to_string()))?;
            return Ok((
                pair,
                object([
                    (
                        "schemaVersion",
                        Value::String("temporal_qd_pair_hold_audit_v1".into()),
                    ),
                    ("side", Value::String(side_name.into())),
                    (
                        "holdMutationPlanSha256",
                        Value::String(
                            plan.plan_sha256()
                                .map_err(|error| invalid(error.to_string()))?,
                        ),
                    ),
                ]),
            ));
        }
        let (changed, audit) = if kind == "typed_grammar" {
            let runtime_side = self.inputs.side(side_name)?;
            let native = self.native_adapter();
            let grammar = TypedFragmentGrammar::new(self.grammar_context(runtime_side)?, &native)
                .map_err(|error| RuntimeError::Kernel(error.to_string()))?;
            let parent_program = ModuleProgram::from_canonical(&target.program)
                .map_err(|error| RuntimeError::Kernel(error.to_string()))?;
            let plan = choice.get("plan").expect("typed grammar plan");
            let program = grammar
                .apply(&parent_program, plan)
                .map_err(classify_grammar_operator_error)?;
            let profile = grammar.materialize_profile(&program).map_err(|error| {
                classify_grammar_materialization_error(
                    error,
                    "mutation grammar profile materialization rejected",
                )
            })?;
            let lineage = [
                target.lineage.clone(),
                vec![object([
                    ("operation", Value::String("typed_grammar".into())),
                    ("side", Value::String(side_name.into())),
                    ("plan", plan.clone()),
                    ("planSha256", Value::String(canonical_sha256(plan)?)),
                ])],
            ]
            .concat();
            let changed = self.freeze_module(
                target,
                &program.canonical(),
                &profile,
                &format!("{candidate_id}_{side_name}"),
                &lineage,
            )?;
            (changed, typed_grammar_operation_audit(side_name, plan))
        } else if kind == "indicator_learning" {
            let runtime_side = self.inputs.side(side_name)?;
            let plan = choice.get("plan").expect("indicator plan");
            let operator = runtime_side
                .indicator_registry
                .get(
                    plan.get("operatorId")
                        .and_then(Value::as_str)
                        .ok_or_else(|| invalid("indicator plan lacks operator"))?,
                )
                .map_err(|error| RuntimeError::Kernel(error.to_string()))?;
            let preview = operator
                .preview(&target.profile, plan)
                .map_err(|error| RuntimeError::Kernel(error.to_string()))?;
            let report = self
                .dashboard
                .validate_v2(&preview, &format!("{candidate_id}_{side_name}"))?;
            let program_hash = report
                .get("programSha256")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("Dashboard report lacks program hash"))?;
            let (profile, application) = operator
                .apply(
                    &target.profile,
                    plan,
                    &target.native_program_sha256,
                    program_hash,
                )
                .map_err(|error| RuntimeError::Kernel(error.to_string()))?;
            if profile != preview {
                return Err(invalid("indicator mutation preview/application diverged"));
            }
            let lineage = [
                target.lineage.clone(),
                vec![object([
                    ("operation", Value::String("indicator_learning".into())),
                    ("side", Value::String(side_name.into())),
                    ("plan", plan.clone()),
                    (
                        "planSha256",
                        plan.get("planSha256").cloned().unwrap_or(Value::Null),
                    ),
                    ("application", application.clone()),
                ])],
            ]
            .concat();
            let changed = self.freeze_module_with_report(
                target,
                &target.program,
                &profile,
                &report,
                &lineage,
            )?;
            (
                changed,
                object([
                    (
                        "schemaVersion",
                        Value::String("temporal_qd_indicator_operation_audit_v1".into()),
                    ),
                    ("side", Value::String(side_name.into())),
                    (
                        "operatorId",
                        plan.get("operatorId").cloned().unwrap_or(Value::Null),
                    ),
                    (
                        "planSha256",
                        plan.get("planSha256").cloned().unwrap_or(Value::Null),
                    ),
                    (
                        "applicationSha256",
                        application
                            .get("applicationSha256")
                            .cloned()
                            .unwrap_or(Value::Null),
                    ),
                ]),
            )
        } else if kind == "initial_protection" {
            let plan = choice.get("plan").expect("initial protection plan");
            let (profile, application, native_report) =
                if plan.get("kind").and_then(Value::as_str) == Some("dynamic_construction") {
                    let runtime_side = self.inputs.side(side_name)?;
                    let (operator, construction_plan) = dynamic_initial_protection_operator_plan(
                        &runtime_side.catalog,
                        &target.profile,
                        plan,
                    )?;
                    let preview = operator
                        .preview(&target.profile, &construction_plan)
                        .map_err(|error| {
                            RuntimeError::Kernel(format!(
                                "dynamic initial protection preview failed: {error}"
                            ))
                        })?;
                    let report = self
                        .dashboard
                        .validate_v2(&preview, &format!("{candidate_id}_{side_name}"))?;
                    let child_program = require_string(
                        require_object(&report, "dynamic initial protection validation")?,
                        "programSha256",
                        "dynamic initial protection validation",
                    )?;
                    let (profile, application) = operator
                        .apply(
                            &target.profile,
                            &construction_plan,
                            &target.native_program_sha256,
                            &child_program,
                        )
                        .map_err(|error| {
                            RuntimeError::Kernel(format!(
                                "dynamic initial protection application failed: {error}"
                            ))
                        })?;
                    if profile != preview {
                        return Err(invalid(
                            "dynamic initial protection preview/application diverged",
                        ));
                    }
                    (profile, application, Some(report))
                } else {
                    let (profile, application) = apply_initial_protection_plan(
                        &target.profile,
                        plan,
                        &self.inputs.initial_protection_policy,
                    )
                    .map_err(|error| RuntimeError::Kernel(error.to_string()))?;
                    (profile, application, None)
                };
            let lineage = [
                target.lineage.clone(),
                vec![object([
                    ("operation", Value::String("initial_protection".into())),
                    ("side", Value::String(side_name.into())),
                    ("plan", plan.clone()),
                    (
                        "planSha256",
                        plan.get("planSha256").cloned().unwrap_or(Value::Null),
                    ),
                    ("application", application.clone()),
                ])],
            ]
            .concat();
            let changed = match native_report {
                Some(report) => self.freeze_module_with_report(
                    target,
                    &target.program,
                    &profile,
                    &report,
                    &lineage,
                )?,
                None => self.freeze_module(
                    target,
                    &target.program,
                    &profile,
                    &format!("{candidate_id}_{side_name}"),
                    &lineage,
                )?,
            };
            (
                changed,
                object([
                    (
                        "schemaVersion",
                        Value::String("temporal_qd_initial_protection_operation_audit_v1".into()),
                    ),
                    ("side", Value::String(side_name.into())),
                    (
                        "planSha256",
                        plan.get("planSha256").cloned().unwrap_or(Value::Null),
                    ),
                    (
                        "applicationSha256",
                        application
                            .get("applicationSha256")
                            .cloned()
                            .unwrap_or(Value::Null),
                    ),
                ]),
            )
        } else {
            return Err(invalid("pair mutation operation kind is unknown"));
        };
        let mut audit = audit;
        audit.as_object_mut().expect("audit").extend([
            (
                "parentModuleIdentitySha256".into(),
                Value::String(
                    target
                        .identity_sha256()
                        .map_err(|error| invalid(error.to_string()))?,
                ),
            ),
            (
                "childModuleIdentitySha256".into(),
                Value::String(
                    changed
                        .identity_sha256()
                        .map_err(|error| invalid(error.to_string()))?,
                ),
            ),
            (
                "nativeValidationReportSha256".into(),
                Value::String(changed.native_validation_report_sha256.clone()),
            ),
        ]);
        let audit_sha = canonical_sha256(&audit)?;
        audit
            .as_object_mut()
            .expect("audit")
            .insert("auditSha256".into(), Value::String(audit_sha));
        let lineage = [
            parent.side_targeted_lineage.clone(),
            vec![object([
                ("operation", Value::String(kind.into())),
                ("side", Value::String(side_name.into())),
                ("proposalSeed", Value::String(seed.into())),
                ("operationSha256", Value::String(canonical_sha256(choice)?)),
                ("audit", audit.clone()),
            ])],
        ]
        .concat();
        let pair = self.compile_pair(
            if side_name == "long" {
                changed.clone()
            } else {
                other.clone()
            },
            if side_name == "long" {
                other.clone()
            } else {
                changed
            },
            &candidate_id,
            &lineage,
        )?;
        Ok((pair, audit))
    }

    fn structural_mutation(
        &self,
        proposal_seed: &str,
        root: FrozenPair,
        depth: u8,
        parent_selection: Option<Value>,
        context: &NativeConstructionContext,
    ) -> Result<NativeProposal> {
        let root_payload = root
            .canonical_payload()
            .map_err(|error| invalid(error.to_string()))?;
        let root_id = root
            .identity_sha256()
            .map_err(|error| invalid(error.to_string()))?;
        let mut current = root;
        let mut steps = Vec::new();
        for index in 0..u64::from(depth) {
            let current_id = current
                .identity_sha256()
                .map_err(|error| invalid(error.to_string()))?;
            let step_seed = mutation_step_seed(proposal_seed, index, &current_id);
            let side = proposal_side(&step_seed)
                .map_err(|error| invalid(error.to_string()))?
                .as_str();
            let choices = self.mutation_choices(if side == "long" {
                &current.long
            } else {
                &current.short
            })?;
            if choices.is_empty() {
                let mut rejected_step = object([
                    (
                        "schemaVersion",
                        Value::String("temporal_qd_pair_proposal_v2".into()),
                    ),
                    ("proposalSeed", Value::String(step_seed)),
                    ("originKind", Value::String("structural_offspring".into())),
                    ("side", Value::String(side.into())),
                    (
                        "parentPair",
                        current
                            .canonical_payload()
                            .map_err(|error| invalid(error.to_string()))?,
                    ),
                    ("parentPairIdentitySha256", Value::String(current_id)),
                    (
                        "untouchedOppositeModuleIdentitySha256",
                        Value::String(
                            (if side == "long" {
                                current.short.identity_sha256()
                            } else {
                                current.long.identity_sha256()
                            })
                            .map_err(|error| invalid(error.to_string()))?,
                        ),
                    ),
                    (
                        "disposition",
                        Value::String("no_eligible_side_operation".into()),
                    ),
                    ("eligibleOperationCount", Value::from(0_u64)),
                    (
                        "rejection",
                        object([
                            (
                                "schemaVersion",
                                Value::String("temporal_qd_pair_rejection_audit_v1".into()),
                            ),
                            (
                                "reasonCode",
                                Value::String("no_eligible_side_operation".into()),
                            ),
                            ("side", Value::String(side.into())),
                            ("eligibleOperationCount", Value::from(0_u64)),
                        ]),
                    ),
                ]);
                let step_sha = canonical_sha256(&rejected_step)?;
                rejected_step
                    .as_object_mut()
                    .expect("rejected proposal step")
                    .insert("proposalSha256".into(), Value::String(step_sha));
                steps.push(rejected_step);
                return self.sequence_disposition(
                    proposal_seed,
                    &root_payload,
                    &root_id,
                    depth,
                    steps,
                    side,
                    "operation_rejected",
                    parent_selection,
                );
            }
            let choice = self.select_mutation_choice(&step_seed, &current_id, &choices)?;
            let (child, audit) = match self.apply_mutation_choice(&current, &step_seed, &choice) {
                Ok(result) => result,
                // A completed validator response that rejects an otherwise
                // well-formed local operation is candidate-local, exactly as
                // on the Python oracle path.  Journal the deterministic
                // rejection and continue at the next proposal ordinal.  Do
                // not catch transport, protocol, frozen-authority, or local
                // contract failures here: those remain fail-closed.
                Err(RuntimeError::OperatorRejected { exception_type, .. }) => {
                    let rejected_step = operator_rejected_mutation_step(
                        step_seed,
                        side,
                        current
                            .canonical_payload()
                            .map_err(|error| invalid(error.to_string()))?,
                        current_id,
                        (if side == "long" {
                            current.short.identity_sha256()
                        } else {
                            current.long.identity_sha256()
                        })
                        .map_err(|error| invalid(error.to_string()))?,
                        &choice,
                        exception_type,
                    )?;
                    steps.push(rejected_step);
                    return self.sequence_disposition(
                        proposal_seed,
                        &root_payload,
                        &root_id,
                        depth,
                        steps,
                        side,
                        "operation_rejected",
                        parent_selection,
                    );
                }
                Err(RuntimeError::SemanticRejection(_)) => {
                    let exception_type = match choice.get("kind").and_then(Value::as_str) {
                        Some("typed_grammar") => "GrammarError",
                        _ => "TemporalDiscoveryContractError",
                    };
                    let rejected_step = operator_rejected_mutation_step(
                        step_seed,
                        side,
                        current
                            .canonical_payload()
                            .map_err(|error| invalid(error.to_string()))?,
                        current_id,
                        (if side == "long" {
                            current.short.identity_sha256()
                        } else {
                            current.long.identity_sha256()
                        })
                        .map_err(|error| invalid(error.to_string()))?,
                        &choice,
                        exception_type,
                    )?;
                    steps.push(rejected_step);
                    return self.sequence_disposition(
                        proposal_seed,
                        &root_payload,
                        &root_id,
                        depth,
                        steps,
                        side,
                        "operation_rejected",
                        parent_selection,
                    );
                }
                Err(error) => return Err(error),
            };
            let mut step = object([
                (
                    "schemaVersion",
                    Value::String("temporal_qd_pair_proposal_v2".into()),
                ),
                ("proposalSeed", Value::String(step_seed.clone())),
                ("originKind", Value::String("structural_offspring".into())),
                ("side", Value::String(side.into())),
                (
                    "parentPair",
                    current
                        .canonical_payload()
                        .map_err(|error| invalid(error.to_string()))?,
                ),
                (
                    "parentPairIdentitySha256",
                    Value::String(current_id.clone()),
                ),
                (
                    "untouchedOppositeModuleIdentitySha256",
                    Value::String(
                        (if side == "long" {
                            current.short.identity_sha256()
                        } else {
                            current.long.identity_sha256()
                        })
                        .map_err(|error| invalid(error.to_string()))?,
                    ),
                ),
                ("disposition", Value::String("materialized".into())),
                ("operation", choice.clone()),
                ("operationAudit", audit),
                (
                    "pair",
                    child
                        .canonical_payload()
                        .map_err(|error| invalid(error.to_string()))?,
                ),
                (
                    "pairIdentitySha256",
                    Value::String(
                        child
                            .identity_sha256()
                            .map_err(|error| invalid(error.to_string()))?,
                    ),
                ),
            ]);
            if choice.get("kind").and_then(Value::as_str) != Some("hold") {
                step.as_object_mut().expect("proposal step").insert(
                    "changedModule".into(),
                    (if side == "long" {
                        &child.long
                    } else {
                        &child.short
                    })
                    .canonical_payload()
                    .map_err(|error| invalid(error.to_string()))?,
                );
            } else {
                let hold_plan = HoldMutationPlan::create(
                    if side == "long" {
                        &current.long
                    } else {
                        &current.short
                    },
                    choice
                        .get("planId")
                        .and_then(Value::as_str)
                        .ok_or_else(|| invalid("hold choice lacks plan id"))?,
                    choice.get("newHold"),
                )
                .map_err(|error| RuntimeError::Kernel(error.to_string()))?;
                step.as_object_mut()
                    .expect("proposal step")
                    .insert("holdMutationPlan".into(), hold_plan.canonical_payload());
            }
            let step_sha = canonical_sha256(&step)?;
            step.as_object_mut()
                .expect("step")
                .insert("proposalSha256".into(), Value::String(step_sha));
            let before = executable_pair_semantic_sha256(
                &current.long.profile_sha256,
                &current.short.profile_sha256,
            );
            let after = executable_pair_semantic_sha256(
                &child.long.profile_sha256,
                &child.short.profile_sha256,
            );
            steps.push(step);
            if before == after {
                return self.sequence_disposition(
                    proposal_seed,
                    &root_payload,
                    &root_id,
                    depth,
                    steps,
                    side,
                    "no_op_proposal",
                    parent_selection,
                );
            }
            current = child;
        }
        let side = steps
            .last()
            .and_then(|step| step.get("side"))
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("structural mutation has no steps"))?;
        let mut proposal = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_pair_proposal_v2".into()),
            ),
            ("proposalSeed", Value::String(proposal_seed.into())),
            ("originKind", Value::String("structural_offspring".into())),
            ("side", Value::String(side.into())),
            ("parentPair", root_payload),
            ("parentPairIdentitySha256", Value::String(root_id)),
            ("mutationDepth", Value::from(depth)),
            ("mutationSteps", Value::Array(steps)),
            ("disposition", Value::String("materialized".into())),
            (
                "pair",
                current
                    .canonical_payload()
                    .map_err(|error| invalid(error.to_string()))?,
            ),
            (
                "pairIdentitySha256",
                Value::String(
                    current
                        .identity_sha256()
                        .map_err(|error| invalid(error.to_string()))?,
                ),
            ),
        ]);
        if let Some(audit) = parent_selection {
            proposal
                .as_object_mut()
                .expect("proposal")
                .insert("parentSelection".into(), audit);
        }
        let proposal_sha = canonical_sha256(&proposal)?;
        proposal
            .as_object_mut()
            .expect("proposal")
            .insert("proposalSha256".into(), Value::String(proposal_sha));
        self.materialized_native(current, proposal, context)
    }

    #[allow(clippy::too_many_arguments)]
    fn sequence_disposition(
        &self,
        proposal_seed: &str,
        root: &Value,
        root_id: &str,
        depth: u8,
        steps: Vec<Value>,
        side: &str,
        disposition: &str,
        parent_selection: Option<Value>,
    ) -> Result<NativeProposal> {
        let mut proposal = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_pair_proposal_v2".into()),
            ),
            ("proposalSeed", Value::String(proposal_seed.into())),
            ("originKind", Value::String("structural_offspring".into())),
            ("side", Value::String(side.into())),
            ("parentPair", root.clone()),
            ("parentPairIdentitySha256", Value::String(root_id.into())),
            ("mutationDepth", Value::from(depth)),
            ("mutationSteps", Value::Array(steps)),
            ("disposition", Value::String(disposition.into())),
        ]);
        if let Some(audit) = parent_selection {
            proposal
                .as_object_mut()
                .expect("proposal")
                .insert("parentSelection".into(), audit);
        }
        let proposal_sha = canonical_sha256(&proposal)?;
        proposal
            .as_object_mut()
            .expect("proposal")
            .insert("proposalSha256".into(), Value::String(proposal_sha));
        Ok(NativeProposal::rejected(proposal))
    }
}
