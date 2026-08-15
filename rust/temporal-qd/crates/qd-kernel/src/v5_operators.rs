//! Native later-generation operators for the evolvable-module v5 genome.
//!
//! This module is deliberately value-oriented.  The native v5 proposal
//! authority owns frozen module/pair envelopes, compiler receipts, compact
//! persistence, and worker hand-off; the operator engine owns only the closed
//! side-local genome transformations.  Keeping this seam small prevents a
//! Python-side frozen wrapper from becoming an accidental runtime dependency
//! for mutation or crossover.
//!
//! The source contract is the existing Python evolvable-module operator
//! surface.  Every plan is content-addressed, binds its canonical parent (and
//! donor when applicable), carries a frozen authority hash, and is rejected
//! rather than reinterpreted when any of those facts drift.

// This module is also compiled directly by the external contract fixture. It
// deliberately exposes the complete sealed operator surface, including paths
// that are only selected by particular authority fixtures.
#![allow(dead_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use temporal_qd_contract::{
    ContractError, Map, Value, canonical_json, canonical_json_bytes, canonical_sha256,
};

// Kept private to this authority-bound engine: the topology port itself is
// intentionally value-only, while this module owns authority binding and the
// post-transform trusted validator.
#[path = "v5_topology_operators.rs"]
mod v5_topology_operators;

pub const V5_OPERATOR_PLAN_SCHEMA: &str = "temporal_qd_v5_operator_plan_v1";
pub const V5_OPERATOR_APPLICATION_SCHEMA: &str = "temporal_qd_v5_operator_application_v1";
pub const V5_OPERATOR_RESULT_SCHEMA: &str = "temporal_qd_v5_operator_result_v1";
pub const V5_SAME_SIDE_CROSSOVER_APPLICATION_SCHEMA: &str =
    "temporal_qd_v5_same_side_crossover_application_v1";
pub const V5_RESOURCE_OPERATOR_ID: &str = "evolvable_resource_v1";
pub const V5_TEMPORAL_OPERATOR_ID: &str = "evolvable_temporal_v1";
pub const V5_TOPOLOGY_OPERATOR_ID: &str = "evolvable_topology_v1";
pub const V5_HOLD_OPERATOR_ID: &str = "evolvable_hold_policy_v1";
pub const V5_INITIAL_PROTECTION_OPERATOR_ID: &str = "evolvable_initial_protection_v1";
pub const V5_CROSSOVER_OPERATOR_ID: &str = "evolvable_same_side_crossover_v1";

const GENOME_SCHEMA: &str = "evolvable_module_genome_v1";
const PROGRAM_KIND: &str = "evolvable_module_genome_v1";
const GENOME_CODEC: &str = "evolvable_module_genome_json_v1";
const BIDIRECTIONAL_GENOME_SCHEMA: &str = "temporal_bidirectional_genome_v1";
const RESOURCE_OPERATOR_VERSION: &str = "evolvable_module_resource_operators_v1";
const TEMPORAL_OPERATOR_VERSION: &str = "evolvable_module_temporal_operators_v1";
const TOPOLOGY_OPERATOR_SCHEMA: &str = "evolvable_module_topology_operator_v1";
const TOPOLOGY_PLAN_SCHEMA: &str = "evolvable_module_topology_plan_v1";
const CROSSOVER_SCHEMA: &str = "evolvable_module_motif_crossover_v1";
const STRUCTURAL_OPERATOR_PLAN_SCHEMA: &str = "temporal_structural_operator_plan_v1";
const RESOURCE_OPERATOR_PLAN_SCHEMA: &str = "evolvable_module_resource_operator_plan_v1";
const TEMPORAL_OPERATOR_PLAN_SCHEMA: &str = "evolvable_module_temporal_operator_plan_v1";
const CONSTRUCTION_OPERATOR_SPEC_SCHEMA: &str = "temporal_construction_operator_spec_v1";
const CONSTRUCTION_IDENTITY_SCHEMA: &str = "temporal_construction_identity_v1";
const CONSTRUCTION_GENERATOR_VERSION: &str = "temporal_discovery_generator_v3_construction";
const SCALAR_DYNAMIC_MANAGEMENT_OPERATOR_ID: &str = "scalar_dynamic_management_v3";
const SCALAR_DYNAMIC_MANAGEMENT_OPERATOR_VERSION: &str = "1";

#[derive(Debug, thiserror::Error)]
pub enum V5OperatorError {
    #[error("v5 operator canonical contract failure: {0}")]
    Canonical(#[from] ContractError),
    #[error("v5 operator: {0}")]
    Invalid(String),
}

pub type Result<T> = std::result::Result<T, V5OperatorError>;

#[derive(Clone, Debug, PartialEq)]
pub struct V5OperatorApplication {
    pub plan: Value,
    pub child_program: Value,
    pub audit: Value,
}

/// A two-parent, same-side motif crossover application.  This stays distinct
/// from the one-parent mutation envelope so neither parent can be silently
/// dropped from a durable attempt record.
#[derive(Clone, Debug, PartialEq)]
pub struct V5SameSideCrossoverApplication {
    pub plan: Value,
    pub child_program: Value,
    pub audit: Value,
}

/// Deterministic same-side crossover selection, kept separate from the
/// one-parent mutation receipt because its ordered donor/recipient pair is
/// part of the proposal identity.
#[derive(Clone, Debug, PartialEq)]
pub struct V5SameSideCrossoverSelection {
    pub plan: Value,
    pub receipt: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct V5OperatorResult {
    /// A canonical, self-contained result envelope.  It is deliberately a
    /// JSON value because the compact v5 journal owns the eventual projection
    /// into legacy Python proposal records.
    pub value: Value,
}

/// Static facts needed to rebuild the *legacy Python choice object* for a
/// native plan.  This is deliberately separate from the execution validator:
/// the legacy wrapper participates in sampling and candidate IDs, while the
/// Rust-native plan remains the durable apply/replay envelope.
#[derive(Clone, Debug)]
struct V5LegacySelectionStatic {
    catalog_sha256: String,
    resource_operator_spec_sha256: String,
    compiler_policy_sha256: String,
    temporal_operator_specification: Arc<Value>,
    temporal_operator_spec_sha256: String,
}

/// Parsed, content-addressed static authority for the later-generation
/// operator layer.  The public engine accepts this object rather than an
/// ambient catalog/config lookup: the catalog and policy axes are indexed
/// once, shared cheaply by workers, and every generated plan remains bound to
/// `authority_sha256`.
#[derive(Clone, Debug)]
pub struct V5OperatorAuthority {
    authority_sha256: String,
    side: String,
    instrument: String,
    budget: Arc<Value>,
    catalog: Arc<Value>,
    catalog_entries: Arc<BTreeMap<String, Value>>,
    timeframe_policy: Arc<Vec<String>>,
    evidence_lookback_choices: Arc<Vec<u64>>,
    hold_choices: Arc<Vec<Value>>,
    initial_protection_policy: Arc<Value>,
    temporal_domains: Arc<Value>,
    legacy_selection: Option<Arc<V5LegacySelectionStatic>>,
}

impl V5OperatorAuthority {
    /// Construct from core's already-verified *single-side* v2 projection.
    /// This intentionally takes explicit static objects instead of recursively
    /// finding names in a large JSON tree: a duplicate/shadow catalog or
    /// policy must be a hard error at the core projection boundary, never a
    /// layout-dependent operator choice.
    pub fn from_sealed_static_parts(
        authority_sha256: &str,
        side: &str,
        instrument: &str,
        budget: &Value,
        catalog: &Value,
        indicator_policy: &Value,
        hold_operator_policy: &Value,
        initial_protection_operator_policy: &Value,
        temporal_domains: &Value,
    ) -> Result<Self> {
        let authority_sha256 = sha256_identifier(
            &Value::String(authority_sha256.to_owned()),
            "v5 operator shared authority SHA-256",
        )?;
        let side = exact_side(side)?.to_owned();
        let instrument = text(
            &Value::String(instrument.to_owned()),
            "v5 operator side instrument",
        )?;
        if instrument != instrument.to_ascii_uppercase() {
            return Err(invalid("v5 operator side instrument must be uppercase"));
        }
        validate_v5_operator_budget(budget)?;
        let catalog_entries = Arc::new(catalog_entries_from_catalog(catalog)?);
        let timeframe_policy = Arc::new(timeframe_policy_from_indicator_policy(
            indicator_policy,
            catalog,
        )?);
        let evidence_lookback_choices = Arc::new(evidence_lookback_choices_from_indicator_policy(
            indicator_policy,
        )?);
        let hold_choices = Arc::new(hold_choices_from_policy(hold_operator_policy)?);
        let initial_protection_policy = Arc::new(validate_initial_protection_policy(
            initial_protection_operator_policy,
        )?);
        let temporal_domains = Arc::new(validate_temporal_domains(temporal_domains)?);
        Ok(Self {
            authority_sha256,
            side,
            instrument,
            budget: Arc::new(canonical_clone(budget)?),
            catalog: Arc::new(canonical_clone(catalog)?),
            catalog_entries,
            timeframe_policy,
            evidence_lookback_choices,
            hold_choices,
            initial_protection_policy,
            temporal_domains,
            legacy_selection: None,
        })
    }

    pub fn authority_sha256(&self) -> &str {
        &self.authority_sha256
    }
    pub fn side(&self) -> &str {
        &self.side
    }
    pub fn instrument(&self) -> &str {
        &self.instrument
    }
    pub fn budget(&self) -> &Value {
        self.budget.as_ref()
    }
    pub fn catalog_entries(&self) -> &BTreeMap<String, Value> {
        self.catalog_entries.as_ref()
    }
    pub fn timeframe_policy(&self) -> &[String] {
        self.timeframe_policy.as_slice()
    }
    pub fn evidence_lookback_choices(&self) -> &[u64] {
        self.evidence_lookback_choices.as_slice()
    }
    pub fn hold_choices(&self) -> &[Value] {
        self.hold_choices.as_slice()
    }
    pub fn initial_protection_policy(&self) -> &Value {
        self.initial_protection_policy.as_ref()
    }
    pub fn temporal_domains(&self) -> &Value {
        self.temporal_domains.as_ref()
    }

    /// Bind the exact static material used by historic Python selection
    /// wrappers.  Core calls this only with its already-parsed v2 projection;
    /// this method repeats every identity/domain cross-check so an opaque or
    /// shadowed static object can never influence deterministic selection.
    pub fn with_legacy_selection_static(
        mut self,
        catalog_sha256: &str,
        resource_operator_spec_sha256: &str,
        compiler_policy_sha256: &str,
        temporal_operator_specification: &Value,
        temporal_operator_spec_sha256: &str,
    ) -> Result<Self> {
        let catalog_sha256 = sha256_identifier(
            &Value::String(catalog_sha256.to_owned()),
            "legacy resource catalog SHA-256",
        )?;
        let expected_catalog_sha256 = sha(&object([
            ("payload", canonical_clone(self.catalog.as_ref())?),
            (
                "timeframePolicy",
                array(self.timeframe_policy().iter().cloned().map(Value::String)),
            ),
        ]))?;
        if catalog_sha256 != expected_catalog_sha256 {
            return Err(invalid(
                "legacy resource catalog SHA does not bind the sealed catalog/timeframe policy",
            ));
        }
        let expected_resource_spec = object([
            (
                "schemaVersion",
                Value::String("evolvable_module_resource_operator_plan_v1".to_owned()),
            ),
            (
                "operatorVersion",
                Value::String(RESOURCE_OPERATOR_VERSION.to_owned()),
            ),
            ("catalogSha256", Value::String(catalog_sha256.clone())),
            (
                "timeframePolicy",
                array(self.timeframe_policy().iter().cloned().map(Value::String)),
            ),
            ("rawEvents", Value::String("fresh_only_v1".to_owned())),
            (
                "weights",
                object([
                    ("positive", Value::Bool(true)),
                    ("normalizedWithinExclusiveGroup", Value::Bool(true)),
                    ("minimum", value_number(0.25, "resource weight minimum")?),
                ]),
            ),
        ]);
        let expected_resource_spec_sha256 = sha(&expected_resource_spec)?;
        let resource_operator_spec_sha256 = sha256_identifier(
            &Value::String(resource_operator_spec_sha256.to_owned()),
            "legacy resource operator specification SHA-256",
        )?;
        if resource_operator_spec_sha256 != expected_resource_spec_sha256 {
            return Err(invalid(
                "legacy resource operator specification does not bind sealed catalog policy",
            ));
        }
        let compiler_policy_sha256 = sha256_identifier(
            &Value::String(compiler_policy_sha256.to_owned()),
            "legacy temporal compiler policy SHA-256",
        )?;
        let temporal_operator_spec_sha256 = sha256_identifier(
            &Value::String(temporal_operator_spec_sha256.to_owned()),
            "legacy temporal operator specification SHA-256",
        )?;
        let specification = validate_legacy_temporal_operator_specification(
            temporal_operator_specification,
            &temporal_operator_spec_sha256,
            &compiler_policy_sha256,
            self.temporal_domains(),
        )?;
        self.legacy_selection = Some(Arc::new(V5LegacySelectionStatic {
            catalog_sha256,
            resource_operator_spec_sha256,
            compiler_policy_sha256,
            temporal_operator_specification: Arc::new(specification),
            temporal_operator_spec_sha256,
        }));
        Ok(self)
    }

    fn legacy_selection_static(&self) -> Result<&V5LegacySelectionStatic> {
        self.legacy_selection
            .as_deref()
            .ok_or_else(|| invalid("legacy selection requires the sealed operator specifications"))
    }
}

/// Stable journal-level result facts.  Rejections are not exceptional at the
/// evolutionary level; they must remain deterministic/replayable data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum V5OperatorDisposition {
    Accepted,
    NoOp,
    Rejected,
}

impl V5OperatorDisposition {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::NoOp => "no_op",
            Self::Rejected => "rejected",
        }
    }
}

/// Outcome returned to the durable later-generation journal.  A rejected plan
/// is data, not an exceptional scheduler event: retry accounting must know
/// exactly which parent/plan/authority tuple was attempted.
#[derive(Clone, Debug, PartialEq)]
pub struct V5OperatorExecution {
    pub disposition: V5OperatorDisposition,
    pub application: Option<V5OperatorApplication>,
    pub result: V5OperatorResult,
}

#[derive(Clone, Debug, PartialEq)]
pub struct V5OperatorSelection {
    pub plan: Value,
    pub receipt: Value,
}

/// A production later-generation selection.  `native_plan` is the compact,
/// authority-bound Rust apply envelope; `legacy_choice` is the exact Python
/// `_operation_choices` object which participates in historic proposal and
/// candidate identity material.  Keeping both prevents the native transport
/// schema from silently becoming a replacement RNG vocabulary.
#[derive(Clone, Debug, PartialEq)]
pub struct V5EvolvedOperatorSelection {
    pub native_plan: Value,
    pub legacy_choice: Value,
    pub receipt: Value,
}

/// Typed, side-local material for a transaction to turn an accepted mutation
/// into its next frozen pair.  The operator layer cannot compile the opposite
/// side or assign a pair identity; it does make every required parent/child
/// program identity, selection object, and semantic trace explicit so the
/// transaction must update the pair identity before asking for another step.
#[derive(Clone, Debug, PartialEq)]
pub struct V5EvolvedOperatorDelta {
    pub side: String,
    pub parent_pair_identity_sha256: String,
    pub parent_program_sha256: String,
    pub child_program: Value,
    pub child_program_sha256: String,
    pub native_plan: Value,
    pub legacy_choice: Value,
    pub trace: Value,
}

/// Journal-safe result for one profile-bound evolved side operation.  Rejected
/// and no-op attempts retain a deterministic reason/result envelope; accepted
/// attempts additionally expose the material needed to compile the next pair.
#[derive(Clone, Debug, PartialEq)]
pub struct V5EvolvedOperatorExecution {
    pub disposition: V5OperatorDisposition,
    pub reason_code: String,
    pub selection: V5EvolvedOperatorSelection,
    pub application: Option<V5OperatorApplication>,
    pub delta: Option<V5EvolvedOperatorDelta>,
    pub result: V5OperatorResult,
}

/// A core-produced, authority-validated native profile view.  Later-generation
/// initial-protection choices are deliberately derived from this profile: the
/// Python surface has always used the compiled management library as its
/// vocabulary oracle, rather than guessing from a genome resource row.
///
/// This is crate-visible on purpose.  `v5.rs` is the only production caller
/// that can build it, immediately after its typed compiler/native-admission
/// seam.  The constructor repeats the report/profile identity bindings so a
/// caller cannot smuggle a self-hashed but uncompiled profile into selection.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct V5CompiledProfileView {
    genome_program_sha256: String,
    profile: Arc<Value>,
    raw_profile_sha256: String,
    profile_snapshot_sha256: String,
    native_program_sha256: String,
    native_validation_report_sha256: String,
    native_validation_report: Arc<Value>,
}

/// The compiled-child admission seam for production evolved selection.
/// Structural validation alone is intentionally not enough for Python parity:
/// the historic resource/temporal/topology enumerators suppress a candidate
/// unless the full compiled child is accepted.  Transactions implement this
/// trait with their sealed v5 compiler projection, keeping that expensive
/// compilation and its pair-level ownership outside the side-local operator
/// engine.
pub(crate) trait V5EvolvedChildAdmission {
    fn admit_evolved_child(&self, operator_id: &str, child_program: &Value) -> Result<()>;
}

struct V5StructuralChildAdmission<'a> {
    authority: &'a V5OperatorAuthority,
}

impl V5EvolvedChildAdmission for V5StructuralChildAdmission<'_> {
    fn admit_evolved_child(&self, _operator_id: &str, child_program: &Value) -> Result<()> {
        validate_program(child_program, self.authority)
    }
}

impl V5CompiledProfileView {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_core_compilation(
        program: &Value,
        authority: &V5OperatorAuthority,
        genome_program_sha256: String,
        profile: Value,
        raw_profile_sha256: String,
        profile_snapshot_sha256: String,
        native_program_sha256: String,
        native_validation_report_sha256: String,
        native_validation_report: Value,
    ) -> Result<Self> {
        validate_program(program, authority)?;
        let expected_genome_program_sha256 = sha(program)?;
        let genome_program_sha256 = sha256_identifier(
            &Value::String(genome_program_sha256),
            "compiled profile genome program SHA-256",
        )?;
        if genome_program_sha256 != expected_genome_program_sha256 {
            return Err(invalid(
                "compiled profile does not bind the current authority-validated genome",
            ));
        }
        let raw_profile_sha256 = sha256_identifier(
            &Value::String(raw_profile_sha256),
            "compiled raw profile SHA-256",
        )?;
        if raw_profile_sha256 != sha(&profile)? {
            return Err(invalid(
                "compiled raw profile SHA does not match the supplied profile",
            ));
        }
        let profile_snapshot_sha256 = sha256_identifier(
            &Value::String(profile_snapshot_sha256),
            "compiled profile snapshot SHA-256",
        )?;
        let native_program_sha256 = sha256_identifier(
            &Value::String(native_program_sha256),
            "compiled native program SHA-256",
        )?;
        let native_validation_report_sha256 = sha256_identifier(
            &Value::String(native_validation_report_sha256),
            "compiled native validation report SHA-256",
        )?;
        let supplied_report_identity = text(
            required(
                &native_validation_report,
                "validationReportSha256",
                "compiled native validation report",
            )?,
            "compiled native validation report SHA-256",
        )?;
        // `validationReportSha256` identifies the validator's closed
        // `{coreValidationReport, searchIssues}` material, not the public
        // search-report envelope with its candidate ID.  The typed core seam
        // has already rebuilt that report; here we bind its declared identity
        // and every profile/program identity rather than incorrectly hashing
        // a different envelope shape.
        if supplied_report_identity != native_validation_report_sha256
            || object_get(&native_validation_report, "rawSourceProfileSha256")
                .and_then(Value::as_str)
                != Some(raw_profile_sha256.as_str())
            || object_get(&native_validation_report, "profileSnapshotSha256")
                .and_then(Value::as_str)
                != Some(profile_snapshot_sha256.as_str())
            || object_get(&native_validation_report, "programSha256").and_then(Value::as_str)
                != Some(native_program_sha256.as_str())
        {
            return Err(invalid(
                "compiled native validation report does not close over profile identities",
            ));
        }
        if object_get(&native_validation_report, "candidateAcceptable") != Some(&Value::Bool(true))
            || object_get(&native_validation_report, "status").and_then(Value::as_str)
                != Some("valid_evaluable")
        {
            return Err(invalid(
                "compiled native validation report did not admit the profile",
            ));
        }
        let profile_fields = map_ref(&profile, "compiled evolvable profile")?;
        if profile_fields.get("directionMode").and_then(Value::as_str) != Some(authority.side()) {
            return Err(invalid(
                "compiled profile direction does not match the sealed operator side",
            ));
        }
        let instruments = array_ref(
            required(&profile, "instruments", "compiled evolvable profile")?,
            "compiled profile instruments",
        )?;
        if instruments.len() != 1 || instruments[0].as_str() != Some(authority.instrument()) {
            return Err(invalid(
                "compiled profile instrument does not match the sealed operator side",
            ));
        }
        Ok(Self {
            genome_program_sha256,
            profile: Arc::new(canonical_clone(&profile)?),
            raw_profile_sha256,
            profile_snapshot_sha256,
            native_program_sha256,
            native_validation_report_sha256,
            native_validation_report: Arc::new(canonical_clone(&native_validation_report)?),
        })
    }

    pub(crate) fn genome_program_sha256(&self) -> &str {
        &self.genome_program_sha256
    }

    pub(crate) fn profile(&self) -> &Value {
        self.profile.as_ref()
    }

    pub(crate) fn raw_profile_sha256(&self) -> &str {
        &self.raw_profile_sha256
    }

    pub(crate) fn profile_snapshot_sha256(&self) -> &str {
        &self.profile_snapshot_sha256
    }

    pub(crate) fn native_program_sha256(&self) -> &str {
        &self.native_program_sha256
    }

    pub(crate) fn native_validation_report_sha256(&self) -> &str {
        &self.native_validation_report_sha256
    }

    pub(crate) fn native_validation_report(&self) -> &Value {
        self.native_validation_report.as_ref()
    }
}

/// The only side-local state a later-generation transaction may carry into a
/// subsequent selection.  It intentionally joins the pair identity, evolved
/// program, and its freshly compiled profile view: passing the prior step's
/// profile with a new program is rejected before a draw can occur.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct V5EvolvedSideState {
    pub pair_identity_sha256: String,
    /// Frozen-module identity from the freshly compiled pair.  It is distinct
    /// from the genome hash and is the historical Python crossover ordering
    /// key, so a later crossover cannot substitute an older parent module.
    pub module_identity_sha256: String,
    pub side: String,
    pub program: Value,
    pub compiled_profile: V5CompiledProfileView,
}

/// Compact result of the transaction-owned pair compiler after one side-local
/// mutation.  It deliberately carries neither a rich candidate nor the
/// opposite module: the caller proves pair admission by returning the new
/// pair identity and the freshly compiled view for the evolved side.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct V5RecompiledEvolvedPair {
    pub pair_identity_sha256: String,
    pub module_identity_sha256: String,
    pub compiled_profile: V5CompiledProfileView,
}

/// Pair compilation is owned above this operator module because it has to
/// retain the untouched opposite side, run the bidirectional compiler, and
/// assign the next pair identity.  The callback turns a compact evolved delta
/// into the only state accepted by the next selection.
pub(crate) trait V5EvolvedPairRecompiler {
    fn recompile_evolved_pair(
        &self,
        delta: &V5EvolvedOperatorDelta,
    ) -> Result<V5RecompiledEvolvedPair>;
}

/// Journal-safe result of executing one selection and, on success, compiling
/// and re-identifying the resulting pair before a later step can begin.
/// `delta` preserves the side-local trace even when pair compilation rejects
/// it; `next_side_state` is present only for a fully admitted next pair.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct V5EvolvedPairStepResult {
    pub disposition: V5OperatorDisposition,
    pub reason_code: String,
    pub reason_detail: Value,
    pub operator_execution: V5EvolvedOperatorExecution,
    pub delta: Option<V5EvolvedOperatorDelta>,
    pub next_side_state: Option<V5EvolvedSideState>,
}

/// Exact two-parent selection material for Python-compatible same-side
/// crossover.  The typed identity fields keep the selected recipient/donor
/// pair explicit, while `selection` is the byte-for-byte legacy selection
/// object used by production authority transcripts.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct V5EvolvedSameSideCrossoverSelection {
    pub proposal_seed: String,
    pub side: String,
    pub recipient_pair_identity_sha256: String,
    pub donor_pair_identity_sha256: String,
    pub recipient_module_identity_sha256: String,
    pub donor_module_identity_sha256: String,
    pub native_plan: Value,
    pub selection: Value,
}

/// Compile-ready material produced by an accepted same-side crossover.  The
/// transaction owns the opposite side and pair compiler; this layer binds
/// every two-parent fact needed to create that next compact pair.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct V5EvolvedSameSideCrossoverDelta {
    pub side: String,
    pub recipient_pair_identity_sha256: String,
    pub donor_pair_identity_sha256: String,
    pub recipient_module_identity_sha256: String,
    pub donor_module_identity_sha256: String,
    pub recipient_program_sha256: String,
    pub donor_program_sha256: String,
    pub child_program: Value,
    pub child_program_sha256: String,
    pub native_plan: Value,
    pub selection: Value,
    /// Python's `TopologySemanticDeltaV1`, including its application hash.
    pub trace: Value,
}

/// A deterministic same-side crossover attempt.  Rejections remain typed
/// data, including the no-compatible-port terminal path, so a transaction
/// never has to turn an operator exception into an ambiguous journal entry.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct V5EvolvedSameSideCrossoverExecution {
    pub disposition: V5OperatorDisposition,
    pub reason_code: String,
    pub reason_detail: Value,
    pub selection: Option<V5EvolvedSameSideCrossoverSelection>,
    pub application: Option<V5SameSideCrossoverApplication>,
    pub delta: Option<V5EvolvedSameSideCrossoverDelta>,
}

impl V5EvolvedSideState {
    /// Bind a current program to a freshly recompiled pair/profile.  This is
    /// deliberately the sole constructor so callers cannot silently retain a
    /// compiled profile from an earlier pair identity.
    pub(crate) fn from_recompiled_pair(
        pair_identity_sha256: String,
        module_identity_sha256: String,
        authority: &V5OperatorAuthority,
        program: Value,
        compiled_profile: V5CompiledProfileView,
    ) -> Result<Self> {
        let pair_identity_sha256 = sha256_identifier(
            &Value::String(pair_identity_sha256),
            "evolved side state pair identity SHA-256",
        )?;
        let module_identity_sha256 = sha256_identifier(
            &Value::String(module_identity_sha256),
            "evolved side state frozen module identity SHA-256",
        )?;
        let side = program_side(&program)?;
        if side != authority.side() {
            return Err(invalid(
                "recompiled evolved side state direction does not match its sealed authority",
            ));
        }
        validate_program(&program, authority)?;
        let program_sha256 = sha(&program)?;
        if compiled_profile.genome_program_sha256() != program_sha256 {
            return Err(invalid(
                "recompiled evolved side state profile does not bind its current program",
            ));
        }
        Ok(Self {
            pair_identity_sha256,
            module_identity_sha256,
            side,
            program,
            compiled_profile,
        })
    }

    fn validate_for_authority(&self, authority: &V5OperatorAuthority) -> Result<()> {
        let _ = sha256_identifier(
            &Value::String(self.pair_identity_sha256.clone()),
            "evolved side state pair identity SHA-256",
        )?;
        let _ = sha256_identifier(
            &Value::String(self.module_identity_sha256.clone()),
            "evolved side state frozen module identity SHA-256",
        )?;
        if self.side != authority.side() || program_side(&self.program)? != self.side {
            return Err(invalid(
                "evolved side state direction does not match its sealed authority",
            ));
        }
        validate_program(&self.program, authority)?;
        if self.compiled_profile.genome_program_sha256() != sha(&self.program)? {
            return Err(invalid(
                "evolved side state compiled profile is stale for its program",
            ));
        }
        Ok(())
    }
}

/// One exact Python operation choice plus the authority-bound native plan
/// which replays it.  The two identities intentionally remain separate:
/// legacyChoice participates in deterministic proposal/candidate lineage,
/// while nativePlanSha256 is the Rust journal/apply identity.
#[derive(Clone, Debug, PartialEq)]
pub struct V5LegacyOperatorChoice {
    pub native_plan: Value,
    pub legacy_choice: Value,
    pub legacy_choice_sha256: String,
    pub legacy_choice_ordering_sha256: String,
}

fn hold_choices_from_policy(policy: &Value) -> Result<Vec<Value>> {
    if object_get(policy, "enabled") != Some(&Value::Bool(true)) {
        return Ok(Vec::new());
    }
    let choices = array_ref(
        required(policy, "choices", "hold operator policy")?,
        "hold operator choices",
    )?;
    if choices.is_empty() {
        return Err(invalid("enabled hold operator policy has no choices"));
    }
    let mut output = choices
        .iter()
        .map(canonical_hold)
        .collect::<Result<Vec<_>>>()?;
    output = sorted_values(output)?;
    Ok(output)
}

fn invalid(message: impl Into<String>) -> V5OperatorError {
    V5OperatorError::Invalid(message.into())
}

fn object(rows: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    let mut output = Map::new();
    for (key, value) in rows {
        output.insert(key.to_owned(), value);
    }
    Value::Object(output)
}

fn array(rows: impl IntoIterator<Item = Value>) -> Value {
    Value::Array(rows.into_iter().collect())
}

fn canonical_clone(value: &Value) -> Result<Value> {
    let bytes = canonical_json_bytes(value)?;
    Ok(serde_json::from_slice(&bytes).map_err(ContractError::from)?)
}

fn sha(value: &Value) -> Result<String> {
    Ok(canonical_sha256(value)?)
}

fn text(value: &Value, label: &str) -> Result<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|item| !item.is_empty() && item.len() <= 240)
        .map(str::to_owned)
        .ok_or_else(|| invalid(format!("{label} must be a nonempty explicit identifier")))
}

fn identifier(value: &Value, label: &str) -> Result<String> {
    let token = text(value, label)?;
    let bytes = token.as_bytes();
    if bytes.is_empty()
        || bytes.len() > 64
        || !bytes[0].is_ascii_lowercase()
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
    {
        return Err(invalid(format!("{label} must be a canonical identifier")));
    }
    Ok(token)
}

fn sha256_identifier(value: &Value, label: &str) -> Result<String> {
    let token = text(value, label)?;
    let bytes = token.as_bytes();
    if bytes.len() != 71
        || !token.starts_with("sha256:")
        || !bytes[7..]
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(invalid(format!(
            "{label} must match sha256:<64 lowercase hex>"
        )));
    }
    Ok(token)
}

fn reason_code(value: &Value, label: &str) -> Result<String> {
    let token = text(value, label)?;
    let bytes = token.as_bytes();
    if bytes.len() > 96
        || !bytes.first().is_some_and(u8::is_ascii_lowercase)
        || !bytes.iter().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(*byte, b'_' | b'.' | b':')
        })
    {
        return Err(invalid(format!("{label} is not a canonical reason code")));
    }
    Ok(token)
}

fn exact_side(value: &str) -> Result<&'static str> {
    match value {
        "long" => Ok("long"),
        "short" => Ok("short"),
        _ => Err(invalid("v5 operator side must be long or short")),
    }
}

fn map_ref<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| invalid(format!("{label} must be an object")))
}

fn map_mut<'a>(value: &'a mut Value, label: &str) -> Result<&'a mut Map<String, Value>> {
    value
        .as_object_mut()
        .ok_or_else(|| invalid(format!("{label} must be an object")))
}

fn array_ref<'a>(value: &'a Value, label: &str) -> Result<&'a [Value]> {
    value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| invalid(format!("{label} must be an ordered array")))
}

fn required<'a>(value: &'a Value, key: &str, label: &str) -> Result<&'a Value> {
    value
        .as_object()
        .and_then(|row| row.get(key))
        .ok_or_else(|| invalid(format!("{label} lacks {key}")))
}

fn required_mut<'a>(
    value: &'a mut Map<String, Value>,
    key: &str,
    label: &str,
) -> Result<&'a mut Value> {
    value
        .get_mut(key)
        .ok_or_else(|| invalid(format!("{label} lacks {key}")))
}

fn finite_number(value: &Value, label: &str) -> Result<f64> {
    value
        .as_f64()
        .filter(|item| item.is_finite())
        .ok_or_else(|| invalid(format!("{label} must be finite numeric")))
}

fn as_i64(value: &Value, label: &str) -> Result<i64> {
    value
        .as_i64()
        .ok_or_else(|| invalid(format!("{label} must be an integer")))
}

fn as_u64(value: &Value, label: &str) -> Result<u64> {
    value
        .as_u64()
        .ok_or_else(|| invalid(format!("{label} must be an unsigned integer")))
}

fn object_get<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value.as_object().and_then(|row| row.get(key))
}

fn row_id(value: &Value, label: &str) -> Result<String> {
    text(required(value, "id", label)?, &format!("{label} ID"))
}

fn indicator_id(value: &Value) -> Result<String> {
    let meta = required(value, "meta", "indicator")?;
    text(
        required(meta, "instanceId", "indicator meta")?,
        "indicator instance ID",
    )
}

fn effect(value: &Value) -> Option<&str> {
    value.as_str()
}

fn value_number(value: f64, label: &str) -> Result<Value> {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| invalid(format!("{label} is not finite")))
}

fn sorted_values(mut rows: Vec<Value>) -> Result<Vec<Value>> {
    let mut keyed = rows
        .drain(..)
        .map(|row| Ok((sha(&row)?, row)))
        .collect::<Result<Vec<_>>>()?;
    keyed.sort_by(|left, right| left.0.cmp(&right.0));
    keyed.dedup_by(|left, right| left.0 == right.0);
    Ok(keyed.into_iter().map(|(_, row)| row).collect())
}

fn normalize_rows_by_id(rows: &mut [Value], kind: &str) -> Result<()> {
    rows.sort_by(|left, right| {
        let key = if kind == "indicator" {
            indicator_id(left).unwrap_or_default()
        } else {
            row_id(left, kind).unwrap_or_default()
        };
        let other = if kind == "indicator" {
            indicator_id(right).unwrap_or_default()
        } else {
            row_id(right, kind).unwrap_or_default()
        };
        key.cmp(&other)
    });
    let mut seen = BTreeSet::new();
    for row in rows.iter() {
        let id = if kind == "indicator" {
            indicator_id(row)?
        } else {
            row_id(row, kind)?
        };
        if !seen.insert(id) {
            return Err(invalid(format!("duplicate {kind} ID")));
        }
    }
    Ok(())
}

fn normalize_program(program: &Value) -> Result<Value> {
    let mut output = canonical_clone(program)?;
    let fields = map_mut(&mut output, "v5 program")?;
    let resources = required_mut(fields, "resources", "v5 program")?;
    let resources = map_mut(resources, "v5 resources")?;
    for (key, kind) in [
        ("indicators", "indicator"),
        ("evidenceGroups", "evidence group"),
        ("events", "event"),
        ("managementRefs", "management reference"),
    ] {
        let rows = required_mut(resources, key, "v5 resources")?;
        let rows = rows
            .as_array_mut()
            .ok_or_else(|| invalid(format!("v5 resources {key} must be an ordered array")))?;
        normalize_rows_by_id(rows, kind)?;
    }
    for (key, kind) in [("nodes", "node"), ("edges", "edge")] {
        let rows = required_mut(fields, key, "v5 program")?;
        let rows = rows
            .as_array_mut()
            .ok_or_else(|| invalid(format!("v5 program {key} must be an ordered array")))?;
        normalize_rows_by_id(rows, kind)?;
    }
    // Resource-use ordering is part of the Python canonical node form.
    let nodes = required_mut(fields, "nodes", "v5 program")?
        .as_array_mut()
        .ok_or_else(|| invalid("v5 program nodes must be an ordered array"))?;
    for node in nodes {
        let node = map_mut(node, "v5 node")?;
        let uses = required_mut(node, "resources", "v5 node")?
            .as_array_mut()
            .ok_or_else(|| invalid("v5 node resources must be an ordered array"))?;
        uses.sort_by(|left, right| {
            let key = (
                object_get(left, "kind")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                object_get(left, "id")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            );
            let other = (
                object_get(right, "kind")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                object_get(right, "id")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            );
            key.cmp(&other)
        });
    }
    Ok(output)
}

/// Closed structural validation of the generated evolvable-program domain.
///
/// This is intentionally stronger than simply checking a handful of JSON
/// fields, but it is not a general Dashboard validator.  The v5 compiler
/// performs the final native profile compilation/validation after this layer
/// has proven program, resource, and graph closure.
pub fn validate_v5_operator_program(program: &Value) -> Result<()> {
    let fields = map_ref(program, "v5 operator program")?;
    let expected = [
        "schemaVersion",
        "programKind",
        "codec",
        "direction",
        "instrument",
        "resources",
        "nodes",
        "edges",
        "budget",
    ];
    if fields.len() != expected.len() || expected.iter().any(|key| !fields.contains_key(*key)) {
        return Err(invalid("v5 operator program envelope is not exact"));
    }
    if object_get(program, "schemaVersion").and_then(Value::as_str) != Some(GENOME_SCHEMA)
        || object_get(program, "programKind").and_then(Value::as_str) != Some(PROGRAM_KIND)
        || object_get(program, "codec").and_then(Value::as_str) != Some(GENOME_CODEC)
    {
        return Err(invalid(
            "v5 operator program schema, kind, or codec drifted",
        ));
    }
    let side = text(
        required(program, "direction", "v5 operator program")?,
        "program direction",
    )?;
    exact_side(&side)?;
    text(
        required(program, "instrument", "v5 operator program")?,
        "program instrument",
    )?;
    let resources = required(program, "resources", "v5 operator program")?;
    let resource_fields = map_ref(resources, "v5 operator resources")?;
    let resource_keys = ["indicators", "evidenceGroups", "events", "managementRefs"];
    if resource_fields.len() != resource_keys.len()
        || resource_keys
            .iter()
            .any(|key| !resource_fields.contains_key(*key))
    {
        return Err(invalid("v5 operator resource envelope is not exact"));
    }
    let indicators = array_ref(
        required(resources, "indicators", "v5 resources")?,
        "v5 indicators",
    )?;
    let groups = array_ref(
        required(resources, "evidenceGroups", "v5 resources")?,
        "v5 evidence groups",
    )?;
    let events = array_ref(required(resources, "events", "v5 resources")?, "v5 events")?;
    let management = array_ref(
        required(resources, "managementRefs", "v5 resources")?,
        "v5 management references",
    )?;
    let budget = map_ref(
        required(program, "budget", "v5 operator program")?,
        "v5 budget",
    )?;
    let budget_u64 = |key: &str| -> Result<u64> {
        budget
            .get(key)
            .ok_or_else(|| invalid(format!("v5 budget lacks {key}")))
            .and_then(|value| as_u64(value, &format!("v5 budget {key}")))
    };
    if indicators.len() > budget_u64("maxIndicators")? as usize
        || groups.len() > budget_u64("maxEvidenceGroups")? as usize
        || events.len() > budget_u64("maxEvents")? as usize
    {
        return Err(invalid("v5 operator resources exceed frozen budget"));
    }
    let mut indicator_ids = BTreeSet::new();
    for row in indicators {
        let id = indicator_id(row)?;
        if !indicator_ids.insert(id)
            || object_get(row, "ownerSide").and_then(Value::as_str) != Some(side.as_str())
        {
            return Err(invalid("v5 indicator ownership or ID closure failed"));
        }
        let config = map_ref(
            required(row, "config", "v5 indicator")?,
            "v5 indicator config",
        )?;
        if config.get("useFormingBar") != Some(&Value::Bool(false)) {
            return Err(invalid("v5 indicators must be completed-bar only"));
        }
    }
    let mut group_ids = BTreeSet::new();
    for row in groups {
        let id = row_id(row, "evidence group")?;
        if !group_ids.insert(id)
            || object_get(row, "ownerSide").and_then(Value::as_str) != Some(side.as_str())
        {
            return Err(invalid("v5 evidence group ownership or ID closure failed"));
        }
        let members = array_ref(
            required(row, "indicatorInstanceIds", "evidence group")?,
            "evidence group members",
        )?;
        if members.is_empty()
            || members.len() > budget_u64("maxGroupMembers")? as usize
            || members
                .iter()
                .any(|member| member.as_str().is_none_or(|id| !indicator_ids.contains(id)))
        {
            return Err(invalid("v5 evidence group membership is invalid"));
        }
    }
    let mut event_ids = BTreeSet::new();
    for row in events {
        let id = row_id(row, "event")?;
        if !event_ids.insert(id)
            || object_get(row, "ownerSide").and_then(Value::as_str) != Some(side.as_str())
        {
            return Err(invalid("v5 event ownership or ID closure failed"));
        }
        if object_get(row, "indicatorInstanceId")
            .and_then(Value::as_str)
            .is_none_or(|id| !indicator_ids.contains(id))
        {
            return Err(invalid("v5 event source is invalid"));
        }
    }
    let mut management_ids = BTreeSet::new();
    for row in management {
        let id = row_id(row, "management reference")?;
        if !management_ids.insert(id)
            || object_get(row, "ownerSide").and_then(Value::as_str) != Some(side.as_str())
        {
            return Err(invalid("v5 management ownership or ID closure failed"));
        }
    }
    let nodes = array_ref(
        required(program, "nodes", "v5 operator program")?,
        "v5 nodes",
    )?;
    let edges = array_ref(
        required(program, "edges", "v5 operator program")?,
        "v5 edges",
    )?;
    if nodes.len() > budget_u64("maxStates")? as usize
        || edges.len() > budget_u64("maxTransitions")? as usize
    {
        return Err(invalid("v5 graph exceeds state or transition budget"));
    }
    let mut node_ids = BTreeSet::new();
    let mut zones = BTreeMap::new();
    let mut entry_start = 0_usize;
    let mut hubs = 0_usize;
    for row in nodes {
        let id = row_id(row, "node")?;
        let zone = text(required(row, "zone", "node")?, "node zone")?;
        let kind = text(required(row, "kind", "node")?, "node kind")?;
        let _ = map_ref(required(row, "guard", "node")?, "node guard")?;
        let uses = array_ref(required(row, "resources", "node")?, "node resources")?;
        for use_row in uses {
            let kind = object_get(use_row, "kind")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let resource_id = object_get(use_row, "id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let known = match kind {
                "indicator" => indicator_ids.contains(resource_id),
                "evidence_group" => group_ids.contains(resource_id),
                "event" => event_ids.contains(resource_id),
                "management_ref" => management_ids.contains(resource_id),
                _ => false,
            };
            if !known {
                return Err(invalid("v5 node names an unknown typed resource"));
            }
        }
        if object_get(row, "timeoutBars").is_some_and(|value| !value.is_null()) {
            let timeout = as_u64(
                object_get(row, "timeoutBars").expect("checked"),
                "node timeout",
            )?;
            if timeout < 1 || timeout > budget_u64("maxTimeoutBars")? {
                return Err(invalid("v5 node timeout is outside frozen budget"));
            }
        }
        if !node_ids.insert(id.clone()) {
            return Err(invalid("v5 graph has duplicate node IDs"));
        }
        if zone == "entry" && kind == "start" {
            entry_start += 1;
        }
        if zone == "position" && kind == "position_hub" {
            hubs += 1;
        }
        zones.insert(id, zone);
    }
    if entry_start != 1 || hubs != 1 {
        return Err(invalid("v5 graph requires one start and one position hub"));
    }
    let mut edge_ids = BTreeSet::new();
    let mut priority_keys = BTreeSet::new();
    for row in edges {
        let id = row_id(row, "edge")?;
        let source = text(required(row, "source", "edge")?, "edge source")?;
        let target = text(required(row, "target", "edge")?, "edge target")?;
        if !node_ids.contains(&source)
            || !node_ids.contains(&target)
            || object_get(row, "eventClass").and_then(Value::as_str) != Some("decision")
        {
            return Err(invalid("v5 edge endpoint or event class is invalid"));
        }
        let priority = as_u64(required(row, "priority", "edge")?, "edge priority")?;
        if priority > 999 || !priority_keys.insert((source.clone(), priority)) {
            return Err(invalid("v5 edge priority conflicts at a source"));
        }
        let _ = map_ref(required(row, "guard", "edge")?, "edge guard")?;
        if !edge_ids.insert(id) {
            return Err(invalid("v5 graph has duplicate edge IDs"));
        }
        let target_zone = zones.get(&target).map(String::as_str).unwrap_or_default();
        let source_zone = zones.get(&source).map(String::as_str).unwrap_or_default();
        let edge_effect = object_get(row, "effect").and_then(effect);
        match target_zone {
            "entry" => {
                if source_zone != "entry" && source_zone != "setup" {
                    return Err(invalid("v5 entry branch has an invalid source"));
                }
            }
            "setup" => {
                if !matches!(source_zone, "entry" | "setup") || edge_effect.is_some() {
                    return Err(invalid("v5 setup route must be side-effect-free"));
                }
            }
            "management" | "exit" => {
                if source_zone != "position" || edge_effect.is_none() {
                    return Err(invalid(
                        "v5 position region must dispatch from the shared hub",
                    ));
                }
            }
            "recovery" => {
                if !matches!(source_zone, "entry" | "setup") || edge_effect.is_some() {
                    return Err(invalid(
                        "v5 recovery routes must be side-effect-free pre-position paths",
                    ));
                }
            }
            "position" => {
                if source_zone != "entry" || edge_effect.is_some() {
                    return Err(invalid("v5 position hub connector is invalid"));
                }
            }
            _ => return Err(invalid("v5 graph has an unsupported target zone")),
        }
    }
    // The broad field/graph/resource closure check is kept separately from
    // the easy-to-read envelope checks above.  It is deliberately run before
    // the canonical ordering check so malformed-but-self-consistent JSON is
    // never normalized into an apparently valid parent.
    validate_v5_operator_program_detail(program)?;
    // Program ordering is identity-bearing.  Reject rather than silently
    // normalizing an input parent in a plan replay path.
    if normalize_program(program)? != *program {
        return Err(invalid("v5 operator parent program is not canonical"));
    }
    Ok(())
}

/// Trusted later-generation admission gate.  Unlike the structural helper,
/// this binds a program to one parsed side of the sealed authority and checks
/// the catalog/capability closure used by actual operator enumeration.
pub(crate) fn validate_program(program: &Value, authority: &V5OperatorAuthority) -> Result<()> {
    validate_v5_operator_program(program)?;
    if program_side(program)? != authority.side()
        || text(
            required(program, "instrument", "v5 program")?,
            "program instrument",
        )? != authority.instrument()
        || required(program, "budget", "v5 program")? != authority.budget()
    {
        return Err(invalid(
            "v5 program direction, instrument, or budget is not bound to its selected authority side",
        ));
    }
    validate_catalog_bound_resources(program, authority)?;
    validate_generated_cooldown_closure(program)?;
    Ok(())
}

fn exact_keys(fields: &Map<String, Value>, expected: &[&str], label: &str) -> Result<()> {
    if fields.len() != expected.len() || expected.iter().any(|key| !fields.contains_key(*key)) {
        return Err(invalid(format!("{label} fields are not exact")));
    }
    Ok(())
}

fn guard_depth(guard: &Value) -> Result<u64> {
    let fields = map_ref(guard, "guard")?;
    if fields.is_empty() {
        return Ok(0);
    }
    let kind = text(required(guard, "kind", "guard")?, "guard kind")?;
    let nested = match kind.as_str() {
        "all" | "any" => array_ref(
            required(guard, "guards", "composite guard")?,
            "composite guard children",
        )?
        .iter()
        .map(guard_depth)
        .collect::<Result<Vec<_>>>()?,
        "not" => vec![guard_depth(required(guard, "guard", "not guard")?)?],
        "predicate_edge" | "consecutive_true" => {
            vec![guard_depth(required(
                guard,
                "predicate",
                "wrapped predicate",
            )?)?]
        }
        _ => Vec::new(),
    };
    Ok(1 + nested.into_iter().max().unwrap_or(0))
}

fn bounded_number(
    value: &Value,
    label: &str,
    lower: f64,
    upper: f64,
    lower_open: bool,
    upper_open: bool,
) -> Result<f64> {
    let numeric = finite_number(value, label)?;
    let lower_ok = if lower_open {
        numeric > lower
    } else {
        numeric >= lower
    };
    let upper_ok = if upper_open {
        numeric < upper
    } else {
        numeric <= upper
    };
    if !lower_ok || !upper_ok {
        return Err(invalid(format!("{label} is outside its native bounds")));
    }
    Ok(numeric)
}

fn binding_kind(
    bindings: &BTreeMap<String, Value>,
    value: &Value,
    label: &str,
    expected: &str,
) -> Result<()> {
    let id = text(
        required(value, "bindingId", label)?,
        &format!("{label} binding ID"),
    )?;
    if bindings
        .get(&id)
        .and_then(|row| object_get(row, "valueKind"))
        .and_then(Value::as_str)
        != Some(expected)
    {
        return Err(invalid(format!(
            "{label} does not name a {expected} scalar binding"
        )));
    }
    Ok(())
}

// These locator validators intentionally mirror the Dashboard's discriminated
// unions.  A generic "known locator" accepted legal-looking values in the
// wrong site (for example reward_multiple as a stop), which would make the
// Rust boundary wider than the execution contract.
fn validate_initial_stop_locator(value: &Value, bindings: &BTreeMap<String, Value>) -> Result<()> {
    let fields = map_ref(value, "initial stop")?;
    match text(
        required(value, "kind", "initial stop")?,
        "initial stop kind",
    )?
    .as_str()
    {
        "fixed_percent" => {
            exact_keys(fields, &["kind", "percent"], "initial stop")?;
            bounded_number(
                required(value, "percent", "initial stop")?,
                "initial stop percent",
                0.0,
                100.0,
                true,
                true,
            )?;
        }
        "indicator_price_level" => {
            exact_keys(fields, &["kind", "bindingId"], "initial stop")?;
            binding_kind(bindings, value, "initial stop", "price_level")?;
        }
        "indicator_distance_multiple" => {
            exact_keys(fields, &["kind", "bindingId", "multiple"], "initial stop")?;
            binding_kind(bindings, value, "initial stop", "price_distance")?;
            bounded_number(
                required(value, "multiple", "initial stop")?,
                "initial stop multiple",
                0.0,
                100.0,
                true,
                false,
            )?;
        }
        _ => {
            return Err(invalid(
                "initial stop locator kind is outside its native union",
            ));
        }
    }
    Ok(())
}

fn validate_initial_target_locator(
    value: &Value,
    bindings: &BTreeMap<String, Value>,
    allow_none: bool,
    label: &str,
) -> Result<()> {
    let fields = map_ref(value, label)?;
    match text(required(value, "kind", label)?, &format!("{label} kind"))?.as_str() {
        "reward_multiple" => {
            exact_keys(fields, &["kind", "multiple"], label)?;
            // The Dashboard has no upper bound on initial target reward
            // multiple, only a finite positive constraint.
            if finite_number(
                required(value, "multiple", label)?,
                &format!("{label} multiple"),
            )? <= 0.0
            {
                return Err(invalid(format!("{label} multiple must be positive")));
            }
        }
        "fixed_percent" => {
            exact_keys(fields, &["kind", "percent"], label)?;
            bounded_number(
                required(value, "percent", label)?,
                &format!("{label} percent"),
                0.0,
                100.0,
                true,
                true,
            )?;
        }
        "indicator_price_level" => {
            exact_keys(fields, &["kind", "bindingId"], label)?;
            binding_kind(bindings, value, label, "price_level")?;
        }
        "indicator_distance_multiple" => {
            exact_keys(fields, &["kind", "bindingId", "multiple"], label)?;
            binding_kind(bindings, value, label, "price_distance")?;
            bounded_number(
                required(value, "multiple", label)?,
                &format!("{label} multiple"),
                0.0,
                100.0,
                true,
                false,
            )?;
        }
        "none" if allow_none => exact_keys(fields, &["kind"], label)?,
        _ => {
            return Err(invalid(format!(
                "{label} locator kind is outside its native union"
            )));
        }
    }
    Ok(())
}

fn validate_trailing_anchor(value: &Value, bindings: &BTreeMap<String, Value>) -> Result<()> {
    let fields = map_ref(value, "trailing stop anchor")?;
    match text(
        required(value, "kind", "trailing stop anchor")?,
        "trailing stop anchor kind",
    )?
    .as_str()
    {
        "bar_close" | "favorable_bar_extreme" => {
            exact_keys(fields, &["kind"], "trailing stop anchor")?
        }
        "indicator_price_level" => {
            exact_keys(fields, &["kind", "bindingId"], "trailing stop anchor")?;
            binding_kind(bindings, value, "trailing stop anchor", "price_level")?;
        }
        _ => {
            return Err(invalid(
                "trailing stop anchor kind is outside its native union",
            ));
        }
    }
    Ok(())
}

fn validate_trailing_distance(value: &Value, bindings: &BTreeMap<String, Value>) -> Result<()> {
    let fields = map_ref(value, "trailing stop distance")?;
    match text(
        required(value, "kind", "trailing stop distance")?,
        "trailing stop distance kind",
    )?
    .as_str()
    {
        "fixed_percent_of_entry" => {
            exact_keys(fields, &["kind", "percent"], "trailing stop distance")?;
            bounded_number(
                required(value, "percent", "trailing stop distance")?,
                "trailing stop distance percent",
                0.0,
                100.0,
                true,
                true,
            )?;
        }
        "fixed_initial_r" => {
            exact_keys(fields, &["kind", "multiple"], "trailing stop distance")?;
            bounded_number(
                required(value, "multiple", "trailing stop distance")?,
                "trailing stop distance multiple",
                0.0,
                100.0,
                true,
                false,
            )?;
        }
        "indicator_distance_multiple" => {
            exact_keys(
                fields,
                &["kind", "bindingId", "multiple"],
                "trailing stop distance",
            )?;
            binding_kind(bindings, value, "trailing stop distance", "price_distance")?;
            bounded_number(
                required(value, "multiple", "trailing stop distance")?,
                "trailing stop distance multiple",
                0.0,
                100.0,
                true,
                false,
            )?;
        }
        _ => {
            return Err(invalid(
                "trailing stop distance kind is outside its native union",
            ));
        }
    }
    Ok(())
}

fn validate_trailing_activation(value: &Value) -> Result<()> {
    let fields = map_ref(value, "trailing stop activation")?;
    match text(
        required(value, "kind", "trailing stop activation")?,
        "trailing stop activation kind",
    )?
    .as_str()
    {
        "immediate" | "explicit" => exact_keys(fields, &["kind"], "trailing stop activation")?,
        "after_unrealized_r" => {
            exact_keys(fields, &["kind", "value"], "trailing stop activation")?;
            bounded_number(
                required(value, "value", "trailing stop activation")?,
                "trailing activation R",
                0.0,
                100.0,
                false,
                false,
            )?;
        }
        "after_position_age" => {
            exact_keys(fields, &["kind", "bars"], "trailing stop activation")?;
            let bars = as_u64(
                required(value, "bars", "trailing stop activation")?,
                "trailing activation bars",
            )?;
            if !(1..=1_000_000).contains(&bars) {
                return Err(invalid(
                    "trailing activation bars are outside native bounds",
                ));
            }
        }
        "after_r_and_age" => {
            exact_keys(
                fields,
                &["kind", "value", "bars"],
                "trailing stop activation",
            )?;
            bounded_number(
                required(value, "value", "trailing stop activation")?,
                "trailing activation R",
                0.0,
                100.0,
                false,
                false,
            )?;
            let bars = as_u64(
                required(value, "bars", "trailing stop activation")?,
                "trailing activation bars",
            )?;
            if !(1..=1_000_000).contains(&bars) {
                return Err(invalid(
                    "trailing activation bars are outside native bounds",
                ));
            }
        }
        _ => {
            return Err(invalid(
                "trailing stop activation kind is outside its native union",
            ));
        }
    }
    Ok(())
}

fn validate_management_ref(
    row: &Value,
    side: &str,
    indicators: &BTreeSet<String>,
) -> Result<BTreeSet<String>> {
    let fields = map_ref(row, "management reference")?;
    if !fields.contains_key("id") || fields.get("ownerSide").and_then(Value::as_str) != Some(side) {
        return Err(invalid(
            "management reference identity or ownership is invalid",
        ));
    }
    row_id(row, "management reference")?;
    let binding_rows = object_get(row, "scalarBindings")
        .map(|value| array_ref(value, "management scalar bindings"))
        .transpose()?
        .unwrap_or(&[]);
    if binding_rows.len() > 32 {
        return Err(invalid(
            "management scalar binding count exceeds closed cap",
        ));
    }
    let mut bindings = BTreeMap::new();
    let mut sources = BTreeSet::new();
    for binding in binding_rows {
        let binding_fields = map_ref(binding, "management scalar binding")?;
        exact_keys(
            binding_fields,
            &[
                "id",
                "indicatorInstanceId",
                "outputKey",
                "valueKind",
                "availability",
            ],
            "management scalar binding",
        )?;
        let id = row_id(binding, "management scalar binding")?;
        let instance = text(
            required(binding, "indicatorInstanceId", "management scalar binding")?,
            "management scalar indicator",
        )?;
        let output = text(
            required(binding, "outputKey", "management scalar binding")?,
            "management scalar output",
        )?;
        let value_kind = text(
            required(binding, "valueKind", "management scalar binding")?,
            "management scalar value kind",
        )?;
        if !indicators.contains(&instance)
            || !matches!(value_kind.as_str(), "price_level" | "price_distance")
            || object_get(binding, "availability").and_then(Value::as_str) != Some("completed_bar")
            || !sources.insert((instance, output))
            || bindings.insert(id, canonical_clone(binding)?).is_some()
        {
            return Err(invalid("management scalar binding closure is invalid"));
        }
    }
    let initial_stop = required(row, "initialStop", "management reference")?;
    let initial_target = required(row, "initialTarget", "management reference")?;
    validate_initial_stop_locator(initial_stop, &bindings)?;
    validate_initial_target_locator(initial_target, &bindings, true, "initial target")?;
    if let Some(trailing) = object_get(row, "trailingStop") {
        let trailing_fields = map_ref(trailing, "trailing stop")?;
        exact_keys(
            trailing_fields,
            &["anchor", "distance", "activation", "minimumStepInitialR"],
            "trailing stop",
        )?;
        validate_trailing_anchor(required(trailing, "anchor", "trailing stop")?, &bindings)?;
        validate_trailing_distance(required(trailing, "distance", "trailing stop")?, &bindings)?;
        validate_trailing_activation(required(trailing, "activation", "trailing stop")?)?;
        bounded_number(
            required(trailing, "minimumStepInitialR", "trailing stop")?,
            "trailing stop minimum step",
            0.0,
            100.0,
            false,
            false,
        )?;
    }
    let mut references = BTreeSet::new();
    for locator in [initial_stop, initial_target] {
        if let Some(id) = object_get(locator, "bindingId").and_then(Value::as_str) {
            references.insert(id.to_owned());
        }
    }
    if let Some(trailing) = object_get(row, "trailingStop") {
        for key in ["anchor", "distance"] {
            if let Some(id) = object_get(required(trailing, key, "trailing stop")?, "bindingId")
                .and_then(Value::as_str)
            {
                references.insert(id.to_owned());
            }
        }
    }
    let supplied = bindings.keys().cloned().collect::<BTreeSet<_>>();
    if supplied != references {
        return Err(invalid("management scalar binding is orphaned or unbound"));
    }
    Ok(references)
}

fn validate_level_predicate(
    predicate: &Value,
    group_ids: &BTreeSet<String>,
    event_ids: &BTreeSet<String>,
    max_depth: u64,
    depth: u64,
    used_groups: &mut BTreeSet<String>,
    used_events: &mut BTreeSet<String>,
) -> Result<()> {
    validate_guard(
        predicate,
        group_ids,
        event_ids,
        max_depth,
        depth,
        used_groups,
        used_events,
    )?;
    let kind = text(
        required(predicate, "kind", "level predicate")?,
        "level predicate kind",
    )?;
    if !matches!(
        kind.as_str(),
        "evidence_at_least"
            | "evidence_below"
            | "utc_time_window"
            | "state_age_at_least"
            | "state_age_at_most"
            | "position_exists"
            | "position_age_at_least"
            | "unrealized_r_at_least"
            | "unrealized_r_at_most"
    ) {
        return Err(invalid(
            "edge/streak predicate is outside the Dashboard level-predicate union",
        ));
    }
    Ok(())
}

fn validate_guard(
    guard: &Value,
    group_ids: &BTreeSet<String>,
    event_ids: &BTreeSet<String>,
    max_depth: u64,
    depth: u64,
    used_groups: &mut BTreeSet<String>,
    used_events: &mut BTreeSet<String>,
) -> Result<()> {
    let fields = map_ref(guard, "guard")?;
    if fields.is_empty() {
        return Ok(());
    }
    if depth > max_depth {
        return Err(invalid("guard exceeds frozen depth budget"));
    }
    let kind = text(required(guard, "kind", "guard")?, "guard kind")?;
    let exact_u64 = |key: &str| -> Result<u64> {
        as_u64(required(guard, key, "guard")?, &format!("guard {key}"))
    };
    match kind.as_str() {
        "always" => exact_keys(fields, &["kind"], "always guard")?,
        "all" | "any" => {
            exact_keys(fields, &["kind", "guards"], "composite guard")?;
            let rows = array_ref(
                required(guard, "guards", "composite guard")?,
                "composite guard children",
            )?;
            if rows.is_empty() || rows.len() > 8 {
                return Err(invalid("composite guard child count is invalid"));
            }
            for item in rows {
                validate_guard(
                    item,
                    group_ids,
                    event_ids,
                    max_depth,
                    depth + 1,
                    used_groups,
                    used_events,
                )?;
            }
        }
        "not" => {
            exact_keys(fields, &["kind", "guard"], "not guard")?;
            validate_guard(
                required(guard, "guard", "not guard")?,
                group_ids,
                event_ids,
                max_depth,
                depth + 1,
                used_groups,
                used_events,
            )?;
        }
        "predicate_edge" => {
            exact_keys(
                fields,
                &[
                    "kind",
                    "operatorId",
                    "operatorVersion",
                    "occurrenceSha256",
                    "direction",
                    "predicate",
                ],
                "predicate edge guard",
            )?;
            if !matches!(
                object_get(guard, "operatorId").and_then(Value::as_str),
                Some("edge_trigger_predicate_v1") | Some("sequence_action_gate_v1")
            ) || object_get(guard, "operatorVersion").and_then(Value::as_str) != Some("1")
                || !matches!(
                    object_get(guard, "direction").and_then(Value::as_str),
                    Some("falling") | Some("rising")
                )
            {
                return Err(invalid("predicate edge guard contract is invalid"));
            }
            sha256_identifier(
                required(guard, "occurrenceSha256", "predicate edge guard")?,
                "predicate edge occurrence",
            )?;
            validate_level_predicate(
                required(guard, "predicate", "predicate edge guard")?,
                group_ids,
                event_ids,
                max_depth,
                depth + 1,
                used_groups,
                used_events,
            )?;
        }
        "consecutive_true" => {
            exact_keys(
                fields,
                &[
                    "kind",
                    "operatorId",
                    "operatorVersion",
                    "occurrenceSha256",
                    "predicate",
                    "evaluations",
                ],
                "consecutive guard",
            )?;
            if object_get(guard, "operatorId").and_then(Value::as_str)
                != Some("require_consecutive_true_v1")
                || object_get(guard, "operatorVersion").and_then(Value::as_str) != Some("1")
                || !(2..=1_000_000).contains(&exact_u64("evaluations")?)
            {
                return Err(invalid("consecutive guard contract is invalid"));
            }
            sha256_identifier(
                required(guard, "occurrenceSha256", "consecutive guard")?,
                "consecutive occurrence",
            )?;
            validate_level_predicate(
                required(guard, "predicate", "consecutive guard")?,
                group_ids,
                event_ids,
                max_depth,
                depth + 1,
                used_groups,
                used_events,
            )?;
        }
        "position_exists" => {
            exact_keys(fields, &["kind", "expected"], "position-exists guard")?;
            if required(guard, "expected", "position-exists guard")?
                .as_bool()
                .is_none()
            {
                return Err(invalid("position-exists expected must be boolean"));
            }
        }
        "evidence_at_least" | "evidence_below" => {
            exact_keys(
                fields,
                &["kind", "groupId", "thresholdPercent"],
                "evidence guard",
            )?;
            let id = text(
                required(guard, "groupId", "evidence guard")?,
                "evidence group ID",
            )?;
            let threshold = finite_number(
                required(guard, "thresholdPercent", "evidence guard")?,
                "evidence threshold",
            )?;
            if !group_ids.contains(&id) || !(0.0..=100.0).contains(&threshold) {
                return Err(invalid("evidence guard resource or threshold is invalid"));
            }
            used_groups.insert(id);
        }
        "fresh_event" => {
            exact_keys(fields, &["kind", "eventId"], "fresh-event guard")?;
            let id = text(required(guard, "eventId", "fresh-event guard")?, "event ID")?;
            if !event_ids.contains(&id) {
                return Err(invalid("fresh-event guard names an unknown event"));
            }
            used_events.insert(id);
        }
        "event_age_window" => {
            exact_keys(
                fields,
                &["kind", "eventId", "minimumEvents", "maximumEvents"],
                "event-age guard",
            )?;
            let id = text(required(guard, "eventId", "event-age guard")?, "event ID")?;
            let minimum = exact_u64("minimumEvents")?;
            let maximum = exact_u64("maximumEvents")?;
            if !event_ids.contains(&id) || minimum > maximum || maximum > 1_000_000 {
                return Err(invalid("event-age guard contract is invalid"));
            }
            used_events.insert(id);
        }
        "event_age_at_most" => {
            exact_keys(
                fields,
                &["kind", "eventId", "events"],
                "event-age-at-most guard",
            )?;
            let id = identifier(
                required(guard, "eventId", "event-age-at-most guard")?,
                "event-age-at-most event ID",
            )?;
            if !event_ids.contains(&id) || exact_u64("events")? > 1_000_000 {
                return Err(invalid(
                    "event-age-at-most guard resource or age is invalid",
                ));
            }
            used_events.insert(id);
        }
        "condition_streak_at_least" => {
            exact_keys(
                fields,
                &[
                    "kind",
                    "groupId",
                    "comparison",
                    "thresholdPercent",
                    "events",
                ],
                "condition-streak guard",
            )?;
            let id = identifier(
                required(guard, "groupId", "condition-streak guard")?,
                "condition-streak group ID",
            )?;
            let comparison = text(
                required(guard, "comparison", "condition-streak guard")?,
                "condition-streak comparison",
            )?;
            let threshold = finite_number(
                required(guard, "thresholdPercent", "condition-streak guard")?,
                "condition-streak threshold",
            )?;
            if !group_ids.contains(&id)
                || !matches!(comparison.as_str(), "at_least" | "below")
                || !(0.0..=100.0).contains(&threshold)
                || !(1..=1_000_000).contains(&exact_u64("events")?)
            {
                return Err(invalid("condition-streak guard contract is invalid"));
            }
            used_groups.insert(id);
        }
        "state_age_at_least" | "state_age_at_most" => {
            exact_keys(fields, &["kind", "events"], "age guard")?;
            if exact_u64("events")? > 1_000_000 {
                return Err(invalid("state age exceeds native maximum"));
            }
        }
        "position_age_at_least" => {
            exact_keys(fields, &["kind", "events"], "position-age guard")?;
            if exact_u64("events")? > 10_000_000 {
                return Err(invalid("position age exceeds native maximum"));
            }
        }
        "utc_time_window" => {
            exact_keys(
                fields,
                &["kind", "startMinute", "endMinute", "weekdays"],
                "UTC session guard",
            )?;
            let start = exact_u64("startMinute")?;
            let end = exact_u64("endMinute")?;
            let weekdays = required(guard, "weekdays", "UTC session guard")?;
            let days = if weekdays.is_null() {
                None
            } else {
                Some(array_ref(weekdays, "UTC session weekdays")?)
            };
            let mut unique = BTreeSet::new();
            if start > 1439
                || end > 1439
                || start == end
                || days.is_some_and(|items| {
                    items.is_empty()
                        || items.iter().any(|item| {
                            item.as_u64()
                                .is_none_or(|day| day > 6 || !unique.insert(day))
                        })
                })
            {
                return Err(invalid("UTC session guard is invalid"));
            }
        }
        "action_cooldown_elapsed" => {
            exact_keys(
                fields,
                &["kind", "transitionId", "actionOrdinal", "evaluations"],
                "action cooldown guard",
            )?;
            identifier(
                required(guard, "transitionId", "action cooldown guard")?,
                "cooldown transition ID",
            )?;
            if exact_u64("actionOrdinal")? > 3
                || !(1..=1_000_000).contains(&exact_u64("evaluations")?)
            {
                return Err(invalid("action cooldown is outside native bounds"));
            }
        }
        "unrealized_r_at_least" | "unrealized_r_at_most" => {
            exact_keys(fields, &["kind", "value"], "R-value guard")?;
            let _ = finite_number(
                required(guard, "value", "R-value guard")?,
                "R-value guard value",
            )?;
        }
        "execution_status_is" => {
            exact_keys(fields, &["kind", "status"], "execution-status guard")?;
            if !matches!(
                object_get(guard, "status").and_then(Value::as_str),
                Some("scheduled")
                    | Some("applied")
                    | Some("rejected")
                    | Some("canceled")
                    | Some("filled")
                    | Some("closed")
            ) {
                return Err(invalid("execution status is outside native vocabulary"));
            }
        }
        "execution_reason_is" => {
            exact_keys(fields, &["kind", "reasonCode"], "execution-reason guard")?;
            reason_code(
                required(guard, "reasonCode", "execution-reason guard")?,
                "execution reason code",
            )?;
        }
        _ => return Err(invalid("guard kind is outside the sealed v5 vocabulary")),
    }
    Ok(())
}

fn validate_v5_operator_budget(value: &Value) -> Result<()> {
    let budget = map_ref(value, "v5 budget")?;
    let budget_keys = [
        "maxStates",
        "maxTransitions",
        "maxEvidenceGroups",
        "maxGroupMembers",
        "maxEvents",
        "maxIndicators",
        "maxEntryBranches",
        "maxManagementRegions",
        "maxExitRegions",
        "maxRecoveryRegions",
        "maxSccNodes",
        "maxTimeoutBars",
        "maxGuardDepth",
    ];
    exact_keys(budget, &budget_keys, "v5 budget")?;
    for (key, cap) in [
        ("maxStates", 14_u64),
        ("maxTransitions", 56),
        ("maxEvidenceGroups", 4),
        ("maxGroupMembers", 3),
        ("maxEvents", 4),
        ("maxIndicators", 12),
        ("maxEntryBranches", 3),
        ("maxManagementRegions", 4),
        ("maxExitRegions", 3),
        ("maxRecoveryRegions", 3),
        ("maxSccNodes", 3),
        ("maxTimeoutBars", 64),
        ("maxGuardDepth", 4),
    ] {
        let current = as_u64(
            budget.get(key).expect("budget key was checked"),
            &format!("v5 budget {key}"),
        )?;
        if current < 1 || current > cap {
            return Err(invalid("v5 budget is outside the sealed initial cap"));
        }
    }
    Ok(())
}

fn validate_v5_operator_program_detail(program: &Value) -> Result<()> {
    let side = program_side(program)?;
    let instrument = text(required(program, "instrument", "v5 program")?, "instrument")?;
    if instrument != instrument.to_ascii_uppercase() {
        return Err(invalid("instrument must use canonical uppercase spelling"));
    }
    let budget = map_ref(required(program, "budget", "v5 program")?, "v5 budget")?;
    validate_v5_operator_budget(required(program, "budget", "v5 program")?)?;
    let budget_keys = [
        "maxStates",
        "maxTransitions",
        "maxEvidenceGroups",
        "maxGroupMembers",
        "maxEvents",
        "maxIndicators",
        "maxEntryBranches",
        "maxManagementRegions",
        "maxExitRegions",
        "maxRecoveryRegions",
        "maxSccNodes",
        "maxTimeoutBars",
        "maxGuardDepth",
    ];
    exact_keys(budget, &budget_keys, "v5 budget")?;
    let initial_caps = [
        ("maxStates", 14_u64),
        ("maxTransitions", 56),
        ("maxEvidenceGroups", 4),
        ("maxGroupMembers", 3),
        ("maxEvents", 4),
        ("maxIndicators", 12),
        ("maxEntryBranches", 3),
        ("maxManagementRegions", 4),
        ("maxExitRegions", 3),
        ("maxRecoveryRegions", 3),
        ("maxSccNodes", 3),
        ("maxTimeoutBars", 64),
        ("maxGuardDepth", 4),
    ];
    for (key, cap) in initial_caps {
        let value = as_u64(
            budget.get(key).expect("checked"),
            &format!("v5 budget {key}"),
        )?;
        if value < 1 || value > cap {
            return Err(invalid("v5 budget is outside the sealed initial cap"));
        }
    }
    let indicators = resource_rows(program, "indicators")?;
    let groups = resource_rows(program, "evidenceGroups")?;
    let events = resource_rows(program, "events")?;
    let management = resource_rows(program, "managementRefs")?;
    let indicator_ids = indicators
        .iter()
        .map(indicator_id)
        .collect::<Result<BTreeSet<_>>>()?;
    let mut group_ids = BTreeSet::new();
    for group in &groups {
        let fields = map_ref(group, "evidence group")?;
        exact_keys(
            fields,
            &["id", "indicatorInstanceIds", "ownerSide"],
            "evidence group",
        )?;
        let id = row_id(group, "evidence group")?;
        let member_ids = members(group)?;
        let unique = member_ids.iter().cloned().collect::<BTreeSet<_>>();
        if !group_ids.insert(id)
            || object_get(group, "ownerSide").and_then(Value::as_str) != Some(side.as_str())
            || member_ids.is_empty()
            || member_ids.len() != unique.len()
            || member_ids.len() > budget_u64(program, "maxGroupMembers")? as usize
            || !unique.is_subset(&indicator_ids)
        {
            return Err(invalid("evidence-group row is malformed or cross-side"));
        }
    }
    let mut event_ids = BTreeSet::new();
    for event in &events {
        let fields = map_ref(event, "event")?;
        exact_keys(
            fields,
            &[
                "id",
                "indicatorInstanceId",
                "longOutput",
                "shortOutput",
                "ownerSide",
            ],
            "event",
        )?;
        let id = row_id(event, "event")?;
        let source = text(
            required(event, "indicatorInstanceId", "event")?,
            "event indicator ID",
        )?;
        let long = text(required(event, "longOutput", "event")?, "event long output")?;
        let short = text(
            required(event, "shortOutput", "event")?,
            "event short output",
        )?;
        if !event_ids.insert(id)
            || !indicator_ids.contains(&source)
            || long == short
            || object_get(event, "ownerSide").and_then(Value::as_str) != Some(side.as_str())
        {
            return Err(invalid("event row is malformed or cross-side"));
        }
    }
    let mut management_ids = BTreeSet::new();
    for item in &management {
        let id = row_id(item, "management reference")?;
        if !management_ids.insert(id) {
            return Err(invalid("duplicate management reference"));
        }
        let _ = validate_management_ref(item, &side, &indicator_ids)?;
    }
    for item in &indicators {
        let fields = map_ref(item, "indicator")?;
        exact_keys(fields, &["meta", "config", "ownerSide"], "indicator")?;
        if object_get(item, "ownerSide").and_then(Value::as_str) != Some(side.as_str()) {
            return Err(invalid("indicator ownership is cross-side"));
        }
    }
    let mut declared: BTreeMap<(String, String), usize> = BTreeMap::new();
    let mut guard_groups = BTreeSet::new();
    let mut guard_events = BTreeSet::new();
    let node_rows = node_rows(program)?;
    for node in &node_rows {
        let fields = map_ref(node, "node")?;
        exact_keys(
            fields,
            &["id", "zone", "kind", "guard", "resources", "timeoutBars"],
            "node",
        )?;
        let uses = array_ref(required(node, "resources", "node")?, "node resources")?;
        let mut local = BTreeSet::new();
        for use_row in uses {
            let use_fields = map_ref(use_row, "node resource use")?;
            exact_keys(use_fields, &["kind", "id"], "node resource use")?;
            let kind = text(
                required(use_row, "kind", "node resource use")?,
                "node resource kind",
            )?;
            let id = text(
                required(use_row, "id", "node resource use")?,
                "node resource ID",
            )?;
            let known = match kind.as_str() {
                "indicator" => indicator_ids.contains(&id),
                "evidence_group" => group_ids.contains(&id),
                "event" => event_ids.contains(&id),
                "management_ref" => management_ids.contains(&id),
                _ => false,
            };
            if !known || !local.insert((kind.clone(), id.clone())) {
                return Err(invalid("node resource declaration is malformed"));
            }
            *declared.entry((kind, id)).or_default() += 1;
        }
        validate_guard(
            required(node, "guard", "node")?,
            &group_ids,
            &event_ids,
            budget_u64(program, "maxGuardDepth")?,
            1,
            &mut guard_groups,
            &mut guard_events,
        )?;
    }
    let edges = edge_rows(program)?;
    for edge in &edges {
        let fields = map_ref(edge, "edge")?;
        exact_keys(
            fields,
            &[
                "id",
                "source",
                "target",
                "eventClass",
                "priority",
                "guard",
                "effect",
            ],
            "edge",
        )?;
        validate_guard(
            required(edge, "guard", "edge")?,
            &group_ids,
            &event_ids,
            budget_u64(program, "maxGuardDepth")?,
            1,
            &mut guard_groups,
            &mut guard_events,
        )?;
    }
    for id in guard_groups {
        *declared
            .entry(("evidence_group".to_owned(), id))
            .or_default() += 1;
    }
    for id in guard_events {
        *declared.entry(("event".to_owned(), id)).or_default() += 1;
    }
    let mut used_indicators = BTreeSet::new();
    for (kind, id) in declared.keys() {
        if kind == "indicator" {
            used_indicators.insert(id.clone());
        }
        if kind == "evidence_group" {
            let row = groups
                .iter()
                .find(|row| row_id(row, "evidence group").ok().as_deref() == Some(id.as_str()))
                .ok_or_else(|| invalid("declared group disappeared"))?;
            used_indicators.extend(members(row)?);
        }
        if kind == "event" {
            let row = events
                .iter()
                .find(|row| row_id(row, "event").ok().as_deref() == Some(id.as_str()))
                .ok_or_else(|| invalid("declared event disappeared"))?;
            used_indicators.insert(text(
                required(row, "indicatorInstanceId", "event")?,
                "event indicator ID",
            )?);
        }
    }
    for management_ref in &management {
        let bindings = object_get(management_ref, "scalarBindings")
            .and_then(Value::as_array)
            .map_or(&[] as &[Value], Vec::as_slice);
        for binding in bindings {
            used_indicators.insert(text(
                required(binding, "indicatorInstanceId", "management scalar binding")?,
                "management scalar indicator",
            )?);
        }
    }
    let supplied = [
        ("indicator", indicator_ids),
        ("evidence_group", group_ids),
        ("event", event_ids),
        ("management_ref", management_ids),
    ];
    for (kind, ids) in supplied {
        let used = match kind {
            "indicator" => used_indicators.clone(),
            _ => declared
                .keys()
                .filter(|(declared_kind, _)| declared_kind == kind)
                .map(|(_, id)| id.clone())
                .collect(),
        };
        if ids != used {
            return Err(invalid(
                "module resource pool contains an orphan or unbound resource",
            ));
        }
    }
    validate_v5_operator_graph(program)
}

fn validate_v5_operator_graph(program: &Value) -> Result<()> {
    let nodes = node_rows(program)?;
    let edges = edge_rows(program)?;
    let budget = |key: &str| budget_u64(program, key);
    let node_map = rows_by_id(&nodes, "node")?;
    let start = node_map
        .values()
        .find(|node| {
            object_get(node, "zone").and_then(Value::as_str) == Some("entry")
                && object_get(node, "kind").and_then(Value::as_str) == Some("start")
        })
        .ok_or_else(|| invalid("entry start disappeared"))?;
    let hub = node_map
        .values()
        .find(|node| {
            object_get(node, "zone").and_then(Value::as_str) == Some("position")
                && object_get(node, "kind").and_then(Value::as_str) == Some("position_hub")
        })
        .ok_or_else(|| invalid("position hub disappeared"))?;
    let start_id = row_id(start, "start")?;
    let hub_id = row_id(hub, "hub")?;
    let mut entry_effects = 0_u64;
    let mut priorities = BTreeSet::new();
    let mut adjacency: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for edge in &edges {
        let source = text(required(edge, "source", "edge")?, "edge source")?;
        let target = text(required(edge, "target", "edge")?, "edge target")?;
        let source_node = node_map
            .get(&source)
            .ok_or_else(|| invalid("edge source disappeared"))?;
        let target_node = node_map
            .get(&target)
            .ok_or_else(|| invalid("edge target disappeared"))?;
        let source_zone = object_get(source_node, "zone")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let target_zone = object_get(target_node, "zone")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let source_kind = object_get(source_node, "kind")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let target_kind = object_get(target_node, "kind")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let priority = as_u64(required(edge, "priority", "edge")?, "edge priority")?;
        if !priorities.insert((source.clone(), priority)) {
            return Err(invalid("edge priority collides at a source"));
        }
        let effect = object_get(edge, "effect").and_then(Value::as_str);
        match (target_zone, target_kind) {
            ("entry", "entry") => {
                if !matches!(source_zone, "entry" | "setup") || effect != Some("enter_next_open") {
                    return Err(invalid("entry branch effect or source is invalid"));
                }
                entry_effects += 1;
            }
            ("setup", _) => {
                if !matches!(source_zone, "entry" | "setup") || effect.is_some() {
                    return Err(invalid("setup route is invalid"));
                }
            }
            ("position", "position_hub") => {
                if source_zone != "entry" || source_kind != "entry" || effect.is_some() {
                    return Err(invalid("position-hub connector is invalid"));
                }
            }
            ("management", _) => {
                if source != hub_id
                    || !matches!(
                        effect,
                        Some("move_stop_to_break_even_next_open")
                            | Some("tighten_stop_next_open")
                            | Some("set_target_next_open")
                            | Some("cancel_target_next_open")
                            | Some("activate_trailing_stop_next_open")
                            | Some("deactivate_trailing_stop_next_open")
                    )
                {
                    return Err(invalid("management dispatch is invalid"));
                }
            }
            ("exit", _) => {
                if source != hub_id || effect != Some("exit_next_open") {
                    return Err(invalid("exit dispatch is invalid"));
                }
            }
            ("recovery", _) => {
                if !matches!(source_zone, "entry" | "setup")
                    || effect.is_some()
                    || object_get(target_node, "timeoutBars").is_none_or(Value::is_null)
                {
                    return Err(invalid("recovery route is invalid"));
                }
            }
            _ => return Err(invalid("edge target zone or kind is invalid")),
        }
        adjacency.entry(source).or_default().insert(target);
    }
    if entry_effects < 1 || entry_effects > budget("maxEntryBranches")? {
        return Err(invalid("entry branch count is outside budget"));
    }
    // The compiler's SCC/timeout budget applies to authored decision edges,
    // not its runtime-only entry→hub/protective-close/recovery re-arm routes.
    // Keep the authored graph before enriching reachability below.
    let authored_adjacency = adjacency.clone();
    for node in node_map.values() {
        let id = row_id(node, "node")?;
        let zone = object_get(node, "zone")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let kind = object_get(node, "kind")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if zone == "position" && (id != hub_id || kind != "position_hub") {
            return Err(invalid("position zone admits only shared hub"));
        }
        if zone == "entry" && !matches!(kind, "start" | "entry") {
            return Err(invalid("entry zone node kind is invalid"));
        }
        if zone == "recovery" && object_get(node, "timeoutBars").is_none_or(Value::is_null) {
            return Err(invalid("recovery node requires timeout"));
        }
        if let Some(timeout) = object_get(node, "timeoutBars") {
            if !timeout.is_null() && as_u64(timeout, "node timeout")? > budget("maxTimeoutBars")? {
                return Err(invalid("node timeout exceeds budget"));
            }
        }
    }
    for node in node_map.values() {
        if object_get(node, "zone").and_then(Value::as_str) == Some("entry")
            && object_get(node, "kind").and_then(Value::as_str) == Some("entry")
        {
            adjacency
                .entry(row_id(node, "entry")?)
                .or_default()
                .insert(hub_id.clone());
        }
    }
    // The compiler owns a single ordered post-position recovery path.  A
    // protective/exit/management close routes from the shared hub into the
    // first recovery state, each bounded recovery re-arms into the next one,
    // and the final recovery re-arms the entry start.  Those runtime routes
    // are implicit in the compact genome, so graph admission must model them
    // here rather than demanding an authored entry/setup inbound edge.  An
    // authored pre-position rejection route may additionally target any
    // recovery state and is retained in `adjacency` above.
    let recovery_ids = node_map
        .values()
        .filter(|node| object_get(node, "zone").and_then(Value::as_str) == Some("recovery"))
        .map(|node| row_id(node, "recovery"))
        .collect::<Result<Vec<_>>>()?;
    if let Some(first) = recovery_ids.first() {
        adjacency
            .entry(hub_id.clone())
            .or_default()
            .insert(first.clone());
        for pair in recovery_ids.windows(2) {
            adjacency
                .entry(pair[0].clone())
                .or_default()
                .insert(pair[1].clone());
        }
        adjacency
            .entry(
                recovery_ids
                    .last()
                    .expect("first recovery has a last")
                    .clone(),
            )
            .or_default()
            .insert(start_id.clone());
    }
    let mut seen = BTreeSet::from([start_id.clone()]);
    let mut queue = vec![start_id];
    while let Some(vertex) = queue.pop() {
        for target in adjacency.get(&vertex).into_iter().flatten() {
            if seen.insert(target.clone()) {
                queue.push(target.clone());
            }
        }
    }
    if seen.len() != node_map.len() {
        return Err(invalid("graph has an unreachable node"));
    }
    let active = node_map
        .iter()
        .filter(|(_, node)| {
            matches!(
                object_get(node, "zone").and_then(Value::as_str),
                Some("entry") | Some("setup")
            )
        })
        .map(|(id, _)| id.clone())
        .collect::<BTreeSet<_>>();
    fn visit(
        vertex: &str,
        active: &BTreeSet<String>,
        adjacency: &BTreeMap<String, BTreeSet<String>>,
        visiting: &mut BTreeSet<String>,
        visited: &mut BTreeSet<String>,
    ) -> bool {
        if !visiting.insert(vertex.to_owned()) {
            return false;
        }
        for target in adjacency
            .get(vertex)
            .into_iter()
            .flatten()
            .filter(|target| active.contains(*target))
        {
            if !visited.contains(target) && !visit(target, active, adjacency, visiting, visited) {
                return false;
            }
        }
        visiting.remove(vertex);
        visited.insert(vertex.to_owned());
        true
    }
    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for vertex in &active {
        if !visited.contains(vertex)
            && !visit(vertex, &active, &adjacency, &mut visiting, &mut visited)
        {
            return Err(invalid("entry/setup graph must be acyclic"));
        }
    }
    // Bounded SCC admission: small cyclic components need a timeout owner.
    let all_ids = node_map.keys().cloned().collect::<Vec<_>>();
    let mut index = 0_u64;
    let mut indices = BTreeMap::new();
    let mut low = BTreeMap::new();
    let mut stack = Vec::new();
    let mut on_stack = BTreeSet::new();
    fn scc(
        vertex: &str,
        adjacency: &BTreeMap<String, BTreeSet<String>>,
        node_map: &BTreeMap<String, Value>,
        index: &mut u64,
        indices: &mut BTreeMap<String, u64>,
        low: &mut BTreeMap<String, u64>,
        stack: &mut Vec<String>,
        on_stack: &mut BTreeSet<String>,
        max_nodes: u64,
    ) -> Result<()> {
        indices.insert(vertex.to_owned(), *index);
        low.insert(vertex.to_owned(), *index);
        *index += 1;
        stack.push(vertex.to_owned());
        on_stack.insert(vertex.to_owned());
        for target in adjacency.get(vertex).into_iter().flatten() {
            if !indices.contains_key(target) {
                scc(
                    target, adjacency, node_map, index, indices, low, stack, on_stack, max_nodes,
                )?;
                let value = *low
                    .get(vertex)
                    .expect("vertex low")
                    .min(low.get(target).expect("target low"));
                low.insert(vertex.to_owned(), value);
            } else if on_stack.contains(target) {
                let value = *low
                    .get(vertex)
                    .expect("vertex low")
                    .min(indices.get(target).expect("target index"));
                low.insert(vertex.to_owned(), value);
            }
        }
        if low.get(vertex) == indices.get(vertex) {
            let mut component = Vec::new();
            loop {
                let item = stack.pop().ok_or_else(|| invalid("SCC stack underflow"))?;
                on_stack.remove(&item);
                let done = item == vertex;
                component.push(item);
                if done {
                    break;
                }
            }
            let cyclic = component.len() > 1
                || adjacency
                    .get(vertex)
                    .is_some_and(|targets| targets.contains(vertex));
            if cyclic
                && (component.len() as u64 > max_nodes
                    || !component.iter().any(|id| {
                        object_get(node_map.get(id).expect("node"), "timeoutBars")
                            .is_some_and(|value| !value.is_null())
                    }))
            {
                return Err(invalid(
                    "cyclic graph violates bounded timeout/SCC contract",
                ));
            }
        }
        Ok(())
    }
    for id in all_ids {
        if !indices.contains_key(&id) {
            scc(
                &id,
                &authored_adjacency,
                &node_map,
                &mut index,
                &mut indices,
                &mut low,
                &mut stack,
                &mut on_stack,
                budget("maxSccNodes")?,
            )?;
        }
    }
    Ok(())
}

fn construction_plan(
    program: &Value,
    authority: &V5OperatorAuthority,
    choice_kind: &str,
    operator_id: &str,
    construction: Value,
) -> Result<Value> {
    validate_program(program, authority)?;
    let operator_version = match operator_id {
        V5_RESOURCE_OPERATOR_ID => RESOURCE_OPERATOR_VERSION,
        V5_TEMPORAL_OPERATOR_ID => TEMPORAL_OPERATOR_VERSION,
        V5_TOPOLOGY_OPERATOR_ID => TOPOLOGY_OPERATOR_SCHEMA,
        V5_HOLD_OPERATOR_ID => "evolvable_module_hold_policy_v1",
        V5_INITIAL_PROTECTION_OPERATOR_ID => "temporal_qd_initial_protection_policy_v2",
        V5_CROSSOVER_OPERATOR_ID => CROSSOVER_SCHEMA,
        _ => return Err(invalid("unknown v5 operator ID")),
    };
    let mut plan = object([
        (
            "schemaVersion",
            Value::String(V5_OPERATOR_PLAN_SCHEMA.to_owned()),
        ),
        ("choiceKind", Value::String(choice_kind.to_owned())),
        ("operatorId", Value::String(operator_id.to_owned())),
        (
            "operatorVersion",
            Value::String(operator_version.to_owned()),
        ),
        ("parentProgramSha256", Value::String(sha(program)?)),
        (
            "authoritySha256",
            Value::String(authority.authority_sha256().to_owned()),
        ),
        ("construction", canonical_clone(&construction)?),
    ]);
    let plan_sha = sha(&plan)?;
    map_mut(&mut plan, "v5 operator plan")?
        .insert("planSha256".to_owned(), Value::String(plan_sha));
    Ok(plan)
}

fn verify_plan(program: &Value, authority: &V5OperatorAuthority, plan: &Value) -> Result<()> {
    validate_program(program, authority)?;
    let fields = map_ref(plan, "v5 operator plan")?;
    let expected = [
        "schemaVersion",
        "choiceKind",
        "operatorId",
        "operatorVersion",
        "parentProgramSha256",
        "authoritySha256",
        "construction",
        "planSha256",
    ];
    if fields.len() != expected.len()
        || expected.iter().any(|key| !fields.contains_key(*key))
        || object_get(plan, "schemaVersion").and_then(Value::as_str)
            != Some(V5_OPERATOR_PLAN_SCHEMA)
    {
        return Err(invalid("v5 operator plan envelope is not exact"));
    }
    let operator_id = text(
        required(plan, "operatorId", "v5 operator plan")?,
        "operator ID",
    )?;
    let expected_version = match operator_id.as_str() {
        V5_RESOURCE_OPERATOR_ID => RESOURCE_OPERATOR_VERSION,
        V5_TEMPORAL_OPERATOR_ID => TEMPORAL_OPERATOR_VERSION,
        V5_TOPOLOGY_OPERATOR_ID => TOPOLOGY_OPERATOR_SCHEMA,
        V5_HOLD_OPERATOR_ID => "evolvable_module_hold_policy_v1",
        V5_INITIAL_PROTECTION_OPERATOR_ID => "temporal_qd_initial_protection_policy_v2",
        V5_CROSSOVER_OPERATOR_ID => CROSSOVER_SCHEMA,
        _ => return Err(invalid("v5 operator plan has an unknown operator ID")),
    };
    if object_get(plan, "operatorVersion").and_then(Value::as_str) != Some(expected_version) {
        return Err(invalid("v5 operator plan version is incompatible"));
    }
    let supplied = text(
        required(plan, "planSha256", "v5 operator plan")?,
        "plan SHA-256",
    )?;
    let mut body = canonical_clone(plan)?;
    map_mut(&mut body, "v5 operator plan")?.remove("planSha256");
    if supplied != sha(&body)? {
        return Err(invalid("v5 operator plan identity mismatch"));
    }
    if object_get(plan, "parentProgramSha256").and_then(Value::as_str)
        != Some(sha(program)?.as_str())
        || object_get(plan, "authoritySha256").and_then(Value::as_str)
            != Some(authority.authority_sha256())
    {
        return Err(invalid(
            "v5 operator plan is stale, foreign, or authority-drifted",
        ));
    }
    Ok(())
}

fn application(
    plan: &Value,
    parent: &Value,
    child: &Value,
    authority: &V5OperatorAuthority,
    trace: Value,
) -> Result<Value> {
    verify_plan(parent, authority, plan)?;
    validate_program(child, authority)?;
    let mut audit = object([
        (
            "schemaVersion",
            Value::String(V5_OPERATOR_APPLICATION_SCHEMA.to_owned()),
        ),
        (
            "planSha256",
            canonical_clone(required(plan, "planSha256", "v5 operator plan")?)?,
        ),
        (
            "operatorId",
            canonical_clone(required(plan, "operatorId", "v5 operator plan")?)?,
        ),
        ("parentProgramSha256", Value::String(sha(parent)?)),
        ("childProgramSha256", Value::String(sha(child)?)),
        ("mutationTrace", trace),
        ("allChecksPassed", Value::Bool(true)),
    ]);
    let audit_sha = sha(&audit)?;
    map_mut(&mut audit, "v5 operator application")?
        .insert("applicationSha256".to_owned(), Value::String(audit_sha));
    Ok(audit)
}

fn program_side(program: &Value) -> Result<String> {
    let side = text(
        required(program, "direction", "v5 program")?,
        "v5 program direction",
    )?;
    exact_side(&side)?;
    Ok(side)
}

fn resource_rows(program: &Value, key: &str) -> Result<Vec<Value>> {
    Ok(array_ref(
        required(
            required(program, "resources", "v5 program")?,
            key,
            "v5 resources",
        )?,
        &format!("v5 resources {key}"),
    )?
    .to_vec())
}

fn node_rows(program: &Value) -> Result<Vec<Value>> {
    Ok(array_ref(required(program, "nodes", "v5 program")?, "v5 nodes")?.to_vec())
}

fn edge_rows(program: &Value) -> Result<Vec<Value>> {
    Ok(array_ref(required(program, "edges", "v5 program")?, "v5 edges")?.to_vec())
}

fn rows_by_id(rows: &[Value], kind: &str) -> Result<BTreeMap<String, Value>> {
    let mut output = BTreeMap::new();
    for row in rows {
        let id = if kind == "indicator" {
            indicator_id(row)?
        } else {
            row_id(row, kind)?
        };
        if output.insert(id, canonical_clone(row)?).is_some() {
            return Err(invalid(format!("duplicate {kind} ID")));
        }
    }
    Ok(output)
}

fn node_with_resource(node: &Value, kind: &str, resource_id: &str) -> Result<Value> {
    let mut node = canonical_clone(node)?;
    let fields = map_mut(&mut node, "node")?;
    let uses = required_mut(fields, "resources", "node")?
        .as_array_mut()
        .ok_or_else(|| invalid("node resources must be an array"))?;
    if uses.iter().any(|row| {
        object_get(row, "kind").and_then(Value::as_str) == Some(kind)
            && object_get(row, "id").and_then(Value::as_str) == Some(resource_id)
    }) {
        return Err(invalid("resource is already owned by this route"));
    }
    uses.push(object([
        ("kind", Value::String(kind.to_owned())),
        ("id", Value::String(resource_id.to_owned())),
    ]));
    Ok(node)
}

fn node_without_resource(node: &Value, kind: &str, resource_id: &str) -> Result<Value> {
    let mut node = canonical_clone(node)?;
    let fields = map_mut(&mut node, "node")?;
    let uses = required_mut(fields, "resources", "node")?
        .as_array_mut()
        .ok_or_else(|| invalid("node resources must be an array"))?;
    let before = uses.len();
    uses.retain(|row| {
        !(object_get(row, "kind").and_then(Value::as_str) == Some(kind)
            && object_get(row, "id").and_then(Value::as_str) == Some(resource_id))
    });
    if uses.len() == before {
        return Err(invalid("resource is not owned by this route"));
    }
    Ok(node)
}

fn replace_node(program: &Value, changed: &Value) -> Result<Value> {
    let changed_id = row_id(changed, "node")?;
    let mut output = canonical_clone(program)?;
    let fields = map_mut(&mut output, "v5 program")?;
    let nodes = required_mut(fields, "nodes", "v5 program")?
        .as_array_mut()
        .ok_or_else(|| invalid("v5 nodes must be an array"))?;
    let mut found = false;
    for row in nodes.iter_mut() {
        if row_id(row, "node")? == changed_id {
            *row = canonical_clone(changed)?;
            found = true;
            break;
        }
    }
    if !found {
        return Err(invalid("changed node does not exist in program"));
    }
    normalize_program(&output)
}

fn replace_edge(program: &Value, changed: &Value) -> Result<Value> {
    let changed_id = row_id(changed, "edge")?;
    let mut output = canonical_clone(program)?;
    let fields = map_mut(&mut output, "v5 program")?;
    let edges = required_mut(fields, "edges", "v5 program")?
        .as_array_mut()
        .ok_or_else(|| invalid("v5 edges must be an array"))?;
    let mut found = false;
    for row in edges.iter_mut() {
        if row_id(row, "edge")? == changed_id {
            *row = canonical_clone(changed)?;
            found = true;
            break;
        }
    }
    if !found {
        return Err(invalid("changed edge does not exist in program"));
    }
    normalize_program(&output)
}

fn replace_resource_rows(
    program: &Value,
    indicators: Option<Vec<Value>>,
    groups: Option<Vec<Value>>,
    events: Option<Vec<Value>>,
    management: Option<Vec<Value>>,
) -> Result<Value> {
    let mut output = canonical_clone(program)?;
    let fields = map_mut(&mut output, "v5 program")?;
    let resources = map_mut(
        required_mut(fields, "resources", "v5 program")?,
        "v5 resources",
    )?;
    if let Some(rows) = indicators {
        resources.insert("indicators".to_owned(), array(rows));
    }
    if let Some(rows) = groups {
        resources.insert("evidenceGroups".to_owned(), array(rows));
    }
    if let Some(rows) = events {
        resources.insert("events".to_owned(), array(rows));
    }
    if let Some(rows) = management {
        resources.insert("managementRefs".to_owned(), array(rows));
    }
    normalize_program(&output)
}

fn replace_nodes_edges(
    program: &Value,
    nodes: Option<Vec<Value>>,
    edges: Option<Vec<Value>>,
) -> Result<Value> {
    let mut output = canonical_clone(program)?;
    let fields = map_mut(&mut output, "v5 program")?;
    if let Some(rows) = nodes {
        fields.insert("nodes".to_owned(), array(rows));
    }
    if let Some(rows) = edges {
        fields.insert("edges".to_owned(), array(rows));
    }
    normalize_program(&output)
}

fn references(program: &Value, resource_kind: &str, resource_id: &str) -> Result<Vec<Value>> {
    Ok(node_rows(program)?
        .into_iter()
        .filter(|node| {
            array_ref(
                required(node, "resources", "node").unwrap_or(&Value::Null),
                "node resources",
            )
            .map(|uses| {
                uses.iter().any(|use_row| {
                    object_get(use_row, "kind").and_then(Value::as_str) == Some(resource_kind)
                        && object_get(use_row, "id").and_then(Value::as_str) == Some(resource_id)
                })
            })
            .unwrap_or(false)
        })
        .collect())
}

fn groups_for_indicator(program: &Value, instance_id: &str) -> Result<Vec<String>> {
    let mut result = Vec::new();
    for row in resource_rows(program, "evidenceGroups")? {
        let members = array_ref(
            required(&row, "indicatorInstanceIds", "evidence group")?,
            "evidence group members",
        )?;
        if members
            .iter()
            .any(|item| item.as_str() == Some(instance_id))
        {
            result.push(row_id(&row, "evidence group")?);
        }
    }
    Ok(result)
}

fn event_ids_for_indicator(program: &Value, instance_id: &str) -> Result<Vec<String>> {
    let mut result = Vec::new();
    for row in resource_rows(program, "events")? {
        if object_get(&row, "indicatorInstanceId").and_then(Value::as_str) == Some(instance_id) {
            result.push(row_id(&row, "event")?);
        }
    }
    Ok(result)
}

fn indicator_exclusive_to_group(
    program: &Value,
    instance_id: &str,
    group_id: &str,
) -> Result<bool> {
    Ok(
        groups_for_indicator(program, instance_id)? == [group_id.to_owned()]
            && event_ids_for_indicator(program, instance_id)?.is_empty(),
    )
}

fn all_with(guard: &Value, clause: &Value) -> Result<Value> {
    let guard = canonical_clone(guard)?;
    let clause = canonical_clone(clause)?;
    if object_get(&guard, "kind").and_then(Value::as_str) == Some("all")
        && object_get(&guard, "guards")
            .and_then(Value::as_array)
            .is_some()
    {
        let mut value = guard;
        let rows = map_mut(&mut value, "all guard")?
            .get_mut("guards")
            .and_then(Value::as_array_mut)
            .ok_or_else(|| invalid("all guard rows drifted"))?;
        rows.push(clause);
        Ok(value)
    } else {
        Ok(object([
            ("kind", Value::String("all".to_owned())),
            ("guards", array([guard, clause])),
        ]))
    }
}

fn direct_all_remove(
    guard: &Value,
    kind: &str,
    field: &str,
    expected: &str,
) -> Result<Option<Value>> {
    if object_get(guard, "kind").and_then(Value::as_str) != Some("all") {
        return Ok(None);
    }
    let rows = match object_get(guard, "guards").and_then(Value::as_array) {
        Some(rows) => rows,
        None => return Ok(None),
    };
    let kept = rows
        .iter()
        .filter(|row| {
            !(object_get(row, "kind").and_then(Value::as_str) == Some(kind)
                && object_get(row, field).and_then(Value::as_str) == Some(expected))
        })
        .map(canonical_clone)
        .collect::<Result<Vec<_>>>()?;
    if kept.len() == rows.len() || kept.is_empty() {
        return Ok(None);
    }
    if kept.len() == 1 {
        Ok(Some(kept[0].clone()))
    } else {
        Ok(Some(object([
            ("kind", Value::String("all".to_owned())),
            ("guards", array(kept)),
        ])))
    }
}

fn stable_id(prefix: &str, seed: Value, existing: &BTreeSet<String>) -> Result<String> {
    let hash = sha(&seed)?;
    let base = format!("{prefix}_{}", &hash[7..19]);
    let mut candidate = base.clone();
    let mut suffix = 2_u64;
    while existing.contains(&candidate) {
        candidate = format!("{base}_{suffix}");
        suffix = suffix
            .checked_add(1)
            .ok_or_else(|| invalid("resource ID suffix overflowed"))?;
    }
    Ok(candidate)
}

fn timeframe_policy_from_indicator_policy(
    indicator_policy: &Value,
    catalog: &Value,
) -> Result<Vec<String>> {
    let available = map_ref(
        required(catalog, "timeframes", "frozen catalog")?,
        "catalog timeframes",
    )?
    .keys()
    .map(|item| item.trim().to_ascii_uppercase())
    .filter(|item| !item.is_empty())
    .collect::<BTreeSet<_>>();
    let rows = array_ref(
        required(
            indicator_policy,
            "timeframePolicy",
            "sealed indicator policy",
        )?,
        "sealed indicator policy timeframePolicy",
    )?;
    let mut policy = rows
        .iter()
        .map(|row| text(row, "timeframe policy item").map(|item| item.to_ascii_uppercase()))
        .collect::<Result<Vec<_>>>()?;
    policy.sort();
    policy.dedup();
    if policy.is_empty()
        || policy.len() != rows.len()
        || policy.iter().any(|frame| !available.contains(frame))
    {
        return Err(invalid(
            "v5 operator timeframe policy is not catalog-backed",
        ));
    }
    Ok(policy)
}

/// The lookback grid is sealed in the same indicator-learning policy as the
/// timeframe grid.  Do not quietly fall back to the historical Python
/// constant here: a new authority must explicitly carry its searchable
/// domain or later-generation mutation is not safe to replay.
fn evidence_lookback_choices_from_indicator_policy(indicator_policy: &Value) -> Result<Vec<u64>> {
    let rows = array_ref(
        required(
            indicator_policy,
            "evidenceLookbackChoices",
            "sealed indicator policy",
        )?,
        "sealed indicator policy evidenceLookbackChoices",
    )?;
    let mut choices = rows
        .iter()
        .map(|row| as_u64(row, "evidence lookback choice"))
        .collect::<Result<Vec<_>>>()?;
    choices.sort_unstable();
    choices.dedup();
    if choices.is_empty() || choices.len() != rows.len() || choices.contains(&0) {
        return Err(invalid(
            "v5 operator evidence lookback policy is not a nonempty sorted unique positive domain",
        ));
    }
    Ok(choices)
}

fn catalog_entries_from_catalog(catalog: &Value) -> Result<BTreeMap<String, Value>> {
    let mut output = BTreeMap::new();
    for row in array_ref(
        required(catalog, "indicators", "frozen catalog")?,
        "catalog indicators",
    )? {
        let meta = required(row, "meta", "catalog indicator")?;
        let id = text(
            required(meta, "id", "catalog indicator meta")?,
            "catalog indicator ID",
        )?;
        if output.insert(id, canonical_clone(row)?).is_some() {
            return Err(invalid("frozen catalog has duplicate indicator IDs"));
        }
    }
    if output.is_empty() {
        return Err(invalid("frozen catalog has no indicators"));
    }
    Ok(output)
}

fn strip_catalog_meta(value: &Value) -> Result<Value> {
    let mut output = canonical_clone(value)?;
    let fields = map_mut(&mut output, "indicator metadata")?;
    fields.remove("instanceId");
    fields.remove("docs");
    Ok(output)
}

fn catalog_meta_matches(authored: &Value, source: &Value) -> Result<bool> {
    // Frozen v5 construction material is hydrated straight from catalog JSON.
    // The documented Python compatibility carve-out is instance ID and docs;
    // all capability-bearing fields must still match exactly.
    Ok(canonical_json(&strip_catalog_meta(authored)?)?
        == canonical_json(&strip_catalog_meta(source)?)?)
}

fn numeric_range(meta: &Value) -> Result<Option<Value>> {
    let Some(range) = object_get(meta, "valueRange").and_then(Value::as_object) else {
        return Ok(None);
    };
    let minimum = match range.get("min") {
        Some(value) => finite_number(value, "value range min")?,
        None => return Ok(None),
    };
    let maximum = match range.get("max") {
        Some(value) => finite_number(value, "value range max")?,
        None => return Ok(None),
    };
    let step = match range.get("step") {
        Some(value) => finite_number(value, "value range step")?,
        None => return Ok(None),
    };
    let width = match range.get("minRange") {
        Some(value) => finite_number(value, "value range minRange")?,
        None => return Ok(None),
    };
    if step <= 0.0 || width <= 0.0 || maximum - minimum < width {
        return Ok(None);
    }
    Ok(Some(object([
        ("min", value_number(minimum, "range min")?),
        ("max", value_number(maximum, "range max")?),
        ("step", value_number(step, "range step")?),
        ("minRange", value_number(width, "range minRange")?),
    ])))
}

fn scalar_outputs(meta: &Value) -> Result<Option<Vec<Value>>> {
    let Some(rows) = object_get(meta, "managementScalarOutputs").and_then(Value::as_array) else {
        return Ok(None);
    };
    if rows.is_empty() {
        return Ok(None);
    }
    let mut output = Vec::new();
    let mut seen = BTreeSet::new();
    for row in rows {
        let output_key = object_get(row, "outputKey")
            .and_then(Value::as_str)
            .filter(|item| !item.is_empty())
            .ok_or_else(|| invalid("scalar output key is invalid"))?;
        let value_kind = object_get(row, "valueKind")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("scalar output kind is invalid"))?;
        let unit = object_get(row, "unit")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("scalar output unit is invalid"))?;
        let expected = match value_kind {
            "price_level" => "price",
            "price_distance" => "price_distance",
            _ => return Ok(None),
        };
        if unit != expected {
            return Ok(None);
        }
        let token = (
            output_key.to_owned(),
            value_kind.to_owned(),
            unit.to_owned(),
        );
        if !seen.insert(token.clone()) {
            return Ok(None);
        }
        output.push(object([
            ("outputKey", Value::String(token.0)),
            ("valueKind", Value::String(token.1)),
            ("unit", Value::String(token.2)),
        ]));
    }
    output.sort_by(|left, right| {
        canonical_json(left)
            .unwrap_or_default()
            .cmp(&canonical_json(right).unwrap_or_default())
    });
    Ok(Some(output))
}

fn substitution_contract(meta: &Value) -> Result<Option<Value>> {
    let Some(raw) = object_get(meta, "familySubstitution").and_then(Value::as_object) else {
        return Ok(None);
    };
    let required = [
        "substitutionClass",
        "polarity",
        "scoreUnit",
        "rawUnit",
        "eventOutputSchema",
        "persistenceCompatibility",
    ];
    if required.iter().any(|key| !raw.contains_key(*key)) {
        return Ok(None);
    }
    if [
        "substitutionClass",
        "polarity",
        "scoreUnit",
        "rawUnit",
        "persistenceCompatibility",
    ]
    .iter()
    .any(|key| {
        raw.get(*key)
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
    }) || raw.get("persistenceCompatibility") != object_get(meta, "signalPersistence")
    {
        return Ok(None);
    }
    Ok(Some(canonical_clone(&Value::Object(raw.clone()))?))
}

fn fuzzy_contract(meta: &Value) -> Result<Option<Value>> {
    if object_get(meta, "signalPersistence").and_then(Value::as_str) != Some("state")
        || object_get(meta, "usesRangeConfiguration") != Some(&Value::Bool(true))
        || numeric_range(meta)?.is_none()
    {
        return Ok(None);
    }
    let scalar = scalar_outputs(meta)?.unwrap_or_default();
    if object_get(meta, "familySubstitution").is_none() {
        return Ok(Some(object([
            ("kind", Value::String("fuzzy_evidence".to_owned())),
            (
                "schema",
                Value::String("derived_ranged_state_score_v1".to_owned()),
            ),
            ("scalarOutputs", array(scalar)),
        ])));
    }
    let Some(contract) = substitution_contract(meta)? else {
        return Ok(None);
    };
    if object_get(&contract, "eventOutputSchema")
        .and_then(Value::as_object)
        .is_none()
    {
        return Ok(None);
    }
    Ok(Some(object([
        ("kind", Value::String("fuzzy_evidence".to_owned())),
        (
            "schema",
            Value::String("explicit_family_substitution_v1".to_owned()),
        ),
        ("contract", contract),
        ("scalarOutputs", array(scalar)),
    ])))
}

fn event_contract(meta: &Value) -> Result<Option<Value>> {
    let persistence = object_get(meta, "signalPersistence").and_then(Value::as_str);
    if !matches!(persistence, Some("event") | Some("event-with-lookback")) {
        return Ok(None);
    }
    let Some(contract) = substitution_contract(meta)? else {
        return Ok(None);
    };
    let Some(schema) = object_get(&contract, "eventOutputSchema").and_then(Value::as_object) else {
        return Ok(None);
    };
    if schema.get("kind").and_then(Value::as_str) != Some("directional_tokens") {
        return Ok(None);
    }
    let long = schema
        .get("longOutput")
        .and_then(Value::as_str)
        .filter(|item| !item.is_empty());
    let short = schema
        .get("shortOutput")
        .and_then(Value::as_str)
        .filter(|item| !item.is_empty());
    let Some((long, short)) = long.zip(short) else {
        return Ok(None);
    };
    if long == short {
        return Ok(None);
    }
    Ok(Some(object([
        ("kind", Value::String("raw_event".to_owned())),
        (
            "signalPersistence",
            Value::String(persistence.expect("checked").to_owned()),
        ),
        (
            "eventOutputSchema",
            object([
                ("kind", Value::String("directional_tokens".to_owned())),
                ("longOutput", Value::String(long.to_owned())),
                ("shortOutput", Value::String(short.to_owned())),
            ]),
        ),
    ])))
}

fn binding_contract(meta: &Value, fuzzy: bool, event: bool, scalar: bool) -> Result<Option<Value>> {
    if event && (fuzzy || scalar) {
        return Ok(None);
    }
    let mut capabilities = Vec::new();
    if fuzzy {
        let Some(value) = fuzzy_contract(meta)? else {
            return Ok(None);
        };
        capabilities.push(value);
    }
    if event {
        let Some(value) = event_contract(meta)? else {
            return Ok(None);
        };
        capabilities.push(value);
    }
    if scalar {
        let Some(outputs) = scalar_outputs(meta)? else {
            return Ok(None);
        };
        if object_get(meta, "signalPersistence").and_then(Value::as_str) != Some("state") {
            return Ok(None);
        }
        capabilities.push(object([
            ("kind", Value::String("scalar_management".to_owned())),
            ("outputs", array(outputs)),
        ]));
    }
    if capabilities.is_empty() {
        return Ok(None);
    }
    Ok(Some(object([
        (
            "schemaVersion",
            Value::String("temporal_indicator_binding_contract_v1".to_owned()),
        ),
        ("capabilities", array(capabilities)),
    ])))
}

fn catalog_item(
    authority: &V5OperatorAuthority,
    indicator_key: &str,
    instance_id: &str,
    side: &str,
) -> Result<Value> {
    let entry = authority
        .catalog_entries()
        .get(indicator_key)
        .cloned()
        .ok_or_else(|| invalid("catalog indicator disappeared"))?;
    let mut output = object([
        (
            "meta",
            canonical_clone(required(&entry, "meta", "catalog indicator")?)?,
        ),
        (
            "config",
            canonical_clone(required(&entry, "config", "catalog indicator")?)?,
        ),
        ("ownerSide", Value::String(side.to_owned())),
    ]);
    let fields = map_mut(&mut output, "catalog item")?;
    let meta = map_mut(
        required_mut(fields, "meta", "catalog item")?,
        "catalog item meta",
    )?;
    meta.insert(
        "instanceId".to_owned(),
        Value::String(instance_id.to_owned()),
    );
    let config = map_mut(
        required_mut(fields, "config", "catalog item")?,
        "catalog item config",
    )?;
    config.insert("isActive".to_owned(), Value::Bool(true));
    config.insert("useFormingBar".to_owned(), Value::Bool(false));
    let policy = authority.timeframe_policy();
    let existing = config
        .get("timeframe")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_uppercase();
    config.insert(
        "timeframe".to_owned(),
        Value::String(if policy.contains(&existing) {
            existing
        } else {
            policy[0].clone()
        }),
    );
    Ok(output)
}

fn replace_indicator_from_catalog(
    authority: &V5OperatorAuthority,
    item: &Value,
    replacement: &str,
    side: &str,
) -> Result<Value> {
    let meta = required(item, "meta", "indicator")?;
    let config = map_ref(required(item, "config", "indicator")?, "indicator config")?;
    let instance_id = text(
        required(meta, "instanceId", "indicator meta")?,
        "indicator instance ID",
    )?;
    let mut output = catalog_item(authority, replacement, &instance_id, side)?;
    let target = map_mut(
        required_mut(
            map_mut(&mut output, "catalog replacement")?,
            "config",
            "catalog replacement",
        )?,
        "catalog replacement config",
    )?;
    for key in [
        "isActive",
        "useFormingBar",
        "timeframe",
        "lookbackBars",
        "weight",
        "ranges",
    ] {
        if let Some(value) = config.get(key) {
            target.insert(key.to_owned(), canonical_clone(value)?);
        }
    }
    target.insert("useFormingBar".to_owned(), Value::Bool(false));
    Ok(output)
}

fn source_catalog_matches(authority: &V5OperatorAuthority, item: &Value) -> Result<bool> {
    let meta = match object_get(item, "meta") {
        Some(value) => value,
        None => return Ok(false),
    };
    let id = match object_get(meta, "id").and_then(Value::as_str) {
        Some(value) => value,
        None => return Ok(false),
    };
    let Some(source) = authority.catalog_entries().get(id) else {
        return Ok(false);
    };
    catalog_meta_matches(meta, required(source, "meta", "catalog indicator")?)
}

fn canonical_eq(left: &Value, right: &Value) -> Result<bool> {
    Ok(canonical_json(left)? == canonical_json(right)?)
}

fn exact_number_eq(left: &Value, right: &Value, label: &str) -> Result<bool> {
    Ok(finite_number(left, label)? == finite_number(right, label)?)
}

/// Validate one TA parameter list against the catalog descriptor.  A child may
/// move an eligible *period* parameter only to the exact fast/nominal/slow
/// choice Python enumerates; all other parameters retain their catalog value.
fn validate_talib_config_against_catalog(
    source_meta: &Value,
    source_config: &Map<String, Value>,
    authored_config: &Map<String, Value>,
) -> Result<()> {
    let source_rows = source_config.get("talibConfig");
    let authored_rows = authored_config.get("talibConfig");
    match (source_rows, authored_rows) {
        (None, None) => return Ok(()),
        (Some(_), None) | (None, Some(_)) => {
            return Err(invalid("indicator TA configuration presence drifted"));
        }
        (Some(source_rows), Some(authored_rows)) => {
            let source_rows = array_ref(source_rows, "catalog TA configuration")?;
            let authored_rows = array_ref(authored_rows, "authored TA configuration")?;
            if source_rows.len() != authored_rows.len() {
                return Err(invalid("indicator TA configuration length drifted"));
            }
            let descriptors = object_get(source_meta, "talibMeta")
                .map(|value| array_ref(value, "catalog TA descriptors"))
                .transpose()?
                .unwrap_or(&[]);
            for (source, authored) in source_rows.iter().zip(authored_rows) {
                let source_fields = map_ref(source, "catalog TA parameter")?;
                let authored_fields = map_ref(authored, "authored TA parameter")?;
                exact_keys(source_fields, &["name", "value"], "catalog TA parameter")?;
                exact_keys(authored_fields, &["name", "value"], "authored TA parameter")?;
                let name = text(required(source, "name", "catalog TA parameter")?, "TA name")?;
                if text(
                    required(authored, "name", "authored TA parameter")?,
                    "TA name",
                )? != name
                {
                    return Err(invalid("indicator TA parameter order/name drifted"));
                }
                let source_value = required(source, "value", "catalog TA parameter")?;
                let authored_value = required(authored, "value", "authored TA parameter")?;
                if canonical_eq(source_value, authored_value)? {
                    continue;
                }
                let descriptor = descriptors.iter().find(|row| {
                    object_get(row, "name").and_then(Value::as_str) == Some(name.as_str())
                });
                let Some(descriptor) = descriptor else {
                    return Err(invalid("indicator TA parameter is not catalog-mutable"));
                };
                let ui_type = object_get(descriptor, "uiType").and_then(Value::as_str);
                if !name.to_ascii_lowercase().contains("period")
                    || !matches!(ui_type, Some("integer_slider") | Some("float_slider"))
                {
                    return Err(invalid("indicator TA parameter is not an admitted period"));
                }
                let default = finite_number(
                    required(descriptor, "default", "catalog TA descriptor")?,
                    "catalog TA default",
                )?;
                let minimum = finite_number(
                    required(descriptor, "min", "catalog TA descriptor")?,
                    "catalog TA minimum",
                )?;
                let maximum = finite_number(
                    required(descriptor, "max", "catalog TA descriptor")?,
                    "catalog TA maximum",
                )?;
                if minimum > default || default > maximum {
                    return Err(invalid("catalog TA period descriptor bounds are invalid"));
                }
                let marks = object_get(descriptor, "marks")
                    .map(|value| array_ref(value, "catalog TA marks"))
                    .transpose()?
                    .unwrap_or(&[]);
                let mut less = marks
                    .iter()
                    .filter_map(|mark| object_get(mark, "value"))
                    .filter_map(Value::as_f64)
                    .filter(|value| *value < default)
                    .collect::<Vec<_>>();
                let mut greater = marks
                    .iter()
                    .filter_map(|mark| object_get(mark, "value"))
                    .filter_map(Value::as_f64)
                    .filter(|value| *value > default)
                    .collect::<Vec<_>>();
                less.sort_by(f64::total_cmp);
                greater.sort_by(f64::total_cmp);
                let fast = less.last().copied().unwrap_or(minimum);
                let slow = greater.first().copied().unwrap_or(maximum);
                let candidate = finite_number(authored_value, "authored TA period")?;
                if ![fast, default, slow].contains(&candidate) {
                    return Err(invalid(
                        "indicator TA period is outside the sealed choice grid",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_ranges_against_catalog(source_meta: &Value, value: &Value) -> Result<()> {
    let range = numeric_range(source_meta)?
        .ok_or_else(|| invalid("indicator range mutation lacks a catalog valueRange"))?;
    let minimum = finite_number(required(&range, "min", "catalog valueRange")?, "range min")?;
    let maximum = finite_number(required(&range, "max", "catalog valueRange")?, "range max")?;
    let step = finite_number(
        required(&range, "step", "catalog valueRange")?,
        "range step",
    )?;
    let minimum_width = finite_number(
        required(&range, "minRange", "catalog valueRange")?,
        "range minimum width",
    )?;
    let fields = map_ref(value, "indicator ranges")?;
    exact_keys(fields, &["buy", "sell"], "indicator ranges")?;
    for side in ["buy", "sell"] {
        let pair = array_ref(
            required(value, side, "indicator ranges")?,
            "indicator range",
        )?;
        if pair.len() != 2 {
            return Err(invalid("indicator range must have lower/upper values"));
        }
        let lower = finite_number(&pair[0], "indicator range lower")?;
        let upper = finite_number(&pair[1], "indicator range upper")?;
        let on_grid = |number: f64| {
            let quotient = (number - minimum) / step;
            (quotient - quotient.round()).abs() <= 1e-9
        };
        if lower < minimum
            || upper > maximum
            || upper - lower < minimum_width
            || !on_grid(lower)
            || !on_grid(upper)
        {
            return Err(invalid(
                "indicator range is outside the sealed catalog grid",
            ));
        }
    }
    Ok(())
}

fn validate_indicator_config_against_catalog(
    source_meta: &Value,
    source_config: &Value,
    authored_config: &Value,
    authority: &V5OperatorAuthority,
) -> Result<()> {
    let source = map_ref(source_config, "catalog indicator config")?;
    let authored = map_ref(authored_config, "authored indicator config")?;
    // `_catalog_item` owns three runtime-normalized fields and subsequent
    // resource mutations may add a fuzzy weight or evidence lookback.  Every
    // other authored key has to originate in the sealed catalog; this keeps
    // the closure exact without rejecting legitimate Python-generated rows
    // whose raw catalog config omitted a normal default.
    const RUNTIME_MUTABLE_KEYS: &[&str] = &[
        "isActive",
        "useFormingBar",
        "timeframe",
        "lookbackBars",
        "weight",
        "ranges",
    ];
    for key in source.keys() {
        if !authored.contains_key(key) {
            return Err(invalid("indicator config key disappeared from catalog row"));
        }
    }
    for key in authored.keys() {
        if !source.contains_key(key) && !RUNTIME_MUTABLE_KEYS.contains(&key.as_str()) {
            return Err(invalid("indicator config introduced a non-catalog field"));
        }
    }
    for (key, authored_value) in authored {
        match key.as_str() {
            "isActive" => {
                if authored_value != &Value::Bool(true) {
                    return Err(invalid("indicator must remain active"));
                }
            }
            "useFormingBar" => {
                if authored_value != &Value::Bool(false) {
                    return Err(invalid("indicator must use completed bars"));
                }
            }
            "timeframe" => {
                let timeframe = text(authored_value, "indicator timeframe")?.to_ascii_uppercase();
                if timeframe != text(authored_value, "indicator timeframe")?
                    || !authority.timeframe_policy().contains(&timeframe)
                {
                    return Err(invalid("indicator timeframe is outside the sealed policy"));
                }
            }
            "lookbackBars" => {
                let lookback = as_u64(authored_value, "indicator lookback")?;
                if !authority.evidence_lookback_choices().contains(&lookback) {
                    return Err(invalid("indicator lookback is outside the sealed policy"));
                }
            }
            "weight" => {
                let weight = finite_number(authored_value, "indicator weight")?;
                if !(0.0..=1.0).contains(&weight) {
                    return Err(invalid("indicator weight is outside native bounds"));
                }
            }
            "ranges" => {
                if !source.contains_key("ranges") {
                    return Err(invalid(
                        "indicator range was not supplied by sealed catalog",
                    ));
                }
                validate_ranges_against_catalog(source_meta, authored_value)?;
            }
            "talibConfig" => {
                validate_talib_config_against_catalog(source_meta, source, authored)?;
            }
            _ => {
                let source_value = source
                    .get(key)
                    .ok_or_else(|| invalid("indicator config key is absent from catalog"))?;
                if !canonical_eq(source_value, authored_value)? {
                    return Err(invalid("indicator config changed a catalog-fixed field"));
                }
            }
        }
    }
    Ok(())
}

fn validate_catalog_bound_resources(
    program: &Value,
    authority: &V5OperatorAuthority,
) -> Result<()> {
    let indicators = resource_rows(program, "indicators")?;
    let groups = resource_rows(program, "evidenceGroups")?;
    let events = resource_rows(program, "events")?;
    let management = resource_rows(program, "managementRefs")?;
    let mut by_instance = BTreeMap::new();

    for indicator in &indicators {
        let meta = required(indicator, "meta", "indicator")?;
        let config = required(indicator, "config", "indicator")?;
        let instance_id = indicator_id(indicator)?;
        let catalog_id = text(
            required(meta, "id", "indicator metadata")?,
            "indicator catalog ID",
        )?;
        let source = authority
            .catalog_entries()
            .get(&catalog_id)
            .ok_or_else(|| invalid("indicator metadata is absent from sealed catalog"))?;
        let source_meta = required(source, "meta", "catalog indicator")?;
        let source_config = required(source, "config", "catalog indicator")?;
        if !catalog_meta_matches(meta, source_meta)? {
            return Err(invalid(
                "indicator metadata/capabilities drifted from sealed catalog",
            ));
        }
        validate_indicator_config_against_catalog(source_meta, source_config, config, authority)?;
        if by_instance.insert(instance_id, indicator).is_some() {
            return Err(invalid(
                "duplicate indicator instance escaped structural validation",
            ));
        }
    }

    for group in &groups {
        let member_ids = members(group)?;
        let mut contract: Option<Value> = None;
        for instance_id in &member_ids {
            let indicator = by_instance
                .get(instance_id)
                .ok_or_else(|| invalid("evidence group member disappeared"))?;
            let meta = required(indicator, "meta", "indicator")?;
            let current = fuzzy_contract(meta)?
                .ok_or_else(|| invalid("evidence group member lacks fuzzy capability"))?;
            if contract.as_ref().is_some_and(|prior| prior != &current) {
                return Err(invalid(
                    "evidence group members have incompatible fuzzy contracts",
                ));
            }
            contract = Some(current);
        }
    }

    // The factory copies catalog indicator configs verbatim.  In particular,
    // a fresh exclusive multi-member group can retain the catalog/default
    // weight for every member (the preserved authority uses `1.0`), so its
    // raw total is deliberately not a normalized probability distribution.
    // Python's genome validation admits that untouched representation.
    //
    // Normalization and the 0.25 floor are instead invariants of the exact
    // `evidence_weight_mutate` construction/application path: it normalizes
    // its parent baseline before emitting a plan and rejects an unnormalized
    // `afterWeights` map while applying it.  Do not promote that operation
    // postcondition into this general parent validator, or compact G0 parents
    // fail before an operator can be selected.
    for group in &groups {
        let member_ids = members(group)?;
        for instance_id in &member_ids {
            let indicator = by_instance
                .get(instance_id)
                .ok_or_else(|| invalid("evidence group indicator disappeared"))?;
            let config = map_ref(
                required(indicator, "config", "indicator")?,
                "indicator config",
            )?;
            // `_catalog_item` preserves a missing source weight and the
            // Python resource operator reads it as the factory default 1.0.
            // Keep that default here so the trusted Rust side accepts the
            // same frozen catalog closure.
            let weight = config
                .get("weight")
                .map(|value| finite_number(value, "fuzzy indicator weight"))
                .transpose()?
                .unwrap_or(1.0);
            if weight <= 0.0 || weight > 1.0 {
                return Err(invalid(
                    "fuzzy indicator weight is outside the admitted domain",
                ));
            }
        }
    }

    let mut event_sources = BTreeSet::new();
    for event in &events {
        let instance_id = text(
            required(event, "indicatorInstanceId", "event")?,
            "event indicator instance",
        )?;
        let indicator = by_instance
            .get(&instance_id)
            .ok_or_else(|| invalid("event source indicator disappeared"))?;
        let meta = required(indicator, "meta", "indicator")?;
        let contract = event_contract(meta)?
            .ok_or_else(|| invalid("event source lacks the sealed directional-event capability"))?;
        let outputs = required(&contract, "eventOutputSchema", "event contract")?;
        if required(event, "longOutput", "event")?
            != required(outputs, "longOutput", "event output schema")?
            || required(event, "shortOutput", "event")?
                != required(outputs, "shortOutput", "event output schema")?
        {
            return Err(invalid(
                "event outputs drifted from the sealed catalog contract",
            ));
        }
        let lookback = as_u64(
            required(
                required(indicator, "config", "event source indicator")?,
                "lookbackBars",
                "event source indicator config",
            )?,
            "event source lookback",
        )?;
        if lookback != 1 || !event_sources.insert(instance_id) {
            return Err(invalid(
                "event source must be unique and fresh (one completed bar)",
            ));
        }
    }

    for management_ref in &management {
        for binding in object_get(management_ref, "scalarBindings")
            .map(|value| array_ref(value, "management scalar bindings"))
            .transpose()?
            .unwrap_or(&[])
        {
            let instance_id = text(
                required(binding, "indicatorInstanceId", "management scalar binding")?,
                "management scalar indicator",
            )?;
            let indicator = by_instance
                .get(&instance_id)
                .ok_or_else(|| invalid("management scalar indicator disappeared"))?;
            let outputs = scalar_outputs(required(indicator, "meta", "indicator")?)?
                .ok_or_else(|| invalid("management scalar source lacks catalog capability"))?;
            let wanted_key = required(binding, "outputKey", "management scalar binding")?;
            let wanted_kind = required(binding, "valueKind", "management scalar binding")?;
            if !outputs.iter().any(|output| {
                object_get(output, "outputKey") == Some(wanted_key)
                    && object_get(output, "valueKind") == Some(wanted_kind)
            }) {
                return Err(invalid(
                    "management scalar binding output is not catalog-authorized",
                ));
            }
        }
    }
    Ok(())
}

fn validate_generated_cooldown_closure(program: &Value) -> Result<()> {
    let nodes = rows_by_id(&node_rows(program)?, "node")?;
    for node in node_rows(program)? {
        let mut walked = Vec::new();
        guard_walk(
            required(&node, "guard", "node guard owner")?,
            Vec::new(),
            &mut walked,
        )?;
        if walked.iter().any(|(_, value)| {
            object_get(value, "kind").and_then(Value::as_str) == Some("action_cooldown_elapsed")
        }) {
            return Err(invalid(
                "action cooldown must be owned by its authored management edge",
            ));
        }
    }
    for edge in edge_rows(program)? {
        let target = text(required(&edge, "target", "edge")?, "edge target")?;
        let target_is_management = nodes
            .get(&target)
            .and_then(|node| object_get(node, "zone"))
            .and_then(Value::as_str)
            == Some("management");
        let expected_transition = (target_is_management
            && object_get(&edge, "effect")
                .and_then(Value::as_str)
                .is_some())
        .then(|| row_id(&edge, "management edge").map(|id| format!("e_{id}")))
        .transpose()?;
        let guard = required(&edge, "guard", "edge guard owner")?;
        let mut walked = Vec::new();
        guard_walk(guard, Vec::new(), &mut walked)?;
        for (_, value) in walked {
            if object_get(&value, "kind").and_then(Value::as_str) != Some("action_cooldown_elapsed")
            {
                continue;
            }
            let transition = text(
                required(&value, "transitionId", "action cooldown guard")?,
                "action cooldown transition ID",
            )?;
            if expected_transition.as_deref() != Some(transition.as_str())
                || as_u64(
                    required(&value, "actionOrdinal", "action cooldown guard")?,
                    "action cooldown action ordinal",
                )? != 0
            {
                return Err(invalid(
                    "action cooldown does not name its containing authored management action",
                ));
            }
        }
    }
    Ok(())
}

fn normal_weights(rows: &mut [Value], member_ids: &[String]) -> Result<()> {
    if member_ids.is_empty() {
        return Err(invalid("evidence group must retain one member"));
    }
    let wanted = member_ids.iter().cloned().collect::<BTreeSet<_>>();
    let weight = 1.0 / member_ids.len() as f64;
    let mut found = 0_usize;
    for row in rows {
        if wanted.contains(&indicator_id(row)?) {
            let config = map_mut(
                required_mut(map_mut(row, "indicator")?, "config", "indicator")?,
                "indicator config",
            )?;
            config.insert(
                "weight".to_owned(),
                value_number(weight, "evidence weight")?,
            );
            found += 1;
        }
    }
    if found != member_ids.len() {
        return Err(invalid("weight mutation has a dangling member"));
    }
    Ok(())
}

fn guard_evidence_clause(group_id: &str, threshold: f64) -> Result<Value> {
    Ok(object([
        ("kind", Value::String("evidence_at_least".to_owned())),
        ("groupId", Value::String(group_id.to_owned())),
        (
            "thresholdPercent",
            value_number(threshold, "evidence threshold")?,
        ),
    ]))
}

fn guard_event_clause(event_id: &str) -> Value {
    object([
        ("kind", Value::String("fresh_event".to_owned())),
        ("eventId", Value::String(event_id.to_owned())),
    ])
}

fn budget_u64(program: &Value, key: &str) -> Result<u64> {
    as_u64(
        required(required(program, "budget", "v5 program")?, key, "v5 budget")?,
        &format!("v5 budget {key}"),
    )
}

fn members(group: &Value) -> Result<Vec<String>> {
    array_ref(
        required(group, "indicatorInstanceIds", "evidence group")?,
        "evidence group members",
    )?
    .iter()
    .map(|row| text(row, "evidence group member"))
    .collect()
}

fn node_guard(node: &Value) -> Result<Value> {
    canonical_clone(required(node, "guard", "node")?)
}

fn node_set_guard(node: &Value, guard: Value) -> Result<Value> {
    let mut output = canonical_clone(node)?;
    map_mut(&mut output, "node")?.insert("guard".to_owned(), guard);
    Ok(output)
}

fn edge_set_fields(
    edge: &Value,
    source: Option<&str>,
    target: Option<&str>,
    priority: Option<u64>,
    guard: Option<Value>,
    effect_value: Option<Option<&str>>,
    id: Option<&str>,
) -> Result<Value> {
    let mut output = canonical_clone(edge)?;
    let fields = map_mut(&mut output, "edge")?;
    if let Some(value) = source {
        fields.insert("source".to_owned(), Value::String(value.to_owned()));
    }
    if let Some(value) = target {
        fields.insert("target".to_owned(), Value::String(value.to_owned()));
    }
    if let Some(value) = priority {
        fields.insert("priority".to_owned(), Value::from(value));
    }
    if let Some(value) = guard {
        fields.insert("guard".to_owned(), value);
    }
    if let Some(value) = effect_value {
        fields.insert(
            "effect".to_owned(),
            value
                .map(|item| Value::String(item.to_owned()))
                .unwrap_or(Value::Null),
        );
    }
    if let Some(value) = id {
        fields.insert("id".to_owned(), Value::String(value.to_owned()));
    }
    Ok(output)
}

fn node_set_resources(node: &Value, uses: Vec<Value>) -> Result<Value> {
    let mut output = canonical_clone(node)?;
    map_mut(&mut output, "node")?.insert("resources".to_owned(), array(uses));
    Ok(output)
}

fn group_threshold_sites(program: &Value, group_id: &str) -> Result<Vec<(Value, f64)>> {
    let mut output = Vec::new();
    for node in references(program, "evidence_group", group_id)? {
        let guard = node_guard(&node)?;
        let candidates = if object_get(&guard, "kind").and_then(Value::as_str) == Some("all") {
            object_get(&guard, "guards")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        } else {
            vec![guard]
        };
        for candidate in candidates {
            if matches!(
                object_get(&candidate, "kind").and_then(Value::as_str),
                Some("evidence_at_least") | Some("evidence_below")
            ) && object_get(&candidate, "groupId").and_then(Value::as_str) == Some(group_id)
            {
                if let Some(value) =
                    object_get(&candidate, "thresholdPercent").and_then(Value::as_f64)
                {
                    if value > 0.0 && value < 100.0 {
                        output.push((canonical_clone(&node)?, value));
                    }
                }
            }
        }
    }
    Ok(output)
}

fn replace_direct_threshold(
    guard: &Value,
    group_id: &str,
    before: f64,
    after: f64,
) -> Result<Value> {
    let mut guard = canonical_clone(guard)?;
    let mut changed = false;
    if object_get(&guard, "kind").and_then(Value::as_str) == Some("all") {
        let fields = map_mut(&mut guard, "all guard")?;
        let rows = required_mut(fields, "guards", "all guard")?
            .as_array_mut()
            .ok_or_else(|| invalid("all guard rows must be an array"))?;
        for row in rows {
            if matches!(
                object_get(row, "kind").and_then(Value::as_str),
                Some("evidence_at_least") | Some("evidence_below")
            ) && object_get(row, "groupId").and_then(Value::as_str) == Some(group_id)
                && object_get(row, "thresholdPercent").and_then(Value::as_f64) == Some(before)
            {
                map_mut(row, "evidence guard")?.insert(
                    "thresholdPercent".to_owned(),
                    value_number(after, "threshold")?,
                );
                changed = true;
            }
        }
    } else if matches!(
        object_get(&guard, "kind").and_then(Value::as_str),
        Some("evidence_at_least") | Some("evidence_below")
    ) && object_get(&guard, "groupId").and_then(Value::as_str) == Some(group_id)
        && object_get(&guard, "thresholdPercent").and_then(Value::as_f64) == Some(before)
    {
        map_mut(&mut guard, "evidence guard")?.insert(
            "thresholdPercent".to_owned(),
            value_number(after, "threshold")?,
        );
        changed = true;
    }
    if !changed {
        return Err(invalid("evidence threshold parent drift"));
    }
    Ok(guard)
}

fn fuzzy_contract_for_members(
    indicators: &BTreeMap<String, Value>,
    members: &[String],
) -> Result<Option<Value>> {
    let mut contract: Option<Value> = None;
    for member in members {
        let Some(item) = indicators.get(member) else {
            return Ok(None);
        };
        let Some(value) = fuzzy_contract(required(item, "meta", "indicator")?)? else {
            return Ok(None);
        };
        if let Some(existing) = &contract {
            if canonical_json(existing)? != canonical_json(&value)? {
                return Ok(None);
            }
        } else {
            contract = Some(value);
        }
    }
    Ok(contract)
}

fn semantic_indicator_key(item: &Value) -> Result<String> {
    let mut value = canonical_clone(item)?;
    map_mut(
        required_mut(map_mut(&mut value, "indicator")?, "meta", "indicator")?,
        "indicator meta",
    )?
    .remove("instanceId");
    sha(&value)
}

fn period_choices(meta: &Value, config: &Value) -> Result<Vec<Value>> {
    let current = array_ref(
        required(config, "talibConfig", "indicator config")?,
        "indicator talib config",
    )?
    .iter()
    .filter_map(|row| {
        Some((
            object_get(row, "name")?.as_str()?.to_owned(),
            canonical_clone(object_get(row, "value")?).ok()?,
        ))
    })
    .collect::<BTreeMap<_, _>>();
    let mut output = Vec::new();
    let descriptors = object_get(meta, "talibMeta")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for descriptor in descriptors {
        let name = match object_get(&descriptor, "name").and_then(Value::as_str) {
            Some(value)
                if value.to_ascii_lowercase().contains("period") && current.contains_key(value) =>
            {
                value
            }
            _ => continue,
        };
        if !matches!(
            object_get(&descriptor, "uiType").and_then(Value::as_str),
            Some("integer_slider") | Some("float_slider")
        ) {
            continue;
        }
        // Keep the catalog's original JSON number representation.  Python's
        // `_period_choices` compares numbers numerically, but writes the
        // selected descriptor/mark value back verbatim; rebuilding them via
        // f64 changes `9` into `9.0` and therefore changes the legacy choice
        // wrapper identity.
        let (nominal, minimum, maximum) = match (
            object_get(&descriptor, "default"),
            object_get(&descriptor, "min"),
            object_get(&descriptor, "max"),
        ) {
            (Some(a), Some(b), Some(c))
                if a.as_f64().is_some_and(f64::is_finite)
                    && b.as_f64().is_some_and(f64::is_finite)
                    && c.as_f64().is_some_and(f64::is_finite) =>
            {
                (a, b, c)
            }
            _ => continue,
        };
        let nominal_number = nominal.as_f64().expect("checked finite number");
        let mut marks = object_get(&descriptor, "marks")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|row| {
                let value = object_get(row, "value")?;
                let number = value.as_f64()?;
                number.is_finite().then_some((number, value))
            })
            .fold(Vec::<(f64, &Value)>::new(), |mut unique, candidate| {
                // Python's numeric set deduplicates integer/float equivalent
                // values while retaining the first source value's spelling.
                if !unique.iter().any(|(number, _)| *number == candidate.0) {
                    unique.push(candidate);
                }
                unique
            });
        marks.sort_by(|left, right| left.0.total_cmp(&right.0));
        let fast = marks
            .iter()
            .rfind(|(value, _)| *value < nominal_number)
            .map(|(_, value)| *value)
            .unwrap_or(minimum);
        let slow = marks
            .iter()
            .find(|(value, _)| *value > nominal_number)
            .map(|(_, value)| *value)
            .unwrap_or(maximum);
        for (choice, after) in [("fast", fast), ("nominal", nominal), ("slow", slow)] {
            let before = current.get(name).expect("checked");
            if before.as_f64() != after.as_f64() {
                output.push(object([
                    ("parameter", Value::String(name.to_owned())),
                    ("choice", Value::String(choice.to_owned())),
                    ("before", canonical_clone(before)?),
                    ("after", canonical_clone(after)?),
                    (
                        "descriptor",
                        object([
                            ("name", Value::String(name.to_owned())),
                            ("default", canonical_clone(nominal)?),
                            ("min", canonical_clone(minimum)?),
                            ("max", canonical_clone(maximum)?),
                        ]),
                    ),
                ]));
            }
        }
    }
    Ok(output)
}

fn period_order_valid(config: &Value) -> Result<bool> {
    let rows = array_ref(
        required(config, "talibConfig", "indicator config")?,
        "indicator talib config",
    )?;
    let mut fast = Vec::new();
    let mut slow = Vec::new();
    for row in rows {
        let Some(name) = object_get(row, "name").and_then(Value::as_str) else {
            continue;
        };
        let Some(value) = object_get(row, "value").and_then(Value::as_f64) else {
            continue;
        };
        if name.to_ascii_lowercase().starts_with("fast") {
            fast.push(value);
        }
        if name.to_ascii_lowercase().starts_with("slow") {
            slow.push(value);
        }
    }
    Ok(fast.is_empty()
        || slow.is_empty()
        || fast.into_iter().fold(f64::NEG_INFINITY, f64::max)
            < slow.into_iter().fold(f64::INFINITY, f64::min))
}

fn range_choices(meta: &Value, config: &Value) -> Result<Vec<Value>> {
    if object_get(meta, "usesRangeConfiguration") != Some(&Value::Bool(true)) {
        return Ok(Vec::new());
    }
    let Some(range) = numeric_range(meta)? else {
        return Ok(Vec::new());
    };
    let (minimum, maximum, step, width) = (
        finite_number(required(&range, "min", "range")?, "range min")?,
        finite_number(required(&range, "max", "range")?, "range max")?,
        finite_number(required(&range, "step", "range")?, "range step")?,
        finite_number(required(&range, "minRange", "range")?, "range minRange")?,
    );
    let Some(ranges) = object_get(config, "ranges").and_then(Value::as_object) else {
        return Ok(Vec::new());
    };
    let mut output = Vec::new();
    for side in ["buy", "sell"] {
        let Some(prior) = ranges.get(side).and_then(Value::as_array) else {
            continue;
        };
        if prior.len() != 2 {
            continue;
        }
        let Some((lower, upper)) = prior[0].as_f64().zip(prior[1].as_f64()) else {
            continue;
        };
        let variations = [
            ("shift_lower", [lower - step, upper - step]),
            ("shift_higher", [lower + step, upper + step]),
            ("widen", [lower - step, upper + step]),
            ("narrow", [lower + step, upper - step]),
        ];
        for (choice, candidate) in variations {
            if candidate[0] < minimum
                || candidate[1] > maximum
                || candidate[1] - candidate[0] < width
                || candidate == [lower, upper]
            {
                continue;
            }
            output.push(object([
                ("side", Value::String(side.to_owned())),
                ("choice", Value::String(choice.to_owned())),
                ("before", canonical_clone(&Value::Array(prior.clone()))?),
                (
                    "after",
                    array([
                        value_number(candidate[0], "range lower")?,
                        value_number(candidate[1], "range upper")?,
                    ]),
                ),
                ("catalogValueRange", range.clone()),
            ]));
        }
    }
    Ok(output)
}

fn resource_constructions(program: &Value, authority: &V5OperatorAuthority) -> Result<Vec<Value>> {
    let side = program_side(program)?;
    let groups = rows_by_id(&resource_rows(program, "evidenceGroups")?, "evidence group")?;
    let indicators = rows_by_id(&resource_rows(program, "indicators")?, "indicator")?;
    let events = rows_by_id(&resource_rows(program, "events")?, "event")?;
    let group_ids = groups.keys().cloned().collect::<BTreeSet<_>>();
    let indicator_ids = indicators.keys().cloned().collect::<BTreeSet<_>>();
    let event_ids = events.keys().cloned().collect::<BTreeSet<_>>();
    let mut out = Vec::new();

    // Evidence-group create/remove/split/merge.
    if groups.len() < budget_u64(program, "maxEvidenceGroups")? as usize {
        for (source_id, group) in &groups {
            let existing_members = members(group)?;
            if existing_members.is_empty() {
                continue;
            }
            for node in references(program, "evidence_group", source_id)? {
                let route = row_id(&node, "node")?;
                let group_id = stable_id(
                    "eg",
                    object([
                        ("operation", Value::String("group_create".to_owned())),
                        ("parent", Value::String(sha(program)?)),
                        ("source", Value::String(source_id.clone())),
                        ("route", Value::String(route.clone())),
                    ]),
                    &group_ids,
                )?;
                out.push(object([
                    ("kind", Value::String("evidence_group_create".to_owned())),
                    ("sourceGroupId", Value::String(source_id.clone())),
                    ("groupId", Value::String(group_id)),
                    ("nodeId", Value::String(route)),
                    (
                        "members",
                        array([Value::String(existing_members[0].clone())]),
                    ),
                    ("thresholdPercent", value_number(50.0, "threshold")?),
                ]));
            }
        }
    }
    for (group_id, group) in &groups {
        let owners = references(program, "evidence_group", group_id)?;
        if owners.len() == 1 {
            let node = &owners[0];
            let guard = node_guard(node)?;
            if let Some(after) =
                direct_all_remove(&guard, "evidence_at_least", "groupId", group_id)?
            {
                out.push(object([
                    ("kind", Value::String("evidence_group_remove".to_owned())),
                    ("groupId", Value::String(group_id.clone())),
                    ("nodeId", Value::String(row_id(node, "node")?)),
                    ("beforeGuard", guard),
                    ("afterGuard", after),
                ]));
            }
        }
        let existing_members = members(group)?;
        if groups.len() < budget_u64(program, "maxEvidenceGroups")? as usize
            && existing_members.len() >= 2
            && existing_members.iter().all(|member| {
                indicator_exclusive_to_group(program, member, group_id).unwrap_or(false)
            })
        {
            let owners = references(program, "evidence_group", group_id)?;
            if owners.len() == 1 {
                let node = &owners[0];
                let route = row_id(node, "node")?;
                let group_id_new = stable_id(
                    "eg",
                    object([
                        ("operation", Value::String("group_split".to_owned())),
                        ("parent", Value::String(sha(program)?)),
                        ("source", Value::String(group_id.clone())),
                        ("route", Value::String(route.clone())),
                    ]),
                    &group_ids,
                )?;
                out.push(object([
                    ("kind", Value::String("evidence_group_split".to_owned())),
                    ("sourceGroupId", Value::String(group_id.clone())),
                    ("groupId", Value::String(group_id_new)),
                    ("nodeId", Value::String(route)),
                    (
                        "leftMembers",
                        array([Value::String(existing_members[0].clone())]),
                    ),
                    (
                        "rightMembers",
                        array(existing_members[1..].iter().cloned().map(Value::String)),
                    ),
                    ("thresholdPercent", value_number(50.0, "threshold")?),
                ]));
            }
        }
    }
    let group_values = groups.keys().cloned().collect::<Vec<_>>();
    for (index, left_id) in group_values.iter().enumerate() {
        for right_id in &group_values[index + 1..] {
            let left_members = members(groups.get(left_id).expect("key from map"))?;
            let right_members = members(groups.get(right_id).expect("key from map"))?;
            let mut joined = left_members.clone();
            for member in right_members {
                if !joined.contains(&member) {
                    joined.push(member);
                }
            }
            if joined.len() > budget_u64(program, "maxGroupMembers")? as usize
                || joined.iter().any(|member| {
                    groups_for_indicator(program, member)
                        .map(|owners| {
                            owners != vec![left_id.clone()] && owners != vec![right_id.clone()]
                        })
                        .unwrap_or(true)
                })
            {
                continue;
            }
            let left_nodes = references(program, "evidence_group", left_id)?;
            let right_nodes = references(program, "evidence_group", right_id)?;
            if left_nodes.len() != 1 || left_nodes != right_nodes {
                continue;
            }
            let node = &left_nodes[0];
            let before = node_guard(node)?;
            let Some(after) = direct_all_remove(&before, "evidence_at_least", "groupId", right_id)?
            else {
                continue;
            };
            out.push(object([
                ("kind", Value::String("evidence_group_merge".to_owned())),
                ("leftGroupId", Value::String(left_id.clone())),
                ("rightGroupId", Value::String(right_id.clone())),
                ("nodeId", Value::String(row_id(node, "node")?)),
                ("members", array(joined.into_iter().map(Value::String))),
                ("beforeGuard", before),
                ("afterGuard", after),
            ]));
        }
    }

    // Membership, weights, and direct threshold sites.
    for (group_id, group) in &groups {
        let current_members = members(group)?;
        let contract = fuzzy_contract_for_members(&indicators, &current_members)?;
        if let Some(contract) = contract {
            if current_members.len() < budget_u64(program, "maxGroupMembers")? as usize {
                for (instance_id, item) in &indicators {
                    if current_members.contains(instance_id)
                        || !event_ids_for_indicator(program, instance_id)?.is_empty()
                    {
                        continue;
                    }
                    if fuzzy_contract(required(item, "meta", "indicator")?)?
                        == Some(contract.clone())
                    {
                        out.push(object([
                            ("kind", Value::String("evidence_member_insert".to_owned())),
                            ("groupId", Value::String(group_id.clone())),
                            ("indicatorInstanceId", Value::String(instance_id.clone())),
                            (
                                "beforeMembers",
                                array(current_members.iter().cloned().map(Value::String)),
                            ),
                        ]));
                    }
                }
            }
            if current_members.len() > 1 {
                for instance_id in &current_members {
                    if groups_for_indicator(program, instance_id)?.len() > 1
                        || !event_ids_for_indicator(program, instance_id)?.is_empty()
                    {
                        out.push(object([
                            ("kind", Value::String("evidence_member_remove".to_owned())),
                            ("groupId", Value::String(group_id.clone())),
                            ("indicatorInstanceId", Value::String(instance_id.clone())),
                            (
                                "beforeMembers",
                                array(current_members.iter().cloned().map(Value::String)),
                            ),
                        ]));
                    }
                }
            }
        }
        if current_members.len() > 1
            && current_members.iter().all(|member| {
                indicator_exclusive_to_group(program, member, group_id).unwrap_or(false)
            })
        {
            let weights = current_members
                .iter()
                .map(|member| {
                    indicators
                        .get(member)
                        .and_then(|item| object_get(item, "config"))
                        .and_then(|config| object_get(config, "weight"))
                        .and_then(Value::as_f64)
                        .unwrap_or(1.0)
                })
                .collect::<Vec<_>>();
            let total: f64 = weights.iter().sum();
            if total > 0.0 && weights.iter().all(|value| *value > 0.0) {
                let normalized = weights
                    .iter()
                    .map(|value| *value / total)
                    .collect::<Vec<_>>();
                for (index, member) in current_members.iter().enumerate() {
                    for delta in [-0.25, 0.25] {
                        let mut candidate = normalized.clone();
                        candidate[index] += delta;
                        let rest = candidate.len() - 1;
                        if rest == 0 || candidate[index] < 0.25 {
                            continue;
                        }
                        for (other, weight) in candidate.iter_mut().enumerate() {
                            if other != index {
                                *weight -= delta / rest as f64;
                            }
                        }
                        if candidate.iter().any(|value| *value < 0.25) {
                            continue;
                        }
                        let before = current_members
                            .iter()
                            .cloned()
                            .zip(normalized.iter().copied())
                            .map(|(key, value)| Ok((key, value_number(value, "weight")?)))
                            .collect::<Result<Vec<_>>>()?;
                        let after = current_members
                            .iter()
                            .cloned()
                            .zip(candidate.iter().copied())
                            .map(|(key, value)| Ok((key, value_number(value, "weight")?)))
                            .collect::<Result<Vec<_>>>()?;
                        let mut before_map = Map::new();
                        for (key, value) in before {
                            before_map.insert(key, value);
                        }
                        let mut after_map = Map::new();
                        for (key, value) in after {
                            after_map.insert(key, value);
                        }
                        out.push(object([
                            ("kind", Value::String("evidence_weight_mutate".to_owned())),
                            ("groupId", Value::String(group_id.clone())),
                            ("indicatorInstanceId", Value::String(member.clone())),
                            ("beforeWeights", Value::Object(before_map)),
                            ("afterWeights", Value::Object(after_map)),
                        ]));
                    }
                }
            }
        }
        for (node, before) in group_threshold_sites(program, group_id)? {
            for after in [before - 5.0, before + 5.0] {
                if (5.0..=95.0).contains(&after) {
                    out.push(object([
                        (
                            "kind",
                            Value::String("evidence_threshold_mutate".to_owned()),
                        ),
                        ("groupId", Value::String(group_id.clone())),
                        ("nodeId", Value::String(row_id(&node, "node")?)),
                        ("before", value_number(before, "threshold")?),
                        ("after", value_number(after, "threshold")?),
                    ]));
                }
            }
        }
    }

    // Indicator insert/remove/substitute and its catalog-authorized knobs.
    if indicators.len() < budget_u64(program, "maxIndicators")? as usize {
        let entries = authority.catalog_entries();
        let policy = authority.timeframe_policy();
        for (group_id, group) in &groups {
            let current_members = members(group)?;
            let Some(contract) = fuzzy_contract_for_members(&indicators, &current_members)? else {
                continue;
            };
            if current_members.len() >= budget_u64(program, "maxGroupMembers")? as usize {
                continue;
            }
            for (catalog_id, entry) in entries {
                if fuzzy_contract(required(entry, "meta", "catalog indicator")?)?
                    != Some(contract.clone())
                {
                    continue;
                }
                for timeframe in policy {
                    let instance_id = stable_id(
                        "ind",
                        object([
                            ("operation", Value::String("indicator_insert".to_owned())),
                            ("parent", Value::String(sha(program)?)),
                            ("group", Value::String(group_id.clone())),
                            ("indicator", Value::String(catalog_id.clone())),
                            ("timeframe", Value::String(timeframe.clone())),
                        ]),
                        &indicator_ids,
                    )?;
                    let mut candidate = catalog_item(authority, catalog_id, &instance_id, &side)?;
                    map_mut(
                        required_mut(
                            map_mut(&mut candidate, "catalog indicator")?,
                            "config",
                            "catalog indicator",
                        )?,
                        "catalog indicator config",
                    )?
                    .insert("timeframe".to_owned(), Value::String(timeframe.clone()));
                    let key = semantic_indicator_key(&candidate)?;
                    if current_members
                        .iter()
                        .filter_map(|member| indicators.get(member))
                        .any(|existing| {
                            semantic_indicator_key(existing)
                                .map(|item| item == key)
                                .unwrap_or(false)
                        })
                    {
                        continue;
                    }
                    out.push(object([
                        (
                            "kind",
                            Value::String("indicator_instance_insert".to_owned()),
                        ),
                        ("groupId", Value::String(group_id.clone())),
                        ("indicatorId", Value::String(catalog_id.clone())),
                        ("indicatorInstanceId", Value::String(instance_id)),
                        ("timeframe", Value::String(timeframe.clone())),
                        (
                            "beforeMembers",
                            array(current_members.iter().cloned().map(Value::String)),
                        ),
                    ]));
                }
            }
        }
    }
    for (group_id, group) in &groups {
        let current_members = members(group)?;
        if current_members.len() <= 1 {
            continue;
        }
        for instance_id in &current_members {
            if indicator_exclusive_to_group(program, instance_id, group_id)? {
                out.push(object([
                    (
                        "kind",
                        Value::String("indicator_instance_remove".to_owned()),
                    ),
                    ("groupId", Value::String(group_id.clone())),
                    ("indicatorInstanceId", Value::String(instance_id.clone())),
                    (
                        "beforeMembers",
                        array(current_members.iter().cloned().map(Value::String)),
                    ),
                ]));
            }
        }
    }
    let entries = authority.catalog_entries();
    let policy = authority.timeframe_policy();
    for (instance_id, item) in &indicators {
        if !source_catalog_matches(authority, item)? {
            continue;
        }
        let meta = required(item, "meta", "indicator")?;
        let fuzzy = !groups_for_indicator(program, instance_id)?.is_empty();
        let event = !event_ids_for_indicator(program, instance_id)?.is_empty();
        if fuzzy && event {
            continue;
        }
        let Some(contract) = binding_contract(meta, fuzzy, event, false)? else {
            continue;
        };
        for (replacement_id, entry) in entries {
            if object_get(meta, "id").and_then(Value::as_str) == Some(replacement_id) {
                continue;
            }
            if binding_contract(
                required(entry, "meta", "catalog indicator")?,
                fuzzy,
                event,
                false,
            )? == Some(contract.clone())
            {
                let mut construction = Map::new();
                // The resource layer exposes the general substitution plan
                // even for a raw-event source.  The event-specific family is
                // added below with its extra `eventId` binding; both are
                // intentionally distinct replayable operations in Python.
                construction.insert(
                    "kind".to_owned(),
                    Value::String("indicator_substitute".to_owned()),
                );
                construction.insert(
                    "indicatorInstanceId".to_owned(),
                    Value::String(instance_id.clone()),
                );
                construction.insert(
                    "beforeIndicatorId".to_owned(),
                    canonical_clone(required(meta, "id", "indicator meta")?)?,
                );
                construction.insert(
                    "afterIndicatorId".to_owned(),
                    Value::String(replacement_id.to_owned()),
                );
                construction.insert("bindingContract".to_owned(), contract.clone());
                out.push(Value::Object(construction));
            }
        }
        let config = required(item, "config", "indicator")?;
        if fuzzy || event {
            let current_frame = object_get(config, "timeframe")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_ascii_uppercase();
            if policy.contains(&current_frame) {
                for frame in policy {
                    if frame != &current_frame {
                        out.push(object([
                            (
                                "kind",
                                Value::String("indicator_timeframe_mutate".to_owned()),
                            ),
                            ("indicatorInstanceId", Value::String(instance_id.clone())),
                            ("before", Value::String(current_frame.clone())),
                            ("after", Value::String(frame.clone())),
                        ]));
                    }
                }
            }
        }
        if fuzzy && !event {
            let current = object_get(config, "lookbackBars")
                .and_then(Value::as_i64)
                .unwrap_or(1);
            let domain = authority.evidence_lookback_choices();
            if current >= 0 && domain.contains(&(current as u64)) {
                for after in domain {
                    if *after != current as u64 {
                        out.push(object([
                            (
                                "kind",
                                Value::String("indicator_lookback_mutate".to_owned()),
                            ),
                            ("indicatorInstanceId", Value::String(instance_id.clone())),
                            ("before", Value::from(current)),
                            ("after", Value::from(*after)),
                        ]));
                    }
                }
            }
        }
        for change in period_choices(meta, config)? {
            out.push(object([
                ("kind", Value::String("indicator_period_mutate".to_owned())),
                ("indicatorInstanceId", Value::String(instance_id.clone())),
                ("change", change),
            ]));
        }
        for change in range_choices(meta, config)? {
            out.push(object([
                ("kind", Value::String("indicator_range_mutate".to_owned())),
                ("indicatorInstanceId", Value::String(instance_id.clone())),
                ("change", change),
            ]));
        }
    }

    // Raw event insert/remove/substitute are atomic with their source
    // indicator and retain the one-bar freshness invariant.
    if events.len() < budget_u64(program, "maxEvents")? as usize
        && indicators.len() < budget_u64(program, "maxIndicators")? as usize
    {
        for node in node_rows(program)? {
            let zone = object_get(&node, "zone").and_then(Value::as_str);
            if !matches!(zone, Some("entry") | Some("setup")) {
                continue;
            }
            let has_event = array_ref(required(&node, "resources", "node")?, "node resources")?
                .iter()
                .any(|use_row| {
                    object_get(use_row, "kind").and_then(Value::as_str) == Some("event")
                });
            if has_event {
                continue;
            }
            let node_id = row_id(&node, "node")?;
            for (catalog_id, entry) in entries {
                let Some(contract) = event_contract(required(entry, "meta", "catalog indicator")?)?
                else {
                    continue;
                };
                let instance_id = stable_id(
                    "evtind",
                    object([
                        ("operation", Value::String("event_insert".to_owned())),
                        ("parent", Value::String(sha(program)?)),
                        ("node", Value::String(node_id.clone())),
                        ("indicator", Value::String(catalog_id.clone())),
                    ]),
                    &indicator_ids,
                )?;
                let event_id = stable_id(
                    "evt",
                    object([
                        ("operation", Value::String("event_insert".to_owned())),
                        ("parent", Value::String(sha(program)?)),
                        ("node", Value::String(node_id.clone())),
                        ("instance", Value::String(instance_id.clone())),
                    ]),
                    &event_ids,
                )?;
                out.push(object([
                    ("kind", Value::String("directional_event_insert".to_owned())),
                    ("nodeId", Value::String(node_id.clone())),
                    ("indicatorId", Value::String(catalog_id.clone())),
                    ("indicatorInstanceId", Value::String(instance_id)),
                    ("eventId", Value::String(event_id)),
                    ("contract", contract),
                ]));
            }
        }
    }
    for (event_id, row) in &events {
        let instance_id = text(
            required(row, "indicatorInstanceId", "event")?,
            "event source",
        )?;
        let owners = references(program, "event", event_id)?;
        if owners.len() == 1 && groups_for_indicator(program, &instance_id)?.is_empty() {
            let node = &owners[0];
            let before = node_guard(node)?;
            if let Some(after) = direct_all_remove(&before, "fresh_event", "eventId", event_id)? {
                out.push(object([
                    ("kind", Value::String("directional_event_remove".to_owned())),
                    ("eventId", Value::String(event_id.clone())),
                    ("indicatorInstanceId", Value::String(instance_id.clone())),
                    ("nodeId", Value::String(row_id(node, "node")?)),
                    ("beforeGuard", before),
                    ("afterGuard", after),
                ]));
            }
        }
    }
    for (event_id, event_row) in &events {
        let instance_id = text(
            required(event_row, "indicatorInstanceId", "event")?,
            "event source",
        )?;
        let Some(item) = indicators.get(&instance_id) else {
            continue;
        };
        if !source_catalog_matches(authority, item)? {
            continue;
        }
        let meta = required(item, "meta", "indicator")?;
        let Some(contract) = binding_contract(meta, false, true, false)? else {
            continue;
        };
        for (replacement_id, entry) in entries {
            if object_get(meta, "id").and_then(Value::as_str) == Some(replacement_id) {
                continue;
            }
            if binding_contract(
                required(entry, "meta", "catalog indicator")?,
                false,
                true,
                false,
            )? == Some(contract.clone())
            {
                out.push(object([
                    (
                        "kind",
                        Value::String("directional_event_substitute".to_owned()),
                    ),
                    ("eventId", Value::String(event_id.clone())),
                    ("indicatorInstanceId", Value::String(instance_id.clone())),
                    (
                        "beforeIndicatorId",
                        canonical_clone(required(meta, "id", "indicator meta")?)?,
                    ),
                    ("afterIndicatorId", Value::String(replacement_id.to_owned())),
                    ("bindingContract", contract.clone()),
                ]));
            }
        }
    }
    let _ = side;
    sorted_values(out)
}

fn ctext(construction: &Value, key: &str) -> Result<String> {
    text(
        required(construction, key, "operator construction")?,
        &format!("operator construction {key}"),
    )
}

fn carray_text(construction: &Value, key: &str) -> Result<Vec<String>> {
    array_ref(
        required(construction, key, "operator construction")?,
        &format!("operator construction {key}"),
    )?
    .iter()
    .map(|item| text(item, &format!("operator construction {key} item")))
    .collect()
}

fn find_row(rows: &[Value], kind: &str, id: &str) -> Result<Value> {
    rows.iter()
        .find(|row| {
            if kind == "indicator" {
                indicator_id(row)
                    .map(|candidate| candidate == id)
                    .unwrap_or(false)
            } else {
                row_id(row, kind)
                    .map(|candidate| candidate == id)
                    .unwrap_or(false)
            }
        })
        .map(canonical_clone)
        .transpose()?
        .ok_or_else(|| invalid(format!("operator construction names an unknown {kind}")))
}

fn replace_talib_value(config: &mut Value, parameter: &str, value: &Value) -> Result<()> {
    let fields = map_mut(config, "indicator config")?;
    let rows = required_mut(fields, "talibConfig", "indicator config")?
        .as_array_mut()
        .ok_or_else(|| invalid("indicator talibConfig must be an array"))?;
    for row in rows {
        if object_get(row, "name").and_then(Value::as_str) == Some(parameter) {
            map_mut(row, "talib row")?.insert("value".to_owned(), canonical_clone(value)?);
            return Ok(());
        }
    }
    Err(invalid(
        "catalog period descriptor is absent from indicator config",
    ))
}

fn resource_transform(
    program: &Value,
    authority: &V5OperatorAuthority,
    construction: &Value,
) -> Result<(Value, Value)> {
    let kind = ctext(construction, "kind")?;
    let side = program_side(program)?;
    let groups = rows_by_id(&resource_rows(program, "evidenceGroups")?, "evidence group")?;
    let indicators = rows_by_id(&resource_rows(program, "indicators")?, "indicator")?;
    let events = rows_by_id(&resource_rows(program, "events")?, "event")?;
    match kind.as_str() {
        "evidence_group_create" => {
            let source = ctext(construction, "sourceGroupId")?;
            let group_id = ctext(construction, "groupId")?;
            let node_id = ctext(construction, "nodeId")?;
            let source_group = groups
                .get(&source)
                .ok_or_else(|| invalid("evidence group create parent drift"))?;
            if groups.contains_key(&group_id) {
                return Err(invalid("evidence group create ID collision"));
            }
            let node = find_row(&node_rows(program)?, "node", &node_id)?;
            if !references(program, "evidence_group", &source)?
                .iter()
                .any(|row| row == &node)
            {
                return Err(invalid("evidence group create route ownership drift"));
            }
            let requested = carray_text(construction, "members")?;
            if requested.len() != 1 || !members(source_group)?.contains(&requested[0]) {
                return Err(invalid("evidence group create membership drift"));
            }
            let mut groups_out = resource_rows(program, "evidenceGroups")?;
            groups_out.push(object([
                ("id", Value::String(group_id.clone())),
                (
                    "indicatorInstanceIds",
                    array(requested.iter().cloned().map(Value::String)),
                ),
                ("ownerSide", Value::String(side)),
            ]));
            let changed = node_set_guard(
                &node_with_resource(&node, "evidence_group", &group_id)?,
                all_with(
                    &node_guard(&node)?,
                    &guard_evidence_clause(
                        &group_id,
                        finite_number(
                            required(construction, "thresholdPercent", "operator construction")?,
                            "group threshold",
                        )?,
                    )?,
                )?,
            )?;
            let child = replace_node(
                &replace_resource_rows(program, None, Some(groups_out), None, None)?,
                &changed,
            )?;
            Ok((
                child,
                array([object([
                    ("operation", Value::String(kind)),
                    ("groupId", Value::String(group_id)),
                    ("sourceGroupId", Value::String(source)),
                    ("nodeId", Value::String(node_id)),
                    ("members", array(requested.into_iter().map(Value::String))),
                ])]),
            ))
        }
        "evidence_group_remove" => {
            let group_id = ctext(construction, "groupId")?;
            let node_id = ctext(construction, "nodeId")?;
            if !groups.contains_key(&group_id) {
                return Err(invalid("evidence group remove parent drift"));
            }
            let node = find_row(&node_rows(program)?, "node", &node_id)?;
            if references(program, "evidence_group", &group_id)? != vec![node.clone()]
                || node_guard(&node)?
                    != canonical_clone(required(
                        construction,
                        "beforeGuard",
                        "operator construction",
                    )?)?
            {
                return Err(invalid("evidence group remove parent drift"));
            }
            let groups_out = resource_rows(program, "evidenceGroups")?
                .into_iter()
                .filter(|row| {
                    row_id(row, "evidence group")
                        .map(|id| id != group_id)
                        .unwrap_or(false)
                })
                .collect::<Vec<_>>();
            let changed = node_set_guard(
                &node_without_resource(&node, "evidence_group", &group_id)?,
                canonical_clone(required(
                    construction,
                    "afterGuard",
                    "operator construction",
                )?)?,
            )?;
            let child = replace_node(
                &replace_resource_rows(program, None, Some(groups_out), None, None)?,
                &changed,
            )?;
            Ok((
                child,
                array([object([
                    ("operation", Value::String(kind)),
                    ("groupId", Value::String(group_id)),
                    ("nodeId", Value::String(node_id)),
                ])]),
            ))
        }
        "evidence_group_split" => {
            let source = ctext(construction, "sourceGroupId")?;
            let group_id = ctext(construction, "groupId")?;
            let node_id = ctext(construction, "nodeId")?;
            let left = carray_text(construction, "leftMembers")?;
            let right = carray_text(construction, "rightMembers")?;
            let source_group = groups
                .get(&source)
                .ok_or_else(|| invalid("evidence group split parent drift"))?;
            let node = find_row(&node_rows(program)?, "node", &node_id)?;
            let mut union = left.clone();
            union.extend(right.clone());
            union.sort();
            union.dedup();
            let mut prior = members(source_group)?;
            prior.sort();
            prior.dedup();
            if groups.contains_key(&group_id)
                || left.is_empty()
                || right.is_empty()
                || union != prior
                || references(program, "evidence_group", &source)? != vec![node.clone()]
            {
                return Err(invalid("evidence group split parent drift"));
            }
            let mut groups_out = resource_rows(program, "evidenceGroups")?;
            for row in groups_out.iter_mut() {
                if row_id(row, "evidence group")? == source {
                    map_mut(row, "evidence group")?.insert(
                        "indicatorInstanceIds".to_owned(),
                        array(left.iter().cloned().map(Value::String)),
                    );
                }
            }
            groups_out.push(object([
                ("id", Value::String(group_id.clone())),
                (
                    "indicatorInstanceIds",
                    array(right.iter().cloned().map(Value::String)),
                ),
                ("ownerSide", Value::String(side)),
            ]));
            let mut indicators_out = resource_rows(program, "indicators")?;
            normal_weights(&mut indicators_out, &left)?;
            normal_weights(&mut indicators_out, &right)?;
            let changed = node_set_guard(
                &node_with_resource(&node, "evidence_group", &group_id)?,
                all_with(
                    &node_guard(&node)?,
                    &guard_evidence_clause(
                        &group_id,
                        finite_number(
                            required(construction, "thresholdPercent", "operator construction")?,
                            "group threshold",
                        )?,
                    )?,
                )?,
            )?;
            let child = replace_node(
                &replace_resource_rows(
                    program,
                    Some(indicators_out),
                    Some(groups_out),
                    None,
                    None,
                )?,
                &changed,
            )?;
            Ok((
                child,
                array([object([
                    ("operation", Value::String(kind)),
                    ("sourceGroupId", Value::String(source)),
                    ("groupId", Value::String(group_id)),
                    ("leftMembers", array(left.into_iter().map(Value::String))),
                    ("rightMembers", array(right.into_iter().map(Value::String))),
                    ("nodeId", Value::String(node_id)),
                ])]),
            ))
        }
        "evidence_group_merge" => {
            let left_id = ctext(construction, "leftGroupId")?;
            let right_id = ctext(construction, "rightGroupId")?;
            let node_id = ctext(construction, "nodeId")?;
            let node = find_row(&node_rows(program)?, "node", &node_id)?;
            let left_group = groups
                .get(&left_id)
                .ok_or_else(|| invalid("evidence group merge parent drift"))?;
            let right_group = groups
                .get(&right_id)
                .ok_or_else(|| invalid("evidence group merge parent drift"))?;
            if references(program, "evidence_group", &left_id)? != vec![node.clone()]
                || references(program, "evidence_group", &right_id)? != vec![node.clone()]
                || node_guard(&node)?
                    != canonical_clone(required(
                        construction,
                        "beforeGuard",
                        "operator construction",
                    )?)?
            {
                return Err(invalid("evidence group merge parent drift"));
            }
            let mut expected = members(left_group)?;
            for item in members(right_group)? {
                if !expected.contains(&item) {
                    expected.push(item)
                }
            }
            let given = carray_text(construction, "members")?;
            if expected != given {
                return Err(invalid("evidence group merge membership drift"));
            }
            let mut groups_out = resource_rows(program, "evidenceGroups")?
                .into_iter()
                .filter(|row| {
                    row_id(row, "evidence group")
                        .map(|id| id != right_id)
                        .unwrap_or(false)
                })
                .collect::<Vec<_>>();
            for row in groups_out.iter_mut() {
                if row_id(row, "evidence group")? == left_id {
                    map_mut(row, "evidence group")?.insert(
                        "indicatorInstanceIds".to_owned(),
                        array(given.iter().cloned().map(Value::String)),
                    );
                }
            }
            let mut indicators_out = resource_rows(program, "indicators")?;
            normal_weights(&mut indicators_out, &given)?;
            let changed = node_set_guard(
                &node_without_resource(&node, "evidence_group", &right_id)?,
                canonical_clone(required(
                    construction,
                    "afterGuard",
                    "operator construction",
                )?)?,
            )?;
            let child = replace_node(
                &replace_resource_rows(
                    program,
                    Some(indicators_out),
                    Some(groups_out),
                    None,
                    None,
                )?,
                &changed,
            )?;
            Ok((
                child,
                array([object([
                    ("operation", Value::String(kind)),
                    ("leftGroupId", Value::String(left_id)),
                    ("removedGroupId", Value::String(right_id)),
                    ("members", array(given.into_iter().map(Value::String))),
                    ("nodeId", Value::String(node_id)),
                ])]),
            ))
        }
        "evidence_member_insert" | "evidence_member_remove" => {
            let group_id = ctext(construction, "groupId")?;
            let instance_id = ctext(construction, "indicatorInstanceId")?;
            let group = groups
                .get(&group_id)
                .ok_or_else(|| invalid("evidence membership parent drift"))?;
            if !indicators.contains_key(&instance_id) {
                return Err(invalid("evidence membership indicator drift"));
            }
            let before = carray_text(construction, "beforeMembers")?;
            if members(group)? != before {
                return Err(invalid("evidence membership parent drift"));
            }
            let mut after = if kind.ends_with("insert") {
                let mut value = before.clone();
                value.push(instance_id.clone());
                value
            } else {
                before
                    .iter()
                    .filter(|item| *item != &instance_id)
                    .cloned()
                    .collect()
            };
            after.sort();
            if after.is_empty() || after.len() > budget_u64(program, "maxGroupMembers")? as usize {
                return Err(invalid("evidence membership violates group budget"));
            }
            let mut groups_out = resource_rows(program, "evidenceGroups")?;
            for row in groups_out.iter_mut() {
                if row_id(row, "evidence group")? == group_id {
                    map_mut(row, "evidence group")?.insert(
                        "indicatorInstanceIds".to_owned(),
                        array(after.iter().cloned().map(Value::String)),
                    );
                }
            }
            let should_normalize = after.iter().all(|member| {
                indicator_exclusive_to_group(program, member, &group_id).unwrap_or(false)
                    || (kind.ends_with("insert")
                        && member == &instance_id
                        && groups_for_indicator(program, member)
                            .map(|ids| ids.is_empty())
                            .unwrap_or(false))
            });
            let child = if should_normalize {
                let mut indicators_out = resource_rows(program, "indicators")?;
                normal_weights(&mut indicators_out, &after)?;
                replace_resource_rows(program, Some(indicators_out), Some(groups_out), None, None)?
            } else {
                replace_resource_rows(program, None, Some(groups_out), None, None)?
            };
            Ok((
                child,
                array([object([
                    ("operation", Value::String(kind)),
                    ("groupId", Value::String(group_id)),
                    ("indicatorInstanceId", Value::String(instance_id)),
                    (
                        "beforeMembers",
                        array(before.into_iter().map(Value::String)),
                    ),
                    ("afterMembers", array(after.into_iter().map(Value::String))),
                ])]),
            ))
        }
        "evidence_weight_mutate" => {
            let group_id = ctext(construction, "groupId")?;
            let group = groups
                .get(&group_id)
                .ok_or_else(|| invalid("evidence weight parent drift"))?;
            let group_members = members(group)?;
            let before = map_ref(
                required(construction, "beforeWeights", "operator construction")?,
                "before weights",
            )?;
            let after = map_ref(
                required(construction, "afterWeights", "operator construction")?,
                "after weights",
            )?;
            if before.len() != group_members.len()
                || after.len() != group_members.len()
                || group_members
                    .iter()
                    .any(|member| !before.contains_key(member) || !after.contains_key(member))
            {
                return Err(invalid("evidence weight parent drift"));
            }
            let current = group_members
                .iter()
                .map(|member| {
                    let value = object_get(
                        indicators.get(member).expect("membership checked"),
                        "config",
                    )
                    .and_then(|config| object_get(config, "weight"))
                    .and_then(Value::as_f64)
                    .unwrap_or(1.0);
                    Ok((member.clone(), value))
                })
                .collect::<Result<Vec<_>>>()?;
            let total: f64 = current.iter().map(|(_, value)| value).sum();
            if total <= 0.0 {
                return Err(invalid("evidence weight total is invalid"));
            }
            for (member, value) in &current {
                if before.get(member).and_then(Value::as_f64) != Some(*value / total) {
                    return Err(invalid("evidence weight parent drift"));
                }
            }
            let mut sum = 0.0;
            for value in after.values() {
                let value = finite_number(value, "after evidence weight")?;
                if value < 0.25 {
                    return Err(invalid("evidence weight must remain positive"));
                }
                sum += value;
            }
            if (sum - 1.0).abs() > 1e-12 {
                return Err(invalid("evidence weights must remain normalized"));
            }
            let mut indicators_out = resource_rows(program, "indicators")?;
            for row in indicators_out.iter_mut() {
                let id = indicator_id(row)?;
                if let Some(weight) = after.get(&id) {
                    map_mut(
                        required_mut(map_mut(row, "indicator")?, "config", "indicator")?,
                        "indicator config",
                    )?
                    .insert("weight".to_owned(), canonical_clone(weight)?);
                }
            }
            let child = replace_resource_rows(program, Some(indicators_out), None, None, None)?;
            Ok((
                child,
                array([object([
                    ("operation", Value::String(kind)),
                    ("groupId", Value::String(group_id)),
                    ("beforeWeights", Value::Object(before.clone())),
                    ("afterWeights", Value::Object(after.clone())),
                ])]),
            ))
        }
        "evidence_threshold_mutate" => {
            let group_id = ctext(construction, "groupId")?;
            let node_id = ctext(construction, "nodeId")?;
            let before = finite_number(
                required(construction, "before", "operator construction")?,
                "threshold before",
            )?;
            let after = finite_number(
                required(construction, "after", "operator construction")?,
                "threshold after",
            )?;
            let node = find_row(&node_rows(program)?, "node", &node_id)?;
            let changed = node_set_guard(
                &node,
                replace_direct_threshold(&node_guard(&node)?, &group_id, before, after)?,
            )?;
            let child = replace_node(program, &changed)?;
            Ok((
                child,
                array([object([
                    ("operation", Value::String(kind)),
                    ("groupId", Value::String(group_id)),
                    ("nodeId", Value::String(node_id)),
                    ("before", value_number(before, "threshold")?),
                    ("after", value_number(after, "threshold")?),
                ])]),
            ))
        }
        _ => resource_transform_indicator_or_event(
            program,
            authority,
            construction,
            &kind,
            &groups,
            &indicators,
            &events,
        ),
    }
}

fn resource_transform_indicator_or_event(
    program: &Value,
    authority: &V5OperatorAuthority,
    construction: &Value,
    kind: &str,
    groups: &BTreeMap<String, Value>,
    indicators: &BTreeMap<String, Value>,
    events: &BTreeMap<String, Value>,
) -> Result<(Value, Value)> {
    let side = program_side(program)?;
    match kind {
        "indicator_instance_insert" => {
            let group_id = ctext(construction, "groupId")?;
            let indicator_key = ctext(construction, "indicatorId")?;
            let instance_id = ctext(construction, "indicatorInstanceId")?;
            let timeframe = ctext(construction, "timeframe")?.to_ascii_uppercase();
            let group = groups
                .get(&group_id)
                .ok_or_else(|| invalid("indicator insert parent drift"))?;
            let before = carray_text(construction, "beforeMembers")?;
            if members(group)? != before || indicators.contains_key(&instance_id) {
                return Err(invalid("indicator insert parent drift"));
            }
            if !authority.timeframe_policy().contains(&timeframe) {
                return Err(invalid(
                    "indicator insert timeframe is not catalog-authorized",
                ));
            }
            let mut item = catalog_item(authority, &indicator_key, &instance_id, &side)?;
            map_mut(
                required_mut(
                    map_mut(&mut item, "catalog indicator")?,
                    "config",
                    "catalog indicator",
                )?,
                "catalog indicator config",
            )?
            .insert("timeframe".to_owned(), Value::String(timeframe));
            let existing_map = rows_by_id(&resource_rows(program, "indicators")?, "indicator")?;
            if fuzzy_contract(required(&item, "meta", "catalog indicator")?)?
                != fuzzy_contract_for_members(&existing_map, &before)?
            {
                return Err(invalid("indicator insert fuzzy capability drift"));
            }
            let mut after = before.clone();
            after.push(instance_id.clone());
            after.sort();
            let mut groups_out = resource_rows(program, "evidenceGroups")?;
            for row in groups_out.iter_mut() {
                if row_id(row, "evidence group")? == group_id {
                    map_mut(row, "evidence group")?.insert(
                        "indicatorInstanceIds".to_owned(),
                        array(after.iter().cloned().map(Value::String)),
                    );
                }
            }
            let mut indicators_out = resource_rows(program, "indicators")?;
            indicators_out.push(item);
            normal_weights(&mut indicators_out, &after)?;
            let child =
                replace_resource_rows(program, Some(indicators_out), Some(groups_out), None, None)?;
            Ok((
                child,
                array([object([
                    ("operation", Value::String(kind.to_owned())),
                    ("groupId", Value::String(group_id)),
                    ("indicatorInstanceId", Value::String(instance_id)),
                    ("indicatorId", Value::String(indicator_key)),
                    ("afterMembers", array(after.into_iter().map(Value::String))),
                ])]),
            ))
        }
        "indicator_instance_remove" => {
            let group_id = ctext(construction, "groupId")?;
            let instance_id = ctext(construction, "indicatorInstanceId")?;
            let group = groups
                .get(&group_id)
                .ok_or_else(|| invalid("indicator remove parent drift"))?;
            let before = carray_text(construction, "beforeMembers")?;
            if members(group)? != before
                || !indicator_exclusive_to_group(program, &instance_id, &group_id)?
            {
                return Err(invalid("indicator remove parent drift"));
            }
            let after = before
                .iter()
                .filter(|item| *item != &instance_id)
                .cloned()
                .collect::<Vec<_>>();
            if after.is_empty() {
                return Err(invalid("cannot remove final fuzzy indicator"));
            }
            let mut groups_out = resource_rows(program, "evidenceGroups")?;
            for row in groups_out.iter_mut() {
                if row_id(row, "evidence group")? == group_id {
                    map_mut(row, "evidence group")?.insert(
                        "indicatorInstanceIds".to_owned(),
                        array(after.iter().cloned().map(Value::String)),
                    );
                }
            }
            let mut indicators_out = resource_rows(program, "indicators")?;
            indicators_out.retain(|row| {
                indicator_id(row)
                    .map(|id| id != instance_id)
                    .unwrap_or(false)
            });
            normal_weights(&mut indicators_out, &after)?;
            let child =
                replace_resource_rows(program, Some(indicators_out), Some(groups_out), None, None)?;
            Ok((
                child,
                array([object([
                    ("operation", Value::String(kind.to_owned())),
                    ("groupId", Value::String(group_id)),
                    ("indicatorInstanceId", Value::String(instance_id)),
                    ("afterMembers", array(after.into_iter().map(Value::String))),
                ])]),
            ))
        }
        "indicator_substitute" | "directional_event_substitute" => {
            let instance_id = ctext(construction, "indicatorInstanceId")?;
            let before_id = ctext(construction, "beforeIndicatorId")?;
            let after_id = ctext(construction, "afterIndicatorId")?;
            let item = indicators
                .get(&instance_id)
                .ok_or_else(|| invalid("indicator substitution parent drift"))?;
            if object_get(required(item, "meta", "indicator")?, "id").and_then(Value::as_str)
                != Some(before_id.as_str())
            {
                return Err(invalid("indicator substitution parent drift"));
            }
            let fuzzy = !groups_for_indicator(program, &instance_id)?.is_empty();
            let event = !event_ids_for_indicator(program, &instance_id)?.is_empty();
            let expected_contract = canonical_clone(required(
                construction,
                "bindingContract",
                "operator construction",
            )?)?;
            if binding_contract(required(item, "meta", "indicator")?, fuzzy, event, false)?
                != Some(expected_contract.clone())
            {
                return Err(invalid("indicator substitution binding drift"));
            }
            if kind == "directional_event_substitute" {
                let event_id = ctext(construction, "eventId")?;
                if !events.contains_key(&event_id)
                    || object_get(
                        events.get(&event_id).expect("checked"),
                        "indicatorInstanceId",
                    )
                    .and_then(Value::as_str)
                        != Some(instance_id.as_str())
                {
                    return Err(invalid("directional event substitution binding drift"));
                }
            }
            let replacement = replace_indicator_from_catalog(authority, item, &after_id, &side)?;
            if binding_contract(
                required(&replacement, "meta", "indicator")?,
                fuzzy,
                event,
                false,
            )? != Some(expected_contract)
            {
                return Err(invalid("replacement catalog capability drift"));
            }
            let mut indicators_out = resource_rows(program, "indicators")?;
            for row in indicators_out.iter_mut() {
                if indicator_id(row)? == instance_id {
                    *row = replacement.clone();
                }
            }
            let child = replace_resource_rows(program, Some(indicators_out), None, None, None)?;
            Ok((
                child,
                array([object([
                    ("operation", Value::String(kind.to_owned())),
                    ("indicatorInstanceId", Value::String(instance_id)),
                    ("beforeIndicatorId", Value::String(before_id)),
                    ("afterIndicatorId", Value::String(after_id)),
                ])]),
            ))
        }
        "indicator_timeframe_mutate"
        | "indicator_lookback_mutate"
        | "indicator_period_mutate"
        | "indicator_range_mutate" => {
            let instance_id = ctext(construction, "indicatorInstanceId")?;
            let item = indicators
                .get(&instance_id)
                .ok_or_else(|| invalid("indicator parameter parent drift"))?;
            if !source_catalog_matches(authority, item)? {
                return Err(invalid("indicator parameter catalog or parent drift"));
            }
            let mut revised = canonical_clone(item)?;
            let config = required_mut(map_mut(&mut revised, "indicator")?, "config", "indicator")?;
            match kind {
                "indicator_timeframe_mutate" => {
                    let before = ctext(construction, "before")?.to_ascii_uppercase();
                    let after = ctext(construction, "after")?.to_ascii_uppercase();
                    if object_get(config, "timeframe")
                        .and_then(Value::as_str)
                        .map(str::to_ascii_uppercase)
                        != Some(before)
                        || !authority.timeframe_policy().contains(&after)
                    {
                        return Err(invalid("indicator timeframe parent drift"));
                    }
                    map_mut(config, "indicator config")?
                        .insert("timeframe".to_owned(), Value::String(after));
                }
                "indicator_lookback_mutate" => {
                    let before = as_i64(
                        required(construction, "before", "operator construction")?,
                        "lookback before",
                    )?;
                    let after = as_i64(
                        required(construction, "after", "operator construction")?,
                        "lookback after",
                    )?;
                    if !event_ids_for_indicator(program, &instance_id)?.is_empty()
                        || object_get(config, "lookbackBars")
                            .and_then(Value::as_i64)
                            .unwrap_or(1)
                            != before
                        || after < 0
                        || !authority
                            .evidence_lookback_choices()
                            .contains(&(after as u64))
                    {
                        return Err(invalid("indicator lookback violates fresh-event contract"));
                    }
                    map_mut(config, "indicator config")?
                        .insert("lookbackBars".to_owned(), Value::from(after));
                }
                "indicator_period_mutate" => {
                    let change = required(construction, "change", "operator construction")?;
                    let parameter = ctext(change, "parameter")?;
                    replace_talib_value(
                        config,
                        &parameter,
                        required(change, "after", "period change")?,
                    )?;
                    if !period_order_valid(config)? {
                        return Err(invalid(
                            "indicator period mutation breaks ordered parameters",
                        ));
                    }
                }
                "indicator_range_mutate" => {
                    let change = required(construction, "change", "operator construction")?;
                    let side = ctext(change, "side")?;
                    let ranges = object_get(config, "ranges")
                        .and_then(Value::as_object)
                        .ok_or_else(|| invalid("indicator range config is invalid"))?;
                    if ranges.get(&side) != Some(required(change, "before", "range change")?) {
                        return Err(invalid("indicator range parent drift"));
                    }
                    map_mut(config, "indicator config")?
                        .get_mut("ranges")
                        .and_then(Value::as_object_mut)
                        .ok_or_else(|| invalid("indicator range config is invalid"))?
                        .insert(
                            side,
                            canonical_clone(required(change, "after", "range change")?)?,
                        );
                }
                _ => unreachable!(),
            }
            let mut indicators_out = resource_rows(program, "indicators")?;
            for row in indicators_out.iter_mut() {
                if indicator_id(row)? == instance_id {
                    *row = revised.clone();
                }
            }
            let child = replace_resource_rows(program, Some(indicators_out), None, None, None)?;
            Ok((
                child,
                array([object([
                    ("operation", Value::String(kind.to_owned())),
                    ("indicatorInstanceId", Value::String(instance_id)),
                    ("construction", canonical_clone(construction)?),
                ])]),
            ))
        }
        "directional_event_insert" => {
            let node_id = ctext(construction, "nodeId")?;
            let indicator_key = ctext(construction, "indicatorId")?;
            let instance_id = ctext(construction, "indicatorInstanceId")?;
            let event_id = ctext(construction, "eventId")?;
            let node = find_row(&node_rows(program)?, "node", &node_id)?;
            if indicators.contains_key(&instance_id)
                || events.contains_key(&event_id)
                || array_ref(required(&node, "resources", "node")?, "node resources")?
                    .iter()
                    .any(|use_row| {
                        object_get(use_row, "kind").and_then(Value::as_str) == Some("event")
                    })
            {
                return Err(invalid("event insert parent drift"));
            }
            let item = catalog_item(authority, &indicator_key, &instance_id, &side)?;
            let contract =
                canonical_clone(required(construction, "contract", "operator construction")?)?;
            if event_contract(required(&item, "meta", "catalog indicator")?)?
                != Some(contract.clone())
            {
                return Err(invalid("event catalog capability drift"));
            }
            let mut item = item;
            map_mut(
                required_mut(map_mut(&mut item, "indicator")?, "config", "indicator")?,
                "indicator config",
            )?
            .insert("lookbackBars".to_owned(), Value::from(1));
            let schema = map_ref(
                required(&contract, "eventOutputSchema", "event contract")?,
                "event output schema",
            )?;
            let event = object([
                ("id", Value::String(event_id.clone())),
                ("indicatorInstanceId", Value::String(instance_id.clone())),
                (
                    "longOutput",
                    canonical_clone(
                        schema
                            .get("longOutput")
                            .ok_or_else(|| invalid("event output contract lacks long token"))?,
                    )?,
                ),
                (
                    "shortOutput",
                    canonical_clone(
                        schema
                            .get("shortOutput")
                            .ok_or_else(|| invalid("event output contract lacks short token"))?,
                    )?,
                ),
                ("ownerSide", Value::String(side)),
            ]);
            let mut indicators_out = resource_rows(program, "indicators")?;
            indicators_out.push(item);
            let mut events_out = resource_rows(program, "events")?;
            events_out.push(event);
            let changed = node_set_guard(
                &node_with_resource(&node, "event", &event_id)?,
                all_with(&node_guard(&node)?, &guard_event_clause(&event_id))?,
            )?;
            let child = replace_node(
                &replace_resource_rows(
                    program,
                    Some(indicators_out),
                    None,
                    Some(events_out),
                    None,
                )?,
                &changed,
            )?;
            Ok((
                child,
                array([object([
                    ("operation", Value::String(kind.to_owned())),
                    ("eventId", Value::String(event_id)),
                    ("indicatorInstanceId", Value::String(instance_id)),
                    ("indicatorId", Value::String(indicator_key)),
                    ("nodeId", Value::String(node_id)),
                ])]),
            ))
        }
        "directional_event_remove" => {
            let event_id = ctext(construction, "eventId")?;
            let instance_id = ctext(construction, "indicatorInstanceId")?;
            let node_id = ctext(construction, "nodeId")?;
            let event = events
                .get(&event_id)
                .ok_or_else(|| invalid("event remove parent drift"))?;
            let node = find_row(&node_rows(program)?, "node", &node_id)?;
            if references(program, "event", &event_id)? != vec![node.clone()]
                || object_get(event, "indicatorInstanceId").and_then(Value::as_str)
                    != Some(instance_id.as_str())
                || node_guard(&node)?
                    != canonical_clone(required(
                        construction,
                        "beforeGuard",
                        "operator construction",
                    )?)?
                || !groups_for_indicator(program, &instance_id)?.is_empty()
            {
                return Err(invalid("event remove parent drift"));
            }
            let indicators_out = resource_rows(program, "indicators")?
                .into_iter()
                .filter(|row| {
                    indicator_id(row)
                        .map(|id| id != instance_id)
                        .unwrap_or(false)
                })
                .collect::<Vec<_>>();
            let events_out = resource_rows(program, "events")?
                .into_iter()
                .filter(|row| {
                    row_id(row, "event")
                        .map(|id| id != event_id)
                        .unwrap_or(false)
                })
                .collect::<Vec<_>>();
            let changed = node_set_guard(
                &node_without_resource(&node, "event", &event_id)?,
                canonical_clone(required(
                    construction,
                    "afterGuard",
                    "operator construction",
                )?)?,
            )?;
            let child = replace_node(
                &replace_resource_rows(
                    program,
                    Some(indicators_out),
                    None,
                    Some(events_out),
                    None,
                )?,
                &changed,
            )?;
            Ok((
                child,
                array([object([
                    ("operation", Value::String(kind.to_owned())),
                    ("eventId", Value::String(event_id)),
                    ("indicatorInstanceId", Value::String(instance_id)),
                    ("nodeId", Value::String(node_id)),
                ])]),
            ))
        }
        _ => Err(invalid("unknown resource mutation construction")),
    }
}

fn canonical_hold(value: &Value) -> Result<Value> {
    let fields = map_ref(value, "hold policy")?;
    let kind = text(required(value, "kind", "hold policy")?, "hold policy kind")?;
    match kind.as_str() {
        "none" => {
            exact_keys(fields, &["kind"], "none hold policy")?;
            Ok(object([("kind", Value::String("none".to_owned()))]))
        }
        "market_bars" => {
            if !matches!(fields.len(), 3 | 4)
                || !["kind", "bars", "timeframe"]
                    .iter()
                    .all(|key| fields.contains_key(*key))
                || fields
                    .get("onBreach")
                    .is_some_and(|item| item.as_str() != Some("exit_next_open"))
            {
                return Err(invalid("market-bars hold policy has invalid fields"));
            }
            let bars = as_u64(
                required(value, "bars", "market-bars hold policy")?,
                "market-bars hold bars",
            )?;
            if bars < 1 {
                return Err(invalid("market-bars hold bars must be positive"));
            }
            let timeframe = text(
                required(value, "timeframe", "market-bars hold policy")?,
                "market-bars hold timeframe",
            )?
            .to_ascii_uppercase();
            if timeframe.len() > 32 {
                return Err(invalid("market-bars hold timeframe is too long"));
            }
            Ok(object([
                ("kind", Value::String("market_bars".to_owned())),
                ("bars", Value::from(bars)),
                ("timeframe", Value::String(timeframe)),
            ]))
        }
        "elapsed_calendar" => {
            if !matches!(fields.len(), 2 | 3)
                || !["kind", "hours"]
                    .iter()
                    .all(|key| fields.contains_key(*key))
                || fields
                    .get("onBreach")
                    .is_some_and(|item| item.as_str() != Some("exit_next_open"))
            {
                return Err(invalid("elapsed-calendar hold policy has invalid fields"));
            }
            let hours = finite_number(
                required(value, "hours", "elapsed-calendar hold policy")?,
                "elapsed-calendar hold hours",
            )?;
            if hours <= 0.0 {
                return Err(invalid("elapsed-calendar hold hours must be positive"));
            }
            Ok(object([
                ("kind", Value::String("elapsed_calendar".to_owned())),
                (
                    "hours",
                    canonical_clone(required(value, "hours", "elapsed-calendar hold policy")?)?,
                ),
            ]))
        }
        _ => Err(invalid("hold policy kind is unsupported")),
    }
}

fn policy_f64_rows(policy: &Value, key: &str) -> Result<Vec<f64>> {
    let rows = array_ref(
        required(policy, key, "initial protection policy")?,
        "initial protection values",
    )?;
    let mut values = Vec::new();
    for row in rows {
        let value = finite_number(row, "initial protection value")?;
        if value <= 0.0 || values.contains(&value) {
            return Err(invalid(
                "initial protection policy values must be positive and unique",
            ));
        }
        values.push(value);
    }
    if values.is_empty() {
        return Err(invalid("initial protection policy values cannot be empty"));
    }
    Ok(values)
}

fn default_initial_protection_policy() -> Result<Value> {
    Ok(object([
        (
            "schemaVersion",
            Value::String("temporal_qd_initial_protection_policy_v2".to_owned()),
        ),
        (
            "stopPercentChoices",
            array(
                [0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0]
                    .into_iter()
                    .map(|value| value_number(value, "policy value"))
                    .collect::<Result<Vec<_>>>()?,
            ),
        ),
        (
            "rewardMultipleChoices",
            array(
                [0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 4.0, 6.0]
                    .into_iter()
                    .map(|value| value_number(value, "policy value"))
                    .collect::<Result<Vec<_>>>()?,
            ),
        ),
        (
            "targetPercentChoices",
            array(
                [0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0]
                    .into_iter()
                    .map(|value| value_number(value, "policy value"))
                    .collect::<Result<Vec<_>>>()?,
            ),
        ),
        (
            "distanceMultipleChoices",
            array(
                [0.5, 0.75, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0]
                    .into_iter()
                    .map(|value| value_number(value, "policy value"))
                    .collect::<Result<Vec<_>>>()?,
            ),
        ),
        (
            "mutationClassWeights",
            object([
                ("adjacent", Value::from(70)),
                ("jump", Value::from(25)),
                ("kind_switch", Value::from(5)),
            ]),
        ),
        (
            "immigrantModes",
            array(
                [
                    "coupled_reward_multiple",
                    "decoupled_fixed_percent",
                    "no_fixed_target",
                    "dynamic_catalog_authorized",
                ]
                .into_iter()
                .map(|item| Value::String(item.to_owned())),
            ),
        ),
    ]))
}

fn validate_initial_protection_policy(value: &Value) -> Result<Value> {
    let fields = map_ref(value, "initial protection policy")?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "stopPercentChoices",
            "rewardMultipleChoices",
            "targetPercentChoices",
            "distanceMultipleChoices",
            "mutationClassWeights",
            "immigrantModes",
        ],
        "initial protection policy",
    )?;
    if object_get(value, "schemaVersion").and_then(Value::as_str)
        != Some("temporal_qd_initial_protection_policy_v2")
    {
        return Err(invalid("initial protection policy schema is incompatible"));
    }
    for (key, percent) in [
        ("stopPercentChoices", true),
        ("rewardMultipleChoices", false),
        ("targetPercentChoices", true),
        ("distanceMultipleChoices", false),
    ] {
        let values = policy_f64_rows(value, key)?;
        if values.windows(2).any(|pair| pair[0] >= pair[1])
            || values.iter().any(|item| {
                if percent {
                    *item >= 100.0
                } else {
                    *item > 100.0
                }
            })
        {
            return Err(invalid(
                "initial protection policy choices are not a bounded canonical grid",
            ));
        }
    }
    let weights = map_ref(
        required(value, "mutationClassWeights", "initial protection policy")?,
        "initial protection mutation class weights",
    )?;
    exact_keys(
        weights,
        &["adjacent", "jump", "kind_switch"],
        "initial protection mutation class weights",
    )?;
    let total = ["adjacent", "jump", "kind_switch"]
        .iter()
        .map(|key| {
            as_u64(
                weights.get(*key).expect("exact keys"),
                "mutation class weight",
            )
        })
        .collect::<Result<Vec<_>>>()?;
    if total.contains(&0) || total.iter().sum::<u64>() != 100 {
        return Err(invalid(
            "initial protection mutation class weights must be positive and sum to 100",
        ));
    }
    let modes = array_ref(
        required(value, "immigrantModes", "initial protection policy")?,
        "initial protection immigrant modes",
    )?;
    let mut seen = BTreeSet::new();
    if modes.is_empty()
        || modes
            .iter()
            .map(|mode| text(mode, "initial protection immigrant mode"))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .any(|mode| !seen.insert(mode))
    {
        return Err(invalid(
            "initial protection immigrant modes must be nonempty and unique",
        ));
    }
    canonical_clone(value)
}

fn validate_temporal_domains(value: &Value) -> Result<Value> {
    let fields = map_ref(value, "sealed temporal domains")?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "eventAges",
            "positionAges",
            "utcSessionWindows",
            "eventAgeWindows",
            "consecutiveCounts",
            "cooldownCounts",
            "temporalDomainsSha256",
        ],
        "sealed temporal domains",
    )?;
    if object_get(value, "schemaVersion").and_then(Value::as_str)
        != Some("temporal_qd_v5_temporal_domains_v1")
    {
        return Err(invalid("sealed temporal-domain schema is invalid"));
    }
    let supplied = sha256_identifier(
        required(value, "temporalDomainsSha256", "sealed temporal domains")?,
        "sealed temporal domains SHA-256",
    )?;
    let mut identity = canonical_clone(value)?;
    map_mut(&mut identity, "sealed temporal domains")?.remove("temporalDomainsSha256");
    if supplied != sha(&identity)? {
        return Err(invalid("sealed temporal-domain identity drifted"));
    }

    let strictly_increasing = |rows: &[u64], label: &str| -> Result<()> {
        if rows.is_empty() || rows.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(invalid(format!(
                "{label} must be nonempty, unique, and canonically ordered"
            )));
        }
        Ok(())
    };
    let event_ages = domain_u64s(value, "eventAges")?;
    if event_ages.iter().any(|value| *value > 1_000_000) {
        return Err(invalid(
            "temporal event-age domain exceeds native guard maximum",
        ));
    }
    strictly_increasing(&event_ages, "temporal event-age domain")?;
    let position_ages = domain_u64s(value, "positionAges")?;
    if position_ages.iter().any(|value| *value > 10_000_000) {
        return Err(invalid(
            "temporal position-age domain exceeds native guard maximum",
        ));
    }
    strictly_increasing(&position_ages, "temporal position-age domain")?;
    let consecutive = domain_u64s(value, "consecutiveCounts")?;
    if consecutive
        .iter()
        .any(|value| !(2..=1_000_000).contains(value))
    {
        return Err(invalid(
            "temporal consecutive-count domain exceeds native guard bounds",
        ));
    }
    strictly_increasing(&consecutive, "temporal consecutive-count domain")?;
    let cooldown = domain_u64s(value, "cooldownCounts")?;
    if cooldown
        .iter()
        .any(|value| !(1..=1_000_000).contains(value))
    {
        return Err(invalid(
            "temporal cooldown-count domain exceeds native guard bounds",
        ));
    }
    strictly_increasing(&cooldown, "temporal cooldown-count domain")?;

    let windows = domain_pairs(value, "utcSessionWindows")?;
    if windows.is_empty()
        || windows
            .iter()
            .any(|(start, end)| *start > 1439 || *end > 1439 || start == end)
        || windows.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(invalid(
            "temporal UTC-session domain is invalid or noncanonical",
        ));
    }
    let event_windows = domain_pairs(value, "eventAgeWindows")?;
    if event_windows.is_empty()
        || event_windows
            .iter()
            .any(|(minimum, maximum)| minimum > maximum || *maximum > 1_000_000)
        || event_windows.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(invalid(
            "temporal event-age-window domain is invalid or noncanonical",
        ));
    }
    canonical_clone(value)
}

fn temporal_domain_selection_projection(value: &Value) -> Result<Value> {
    let mut output = canonical_clone(value)?;
    let fields = map_mut(&mut output, "sealed temporal domains")?;
    fields.remove("schemaVersion");
    fields.remove("temporalDomainsSha256");
    Ok(output)
}

/// Re-validate the exact Python temporal operator specification instead of
/// accepting a bare SHA.  The core parser also validates this field, but this
/// local check makes the operator engine fail closed if it is ever called
/// through a different integration seam.
fn validate_legacy_temporal_operator_specification(
    value: &Value,
    supplied_sha256: &str,
    compiler_policy_sha256: &str,
    temporal_domains: &Value,
) -> Result<Value> {
    let fields = map_ref(value, "legacy temporal operator specification")?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "operatorVersion",
            "domains",
            "guardFamilies",
            "compilerPolicySha256",
            "nativeValidation",
            "operatorSpecSha256",
        ],
        "legacy temporal operator specification",
    )?;
    if object_get(value, "schemaVersion").and_then(Value::as_str)
        != Some("evolvable_module_temporal_operator_plan_v1")
        || object_get(value, "operatorVersion").and_then(Value::as_str)
            != Some(TEMPORAL_OPERATOR_VERSION)
        || !object_get(value, "nativeValidation").is_some_and(Value::is_boolean)
    {
        return Err(invalid(
            "legacy temporal operator specification has an incompatible schema/version",
        ));
    }
    let supplied_compiler_policy_sha256 = sha256_identifier(
        required(
            value,
            "compilerPolicySha256",
            "legacy temporal operator specification",
        )?,
        "legacy temporal compiler policy SHA-256",
    )?;
    if supplied_compiler_policy_sha256 != compiler_policy_sha256 {
        return Err(invalid(
            "legacy temporal operator specification compiler policy drifted from sealed authority",
        ));
    }
    let expected_families = [
        "predicate_edge",
        "consecutive_true",
        "event_age_window",
        "fresh_event_absence",
        "state_or_position_age",
        "utc_session_window",
        "action_cooldown_elapsed",
    ];
    let families = array_ref(
        required(
            value,
            "guardFamilies",
            "legacy temporal operator specification",
        )?,
        "legacy temporal guard families",
    )?;
    if families.len() != expected_families.len()
        || families
            .iter()
            .zip(expected_families)
            .any(|(value, expected)| value.as_str() != Some(expected))
    {
        return Err(invalid(
            "legacy temporal operator guard-family order is not canonical",
        ));
    }
    if required(value, "domains", "legacy temporal operator specification")?
        != &temporal_domain_selection_projection(temporal_domains)?
    {
        return Err(invalid(
            "legacy temporal operator specification domains drift from sealed temporal domains",
        ));
    }
    let actual = sha256_identifier(
        required(
            value,
            "operatorSpecSha256",
            "legacy temporal operator specification",
        )?,
        "legacy temporal operator specification SHA-256",
    )?;
    if actual != supplied_sha256 {
        return Err(invalid(
            "legacy temporal operator specification outer SHA binding drifted",
        ));
    }
    let mut identity = canonical_clone(value)?;
    map_mut(&mut identity, "legacy temporal operator specification")?.remove("operatorSpecSha256");
    if actual != sha(&identity)? {
        return Err(invalid(
            "legacy temporal operator specification self identity drifted",
        ));
    }
    canonical_clone(value)
}

fn domain_u64s(domains: &Value, key: &str) -> Result<Vec<u64>> {
    array_ref(
        required(domains, key, "temporal domains")?,
        "temporal-domain values",
    )?
    .iter()
    .map(|row| as_u64(row, &format!("temporal domain {key}")))
    .collect()
}

fn domain_pairs(domains: &Value, key: &str) -> Result<Vec<(u64, u64)>> {
    array_ref(
        required(domains, key, "temporal domains")?,
        "temporal-domain pairs",
    )?
    .iter()
    .map(|row| {
        let items = array_ref(row, "temporal-domain pair")?;
        if items.len() != 2 {
            return Err(invalid("temporal-domain pair must have two values"));
        }
        Ok((
            as_u64(&items[0], "temporal-domain pair start")?,
            as_u64(&items[1], "temporal-domain pair end")?,
        ))
    })
    .collect()
}

fn guard_walk(
    guard: &Value,
    path: Vec<Value>,
    output: &mut Vec<(Vec<Value>, Value)>,
) -> Result<()> {
    output.push((path.clone(), canonical_clone(guard)?));
    let kind = object_get(guard, "kind")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if kind == "all" || kind == "any" {
        for (index, child) in array_ref(
            required(guard, "guards", "composite guard")?,
            "composite guard children",
        )?
        .iter()
        .enumerate()
        {
            let mut next = path.clone();
            next.push(Value::String("guards".to_owned()));
            next.push(Value::from(index as u64));
            guard_walk(child, next, output)?;
        }
    } else if kind == "not" {
        let mut next = path;
        next.push(Value::String("guard".to_owned()));
        guard_walk(required(guard, "guard", "not guard")?, next, output)?;
    } else if kind == "predicate_edge" || kind == "consecutive_true" {
        let mut next = path;
        next.push(Value::String("predicate".to_owned()));
        guard_walk(
            required(guard, "predicate", "wrapped predicate")?,
            next,
            output,
        )?;
    }
    Ok(())
}

fn guard_at(guard: &Value, path: &[Value]) -> Result<Value> {
    let mut current = guard;
    let mut index = 0_usize;
    while index < path.len() {
        let key = path[index]
            .as_str()
            .ok_or_else(|| invalid("guard path key must be text"))?;
        if key == "guards" {
            let row_index = path
                .get(index + 1)
                .and_then(Value::as_u64)
                .ok_or_else(|| invalid("guard path guards index must be unsigned"))?
                as usize;
            current = array_ref(
                required(current, "guards", "composite guard")?,
                "composite guard children",
            )?
            .get(row_index)
            .ok_or_else(|| invalid("guard path is stale"))?;
            index += 2;
        } else if key == "guard" || key == "predicate" {
            current = required(current, key, "guard")?;
            index += 1;
        } else {
            return Err(invalid("guard path has an unsupported segment"));
        }
    }
    canonical_clone(current)
}

fn replace_guard_at(guard: &Value, path: &[Value], replacement: &Value) -> Result<Value> {
    if path.is_empty() {
        return canonical_clone(replacement);
    }
    let key = path[0]
        .as_str()
        .ok_or_else(|| invalid("guard path key must be text"))?;
    let mut output = canonical_clone(guard)?;
    if key == "guards" {
        let index = path
            .get(1)
            .and_then(Value::as_u64)
            .ok_or_else(|| invalid("guard path guards index must be unsigned"))?
            as usize;
        let children = map_mut(&mut output, "composite guard")?
            .get_mut("guards")
            .and_then(Value::as_array_mut)
            .ok_or_else(|| invalid("composite guard children drifted"))?;
        let child = children
            .get(index)
            .ok_or_else(|| invalid("guard path is stale"))?;
        children[index] = replace_guard_at(child, &path[2..], replacement)?;
    } else if key == "guard" || key == "predicate" {
        let child = required(&output, key, "guard")?;
        let changed = replace_guard_at(child, &path[1..], replacement)?;
        map_mut(&mut output, "guard")?.insert(key.to_owned(), changed);
    } else {
        return Err(invalid("guard path has an unsupported segment"));
    }
    Ok(output)
}

fn temporal_replacement(
    owner_kind: &str,
    owner_id: &str,
    path: &[Value],
    before: &Value,
    after: Value,
    family: &str,
    parameters: Value,
) -> Result<Value> {
    Ok(object([
        ("kind", Value::String("typed_guard_replace".to_owned())),
        ("family", Value::String(family.to_owned())),
        (
            "site",
            object([
                ("ownerKind", Value::String(owner_kind.to_owned())),
                ("ownerId", Value::String(owner_id.to_owned())),
                (
                    "guardPath",
                    array(
                        path.iter()
                            .map(canonical_clone)
                            .collect::<Result<Vec<_>>>()?,
                    ),
                ),
            ]),
        ),
        ("beforeGuard", canonical_clone(before)?),
        ("afterGuard", after),
        ("parameters", parameters),
    ]))
}

fn temporal_site_constructions(
    owner_kind: &str,
    owner_id: &str,
    path: &[Value],
    guard: &Value,
    domains: &Value,
    output: &mut Vec<Value>,
) -> Result<()> {
    let kind = object_get(guard, "kind")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match kind {
        "utc_time_window" => {
            let current = (
                object_get(guard, "startMinute").and_then(Value::as_u64),
                object_get(guard, "endMinute").and_then(Value::as_u64),
            );
            for (start, end) in domain_pairs(domains, "utcSessionWindows")? {
                if current == (Some(start), Some(end)) {
                    continue;
                }
                let mut after = canonical_clone(guard)?;
                let fields = map_mut(&mut after, "UTC session guard")?;
                fields.insert("startMinute".to_owned(), Value::from(start));
                fields.insert("endMinute".to_owned(), Value::from(end));
                output.push(temporal_replacement(
                    owner_kind,
                    owner_id,
                    path,
                    guard,
                    after,
                    "utc_session_window",
                    object([
                        ("startMinute", Value::from(start)),
                        ("endMinute", Value::from(end)),
                    ]),
                )?);
            }
        }
        "event_age_at_most"
        | "state_age_at_least"
        | "state_age_at_most"
        | "condition_streak_at_least" => {
            let values = domain_u64s(domains, "eventAges")?;
            for events in values
                .into_iter()
                .filter(|value| kind != "condition_streak_at_least" || *value > 0)
            {
                if object_get(guard, "events").and_then(Value::as_u64) == Some(events) {
                    continue;
                }
                let mut after = canonical_clone(guard)?;
                map_mut(&mut after, "age guard")?.insert("events".to_owned(), Value::from(events));
                output.push(temporal_replacement(
                    owner_kind,
                    owner_id,
                    path,
                    guard,
                    after,
                    "state_or_condition_age",
                    object([("events", Value::from(events))]),
                )?);
            }
        }
        "position_age_at_least" => {
            for events in domain_u64s(domains, "positionAges")? {
                if object_get(guard, "events").and_then(Value::as_u64) == Some(events) {
                    continue;
                }
                let mut after = canonical_clone(guard)?;
                map_mut(&mut after, "position-age guard")?
                    .insert("events".to_owned(), Value::from(events));
                output.push(temporal_replacement(
                    owner_kind,
                    owner_id,
                    path,
                    guard,
                    after,
                    "position_age",
                    object([("events", Value::from(events))]),
                )?);
            }
        }
        "fresh_event" => {
            let event_id = text(required(guard, "eventId", "fresh-event guard")?, "event ID")?;
            for (minimum, maximum) in domain_pairs(domains, "eventAgeWindows")? {
                output.push(temporal_replacement(
                    owner_kind,
                    owner_id,
                    path,
                    guard,
                    object([
                        ("kind", Value::String("event_age_window".to_owned())),
                        ("eventId", Value::String(event_id.clone())),
                        ("minimumEvents", Value::from(minimum)),
                        ("maximumEvents", Value::from(maximum)),
                    ]),
                    "fresh_event_age_window",
                    object([
                        ("minimumEvents", Value::from(minimum)),
                        ("maximumEvents", Value::from(maximum)),
                    ]),
                )?);
            }
            output.push(temporal_replacement(
                owner_kind,
                owner_id,
                path,
                guard,
                object([
                    ("kind", Value::String("not".to_owned())),
                    ("guard", canonical_clone(guard)?),
                ]),
                "fresh_event_absence",
                object([]),
            )?);
        }
        _ => {}
    }
    if matches!(
        kind,
        "evidence_at_least"
            | "evidence_below"
            | "utc_time_window"
            | "state_age_at_least"
            | "state_age_at_most"
            | "position_age_at_least"
            | "unrealized_r_at_least"
            | "unrealized_r_at_most"
    ) {
        let site = object([
            ("ownerKind", Value::String(owner_kind.to_owned())),
            ("ownerId", Value::String(owner_id.to_owned())),
            (
                "guardPath",
                array(
                    path.iter()
                        .map(canonical_clone)
                        .collect::<Result<Vec<_>>>()?,
                ),
            ),
        ]);
        let occurrence = sha(&object([
            ("site", site),
            ("guard", canonical_clone(guard)?),
        ]))?;
        for direction in ["falling", "rising"] {
            output.push(temporal_replacement(
                owner_kind,
                owner_id,
                path,
                guard,
                object([
                    ("kind", Value::String("predicate_edge".to_owned())),
                    (
                        "operatorId",
                        Value::String("edge_trigger_predicate_v1".to_owned()),
                    ),
                    ("operatorVersion", Value::String("1".to_owned())),
                    ("occurrenceSha256", Value::String(occurrence.clone())),
                    ("direction", Value::String(direction.to_owned())),
                    ("predicate", canonical_clone(guard)?),
                ]),
                "predicate_edge",
                object([("direction", Value::String(direction.to_owned()))]),
            )?);
        }
        for evaluations in domain_u64s(domains, "consecutiveCounts")? {
            output.push(temporal_replacement(
                owner_kind,
                owner_id,
                path,
                guard,
                object([
                    ("kind", Value::String("consecutive_true".to_owned())),
                    (
                        "operatorId",
                        Value::String("require_consecutive_true_v1".to_owned()),
                    ),
                    ("operatorVersion", Value::String("1".to_owned())),
                    ("occurrenceSha256", Value::String(occurrence.clone())),
                    ("predicate", canonical_clone(guard)?),
                    ("evaluations", Value::from(evaluations)),
                ]),
                "consecutive_true",
                object([("evaluations", Value::from(evaluations))]),
            )?);
        }
    }
    Ok(())
}

fn temporal_constructions(program: &Value, authority: &V5OperatorAuthority) -> Result<Vec<Value>> {
    let mut output = Vec::new();
    for (owner_kind, rows) in [("node", node_rows(program)?), ("edge", edge_rows(program)?)] {
        for row in rows {
            let owner_id = row_id(&row, owner_kind)?;
            let mut sites = Vec::new();
            guard_walk(required(&row, "guard", owner_kind)?, Vec::new(), &mut sites)?;
            for (path, guard) in sites {
                temporal_site_constructions(
                    owner_kind,
                    &owner_id,
                    &path,
                    &guard,
                    authority.temporal_domains(),
                    &mut output,
                )?;
            }
        }
    }
    for edge in edge_rows(program)? {
        let effect = object_get(&edge, "effect").and_then(Value::as_str);
        if !matches!(
            effect,
            Some("move_stop_to_break_even_next_open")
                | Some("tighten_stop_next_open")
                | Some("set_target_next_open")
                | Some("cancel_target_next_open")
                | Some("activate_trailing_stop_next_open")
                | Some("deactivate_trailing_stop_next_open")
        ) {
            continue;
        }
        let edge_id = row_id(&edge, "edge")?;
        let guard = required(&edge, "guard", "edge")?;
        for evaluations in domain_u64s(authority.temporal_domains(), "cooldownCounts")? {
            output.push(temporal_replacement(
                "edge",
                &edge_id,
                &[],
                guard,
                all_with(
                    guard,
                    &object([
                        ("kind", Value::String("action_cooldown_elapsed".to_owned())),
                        ("transitionId", Value::String(format!("e_{edge_id}"))),
                        ("actionOrdinal", Value::from(0)),
                        ("evaluations", Value::from(evaluations)),
                    ]),
                )?,
                "action_cooldown",
                object([("evaluations", Value::from(evaluations))]),
            )?);
        }
    }
    sorted_values(output)
}

fn temporal_transform(program: &Value, construction: &Value) -> Result<(Value, Value)> {
    if object_get(construction, "kind").and_then(Value::as_str) != Some("typed_guard_replace") {
        return Err(invalid("unsupported temporal construction"));
    }
    let site = required(construction, "site", "temporal construction")?;
    let owner_kind = text(
        required(site, "ownerKind", "temporal site")?,
        "temporal owner kind",
    )?;
    let owner_id = text(
        required(site, "ownerId", "temporal site")?,
        "temporal owner ID",
    )?;
    let path = array_ref(
        required(site, "guardPath", "temporal site")?,
        "temporal guard path",
    )?
    .to_vec();
    let before = required(construction, "beforeGuard", "temporal construction")?;
    let after = required(construction, "afterGuard", "temporal construction")?;
    let rows = match owner_kind.as_str() {
        "node" => node_rows(program)?,
        "edge" => edge_rows(program)?,
        _ => return Err(invalid("temporal owner kind is invalid")),
    };
    let owner = find_row(&rows, &owner_kind, &owner_id)?;
    let root = required(&owner, "guard", "temporal owner")?;
    if guard_at(root, &path)? != canonical_clone(before)? {
        return Err(invalid("temporal guard parent drift"));
    }
    let changed_guard = replace_guard_at(root, &path, after)?;
    let mut changed = canonical_clone(&owner)?;
    map_mut(&mut changed, "temporal owner")?.insert("guard".to_owned(), changed_guard);
    let child = if owner_kind == "node" {
        replace_node(program, &changed)?
    } else {
        replace_edge(program, &changed)?
    };
    let trace = array([object([
        (
            "family",
            canonical_clone(required(construction, "family", "temporal construction")?)?,
        ),
        ("ownerKind", Value::String(owner_kind)),
        ("ownerId", Value::String(owner_id)),
        ("guardPath", array(path)),
        ("beforeGuardSha256", Value::String(sha(before)?)),
        ("afterGuardSha256", Value::String(sha(after)?)),
        (
            "parameters",
            canonical_clone(required(
                construction,
                "parameters",
                "temporal construction",
            )?)?,
        ),
    ])]);
    Ok((child, trace))
}

fn current_hold(plan: &Value) -> Result<Value> {
    match object_get(plan, "holdPolicy") {
        Some(value) => canonical_hold(value),
        None => canonical_hold(&object([("kind", Value::String("none".to_owned()))])),
    }
}

fn hold_constructions(program: &Value, authority: &V5OperatorAuthority) -> Result<Vec<Value>> {
    let mut output = Vec::new();
    for plan in resource_rows(program, "managementRefs")? {
        let plan_id = row_id(&plan, "management reference")?;
        let current = current_hold(&plan)?;
        for choice in authority.hold_choices() {
            if canonical_hold(choice)? == current {
                continue;
            }
            output.push(object([
                ("kind", Value::String("hold".to_owned())),
                ("planId", Value::String(plan_id.clone())),
                ("before", current.clone()),
                ("newHold", canonical_hold(choice)?),
            ]));
        }
    }
    sorted_values(output)
}

fn hold_transform(
    program: &Value,
    construction: &Value,
    authority: &V5OperatorAuthority,
) -> Result<(Value, Value)> {
    if object_get(construction, "kind").and_then(Value::as_str) != Some("hold") {
        return Err(invalid("unsupported hold construction"));
    }
    let plan_id = ctext(construction, "planId")?;
    let before = canonical_hold(required(construction, "before", "hold construction")?)?;
    let replacement = canonical_hold(required(construction, "newHold", "hold construction")?)?;
    if !authority
        .hold_choices()
        .iter()
        .map(canonical_hold)
        .collect::<Result<Vec<_>>>()?
        .contains(&replacement)
    {
        return Err(invalid(
            "hold construction is outside frozen authority policy",
        ));
    }
    let mut rows = resource_rows(program, "managementRefs")?;
    let mut found = false;
    for row in rows.iter_mut() {
        if row_id(row, "management reference")? != plan_id {
            continue;
        }
        if current_hold(row)? != before {
            return Err(invalid("hold construction parent drift"));
        }
        let fields = map_mut(row, "management reference")?;
        if object_get(&replacement, "kind").and_then(Value::as_str) == Some("none") {
            fields.remove("holdPolicy");
        } else {
            fields.insert("holdPolicy".to_owned(), replacement.clone());
        }
        found = true;
    }
    if !found {
        return Err(invalid(
            "hold construction names an unknown management reference",
        ));
    }
    let child = replace_resource_rows(program, None, None, None, Some(rows))?;
    Ok((
        child,
        array([object([
            ("operation", Value::String("hold".to_owned())),
            ("managementPlanId", Value::String(plan_id)),
            ("before", before),
            ("after", replacement),
        ])]),
    ))
}

fn locator_binding_id(value: &Value) -> Option<String> {
    object_get(value, "bindingId")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn management_bound_scalar_ids(plan: &Value) -> Result<BTreeSet<String>> {
    let mut output = BTreeSet::new();
    for key in ["initialStop", "initialTarget"] {
        if let Some(id) = locator_binding_id(required(plan, key, "management reference")?) {
            output.insert(id);
        }
    }
    if let Some(trailing) = object_get(plan, "trailingStop") {
        for key in ["anchor", "distance"] {
            if let Some(id) = locator_binding_id(required(trailing, key, "trailing stop")?) {
                output.insert(id);
            }
        }
    }
    Ok(output)
}

fn remove_orphan_scalar_bindings(plan: &mut Value) -> Result<Vec<Value>> {
    let references = management_bound_scalar_ids(plan)?;
    let fields = map_mut(plan, "management reference")?;
    let Some(bindings) = fields
        .get_mut("scalarBindings")
        .and_then(Value::as_array_mut)
    else {
        return Ok(Vec::new());
    };
    let mut removed = Vec::new();
    bindings.retain(|binding| {
        let id = row_id(binding, "management scalar binding").unwrap_or_default();
        let keep = references.contains(&id);
        if !keep {
            removed.push(canonical_clone(binding).unwrap_or(Value::Null));
        }
        keep
    });
    if bindings.is_empty() {
        fields.remove("scalarBindings");
    }
    removed.sort_by(|left, right| {
        row_id(left, "management scalar binding")
            .unwrap_or_default()
            .cmp(&row_id(right, "management scalar binding").unwrap_or_default())
    });
    Ok(removed)
}

fn classify_scalar(current: f64, choices: &[f64], candidate: f64) -> &'static str {
    let Some(current_index) = choices.iter().position(|value| *value == current) else {
        return "jump";
    };
    let candidate_index = choices
        .iter()
        .position(|value| *value == candidate)
        .unwrap_or(usize::MAX);
    if current_index.abs_diff(candidate_index) == 1 {
        "adjacent"
    } else {
        "jump"
    }
}

fn static_protection_replacements(
    plan: &Value,
    site: &str,
    policy: &Value,
) -> Result<Vec<(Value, &'static str)>> {
    let bindings = object_get(plan, "scalarBindings")
        .and_then(Value::as_array)
        .map_or(&[] as &[Value], Vec::as_slice);
    static_protection_replacements_from_bindings(plan, bindings, site, policy)
}

/// Exact static v2 initial-protection vocabulary, parameterized by the
/// management-library-wide binding table.  The later-generation authority
/// enumerates against the compiled profile's library, while the narrow
/// unprofiled helper above retains the same implementation for local tests.
fn static_protection_replacements_from_bindings(
    plan: &Value,
    bindings: &[Value],
    site: &str,
    policy: &Value,
) -> Result<Vec<(Value, &'static str)>> {
    let key = if site == "stop" {
        "initialStop"
    } else {
        "initialTarget"
    };
    let current = required(plan, key, "management reference")?;
    let current_kind = object_get(current, "kind")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let mut output = Vec::new();
    if site == "stop" {
        let choices = policy_f64_rows(policy, "stopPercentChoices")?;
        for value in choices.iter().copied() {
            let replacement = object([
                ("kind", Value::String("fixed_percent".to_owned())),
                ("percent", value_number(value, "stop percent")?),
            ]);
            if replacement == *current {
                continue;
            }
            let class = if current_kind == "fixed_percent" {
                object_get(current, "percent")
                    .and_then(Value::as_f64)
                    .map(|prior| classify_scalar(prior, &choices, value))
                    .unwrap_or("jump")
            } else {
                "kind_switch"
            };
            output.push((replacement, class));
        }
    } else {
        let reward_choices = policy_f64_rows(policy, "rewardMultipleChoices")?;
        for value in reward_choices.iter().copied() {
            let replacement = object([
                ("kind", Value::String("reward_multiple".to_owned())),
                ("multiple", value_number(value, "reward multiple")?),
            ]);
            if replacement == *current {
                continue;
            }
            let class = if current_kind == "reward_multiple" {
                object_get(current, "multiple")
                    .and_then(Value::as_f64)
                    .map(|prior| classify_scalar(prior, &reward_choices, value))
                    .unwrap_or("jump")
            } else {
                "kind_switch"
            };
            output.push((replacement, class));
        }
        let target_choices = policy_f64_rows(policy, "targetPercentChoices")?;
        for value in target_choices.iter().copied() {
            let replacement = object([
                ("kind", Value::String("fixed_percent".to_owned())),
                ("percent", value_number(value, "target percent")?),
            ]);
            if replacement == *current {
                continue;
            }
            let class = if current_kind == "fixed_percent" {
                object_get(current, "percent")
                    .and_then(Value::as_f64)
                    .map(|prior| classify_scalar(prior, &target_choices, value))
                    .unwrap_or("jump")
            } else {
                "kind_switch"
            };
            output.push((replacement, class));
        }
        let none = object([("kind", Value::String("none".to_owned()))]);
        if none != *current {
            output.push((none, "kind_switch"));
        }
    }
    let distance_choices = policy_f64_rows(policy, "distanceMultipleChoices")?;
    for binding in bindings {
        let id = row_id(binding, "management scalar binding")?;
        match object_get(binding, "valueKind").and_then(Value::as_str) {
            Some("price_level") => {
                let replacement = object([
                    ("kind", Value::String("indicator_price_level".to_owned())),
                    ("bindingId", Value::String(id)),
                ]);
                if replacement != *current {
                    output.push((replacement, "kind_switch"));
                }
            }
            Some("price_distance") => {
                for value in distance_choices.iter().copied() {
                    let replacement = object([
                        (
                            "kind",
                            Value::String("indicator_distance_multiple".to_owned()),
                        ),
                        ("bindingId", Value::String(id.clone())),
                        ("multiple", value_number(value, "distance multiple")?),
                    ]);
                    if replacement == *current {
                        continue;
                    }
                    let class = if current_kind == "indicator_distance_multiple"
                        && object_get(current, "bindingId").and_then(Value::as_str)
                            == replacement.get("bindingId").and_then(Value::as_str)
                    {
                        object_get(current, "multiple")
                            .and_then(Value::as_f64)
                            .map(|prior| classify_scalar(prior, &distance_choices, value))
                            .unwrap_or("jump")
                    } else {
                        "kind_switch"
                    };
                    output.push((replacement, class));
                }
            }
            _ => return Err(invalid("management scalar binding value kind drifted")),
        }
    }
    Ok(output)
}

fn compiled_management_library(profile: &Value) -> Result<&Value> {
    let execution = required(profile, "executionConfig", "compiled evolvable profile")?;
    required(
        execution,
        "managementLibrary",
        "compiled execution configuration",
    )
}

fn compiled_management_plans(profile: &Value) -> Result<&[Value]> {
    array_ref(
        required(
            compiled_management_library(profile)?,
            "plans",
            "compiled management library",
        )?,
        "compiled management library plans",
    )
}

fn compiled_management_plan<'a>(profile: &'a Value, plan_id: &str) -> Result<&'a Value> {
    let matches = compiled_management_plans(profile)?
        .iter()
        .filter(|plan| object_get(plan, "id").and_then(Value::as_str) == Some(plan_id))
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return Err(invalid(
            "compiled initial-protection plan selector did not resolve exactly one management plan",
        ));
    }
    Ok(matches[0])
}

fn compiled_management_bindings(profile: &Value) -> Result<&[Value]> {
    match object_get(compiled_management_library(profile)?, "scalarBindings") {
        Some(value) => array_ref(value, "compiled management scalar bindings"),
        None => Ok(&[]),
    }
}

/// Recreate `enumerate_initial_protection_plans(profile, policy)` from the
/// compiled Rust profile.  This is deliberately profile-first: a genome
/// resource row is only the eventual storage location and must not decide
/// which native scalar locators exist.
fn compiled_static_initial_protection_plans(
    program: &Value,
    authority: &V5OperatorAuthority,
    profile: &V5CompiledProfileView,
) -> Result<Vec<Value>> {
    if profile.genome_program_sha256() != sha(program)? {
        return Err(invalid(
            "compiled profile does not bind the parent for initial-protection enumeration",
        ));
    }
    let management_refs = rows_by_id(
        &resource_rows(program, "managementRefs")?,
        "management reference",
    )?;
    let bindings = compiled_management_bindings(profile.profile())?;
    let mut plans = BTreeMap::<String, Value>::new();
    for source_plan in compiled_management_plans(profile.profile())? {
        let plan_id = row_id(source_plan, "compiled management plan")?;
        if !management_refs.contains_key(&plan_id) {
            continue;
        }
        for site in ["stop", "target"] {
            for (replacement, mutation_class) in static_protection_replacements_from_bindings(
                source_plan,
                bindings,
                site,
                authority.initial_protection_policy(),
            )? {
                let mut plan = object([
                    ("kind", Value::String("initial_protection".to_owned())),
                    ("planId", Value::String(plan_id.clone())),
                    ("site", Value::String(site.to_owned())),
                    ("replacement", replacement),
                    ("mutationClass", Value::String(mutation_class.to_owned())),
                ]);
                let plan_sha = sha(&plan)?;
                map_mut(&mut plan, "compiled initial protection plan")?
                    .insert("planSha256".to_owned(), Value::String(plan_sha.clone()));
                if plans.insert(plan_sha, plan).is_some() {
                    return Err(invalid(
                        "compiled initial-protection enumeration produced a duplicate plan identity",
                    ));
                }
            }
        }
    }
    Ok(plans.into_values().collect())
}

fn locator_binding_references_from_profile(profile: &Value) -> Result<BTreeSet<String>> {
    let mut references = BTreeSet::new();
    let add_locator = |locator: &Value, output: &mut BTreeSet<String>| {
        if matches!(
            object_get(locator, "kind").and_then(Value::as_str),
            Some("indicator_price_level") | Some("indicator_distance_multiple")
        ) {
            if let Some(binding_id) = object_get(locator, "bindingId").and_then(Value::as_str) {
                output.insert(binding_id.to_owned());
            }
        }
    };
    for plan in compiled_management_plans(profile)? {
        for key in ["initialStop", "initialTarget"] {
            add_locator(
                required(plan, key, "compiled management plan")?,
                &mut references,
            );
        }
        if let Some(trailing) = object_get(plan, "trailingStop") {
            for key in ["anchor", "distance"] {
                add_locator(
                    required(trailing, key, "compiled trailing stop")?,
                    &mut references,
                );
            }
        }
    }
    let graph = required(profile, "graph", "compiled evolvable profile")?;
    let transitions = array_ref(
        required(graph, "transitions", "compiled profile graph")?,
        "compiled profile transitions",
    )?;
    for transition in transitions {
        let actions = match object_get(transition, "actions") {
            Some(value) => array_ref(value, "compiled transition actions")?,
            None => continue,
        };
        for action in actions {
            if object_get(action, "kind").and_then(Value::as_str) == Some("set_target_next_open") {
                if let Some(locator) = object_get(action, "targetLocator") {
                    add_locator(locator, &mut references);
                }
            }
        }
    }
    Ok(references)
}

/// The exact subset of v3 reachability which gates scalar construction.
/// Native compile/admission has already checked the graph; this repeats the
/// v3 plan/binding closure rather than accepting an opaque successful report.
fn compiled_v3_referenced_management_plan_ids(profile: &Value) -> Result<Vec<String>> {
    let library = compiled_management_library(profile)?;
    let plans = compiled_management_plans(profile)?;
    if !(1..=16).contains(&plans.len()) {
        return Err(invalid(
            "compiled profile management-plan count is outside v3 construction bounds",
        ));
    }
    let mut known = BTreeSet::new();
    for plan in plans {
        let id = row_id(plan, "compiled management plan")?;
        if !known.insert(id) {
            return Err(invalid(
                "compiled profile has duplicate management plan IDs",
            ));
        }
    }
    let default_plan_id = text(
        required(library, "defaultPlanId", "compiled management library")?,
        "compiled default management plan ID",
    )?;
    if !known.contains(&default_plan_id) {
        return Err(invalid(
            "compiled profile default management plan is unknown",
        ));
    }
    let graph = required(profile, "graph", "compiled evolvable profile")?;
    let transitions = array_ref(
        required(graph, "transitions", "compiled profile graph")?,
        "compiled profile transitions",
    )?;
    let mut referenced = BTreeSet::from([default_plan_id.clone()]);
    let mut entry_count = 0_usize;
    for transition in transitions {
        let actions = match object_get(transition, "actions") {
            Some(value) => array_ref(value, "compiled transition actions")?,
            None => continue,
        };
        for action in actions {
            if object_get(action, "kind").and_then(Value::as_str) != Some("enter_next_open") {
                continue;
            }
            entry_count += 1;
            let selected = match object_get(action, "managementPlanId") {
                Some(value) => text(value, "entry management plan ID")?,
                None => default_plan_id.clone(),
            };
            if !known.contains(&selected) {
                return Err(invalid(
                    "compiled profile entry route names an unknown management plan",
                ));
            }
            referenced.insert(selected);
        }
    }
    if entry_count == 0 || referenced != known {
        return Err(invalid(
            "compiled profile does not meet v3 management-plan reachability",
        ));
    }
    let bindings = compiled_management_bindings(profile)?;
    if bindings.len() > 32 {
        return Err(invalid(
            "compiled profile scalar-binding count exceeds v3 construction cap",
        ));
    }
    let mut ids = BTreeSet::new();
    let mut sources = BTreeSet::new();
    for binding in bindings {
        let id = row_id(binding, "compiled scalar binding")?;
        let indicator = text(
            required(binding, "indicatorInstanceId", "compiled scalar binding")?,
            "compiled scalar indicator instance",
        )?;
        let output = text(
            required(binding, "outputKey", "compiled scalar binding")?,
            "compiled scalar output key",
        )?;
        let value_kind = text(
            required(binding, "valueKind", "compiled scalar binding")?,
            "compiled scalar value kind",
        )?;
        if !matches!(value_kind.as_str(), "price_level" | "price_distance")
            || object_get(binding, "availability").and_then(Value::as_str) != Some("completed_bar")
            || !ids.insert(id)
            || !sources.insert((indicator, output))
        {
            return Err(invalid(
                "compiled profile scalar-binding v3 closure is invalid",
            ));
        }
    }
    if ids != locator_binding_references_from_profile(profile)? {
        return Err(invalid(
            "compiled profile has orphaned or missing scalar bindings for v3 construction",
        ));
    }
    Ok(referenced.into_iter().collect())
}

fn compiled_dynamic_scalar_authorizations(
    authority: &V5OperatorAuthority,
    profile: &V5CompiledProfileView,
) -> Result<Vec<Value>> {
    let bindings = compiled_management_bindings(profile.profile())?;
    let existing_sources = bindings
        .iter()
        .map(|binding| {
            Ok((
                text(
                    required(binding, "indicatorInstanceId", "compiled scalar binding")?,
                    "compiled scalar indicator instance",
                )?,
                text(
                    required(binding, "outputKey", "compiled scalar binding")?,
                    "compiled scalar output key",
                )?,
            ))
        })
        .collect::<Result<BTreeSet<_>>>()?;
    let existing_ids = bindings
        .iter()
        .map(|binding| row_id(binding, "compiled scalar binding"))
        .collect::<Result<BTreeSet<_>>>()?;
    let indicators = array_ref(
        required(
            profile.profile(),
            "indicators",
            "compiled evolvable profile",
        )?,
        "compiled profile indicators",
    )?;
    let mut authorized = BTreeMap::<(String, String, String), Value>::new();
    for indicator in indicators {
        let meta = required(indicator, "meta", "compiled profile indicator")?;
        let config = required(indicator, "config", "compiled profile indicator")?;
        if object_get(config, "isActive") != Some(&Value::Bool(true))
            || object_get(config, "useFormingBar") != Some(&Value::Bool(false))
        {
            continue;
        }
        let indicator_id = text(
            required(meta, "id", "compiled profile indicator meta")?,
            "compiled profile indicator ID",
        )?;
        let instance_id = text(
            required(meta, "instanceId", "compiled profile indicator meta")?,
            "compiled profile indicator instance ID",
        )?;
        let Some(authored_outputs) = scalar_outputs(meta)? else {
            continue;
        };
        let authored = authored_outputs
            .iter()
            .map(|output| {
                Ok((
                    text(
                        required(output, "outputKey", "compiled scalar output")?,
                        "compiled scalar output key",
                    )?,
                    text(
                        required(output, "valueKind", "compiled scalar output")?,
                        "compiled scalar output kind",
                    )?,
                    text(
                        required(output, "unit", "compiled scalar output")?,
                        "compiled scalar output unit",
                    )?,
                ))
            })
            .collect::<Result<BTreeSet<_>>>()?;
        let catalog = authority
            .catalog_entries()
            .get(&indicator_id)
            .ok_or_else(|| invalid("compiled profile indicator is absent from sealed catalog"))?;
        let catalog_meta = required(catalog, "meta", "sealed catalog indicator")?;
        let Some(catalog_outputs) = scalar_outputs(catalog_meta)? else {
            continue;
        };
        for output in catalog_outputs {
            let output_key = text(
                required(&output, "outputKey", "catalog scalar output")?,
                "catalog scalar output key",
            )?;
            let value_kind = text(
                required(&output, "valueKind", "catalog scalar output")?,
                "catalog scalar output kind",
            )?;
            let unit = text(
                required(&output, "unit", "catalog scalar output")?,
                "catalog scalar output unit",
            )?;
            if !authored.contains(&(output_key.clone(), value_kind.clone(), unit))
                || existing_sources.contains(&(instance_id.clone(), output_key.clone()))
            {
                continue;
            }
            let binding_id = format!("scalar_{instance_id}_{output_key}");
            if existing_ids.contains(&binding_id) {
                continue;
            }
            authorized.insert(
                (instance_id.clone(), output_key.clone(), value_kind.clone()),
                object([
                    ("indicatorId", Value::String(indicator_id.clone())),
                    ("indicatorInstanceId", Value::String(instance_id.clone())),
                    ("outputKey", Value::String(output_key)),
                    ("valueKind", Value::String(value_kind)),
                    ("bindingId", Value::String(binding_id)),
                ]),
            );
        }
    }
    Ok(authorized.into_values().collect())
}

fn dynamic_locator_sites(source_plan: &Value, value_kind: &str) -> Result<Vec<Value>> {
    let mut sites = vec![
        object([
            ("path", array([Value::String("initialStop".to_owned())])),
            (
                "multiple",
                value_number(1.0, "dynamic initial stop multiple")?,
            ),
            ("site", Value::String("initial_stop".to_owned())),
        ]),
        object([
            ("path", array([Value::String("initialTarget".to_owned())])),
            (
                "multiple",
                value_number(2.0, "dynamic initial target multiple")?,
            ),
            ("site", Value::String("initial_target".to_owned())),
        ]),
    ];
    let has_trailing = object_get(source_plan, "trailingStop").is_some();
    let (site, path) = match (value_kind, has_trailing) {
        ("price_level", true) => (
            "trailing_anchor",
            array([
                Value::String("trailingStop".to_owned()),
                Value::String("anchor".to_owned()),
            ]),
        ),
        ("price_level", false) => (
            "trailing_anchor_create",
            array([Value::String("trailingStop".to_owned())]),
        ),
        ("price_distance", true) => (
            "trailing_distance",
            array([
                Value::String("trailingStop".to_owned()),
                Value::String("distance".to_owned()),
            ]),
        ),
        ("price_distance", false) => (
            "trailing_distance_create",
            array([Value::String("trailingStop".to_owned())]),
        ),
        _ => return Err(invalid("dynamic scalar value kind is not supported")),
    };
    sites.push(object([
        ("path", path),
        ("multiple", value_number(1.0, "dynamic trailing multiple")?),
        ("site", Value::String(site.to_owned())),
    ]));
    Ok(sites)
}

fn scalar_dynamic_operator_specification(
    authority: &V5OperatorAuthority,
) -> Result<(String, String)> {
    let catalog_sha256 = sha(authority.catalog.as_ref())?;
    let mut specification = object([
        (
            "schemaVersion",
            Value::String(CONSTRUCTION_OPERATOR_SPEC_SCHEMA.to_owned()),
        ),
        (
            "operatorId",
            Value::String(SCALAR_DYNAMIC_MANAGEMENT_OPERATOR_ID.to_owned()),
        ),
        (
            "operatorVersion",
            Value::String(SCALAR_DYNAMIC_MANAGEMENT_OPERATOR_VERSION.to_owned()),
        ),
        (
            "generatorVersion",
            Value::String(CONSTRUCTION_GENERATOR_VERSION.to_owned()),
        ),
        ("catalogSha256", Value::String(catalog_sha256.clone())),
    ]);
    let operator_spec_sha256 = sha(&specification)?;
    map_mut(&mut specification, "scalar dynamic operator specification")?.insert(
        "operatorSpecSha256".to_owned(),
        Value::String(operator_spec_sha256.clone()),
    );
    Ok((catalog_sha256, operator_spec_sha256))
}

fn finalize_structural_plan(mut plan: Value) -> Result<Value> {
    map_mut(&mut plan, "legacy structural operator plan")?.insert(
        "schemaVersion".to_owned(),
        Value::String(STRUCTURAL_OPERATOR_PLAN_SCHEMA.to_owned()),
    );
    let plan_sha256 = sha(&plan)?;
    map_mut(&mut plan, "legacy structural operator plan")?
        .insert("planSha256".to_owned(), Value::String(plan_sha256));
    Ok(plan)
}

fn compiled_dynamic_initial_protection_plans(
    program: &Value,
    authority: &V5OperatorAuthority,
    profile: &V5CompiledProfileView,
) -> Result<Vec<Value>> {
    if profile.genome_program_sha256() != sha(program)? {
        return Err(invalid(
            "compiled profile does not bind the parent for dynamic initial-protection enumeration",
        ));
    }
    let referenced = match compiled_v3_referenced_management_plan_ids(profile.profile()) {
        Ok(value) => value,
        // Python's construction operator simply emits no candidates if its
        // v3 reachability inspection is not acceptable.  Preserve that safe
        // empty vocabulary rather than inventing a partial dynamic mutation.
        Err(_) => return Ok(Vec::new()),
    };
    if compiled_management_bindings(profile.profile())?.len() >= 32 {
        return Ok(Vec::new());
    }
    let management_refs = rows_by_id(
        &resource_rows(program, "managementRefs")?,
        "management reference",
    )?;
    let (catalog_sha256, operator_spec_sha256) = scalar_dynamic_operator_specification(authority)?;
    let parent_source_profile_sha256 = profile.raw_profile_sha256().to_owned();
    let mut plans = BTreeMap::<String, Value>::new();
    for scalar in compiled_dynamic_scalar_authorizations(authority, profile)? {
        let value_kind = ctext(&scalar, "valueKind")?;
        for plan_id in &referenced {
            if !management_refs.contains_key(plan_id) {
                continue;
            }
            let source_plan = compiled_management_plan(profile.profile(), plan_id)?;
            for site in dynamic_locator_sites(source_plan, &value_kind)? {
                let site_kind = ctext(&site, "site")?;
                if !matches!(site_kind.as_str(), "initial_stop" | "initial_target") {
                    continue;
                }
                let construction = object([
                    (
                        "kind",
                        Value::String("scalar_dynamic_management".to_owned()),
                    ),
                    ("scalar", canonical_clone(&scalar)?),
                    ("planId", Value::String(plan_id.clone())),
                    ("site", Value::String(site_kind)),
                    (
                        "locatorPath",
                        canonical_clone(required(&site, "path", "dynamic locator site")?)?,
                    ),
                    (
                        "multiple",
                        canonical_clone(required(&site, "multiple", "dynamic locator site")?)?,
                    ),
                ]);
                let identity = object([
                    (
                        "schemaVersion",
                        Value::String(CONSTRUCTION_IDENTITY_SCHEMA.to_owned()),
                    ),
                    (
                        "generatorVersion",
                        Value::String(CONSTRUCTION_GENERATOR_VERSION.to_owned()),
                    ),
                    (
                        "operatorId",
                        Value::String(SCALAR_DYNAMIC_MANAGEMENT_OPERATOR_ID.to_owned()),
                    ),
                    (
                        "operatorVersion",
                        Value::String(SCALAR_DYNAMIC_MANAGEMENT_OPERATOR_VERSION.to_owned()),
                    ),
                    (
                        "parentSourceProfileSha256",
                        Value::String(parent_source_profile_sha256.clone()),
                    ),
                    ("catalogSha256", Value::String(catalog_sha256.clone())),
                    ("construction", canonical_clone(&construction)?),
                ]);
                let construction_plan = finalize_structural_plan(object([
                    (
                        "operatorId",
                        Value::String(SCALAR_DYNAMIC_MANAGEMENT_OPERATOR_ID.to_owned()),
                    ),
                    (
                        "operatorVersion",
                        Value::String(SCALAR_DYNAMIC_MANAGEMENT_OPERATOR_VERSION.to_owned()),
                    ),
                    (
                        "operatorSpecSha256",
                        Value::String(operator_spec_sha256.clone()),
                    ),
                    (
                        "parentSourceProfileSha256",
                        Value::String(parent_source_profile_sha256.clone()),
                    ),
                    ("catalogSha256", Value::String(catalog_sha256.clone())),
                    ("construction", construction),
                    ("constructionIdentitySha256", Value::String(sha(&identity)?)),
                ]))?;
                let mut wrapped = object([
                    ("kind", Value::String("dynamic_construction".to_owned())),
                    ("constructionPlan", construction_plan),
                    ("mutationClass", Value::String("kind_switch".to_owned())),
                ]);
                let wrapped_sha256 = sha(&wrapped)?;
                map_mut(&mut wrapped, "dynamic initial-protection wrapper")?.insert(
                    "planSha256".to_owned(),
                    Value::String(wrapped_sha256.clone()),
                );
                if plans.insert(wrapped_sha256, wrapped).is_some() {
                    return Err(invalid(
                        "dynamic initial-protection enumeration produced a duplicate plan identity",
                    ));
                }
            }
        }
    }
    Ok(plans.into_values().collect())
}

fn dynamic_locator_from_construction(construction: &Value) -> Result<(String, Value, Value)> {
    if object_get(construction, "kind").and_then(Value::as_str) != Some("scalar_dynamic_management")
    {
        return Err(invalid("dynamic construction kind is invalid"));
    }
    let plan_id = ctext(construction, "planId")?;
    let site = ctext(construction, "site")?;
    let expected_key = match site.as_str() {
        "initial_stop" => "initialStop",
        "initial_target" => "initialTarget",
        _ => {
            return Err(invalid(
                "dynamic construction is outside initial protection",
            ));
        }
    };
    let path = array_ref(
        required(construction, "locatorPath", "dynamic construction")?,
        "dynamic locator path",
    )?;
    if path.len() != 1 || path[0].as_str() != Some(expected_key) {
        return Err(invalid(
            "dynamic initial-protection construction has a foreign locator path",
        ));
    }
    let scalar = required(construction, "scalar", "dynamic construction")?;
    let binding_id = ctext(scalar, "bindingId")?;
    let value_kind = ctext(scalar, "valueKind")?;
    let multiple = finite_number(
        required(construction, "multiple", "dynamic construction")?,
        "dynamic scalar multiple",
    )?;
    if multiple <= 0.0 {
        return Err(invalid("dynamic scalar multiple is not positive"));
    }
    let locator = match value_kind.as_str() {
        "price_level" => object([
            ("kind", Value::String("indicator_price_level".to_owned())),
            ("bindingId", Value::String(binding_id)),
        ]),
        "price_distance" => object([
            (
                "kind",
                Value::String("indicator_distance_multiple".to_owned()),
            ),
            ("bindingId", Value::String(binding_id)),
            (
                "multiple",
                value_number(multiple, "dynamic scalar multiple")?,
            ),
        ]),
        _ => return Err(invalid("dynamic scalar value kind is invalid")),
    };
    Ok((plan_id, Value::String(expected_key.to_owned()), locator))
}

fn authorized_dynamic_scalars(program: &Value, plan: &Value) -> Result<Vec<Value>> {
    let existing = object_get(plan, "scalarBindings")
        .and_then(Value::as_array)
        .map_or(BTreeSet::new(), |rows| {
            rows.iter()
                .filter_map(|row| {
                    Some((
                        object_get(row, "indicatorInstanceId")?.as_str()?.to_owned(),
                        object_get(row, "outputKey")?.as_str()?.to_owned(),
                    ))
                })
                .collect()
        });
    let existing_ids = object_get(plan, "scalarBindings")
        .and_then(Value::as_array)
        .map_or(BTreeSet::new(), |rows| {
            rows.iter()
                .filter_map(|row| {
                    object_get(row, "id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .collect()
        });
    let mut output = Vec::new();
    for indicator in resource_rows(program, "indicators")? {
        let meta = required(&indicator, "meta", "indicator")?;
        let config = required(&indicator, "config", "indicator")?;
        if object_get(config, "isActive") != Some(&Value::Bool(true))
            || object_get(config, "useFormingBar") != Some(&Value::Bool(false))
        {
            continue;
        }
        let instance_id = indicator_id(&indicator)?;
        let indicator_key = text(
            required(meta, "id", "indicator meta")?,
            "catalog indicator ID",
        )?;
        let Some(rows) = object_get(meta, "managementScalarOutputs").and_then(Value::as_array)
        else {
            continue;
        };
        for row in rows {
            let output_key = text(
                required(row, "outputKey", "scalar output")?,
                "scalar output key",
            )?;
            let value_kind = text(
                required(row, "valueKind", "scalar output")?,
                "scalar output kind",
            )?;
            let unit = text(
                required(row, "unit", "scalar output")?,
                "scalar output unit",
            )?;
            if !matches!(value_kind.as_str(), "price_level" | "price_distance")
                || (value_kind == "price_level" && unit != "price")
                || (value_kind == "price_distance" && unit != "price_distance")
                || existing.contains(&(instance_id.clone(), output_key.clone()))
            {
                continue;
            }
            let binding_id = format!("scalar_{instance_id}_{output_key}");
            if existing_ids.contains(&binding_id) {
                continue;
            }
            output.push(object([
                ("indicatorId", Value::String(indicator_key.clone())),
                ("indicatorInstanceId", Value::String(instance_id.clone())),
                ("outputKey", Value::String(output_key)),
                ("valueKind", Value::String(value_kind)),
                ("bindingId", Value::String(binding_id)),
            ]));
        }
    }
    sorted_values(output)
}

fn initial_protection_constructions(
    program: &Value,
    authority: &V5OperatorAuthority,
) -> Result<Vec<Value>> {
    let mut output = Vec::new();
    for plan in resource_rows(program, "managementRefs")? {
        let plan_id = row_id(&plan, "management reference")?;
        for site in ["stop", "target"] {
            for (replacement, class) in
                static_protection_replacements(&plan, site, authority.initial_protection_policy())?
            {
                output.push(object([
                    ("kind", Value::String("initial_protection".to_owned())),
                    ("planId", Value::String(plan_id.clone())),
                    ("site", Value::String(site.to_owned())),
                    ("replacement", replacement),
                    ("mutationClass", Value::String(class.to_owned())),
                ]));
            }
        }
    }
    sorted_values(output)
}

fn initial_protection_transform(
    program: &Value,
    authority: &V5OperatorAuthority,
    construction: &Value,
) -> Result<(Value, Value)> {
    let kind = object_get(construction, "kind")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if kind == "initial_protection" {
        let plan_id = ctext(construction, "planId")?;
        let site = ctext(construction, "site")?;
        if !matches!(site.as_str(), "stop" | "target") {
            return Err(invalid("initial protection site is invalid"));
        }
        let key = if site == "stop" {
            "initialStop"
        } else {
            "initialTarget"
        };
        let replacement = canonical_clone(required(
            construction,
            "replacement",
            "initial protection construction",
        )?)?;
        let mut rows = resource_rows(program, "managementRefs")?;
        let mut removed = Vec::new();
        let mut found = false;
        let mut before = Value::Null;
        for row in rows.iter_mut() {
            if row_id(row, "management reference")? != plan_id {
                continue;
            }
            before = canonical_clone(required(row, key, "management reference")?)?;
            map_mut(row, "management reference")?.insert(key.to_owned(), replacement.clone());
            removed = remove_orphan_scalar_bindings(row)?;
            found = true;
        }
        if !found {
            return Err(invalid("initial protection plan disappeared"));
        }
        let child = replace_resource_rows(program, None, None, None, Some(rows))?;
        let trace = array([object([
            ("operation", Value::String("initial_protection".to_owned())),
            ("managementPlanId", Value::String(plan_id)),
            ("site", Value::String(site)),
            (
                "mutationClass",
                canonical_clone(required(
                    construction,
                    "mutationClass",
                    "initial protection construction",
                )?)?,
            ),
            ("before", before),
            ("after", replacement),
            ("removedUnreferencedScalarBindings", array(removed)),
        ])]);
        return Ok((child, trace));
    }
    if kind != "dynamic_construction" {
        return Err(invalid("unsupported initial protection construction"));
    }
    // `ScalarDynamicManagementConstructionOperator` is itself a structural
    // plan.  The Python initial-protection wrapper stores that complete plan
    // under `constructionPlan`; accepting only the nested construction would
    // erase its catalog/profile identity and let a forged scalar locator
    // bypass the profile-derived vocabulary.
    let wrapped = legacy_initial_protection_plan(construction)?;
    let raw_plan = required(&wrapped, "constructionPlan", "dynamic construction")?;
    let raw_fields = map_ref(raw_plan, "dynamic construction plan")?;
    let raw_expected = [
        "schemaVersion",
        "operatorId",
        "operatorVersion",
        "operatorSpecSha256",
        "parentSourceProfileSha256",
        "catalogSha256",
        "construction",
        "constructionIdentitySha256",
        "planSha256",
    ];
    if raw_fields.len() != raw_expected.len()
        || raw_expected
            .iter()
            .any(|key| !raw_fields.contains_key(*key))
        || object_get(raw_plan, "schemaVersion").and_then(Value::as_str)
            != Some(STRUCTURAL_OPERATOR_PLAN_SCHEMA)
        || object_get(raw_plan, "operatorId").and_then(Value::as_str)
            != Some(SCALAR_DYNAMIC_MANAGEMENT_OPERATOR_ID)
        || object_get(raw_plan, "operatorVersion").and_then(Value::as_str)
            != Some(SCALAR_DYNAMIC_MANAGEMENT_OPERATOR_VERSION)
    {
        return Err(invalid(
            "dynamic initial construction plan shape is incompatible",
        ));
    }
    let supplied_raw_plan_sha256 = sha256_identifier(
        required(raw_plan, "planSha256", "dynamic construction plan")?,
        "dynamic construction plan SHA-256",
    )?;
    let mut raw_body = canonical_clone(raw_plan)?;
    map_mut(&mut raw_body, "dynamic construction plan")?.remove("planSha256");
    if supplied_raw_plan_sha256 != sha(&raw_body)? {
        return Err(invalid(
            "dynamic initial construction plan identity drifted",
        ));
    }
    let (catalog_sha256, operator_spec_sha256) = scalar_dynamic_operator_specification(authority)?;
    if object_get(raw_plan, "catalogSha256").and_then(Value::as_str)
        != Some(catalog_sha256.as_str())
        || object_get(raw_plan, "operatorSpecSha256").and_then(Value::as_str)
            != Some(operator_spec_sha256.as_str())
    {
        return Err(invalid(
            "dynamic initial construction plan is outside the sealed scalar authority",
        ));
    }
    let raw = required(raw_plan, "construction", "dynamic construction plan")?;
    if object_get(raw, "kind").and_then(Value::as_str) != Some("scalar_dynamic_management") {
        return Err(invalid("dynamic initial construction kind is invalid"));
    }
    let plan_id = ctext(raw, "planId")?;
    let site = ctext(raw, "site")?;
    let key = match site.as_str() {
        "initial_stop" => "initialStop",
        "initial_target" => "initialTarget",
        _ => {
            return Err(invalid(
                "dynamic initial construction site is outside initial protection",
            ));
        }
    };
    let scalar = required(raw, "scalar", "dynamic construction")?;
    let binding_id = ctext(scalar, "bindingId")?;
    let instance_id = ctext(scalar, "indicatorInstanceId")?;
    let output_key = ctext(scalar, "outputKey")?;
    let value_kind = ctext(scalar, "valueKind")?;
    let multiple = finite_number(
        required(raw, "multiple", "dynamic construction")?,
        "dynamic scalar multiple",
    )?;
    if !matches!(value_kind.as_str(), "price_level" | "price_distance") || multiple <= 0.0 {
        return Err(invalid("dynamic scalar capability is invalid"));
    }
    let binding = object([
        ("id", Value::String(binding_id.clone())),
        ("indicatorInstanceId", Value::String(instance_id)),
        ("outputKey", Value::String(output_key)),
        ("valueKind", Value::String(value_kind.clone())),
        ("availability", Value::String("completed_bar".to_owned())),
    ]);
    let locator = if value_kind == "price_level" {
        object([
            ("kind", Value::String("indicator_price_level".to_owned())),
            ("bindingId", Value::String(binding_id.clone())),
        ])
    } else {
        object([
            (
                "kind",
                Value::String("indicator_distance_multiple".to_owned()),
            ),
            ("bindingId", Value::String(binding_id.clone())),
            (
                "multiple",
                value_number(multiple, "dynamic scalar multiple")?,
            ),
        ])
    };
    let mut rows = resource_rows(program, "managementRefs")?;
    let mut found = false;
    let mut before = Value::Null;
    for row in rows.iter_mut() {
        if row_id(row, "management reference")? != plan_id {
            continue;
        }
        before = canonical_clone(required(row, key, "management reference")?)?;
        let fields = map_mut(row, "management reference")?;
        let bindings = fields
            .entry("scalarBindings".to_owned())
            .or_insert_with(|| Value::Array(Vec::new()))
            .as_array_mut()
            .ok_or_else(|| invalid("management scalar bindings drifted"))?;
        if bindings.iter().any(|item| {
            row_id(item, "management scalar binding").ok().as_deref() == Some(binding_id.as_str())
        }) {
            return Err(invalid("dynamic scalar binding ID already exists"));
        }
        bindings.push(binding.clone());
        bindings.sort_by(|left, right| {
            row_id(left, "management scalar binding")
                .unwrap_or_default()
                .cmp(&row_id(right, "management scalar binding").unwrap_or_default())
        });
        fields.insert(key.to_owned(), locator.clone());
        found = true;
    }
    if !found {
        return Err(invalid("dynamic initial protection plan disappeared"));
    }
    let child = replace_resource_rows(program, None, None, None, Some(rows))?;
    let trace = array([object([
        (
            "operation",
            Value::String("dynamic_initial_protection".to_owned()),
        ),
        ("managementPlanId", Value::String(plan_id)),
        ("site", Value::String(site)),
        ("before", before),
        ("after", locator),
        ("binding", binding),
    ])]);
    Ok((child, trace))
}

fn topology_sealed_facts(authority: &V5OperatorAuthority) -> Result<Value> {
    Ok(object([
        (
            "schemaVersion",
            Value::String(v5_topology_operators::V5_TOPOLOGY_SEALED_FACTS_SCHEMA.to_owned()),
        ),
        (
            "authoritySha256",
            Value::String(authority.authority_sha256().to_owned()),
        ),
        ("side", Value::String(authority.side().to_owned())),
        ("budget", canonical_clone(authority.budget())?),
    ]))
}

fn topology_transform(
    parent: &Value,
    authority: &V5OperatorAuthority,
    construction: &Value,
) -> Result<(Value, Value)> {
    let sealed_facts = topology_sealed_facts(authority)?;
    let applied = v5_topology_operators::apply_plan(parent, &sealed_facts, construction)
        .map_err(|error| invalid(error.to_string()))?;
    Ok((applied.child_program, applied.trace))
}

fn topology_constructions(program: &Value, authority: &V5OperatorAuthority) -> Result<Vec<Value>> {
    let sealed_facts = topology_sealed_facts(authority)?;
    v5_topology_operators::enumerate_plans(program, &sealed_facts)
        .map_err(|error| invalid(error.to_string()))
}

type CrossoverUse = (String, String);

fn crossover_ports(program: &Value) -> Result<BTreeMap<String, Vec<String>>> {
    let nodes = rows_by_id(&node_rows(program)?, "node")?;
    let edges = rows_by_id(&edge_rows(program)?, "edge")?;
    let mut ports = BTreeMap::<String, Vec<String>>::new();
    for (edge_id, edge) in &edges {
        let target_id = text(
            required(edge, "target", "crossover edge")?,
            "crossover target",
        )?;
        let target = nodes
            .get(&target_id)
            .ok_or_else(|| invalid("crossover edge target disappeared"))?;
        match object_get(target, "zone").and_then(Value::as_str) {
            Some("management") => ports
                .entry("management_hub".to_owned())
                .or_default()
                .push(edge_id.clone()),
            Some("exit") => ports
                .entry("exit_hub".to_owned())
                .or_default()
                .push(edge_id.clone()),
            Some("setup") => {
                let incoming = edges
                    .values()
                    .filter(|item| {
                        object_get(item, "target").and_then(Value::as_str)
                            == Some(target_id.as_str())
                    })
                    .collect::<Vec<_>>();
                let outgoing = edges
                    .values()
                    .filter(|item| {
                        object_get(item, "source").and_then(Value::as_str)
                            == Some(target_id.as_str())
                    })
                    .collect::<Vec<_>>();
                if incoming.len() != 1 || outgoing.len() != 1 {
                    continue;
                }
                let incoming_source = text(
                    required(incoming[0], "source", "setup crossover inbound")?,
                    "setup crossover inbound source",
                )?;
                let outgoing_target = text(
                    required(outgoing[0], "target", "setup crossover continuation")?,
                    "setup crossover continuation target",
                )?;
                let source_zone = nodes
                    .get(&incoming_source)
                    .and_then(|node| object_get(node, "zone"))
                    .and_then(Value::as_str);
                let continuation_zone = nodes
                    .get(&outgoing_target)
                    .and_then(|node| object_get(node, "zone"))
                    .and_then(Value::as_str);
                if matches!(source_zone, Some("entry") | Some("setup"))
                    && matches!(continuation_zone, Some("entry") | Some("setup"))
                {
                    ports
                        .entry("entry_setup".to_owned())
                        .or_default()
                        .push(edge_id.clone());
                }
            }
            _ => {}
        }
    }
    for values in ports.values_mut() {
        values.sort();
        values.dedup();
    }
    Ok(ports)
}

fn guard_crossover_uses(guard: &Value, output: &mut BTreeSet<CrossoverUse>) -> Result<()> {
    // The native graph permits `{}` as the canonical unconditional guard.
    // It contributes no resource dependency just like Python's empty mapping
    // in `_guard_resource_uses`.
    if map_ref(guard, "crossover guard")?.is_empty() {
        return Ok(());
    }
    let kind = object_get(guard, "kind")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("crossover guard lacks kind"))?;
    match kind {
        "all" | "any" => {
            for child in array_ref(
                required(guard, "guards", "compound guard")?,
                "compound guards",
            )? {
                guard_crossover_uses(child, output)?;
            }
        }
        "not" => guard_crossover_uses(required(guard, "guard", "not guard")?, output)?,
        "predicate_edge" | "consecutive_true" => {
            guard_crossover_uses(required(guard, "predicate", "predicate guard")?, output)?;
        }
        "evidence_at_least" | "evidence_below" | "condition_streak_at_least" => {
            output.insert((
                "evidence_group".to_owned(),
                text(
                    required(guard, "groupId", "evidence guard")?,
                    "evidence group ID",
                )?,
            ));
        }
        "fresh_event" | "event_age_window" | "event_age_at_most" => {
            output.insert((
                "event".to_owned(),
                text(required(guard, "eventId", "event guard")?, "event ID")?,
            ));
        }
        // All remaining native guard variants carry only scalar/runtime state
        // and therefore add no catalog resource closure.
        "always"
        | "position_exists"
        | "state_age_at_least"
        | "state_age_at_most"
        | "position_age_at_least"
        | "utc_time_window"
        | "action_cooldown_elapsed"
        | "unrealized_r_at_least"
        | "unrealized_r_at_most"
        | "execution_status_is"
        | "execution_reason_is" => {}
        _ => return Err(invalid("crossover saw an unsupported guard kind")),
    }
    Ok(())
}

fn add_node_crossover_uses(node: &Value, output: &mut BTreeSet<CrossoverUse>) -> Result<()> {
    for use_row in array_ref(
        required(node, "resources", "crossover node")?,
        "crossover resources",
    )? {
        output.insert((
            text(
                required(use_row, "kind", "crossover resource")?,
                "crossover resource kind",
            )?,
            text(
                required(use_row, "id", "crossover resource")?,
                "crossover resource ID",
            )?,
        ));
    }
    guard_crossover_uses(required(node, "guard", "crossover node")?, output)
}

fn crossover_resource_ids(program: &Value) -> Result<BTreeMap<String, BTreeSet<String>>> {
    let mut output = BTreeMap::new();
    for (field, kind) in [
        ("indicators", "indicator"),
        ("evidenceGroups", "evidence_group"),
        ("events", "event"),
        ("managementRefs", "management_ref"),
    ] {
        let ids = resource_rows(program, field)?
            .iter()
            .map(|row| {
                if kind == "indicator" {
                    indicator_id(row)
                } else {
                    row_id(row, kind)
                }
            })
            .collect::<Result<BTreeSet<_>>>()?;
        output.insert(kind.to_owned(), ids);
    }
    Ok(output)
}

fn crossover_motif_resource_closure(
    donor: &Value,
    donor_edge_id: &str,
) -> Result<BTreeSet<CrossoverUse>> {
    let nodes = rows_by_id(&node_rows(donor)?, "node")?;
    let edges = rows_by_id(&edge_rows(donor)?, "edge")?;
    let edge = edges
        .get(donor_edge_id)
        .ok_or_else(|| invalid("crossover donor motif does not exist"))?;
    let target_id = text(
        required(edge, "target", "crossover donor edge")?,
        "donor target",
    )?;
    let target = nodes
        .get(&target_id)
        .ok_or_else(|| invalid("crossover donor target disappeared"))?;
    let mut uses = BTreeSet::new();
    add_node_crossover_uses(target, &mut uses)?;
    guard_crossover_uses(required(edge, "guard", "crossover donor edge")?, &mut uses)?;
    if object_get(target, "zone").and_then(Value::as_str) == Some("setup") {
        let outgoing = edges
            .values()
            .filter(|item| {
                object_get(item, "source").and_then(Value::as_str) == Some(target_id.as_str())
            })
            .collect::<Vec<_>>();
        if outgoing.len() != 1 {
            return Err(invalid("entry/setup donor motif is not linear"));
        }
        guard_crossover_uses(
            required(outgoing[0], "guard", "crossover donor continuation")?,
            &mut uses,
        )?;
    }
    let groups = rows_by_id(&resource_rows(donor, "evidenceGroups")?, "evidence group")?;
    let events = rows_by_id(&resource_rows(donor, "events")?, "event")?;
    let mut expanded = uses.clone();
    for (kind, id) in uses {
        if kind == "evidence_group" {
            let group = groups
                .get(&id)
                .ok_or_else(|| invalid("crossover donor group disappeared"))?;
            for member in members(group)? {
                expanded.insert(("indicator".to_owned(), member));
            }
        } else if kind == "event" {
            let event = events
                .get(&id)
                .ok_or_else(|| invalid("crossover donor event disappeared"))?;
            expanded.insert((
                "indicator".to_owned(),
                text(
                    required(event, "indicatorInstanceId", "crossover donor event")?,
                    "crossover donor event source",
                )?,
            ));
        }
    }
    Ok(expanded)
}

fn recipient_can_supply_crossover_uses(
    recipient: &Value,
    closure: &BTreeSet<CrossoverUse>,
) -> Result<bool> {
    let ids = crossover_resource_ids(recipient)?;
    Ok(closure.iter().all(|(kind, id)| {
        ids.get(kind)
            .is_some_and(|available| available.contains(id))
    }))
}

fn crossover_construction(
    left: &Value,
    right: &Value,
    port: &str,
    donor_edge_id: &str,
) -> Result<Value> {
    let mut segments = Map::new();
    segments.insert(
        port.to_owned(),
        array([Value::String(donor_edge_id.to_owned())]),
    );
    Ok(object([
        ("schemaVersion", Value::String(CROSSOVER_SCHEMA.to_owned())),
        ("leftGenomeSha256", Value::String(sha(left)?)),
        ("rightGenomeSha256", Value::String(sha(right)?)),
        ("segmentMap", Value::Object(segments)),
    ]))
}

fn exact_crossover_construction(
    left: &Value,
    right: &Value,
    construction: &Value,
) -> Result<BTreeMap<String, Vec<String>>> {
    let fields = map_ref(construction, "same-side crossover construction")?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "leftGenomeSha256",
            "rightGenomeSha256",
            "segmentMap",
        ],
        "same-side crossover construction",
    )?;
    if object_get(construction, "schemaVersion").and_then(Value::as_str) != Some(CROSSOVER_SCHEMA)
        || object_get(construction, "leftGenomeSha256").and_then(Value::as_str)
            != Some(sha(left)?.as_str())
        || object_get(construction, "rightGenomeSha256").and_then(Value::as_str)
            != Some(sha(right)?.as_str())
    {
        return Err(invalid(
            "same-side crossover construction is stale or incompatible",
        ));
    }
    let segment_map = map_ref(
        required(
            construction,
            "segmentMap",
            "same-side crossover construction",
        )?,
        "same-side crossover segment map",
    )?;
    if segment_map.is_empty() {
        return Err(invalid("same-side crossover must select a donor motif"));
    }
    let mut output = BTreeMap::new();
    for (port, values) in segment_map {
        if !matches!(port.as_str(), "entry_setup" | "management_hub" | "exit_hub") {
            return Err(invalid("same-side crossover names an unknown motif port"));
        }
        let values = array_ref(values, "same-side crossover donor edges")?;
        if values.is_empty() {
            return Err(invalid("same-side crossover port has no donor motifs"));
        }
        let mut ids = Vec::new();
        for value in values {
            let id = text(value, "same-side crossover donor edge ID")?;
            if value.as_str() != Some(id.as_str()) {
                return Err(invalid(
                    "same-side crossover donor edge ID is not canonical",
                ));
            }
            ids.push(id);
        }
        output.insert(port.clone(), ids);
    }
    Ok(output)
}

/// Same-side crossover is intentionally a separate two-parent boundary.  A
/// one-parent operator authority still validates both programs here, which
/// proves direction, instrument, budget, catalog capability and graph closure
/// against the exact sealed side before either parent can influence a child.
fn validate_same_side_crossover_parents(
    left: &Value,
    right: &Value,
    authority: &V5OperatorAuthority,
) -> Result<()> {
    validate_program(left, authority)?;
    validate_program(right, authority)?;
    if program_side(left)? != program_side(right)?
        || required(left, "instrument", "crossover left program")?
            != required(right, "instrument", "crossover right program")?
        || required(left, "budget", "crossover left program")?
            != required(right, "budget", "crossover right program")?
    {
        return Err(invalid(
            "same-side crossover parents are not direction/instrument/budget compatible",
        ));
    }
    Ok(())
}

fn crossover_port_membership(
    left: &Value,
    right: &Value,
    segment_map: &BTreeMap<String, Vec<String>>,
) -> Result<()> {
    let left_ports = crossover_ports(left)?;
    let right_ports = crossover_ports(right)?;
    for (port, donor_ids) in segment_map {
        let recipients = left_ports
            .get(port)
            .ok_or_else(|| invalid("same-side crossover names an incompatible recipient port"))?;
        let donors = right_ports
            .get(port)
            .ok_or_else(|| invalid("same-side crossover names an incompatible donor port"))?;
        if donor_ids.len() > recipients.len() {
            return Err(invalid(
                "same-side crossover requests more donor motifs than compatible recipient ports",
            ));
        }
        for donor_id in donor_ids {
            if !donors.iter().any(|candidate| candidate == donor_id) {
                return Err(invalid(
                    "same-side crossover names a nonexistent donor motif",
                ));
            }
            if !recipient_can_supply_crossover_uses(
                left,
                &crossover_motif_resource_closure(right, donor_id)?,
            )? {
                return Err(invalid(
                    "same-side crossover donor motif resource closure is incompatible with recipient",
                ));
            }
        }
    }
    Ok(())
}

fn crossover_plan_is_current(
    left: &Value,
    right: &Value,
    authority: &V5OperatorAuthority,
    plan: &Value,
) -> Result<BTreeMap<String, Vec<String>>> {
    validate_same_side_crossover_parents(left, right, authority)?;
    let segment_map = exact_crossover_construction(left, right, plan)?;
    crossover_port_membership(left, right, &segment_map)?;
    Ok(segment_map)
}

fn node_with_crossover_id(node: &Value, id: &str) -> Result<Value> {
    let mut output = canonical_clone(node)?;
    map_mut(&mut output, "crossover node")?.insert("id".to_owned(), Value::String(id.to_owned()));
    Ok(output)
}

fn same_side_crossover_child(
    left: &Value,
    right: &Value,
    plan: &Value,
    segment_map: &BTreeMap<String, Vec<String>>,
) -> Result<(Value, Vec<Value>)> {
    let left_nodes = rows_by_id(&node_rows(left)?, "node")?;
    let right_nodes = rows_by_id(&node_rows(right)?, "node")?;
    let left_edges = rows_by_id(&edge_rows(left)?, "edge")?;
    let right_edges = rows_by_id(&edge_rows(right)?, "edge")?;
    let left_ports = crossover_ports(left)?;
    let plan_sha = sha(plan)?;

    let mut nodes = node_rows(left)?;
    let mut edges = edge_rows(left)?;
    let mut replacements = Vec::new();

    for (port, donor_ids) in segment_map {
        let recipient_ids = left_ports
            .get(port)
            .ok_or_else(|| invalid("crossover recipient port disappeared during application"))?;
        for (ordinal, donor_edge_id) in donor_ids.iter().enumerate() {
            let recipient_edge_id = recipient_ids.get(ordinal).ok_or_else(|| {
                invalid("crossover recipient ordinal disappeared during application")
            })?;
            let donor_edge = right_edges
                .get(donor_edge_id)
                .ok_or_else(|| invalid("crossover donor edge disappeared during application"))?;
            let recipient_edge = left_edges.get(recipient_edge_id).ok_or_else(|| {
                invalid("crossover recipient edge disappeared during application")
            })?;
            let donor_target_id = text(
                required(donor_edge, "target", "crossover donor edge")?,
                "crossover donor target",
            )?;
            let recipient_target_id = text(
                required(recipient_edge, "target", "crossover recipient edge")?,
                "crossover recipient target",
            )?;
            let donor_node = right_nodes
                .get(&donor_target_id)
                .ok_or_else(|| invalid("crossover donor target disappeared during application"))?;
            let recipient_node = left_nodes.get(&recipient_target_id).ok_or_else(|| {
                invalid("crossover recipient target disappeared during application")
            })?;
            if object_get(donor_node, "zone") != object_get(recipient_node, "zone")
                || object_get(donor_edge, "effect") != object_get(recipient_edge, "effect")
            {
                return Err(invalid(
                    "same-side crossover donor motif violates its typed port contract",
                ));
            }

            let suffix = sha(&object([
                ("plan", Value::String(plan_sha.clone())),
                ("port", Value::String(port.clone())),
                ("ordinal", Value::from(ordinal as u64)),
            ]))?;
            let suffix = &suffix[7..19];
            let new_node_id = format!("x_{port}_{suffix}");
            let new_edge_id = format!("x_dispatch_{suffix}");
            let new_node = node_with_crossover_id(donor_node, &new_node_id)?;
            let donor_effect = effect(required(donor_edge, "effect", "crossover donor edge")?);
            let new_edge = edge_set_fields(
                recipient_edge,
                None,
                Some(&new_node_id),
                None,
                Some(canonical_clone(required(
                    donor_edge,
                    "guard",
                    "crossover donor edge",
                )?)?),
                Some(donor_effect),
                Some(&new_edge_id),
            )?;

            if port == "entry_setup" {
                let donor_outgoing = right_edges
                    .values()
                    .filter(|edge| {
                        object_get(edge, "source").and_then(Value::as_str)
                            == Some(donor_target_id.as_str())
                    })
                    .collect::<Vec<_>>();
                let recipient_outgoing = left_edges
                    .values()
                    .filter(|edge| {
                        object_get(edge, "source").and_then(Value::as_str)
                            == Some(recipient_target_id.as_str())
                    })
                    .collect::<Vec<_>>();
                if donor_outgoing.len() != 1 || recipient_outgoing.len() != 1 {
                    return Err(invalid(
                        "entry/setup crossover requires linear donor and recipient segments",
                    ));
                }
                let donor_continue = donor_outgoing[0];
                let recipient_continue = recipient_outgoing[0];
                if object_get(donor_continue, "effect") != object_get(recipient_continue, "effect")
                {
                    return Err(invalid(
                        "entry/setup donor continuation violates typed port contract",
                    ));
                }
                let continuation_id = format!("x_continue_{suffix}");
                let continuation = edge_set_fields(
                    recipient_continue,
                    Some(&new_node_id),
                    None,
                    None,
                    Some(canonical_clone(required(
                        donor_continue,
                        "guard",
                        "crossover donor continuation",
                    )?)?),
                    Some(effect(required(
                        donor_continue,
                        "effect",
                        "crossover donor continuation",
                    )?)),
                    Some(&continuation_id),
                )?;
                nodes.retain(|node| {
                    row_id(node, "node").ok().as_deref() != Some(recipient_target_id.as_str())
                });
                nodes.push(new_node);
                let recipient_continue_id =
                    row_id(recipient_continue, "crossover recipient continuation")?;
                edges.retain(|edge| {
                    let edge_id = row_id(edge, "edge").ok();
                    edge_id.as_deref() != Some(recipient_edge_id.as_str())
                        && edge_id.as_deref() != Some(recipient_continue_id.as_str())
                });
                edges.push(new_edge);
                edges.push(continuation);
            } else {
                nodes.retain(|node| {
                    row_id(node, "node").ok().as_deref() != Some(recipient_target_id.as_str())
                });
                nodes.push(new_node);
                edges.retain(|edge| {
                    row_id(edge, "edge").ok().as_deref() != Some(recipient_edge_id.as_str())
                });
                edges.push(new_edge);
            }
            replacements.push(object([
                ("recipientEdgeId", Value::String(recipient_edge_id.clone())),
                ("donorEdgeId", Value::String(donor_edge_id.clone())),
            ]));
        }
    }
    Ok((
        replace_nodes_edges(left, Some(nodes), Some(edges))?,
        replacements,
    ))
}

fn same_side_crossover_audit(
    left: &Value,
    right: &Value,
    authority: &V5OperatorAuthority,
    plan: &Value,
    child: &Value,
    replacements: Vec<Value>,
) -> Result<Value> {
    let plan_sha = sha(plan)?;
    // This nested delta deliberately mirrors Python's
    // `TopologySemanticDeltaV1` shape.  The outer application envelope is a
    // Rust journal convenience; the delta itself is the cross-language
    // topology fact used by audits and candidate identity diagnostics.
    let mut semantic_delta = object([
        (
            "schemaVersion",
            Value::String(v5_topology_operators::V5_TOPOLOGY_DELTA_SCHEMA.to_owned()),
        ),
        ("crossoverPlanSha256", Value::String(plan_sha.clone())),
        (
            "orderedParents",
            array([Value::String(sha(left)?), Value::String(sha(right)?)]),
        ),
        (
            "segmentMap",
            canonical_clone(required(plan, "segmentMap", "same-side crossover plan")?)?,
        ),
        ("replacements", array(replacements)),
        (
            "beforeTopologySha256",
            Value::String(
                v5_topology_operators::semantic_topology_sha256(left)
                    .map_err(|error| invalid(error.to_string()))?,
            ),
        ),
        (
            "afterTopologySha256",
            Value::String(
                v5_topology_operators::semantic_topology_sha256(child)
                    .map_err(|error| invalid(error.to_string()))?,
            ),
        ),
        ("childGenomeSha256", Value::String(sha(child)?)),
    ]);
    // Python journals the semantic delta itself as the crossover application
    // record.  Keep that inner identity so a Rust replay can be checked
    // byte-for-byte against production authority transcripts; the enclosing
    // Rust audit still receives its own identity below.
    let semantic_delta_sha = sha(&semantic_delta)?;
    map_mut(&mut semantic_delta, "same-side crossover semantic delta")?.insert(
        "applicationSha256".to_owned(),
        Value::String(semantic_delta_sha),
    );
    let mut audit = object([
        (
            "schemaVersion",
            Value::String(V5_SAME_SIDE_CROSSOVER_APPLICATION_SCHEMA.to_owned()),
        ),
        ("crossoverPlanSha256", Value::String(plan_sha)),
        (
            "authoritySha256",
            Value::String(authority.authority_sha256().to_owned()),
        ),
        (
            "orderedParentProgramSha256",
            array([Value::String(sha(left)?), Value::String(sha(right)?)]),
        ),
        ("childProgramSha256", Value::String(sha(child)?)),
        ("semanticDelta", semantic_delta),
        ("allChecksPassed", Value::Bool(true)),
    ]);
    let audit_sha = sha(&audit)?;
    map_mut(&mut audit, "same-side crossover application")?
        .insert("applicationSha256".to_owned(), Value::String(audit_sha));
    Ok(audit)
}

/// Apply one exact Python-compatible, same-side motif-crossover plan.  It
/// never imports resources: the recipient must already close every donor node
/// and guard dependency.  Core's pure evolved-profile compiler remains the
/// final compiler gate above this graph-level transformation.
pub fn apply_same_side_crossover_plan(
    left: &Value,
    right: &Value,
    authority: &V5OperatorAuthority,
    plan: &Value,
) -> Result<V5SameSideCrossoverApplication> {
    let segment_map = crossover_plan_is_current(left, right, authority, plan)?;
    let (child_program, replacements) = same_side_crossover_child(left, right, plan, &segment_map)?;
    if child_program == *left {
        return Err(invalid("same-side crossover applied as a no-op"));
    }
    validate_program(&child_program, authority)?;
    let audit =
        same_side_crossover_audit(left, right, authority, plan, &child_program, replacements)?;
    Ok(V5SameSideCrossoverApplication {
        plan: canonical_clone(plan)?,
        child_program,
        audit,
    })
}

/// Enumerate the same compatible single-motif crossover candidates Python's
/// current live authority can select.  A plan is exposed only after replaying
/// it through the identical closed graph/capability gate used by application.
/// The caller owns deterministic port/donor sampling and durable parent
/// selection; this function owns no scheduler state.
pub fn enumerate_same_side_crossover_plans(
    left: &Value,
    right: &Value,
    authority: &V5OperatorAuthority,
) -> Result<Vec<Value>> {
    validate_same_side_crossover_parents(left, right, authority)?;
    let left_ports = crossover_ports(left)?;
    let right_ports = crossover_ports(right)?;
    let mut plans = BTreeMap::<String, Value>::new();
    for port in ["entry_setup", "management_hub", "exit_hub"] {
        if !left_ports.contains_key(port) || !right_ports.contains_key(port) {
            continue;
        }
        for donor_id in right_ports.get(port).into_iter().flatten() {
            let plan = crossover_construction(left, right, port, donor_id)?;
            if apply_same_side_crossover_plan(left, right, authority, &plan).is_err() {
                continue;
            }
            let plan_sha = sha(&plan)?;
            if plans.insert(plan_sha, plan).is_some() {
                return Err(invalid(
                    "duplicate canonical same-side crossover plan identity",
                ));
            }
        }
    }
    Ok(plans.into_values().collect())
}

fn compatible_same_side_crossover_ports(
    left: &Value,
    right: &Value,
    authority: &V5OperatorAuthority,
) -> Result<Vec<(String, Vec<String>)>> {
    validate_same_side_crossover_parents(left, right, authority)?;
    let left_ports = crossover_ports(left)?;
    let right_ports = crossover_ports(right)?;
    let mut output = Vec::new();
    // Preserve the historical authority's explicit port-loop order.  This is
    // intentionally *not* lexical order (`exit_hub` would otherwise move).
    for port in ["entry_setup", "management_hub", "exit_hub"] {
        if !left_ports.contains_key(port) || !right_ports.contains_key(port) {
            continue;
        }
        let mut compatible = Vec::new();
        for donor_id in right_ports.get(port).into_iter().flatten() {
            let plan = crossover_construction(left, right, port, donor_id)?;
            if apply_same_side_crossover_plan(left, right, authority, &plan).is_ok() {
                compatible.push(donor_id.clone());
            }
        }
        if !compatible.is_empty() {
            output.push((port.to_owned(), compatible));
        }
    }
    Ok(output)
}

/// Reproduce the historical compatibility authority's two-draw crossover
/// selection: first a typed port, then one compatible donor motif.  The
/// selection stream is deliberately different from mutation-family draws and
/// remains pinned to the old factory schema for resume parity.
pub fn select_same_side_crossover_plan(
    proposal_seed: &str,
    left: &Value,
    right: &Value,
    authority: &V5OperatorAuthority,
) -> Result<V5SameSideCrossoverSelection> {
    let proposal_seed = text(
        &Value::String(proposal_seed.to_owned()),
        "same-side crossover proposal seed",
    )?;
    let ports = compatible_same_side_crossover_ports(left, right, authority)?;
    if ports.is_empty() {
        return Err(invalid(
            "same-side crossover has no compatible same-side motif port",
        ));
    }
    let (port, donors) =
        ports[factory_choice_index(&proposal_seed, "crossover_port", ports.len())?].clone();
    let donor_edge_id =
        donors[factory_choice_index(&proposal_seed, "crossover_donor", donors.len())?].clone();
    let plan = crossover_construction(left, right, &port, &donor_edge_id)?;
    // This second admission check is intentionally not implicit in the
    // selector.  It proves a selected raw plan can cross the exact current
    // trusted apply boundary before it is journaled.
    apply_same_side_crossover_plan(left, right, authority, &plan)?;
    let mut receipt = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_v5_same_side_crossover_selection_v1".to_owned()),
        ),
        ("proposalSeed", Value::String(proposal_seed)),
        ("port", Value::String(port)),
        ("donorEdgeId", Value::String(donor_edge_id)),
        ("leftProgramSha256", Value::String(sha(left)?)),
        ("rightProgramSha256", Value::String(sha(right)?)),
        ("crossoverPlanSha256", Value::String(sha(&plan)?)),
        (
            "authoritySha256",
            Value::String(authority.authority_sha256().to_owned()),
        ),
    ]);
    let receipt_sha = sha(&receipt)?;
    map_mut(&mut receipt, "same-side crossover selection receipt")?
        .insert("selectionSha256".to_owned(), Value::String(receipt_sha));
    Ok(V5SameSideCrossoverSelection { plan, receipt })
}

fn crossover_parent_order_key(proposal_seed: &str, state: &V5EvolvedSideState) -> Result<String> {
    sha(&object([
        ("proposalSeed", Value::String(proposal_seed.to_owned())),
        (
            "moduleIdentitySha256",
            Value::String(state.module_identity_sha256.clone()),
        ),
    ]))
}

fn exact_evolved_same_side_crossover_selection(
    proposal_seed: &str,
    first: &V5EvolvedSideState,
    second: &V5EvolvedSideState,
    authority: &V5OperatorAuthority,
) -> Result<V5EvolvedSameSideCrossoverSelection> {
    let proposal_seed = text(
        &Value::String(proposal_seed.to_owned()),
        "same-side crossover proposal seed",
    )?;
    first.validate_for_authority(authority)?;
    second.validate_for_authority(authority)?;
    let side = proposal_side_for_seed(&proposal_seed)?;
    if authority.side() != side || first.side != side || second.side != side {
        return Err(invalid(
            "same-side crossover proposal side does not match fresh parent state",
        ));
    }

    // Python sorts the two module identities by the seed-scoped canonical
    // hash, rather than preserving scheduler parent/mate position.  Preserve
    // the input order on an exact tie, matching Python's stable `sorted`.
    let first_key = crossover_parent_order_key(&proposal_seed, first)?;
    let second_key = crossover_parent_order_key(&proposal_seed, second)?;
    let (recipient, donor) = if first_key <= second_key {
        (first, second)
    } else {
        (second, first)
    };
    let ports =
        compatible_same_side_crossover_ports(&recipient.program, &donor.program, authority)?;
    if ports.is_empty() {
        return Err(invalid(
            "same-side crossover has no compatible same-side motif port",
        ));
    }
    let (selected_port, donor_ids) =
        ports[factory_choice_index(&proposal_seed, "crossover_port", ports.len())?].clone();
    let selected_donor_edge_id = donor_ids
        [factory_choice_index(&proposal_seed, "crossover_donor", donor_ids.len())?]
    .clone();
    let native_plan = crossover_construction(
        &recipient.program,
        &donor.program,
        &selected_port,
        &selected_donor_edge_id,
    )?;
    // Selectors are admission boundaries: a port is only journalable if the
    // exact selected plan immediately crosses the trusted graph gate.
    apply_same_side_crossover_plan(&recipient.program, &donor.program, authority, &native_plan)?;

    let eligible_ports = array(ports.iter().map(|(port, donor_ids)| {
        object([
            ("port", Value::String(port.clone())),
            (
                "donorEdgeIds",
                array(donor_ids.iter().cloned().map(Value::String)),
            ),
        ])
    }));
    let mut selection = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_v5_real_crossover_selection_v1".to_owned()),
        ),
        ("side", Value::String(side.to_owned())),
        (
            "orderedParentModuleIdentitySha256",
            array([
                Value::String(recipient.module_identity_sha256.clone()),
                Value::String(donor.module_identity_sha256.clone()),
            ]),
        ),
        ("eligiblePorts", eligible_ports),
        ("selectedPort", Value::String(selected_port)),
        ("selectedDonorEdgeId", Value::String(selected_donor_edge_id)),
    ]);
    let selection_sha = sha(&selection)?;
    map_mut(&mut selection, "evolved same-side crossover selection")?
        .insert("selectionSha256".to_owned(), Value::String(selection_sha));
    Ok(V5EvolvedSameSideCrossoverSelection {
        proposal_seed,
        side: side.to_owned(),
        recipient_pair_identity_sha256: recipient.pair_identity_sha256.clone(),
        donor_pair_identity_sha256: donor.pair_identity_sha256.clone(),
        recipient_module_identity_sha256: recipient.module_identity_sha256.clone(),
        donor_module_identity_sha256: donor.module_identity_sha256.clone(),
        native_plan,
        selection,
    })
}

/// Rebuild Python's full same-side crossover ordering and selection from two
/// fresh, recompiled side states.  Unlike the lower-level plan selector, this
/// API owns proposal-side routing and the seed-scoped recipient/donor order,
/// so a transaction cannot accidentally reuse its scheduler ordering.
pub(crate) fn select_evolved_same_side_crossover_from_states(
    proposal_seed: &str,
    first: &V5EvolvedSideState,
    second: &V5EvolvedSideState,
    authority: &V5OperatorAuthority,
) -> Result<V5EvolvedSameSideCrossoverSelection> {
    exact_evolved_same_side_crossover_selection(proposal_seed, first, second, authority)
}

fn evolved_same_side_crossover_rejection(
    error: &V5OperatorError,
    selection: Option<V5EvolvedSameSideCrossoverSelection>,
) -> V5EvolvedSameSideCrossoverExecution {
    let no_op = matches!(error, V5OperatorError::Invalid(message) if message.contains("no-op"));
    let (disposition, reason_code) = if no_op {
        (V5OperatorDisposition::NoOp, "crossover_no_op")
    } else {
        (V5OperatorDisposition::Rejected, "crossover_rejected")
    };
    V5EvolvedSameSideCrossoverExecution {
        disposition,
        reason_code: reason_code.to_owned(),
        reason_detail: object([
            (
                "schemaVersion",
                Value::String("temporal_qd_pair_rejection_audit_v1".to_owned()),
            ),
            (
                "exceptionType",
                Value::String("TemporalDiscoveryContractError".to_owned()),
            ),
            ("reasonCode", Value::String(reason_code.to_owned())),
        ]),
        selection,
        application: None,
        delta: None,
    }
}

/// Apply one exact, state-derived crossover selection.  Before application it
/// reconstructs the selection from both current state identities; stale
/// compiled profiles, swapped parents, and stale selections become a typed
/// deterministic rejection rather than an unbound child program.
pub(crate) fn execute_evolved_same_side_crossover_from_states(
    proposal_seed: &str,
    first: &V5EvolvedSideState,
    second: &V5EvolvedSideState,
    authority: &V5OperatorAuthority,
    selection: V5EvolvedSameSideCrossoverSelection,
) -> V5EvolvedSameSideCrossoverExecution {
    if selection.proposal_seed != proposal_seed {
        return evolved_same_side_crossover_rejection(
            &V5OperatorError::Invalid(
                "same-side crossover selection proposal seed does not match the attempt".to_owned(),
            ),
            Some(selection),
        );
    }
    let current = match exact_evolved_same_side_crossover_selection(
        proposal_seed,
        first,
        second,
        authority,
    ) {
        Ok(current) => current,
        Err(error) => return evolved_same_side_crossover_rejection(&error, Some(selection)),
    };
    if current != selection {
        return evolved_same_side_crossover_rejection(
            &V5OperatorError::Invalid(
                "same-side crossover selection is stale, foreign, or no longer current".to_owned(),
            ),
            Some(selection),
        );
    }
    let (recipient, donor) = first_program_for_crossover_selection(first, second, &current);
    let application =
        match apply_same_side_crossover_plan(recipient, donor, authority, &current.native_plan) {
            Ok(application) => application,
            Err(error) => return evolved_same_side_crossover_rejection(&error, Some(current)),
        };
    let trace = match required(
        &application.audit,
        "semanticDelta",
        "evolved same-side crossover application",
    )
    .and_then(canonical_clone)
    {
        Ok(trace) => trace,
        Err(error) => return evolved_same_side_crossover_rejection(&error, Some(current)),
    };
    let child_program_sha256 = match sha(&application.child_program) {
        Ok(value) => value,
        Err(error) => return evolved_same_side_crossover_rejection(&error, Some(current)),
    };
    let delta = V5EvolvedSameSideCrossoverDelta {
        side: current.side.clone(),
        recipient_pair_identity_sha256: current.recipient_pair_identity_sha256.clone(),
        donor_pair_identity_sha256: current.donor_pair_identity_sha256.clone(),
        recipient_module_identity_sha256: current.recipient_module_identity_sha256.clone(),
        donor_module_identity_sha256: current.donor_module_identity_sha256.clone(),
        recipient_program_sha256: match sha(recipient) {
            Ok(value) => value,
            Err(error) => return evolved_same_side_crossover_rejection(&error, Some(current)),
        },
        donor_program_sha256: match sha(donor) {
            Ok(value) => value,
            Err(error) => return evolved_same_side_crossover_rejection(&error, Some(current)),
        },
        child_program: application.child_program.clone(),
        child_program_sha256,
        native_plan: current.native_plan.clone(),
        selection: current.selection.clone(),
        trace,
    };
    V5EvolvedSameSideCrossoverExecution {
        disposition: V5OperatorDisposition::Accepted,
        reason_code: "accepted".to_owned(),
        reason_detail: object([("kind", Value::String("applied".to_owned()))]),
        selection: Some(current),
        application: Some(application),
        delta: Some(delta),
    }
}

fn first_program_for_crossover_selection<'a>(
    first: &'a V5EvolvedSideState,
    second: &'a V5EvolvedSideState,
    selection: &V5EvolvedSameSideCrossoverSelection,
) -> (&'a Value, &'a Value) {
    if first.pair_identity_sha256 == selection.recipient_pair_identity_sha256
        && first.module_identity_sha256 == selection.recipient_module_identity_sha256
        && second.pair_identity_sha256 == selection.donor_pair_identity_sha256
        && second.module_identity_sha256 == selection.donor_module_identity_sha256
    {
        (&first.program, &second.program)
    } else {
        (&second.program, &first.program)
    }
}

/// Plan and execute a same-side crossover as one journal-safe attempt.  This
/// is the transaction-facing entry point for the real no-compatible-port
/// terminal: it returns `Rejected/crossover_rejected`, mirroring the Python
/// proposal disposition instead of leaking a selection exception.
pub(crate) fn attempt_evolved_same_side_crossover_from_states(
    proposal_seed: &str,
    first: &V5EvolvedSideState,
    second: &V5EvolvedSideState,
    authority: &V5OperatorAuthority,
) -> V5EvolvedSameSideCrossoverExecution {
    match select_evolved_same_side_crossover_from_states(proposal_seed, first, second, authority) {
        Ok(selection) => execute_evolved_same_side_crossover_from_states(
            proposal_seed,
            first,
            second,
            authority,
            selection,
        ),
        Err(error) => evolved_same_side_crossover_rejection(&error, None),
    }
}

fn transform_for_operator(
    parent: &Value,
    authority: &V5OperatorAuthority,
    operator_id: &str,
    construction: &Value,
) -> Result<(Value, Value)> {
    match operator_id {
        V5_RESOURCE_OPERATOR_ID => resource_transform(parent, authority, construction),
        V5_TEMPORAL_OPERATOR_ID => temporal_transform(parent, construction),
        V5_HOLD_OPERATOR_ID => hold_transform(parent, construction, authority),
        V5_INITIAL_PROTECTION_OPERATOR_ID => {
            initial_protection_transform(parent, authority, construction)
        }
        V5_TOPOLOGY_OPERATOR_ID => topology_transform(parent, authority, construction),
        V5_CROSSOVER_OPERATOR_ID => Err(invalid(
            "v5 crossover operator is not integrated into this authority-bound engine yet",
        )),
        _ => Err(invalid("unknown v5 operator ID")),
    }
}

fn family_constructions(
    program: &Value,
    authority: &V5OperatorAuthority,
) -> Result<Vec<(&'static str, &'static str, Value)>> {
    let mut output = Vec::new();
    output.extend(
        resource_constructions(program, authority)?
            .into_iter()
            .map(|construction| ("resource", V5_RESOURCE_OPERATOR_ID, construction)),
    );
    output.extend(
        temporal_constructions(program, authority)?
            .into_iter()
            .map(|construction| ("temporal", V5_TEMPORAL_OPERATOR_ID, construction)),
    );
    output.extend(
        topology_constructions(program, authority)?
            .into_iter()
            .map(|construction| ("typed_grammar", V5_TOPOLOGY_OPERATOR_ID, construction)),
    );
    output.extend(
        hold_constructions(program, authority)?
            .into_iter()
            .map(|construction| ("hold", V5_HOLD_OPERATOR_ID, construction)),
    );
    output.extend(
        initial_protection_constructions(program, authority)?
            .into_iter()
            .map(|construction| {
                (
                    "initial_protection",
                    V5_INITIAL_PROTECTION_OPERATOR_ID,
                    construction,
                )
            }),
    );
    Ok(output)
}

/// The production initial-protection vocabulary is profile-derived.  The
/// source genome identifies where an accepted edit is persisted, but only the
/// just-compiled management library can say which scalar bindings and v3
/// construction sites exist.  Keep the old unprofiled helper above for small
/// synthetic support tests; it is deliberately not used for evolved
/// selection.
fn evolved_family_constructions(
    program: &Value,
    authority: &V5OperatorAuthority,
    profile: &V5CompiledProfileView,
) -> Result<Vec<(&'static str, &'static str, Value)>> {
    let mut output = Vec::new();
    output.extend(
        resource_constructions(program, authority)?
            .into_iter()
            .map(|construction| ("resource", V5_RESOURCE_OPERATOR_ID, construction)),
    );
    output.extend(
        temporal_constructions(program, authority)?
            .into_iter()
            .map(|construction| ("temporal", V5_TEMPORAL_OPERATOR_ID, construction)),
    );
    output.extend(
        topology_constructions(program, authority)?
            .into_iter()
            .map(|construction| ("typed_grammar", V5_TOPOLOGY_OPERATOR_ID, construction)),
    );
    output.extend(
        hold_constructions(program, authority)?
            .into_iter()
            .map(|construction| ("hold", V5_HOLD_OPERATOR_ID, construction)),
    );
    output.extend(
        compiled_static_initial_protection_plans(program, authority, profile)?
            .into_iter()
            .map(|construction| {
                (
                    "initial_protection",
                    V5_INITIAL_PROTECTION_OPERATOR_ID,
                    construction,
                )
            }),
    );
    output.extend(
        compiled_dynamic_initial_protection_plans(program, authority, profile)?
            .into_iter()
            .map(|construction| {
                (
                    "initial_protection",
                    V5_INITIAL_PROTECTION_OPERATOR_ID,
                    construction,
                )
            }),
    );
    Ok(output)
}

fn legacy_resource_plan(
    program: &Value,
    selection: &V5LegacySelectionStatic,
    construction: &Value,
) -> Result<Value> {
    let parent_genome_sha256 = sha(program)?;
    let identity = object([
        (
            "schemaVersion",
            Value::String(RESOURCE_OPERATOR_PLAN_SCHEMA.to_owned()),
        ),
        (
            "operatorVersion",
            Value::String(RESOURCE_OPERATOR_VERSION.to_owned()),
        ),
        (
            "catalogSha256",
            Value::String(selection.catalog_sha256.clone()),
        ),
        (
            "parentGenomeSha256",
            Value::String(parent_genome_sha256.clone()),
        ),
        ("construction", canonical_clone(construction)?),
    ]);
    finalize_structural_plan(object([
        (
            "operatorVersion",
            Value::String(RESOURCE_OPERATOR_VERSION.to_owned()),
        ),
        (
            "operatorSpecSha256",
            Value::String(selection.resource_operator_spec_sha256.clone()),
        ),
        ("parentGenomeSha256", Value::String(parent_genome_sha256)),
        (
            "catalogSha256",
            Value::String(selection.catalog_sha256.clone()),
        ),
        ("construction", canonical_clone(construction)?),
        ("constructionIdentitySha256", Value::String(sha(&identity)?)),
    ]))
}

fn legacy_temporal_plan(
    program: &Value,
    selection: &V5LegacySelectionStatic,
    construction: &Value,
) -> Result<Value> {
    let parent_genome_sha256 = sha(program)?;
    let identity = object([
        (
            "schemaVersion",
            Value::String(TEMPORAL_OPERATOR_PLAN_SCHEMA.to_owned()),
        ),
        (
            "operatorVersion",
            Value::String(TEMPORAL_OPERATOR_VERSION.to_owned()),
        ),
        (
            "operatorSpecSha256",
            Value::String(selection.temporal_operator_spec_sha256.clone()),
        ),
        (
            "parentGenomeSha256",
            Value::String(parent_genome_sha256.clone()),
        ),
        ("construction", canonical_clone(construction)?),
    ]);
    finalize_structural_plan(object([
        (
            "operatorVersion",
            Value::String(TEMPORAL_OPERATOR_VERSION.to_owned()),
        ),
        (
            "operatorSpecSha256",
            Value::String(selection.temporal_operator_spec_sha256.clone()),
        ),
        ("parentGenomeSha256", Value::String(parent_genome_sha256)),
        ("construction", canonical_clone(construction)?),
        ("constructionIdentitySha256", Value::String(sha(&identity)?)),
    ]))
}

fn legacy_initial_protection_plan(construction: &Value) -> Result<Value> {
    let mut plan = canonical_clone(construction)?;
    let kind = object_get(&plan, "kind")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("initial protection construction lacks kind"))?;
    if !matches!(kind, "initial_protection" | "dynamic_construction") {
        return Err(invalid("initial protection construction kind is foreign"));
    }
    let fields = map_ref(&plan, "legacy initial-protection plan")?;
    if let Some(value) = fields.get("planSha256") {
        let supplied = sha256_identifier(value, "legacy initial-protection plan SHA-256")?;
        let mut body = canonical_clone(&plan)?;
        map_mut(&mut body, "legacy initial-protection plan")?.remove("planSha256");
        if supplied != sha(&body)? {
            return Err(invalid("legacy initial-protection plan identity drifted"));
        }
        return Ok(plan);
    }
    let plan_sha256 = sha(&plan)?;
    map_mut(&mut plan, "legacy initial-protection plan")?
        .insert("planSha256".to_owned(), Value::String(plan_sha256));
    Ok(plan)
}

fn legacy_choice_for_native_plan(
    program: &Value,
    authority: &V5OperatorAuthority,
    native_plan: &Value,
) -> Result<Value> {
    let selection = authority.legacy_selection_static()?;
    let operator_id = text(
        required(native_plan, "operatorId", "native operator plan")?,
        "native operator ID",
    )?;
    let construction = required(native_plan, "construction", "native operator plan")?;
    match operator_id.as_str() {
        V5_RESOURCE_OPERATOR_ID => {
            let legacy_plan = legacy_resource_plan(program, selection, construction)?;
            let plan_sha256 = canonical_clone(required(
                &legacy_plan,
                "planSha256",
                "legacy resource plan",
            )?)?;
            Ok(object([
                ("kind", Value::String("indicator_learning".to_owned())),
                (
                    "plan",
                    object([
                        (
                            "operatorId",
                            Value::String(V5_RESOURCE_OPERATOR_ID.to_owned()),
                        ),
                        ("plan", legacy_plan),
                        ("planSha256", plan_sha256),
                    ]),
                ),
            ]))
        }
        V5_TEMPORAL_OPERATOR_ID => {
            let legacy_plan = legacy_temporal_plan(program, selection, construction)?;
            let plan_sha256 = canonical_clone(required(
                &legacy_plan,
                "planSha256",
                "legacy temporal plan",
            )?)?;
            Ok(object([
                ("kind", Value::String("indicator_learning".to_owned())),
                (
                    "plan",
                    object([
                        (
                            "operatorId",
                            Value::String(V5_TEMPORAL_OPERATOR_ID.to_owned()),
                        ),
                        ("plan", legacy_plan),
                        ("planSha256", plan_sha256),
                    ]),
                ),
            ]))
        }
        V5_TOPOLOGY_OPERATOR_ID => {
            let legacy_plan = canonical_clone(construction)?;
            // `TopologyPlanV1.canonical()` deliberately contains no
            // self-referential plan hash.  Python exposes its identity only
            // on the surrounding grammar choice, where `planSha256` is the
            // canonical hash of this raw five-field plan.
            let plan_sha256 = Value::String(sha(&legacy_plan)?);
            Ok(object([
                ("kind", Value::String("typed_grammar".to_owned())),
                (
                    "plan",
                    object([
                        (
                            "operatorId",
                            Value::String(V5_TOPOLOGY_OPERATOR_ID.to_owned()),
                        ),
                        ("plan", legacy_plan),
                        ("planSha256", plan_sha256),
                    ]),
                ),
            ]))
        }
        V5_HOLD_OPERATOR_ID => Ok(object([
            ("kind", Value::String("hold".to_owned())),
            (
                "planId",
                canonical_clone(required(construction, "planId", "hold construction")?)?,
            ),
            (
                "newHold",
                canonical_hold(required(construction, "newHold", "hold construction")?)?,
            ),
        ])),
        V5_INITIAL_PROTECTION_OPERATOR_ID => Ok(object([
            ("kind", Value::String("initial_protection".to_owned())),
            ("plan", legacy_initial_protection_plan(construction)?),
        ])),
        _ => Err(invalid("legacy choice saw an unsupported native operator")),
    }
}

fn admitted_native_plan(
    program: &Value,
    authority: &V5OperatorAuthority,
    choice_kind: &str,
    operator_id: &str,
    construction: Value,
    admission: &dyn V5EvolvedChildAdmission,
) -> Result<Option<Value>> {
    let Ok((child, _trace)) =
        transform_for_operator(program, authority, operator_id, &construction)
    else {
        // Python enumerators omit candidates whose fully reconstructed child
        // is inadmissible.  Their exception text is not a selection input.
        return Ok(None);
    };
    if child == *program || validate_program(&child, authority).is_err() {
        return Ok(None);
    }
    if admission.admit_evolved_child(operator_id, &child).is_err() {
        return Ok(None);
    }
    Ok(Some(construction_plan(
        program,
        authority,
        choice_kind,
        operator_id,
        construction,
    )?))
}

/// Rebuild the exact legacy Python choice objects in their historic canonical
/// order while retaining a native plan beside each one.  This is the sole
/// evolved-selection vocabulary: the generic `enumerate_operator_plans`
/// helper remains available only for synthetic unit coverage where no compiled
/// profile exists.
pub fn enumerate_evolved_operator_choices(
    program: &Value,
    authority: &V5OperatorAuthority,
    profile: &V5CompiledProfileView,
) -> Result<Vec<V5LegacyOperatorChoice>> {
    let admission = V5StructuralChildAdmission { authority };
    enumerate_evolved_operator_choices_with_admission(program, authority, profile, &admission)
}

/// Enumerate the exact legacy Python choice objects after applying the
/// transaction-owned compiled-child admission gate.  Production callers must
/// use this entry point; the profile-only convenience method above remains a
/// structural support API for small local tests that have no compiler seam.
pub(crate) fn enumerate_evolved_operator_choices_with_admission(
    program: &Value,
    authority: &V5OperatorAuthority,
    profile: &V5CompiledProfileView,
    admission: &dyn V5EvolvedChildAdmission,
) -> Result<Vec<V5LegacyOperatorChoice>> {
    validate_program(program, authority)?;
    // Require the opaque static authority before any plan construction.  A
    // native-only caller is allowed to use the support API, but cannot claim
    // replay equivalence with the historical Python selection stream.
    authority.legacy_selection_static()?;
    if profile.genome_program_sha256() != sha(program)? {
        return Err(invalid(
            "compiled profile does not bind the evolved choice parent program",
        ));
    }
    let mut choices = Vec::new();
    for (choice_kind, operator_id, construction) in
        evolved_family_constructions(program, authority, profile)?
    {
        let Some(native_plan) = admitted_native_plan(
            program,
            authority,
            choice_kind,
            operator_id,
            construction,
            admission,
        )?
        else {
            continue;
        };
        let legacy_choice = legacy_choice_for_native_plan(program, authority, &native_plan)?;
        let legacy_choice_sha256 = sha(&legacy_choice)?;
        choices.push((legacy_choice_sha256, native_plan, legacy_choice));
    }
    choices.sort_by(|left, right| left.0.cmp(&right.0));
    for pair in choices.windows(2) {
        if pair[0].0 == pair[1].0 {
            return Err(invalid(
                "duplicate canonical legacy v5 operator choice identity",
            ));
        }
    }
    let legacy_choice_ordering_sha256 = sha(&array(
        choices
            .iter()
            .map(|(_, _, choice)| canonical_clone(choice))
            .collect::<Result<Vec<_>>>()?,
    ))?;
    Ok(choices
        .into_iter()
        .map(
            |(legacy_choice_sha256, native_plan, legacy_choice)| V5LegacyOperatorChoice {
                native_plan,
                legacy_choice,
                legacy_choice_sha256,
                legacy_choice_ordering_sha256: legacy_choice_ordering_sha256.clone(),
            },
        )
        .collect())
}

/// Enumerate every currently admissible single-step later-generation mutation
/// for one side.  Enumeration is intentionally deterministic and filters
/// through the same authority-bound validator that will be applied after a
/// chosen plan.  Scheduler sampling lives above this API.
pub fn enumerate_operator_plans(
    program: &Value,
    authority: &V5OperatorAuthority,
) -> Result<Vec<Value>> {
    validate_program(program, authority)?;
    let mut plans = BTreeMap::<String, Value>::new();
    for (choice_kind, operator_id, construction) in family_constructions(program, authority)? {
        let Ok((child, _trace)) =
            transform_for_operator(program, authority, operator_id, &construction)
        else {
            // Python's enumerators suppress a construction that cannot build
            // a fully valid child.  It is not an error in the population.
            continue;
        };
        if child == *program || validate_program(&child, authority).is_err() {
            continue;
        }
        let plan = construction_plan(program, authority, choice_kind, operator_id, construction)?;
        let plan_sha = text(
            required(&plan, "planSha256", "v5 operator plan")?,
            "plan SHA-256",
        )?;
        if plans.insert(plan_sha, plan).is_some() {
            return Err(invalid("duplicate canonical v5 operator plan identity"));
        }
    }
    // Python `_operation_choices` first wraps each concrete plan in its
    // high-level selection family, then canonical-sorts those wrappers.  A
    // plan's own hash is not an equivalent ordering key: resource and
    // temporal plans share `indicator_learning`, and a different wrapper
    // shape changes a deterministic draw.  Preserve that exact ordering at
    // the public boundary so selection/replay cannot quietly diverge.
    sort_plans_by_choice_wrapper(plans.into_values().collect())
}

fn current_applicable_plan(
    parent: &Value,
    authority: &V5OperatorAuthority,
    plan: &Value,
) -> Result<Value> {
    verify_plan(parent, authority, plan)?;
    let plan_sha = text(
        required(plan, "planSha256", "v5 operator plan")?,
        "plan SHA-256",
    )?;
    enumerate_operator_plans(parent, authority)?
        .into_iter()
        .find(|candidate| {
            object_get(candidate, "planSha256").and_then(Value::as_str) == Some(plan_sha.as_str())
                && candidate == plan
        })
        .ok_or_else(|| invalid("v5 operator plan is not canonical and currently applicable"))
}

/// Replay one already-enumerated plan.  Both parent and child pass the trusted
/// authority gate; a raw JSON authority cannot influence this path.
pub fn apply_operator_plan(
    parent: &Value,
    authority: &V5OperatorAuthority,
    plan: &Value,
) -> Result<V5OperatorApplication> {
    let plan = current_applicable_plan(parent, authority, plan)?;
    apply_verified_operator_plan(parent, authority, plan)
}

fn apply_verified_operator_plan(
    parent: &Value,
    authority: &V5OperatorAuthority,
    plan: Value,
) -> Result<V5OperatorApplication> {
    verify_plan(parent, authority, &plan)?;
    let operator_id = text(
        required(&plan, "operatorId", "v5 operator plan")?,
        "operator ID",
    )?;
    let construction = required(&plan, "construction", "v5 operator plan")?;
    let (child_program, trace) =
        transform_for_operator(parent, authority, &operator_id, construction)?;
    if child_program == *parent {
        return Err(invalid("v5 operator plan applied as a no-op"));
    }
    validate_program(&child_program, authority)?;
    let audit = application(&plan, parent, &child_program, authority, trace)?;
    Ok(V5OperatorApplication {
        plan,
        child_program,
        audit,
    })
}

fn execution_result(
    disposition: V5OperatorDisposition,
    authority: &V5OperatorAuthority,
    parent: &Value,
    plan: &Value,
    reason_code: &str,
    reason_detail: Value,
    application: Option<&V5OperatorApplication>,
) -> Result<V5OperatorResult> {
    let plan_sha = object_get(plan, "planSha256")
        .and_then(Value::as_str)
        .filter(|value| {
            sha256_identifier(&Value::String((*value).to_owned()), "plan SHA-256").is_ok()
        })
        .map(|value| Value::String(value.to_owned()))
        .unwrap_or(Value::Null);
    let application_sha = application
        .and_then(|value| object_get(&value.audit, "applicationSha256"))
        .cloned()
        .unwrap_or(Value::Null);
    let child_sha = application
        .map(|value| sha(&value.child_program).map(Value::String))
        .transpose()?
        .unwrap_or(Value::Null);
    let mut value = object([
        (
            "schemaVersion",
            Value::String(V5_OPERATOR_RESULT_SCHEMA.to_owned()),
        ),
        (
            "disposition",
            Value::String(disposition.as_str().to_owned()),
        ),
        ("reasonCode", Value::String(reason_code.to_owned())),
        ("reasonDetail", canonical_clone(&reason_detail)?),
        (
            "authoritySha256",
            Value::String(authority.authority_sha256().to_owned()),
        ),
        ("parentProgramSha256", Value::String(sha(parent)?)),
        ("planSha256", plan_sha),
        ("childProgramSha256", child_sha),
        ("applicationSha256", application_sha),
    ]);
    let result_sha = sha(&value)?;
    map_mut(&mut value, "v5 operator result")?
        .insert("resultSha256".to_owned(), Value::String(result_sha));
    Ok(V5OperatorResult { value })
}

/// Journal-safe adapter around `apply_operator_plan`.  It turns a stale,
/// malformed, incompatible, or no-op attempt into an immutable outcome rather
/// than losing the attempt to an exception.  Successful children still carry
/// the full replayable application record.
pub fn execute_operator_plan(
    parent: &Value,
    authority: &V5OperatorAuthority,
    plan: &Value,
) -> Result<V5OperatorExecution> {
    match apply_operator_plan(parent, authority, plan) {
        Ok(application) => Ok(V5OperatorExecution {
            disposition: V5OperatorDisposition::Accepted,
            result: execution_result(
                V5OperatorDisposition::Accepted,
                authority,
                parent,
                plan,
                "accepted",
                object([("kind", Value::String("applied".to_owned()))]),
                Some(&application),
            )?,
            application: Some(application),
        }),
        Err(error) => {
            let detail = Value::String(error.to_string());
            let no_op =
                matches!(&error, V5OperatorError::Invalid(message) if message.contains("no-op"));
            let disposition = if no_op {
                V5OperatorDisposition::NoOp
            } else {
                V5OperatorDisposition::Rejected
            };
            Ok(V5OperatorExecution {
                disposition,
                result: execution_result(
                    disposition,
                    authority,
                    parent,
                    plan,
                    if no_op { "no_op" } else { "operator_rejected" },
                    detail,
                    None,
                )?,
                application: None,
            })
        }
    }
}

fn sha256_bytes(value: &str, label: &str) -> Result<[u8; 32]> {
    let hex = sha256_identifier(&Value::String(value.to_owned()), label)?;
    let hex = &hex[7..];
    let mut output = [0_u8; 32];
    for (index, slot) in output.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)
            .map_err(|_| invalid(format!("{label} is not hexadecimal")))?;
    }
    Ok(output)
}

/// Exact Python `_unbiased_choice` without a bigint dependency.  The 256-bit
/// hash is rejection-sampled against the largest multiple of `size`, then
/// reduced in a streaming base-256 division.
fn unbiased_choice(seed: &str, size: usize) -> Result<usize> {
    if size == 0 {
        return Err(invalid("operator selection bucket size must be positive"));
    }
    let modulus = size as u128;
    let mut two_256_mod = 1_u128 % modulus;
    // There are 32 base-256 digits in a SHA-256 value.
    for _ in 0..32 {
        two_256_mod = (two_256_mod * 256) % modulus;
    }
    let rejection_tail = two_256_mod;
    for attempt in 0_u64.. {
        let material = object([
            ("seed", Value::String(seed.to_owned())),
            ("attempt", Value::from(attempt)),
        ]);
        let bytes = sha256_bytes(&sha(&material)?, "operator selection hash")?;
        if rejection_tail > 0 {
            // `max - bytes` fits in u128 only when the high 16 bytes are
            // zero; otherwise it is necessarily outside the tiny rejected
            // tail (whose width is < `size`).
            let mut distance = [0_u8; 32];
            for (slot, byte) in distance.iter_mut().zip(bytes) {
                *slot = 0xff_u8.wrapping_sub(byte);
            }
            let high_nonzero = distance[..16].iter().any(|byte| *byte != 0);
            let low = distance[16..]
                .iter()
                .fold(0_u128, |value, byte| (value << 8) | u128::from(*byte));
            if !high_nonzero && low < rejection_tail {
                continue;
            }
        }
        let remainder = bytes.iter().fold(0_u128, |value, byte| {
            (value * 256 + u128::from(*byte)) % modulus
        });
        return usize::try_from(remainder)
            .map_err(|_| invalid("operator selection remainder does not fit usize"));
    }
    unreachable!("u64 attempt stream cannot exhaust")
}

/// Exact `evolvable_module_qd_authority._choice` stream used only by the
/// historic same-side crossover picker.  Mutation-family selection uses the
/// distinct pair-generation `_unbiased_choice` above; merging the two would
/// silently move existing deterministic proposals.
fn factory_choice_index(seed: &str, axis: &str, size: usize) -> Result<usize> {
    if size == 0 {
        return Err(invalid("evolvable crossover selection axis is empty"));
    }
    let modulus = size as u128;
    let mut two_256_mod = 1_u128 % modulus;
    for _ in 0..32 {
        two_256_mod = (two_256_mod * 256) % modulus;
    }
    let rejection_tail = two_256_mod;
    for nonce in 0_u64.. {
        let material = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_evolvable_module_factory_v1".to_owned()),
            ),
            ("seed", Value::String(seed.to_owned())),
            ("axis", Value::String(axis.to_owned())),
            ("nonce", Value::from(nonce)),
        ]);
        let bytes = sha256_bytes(&sha(&material)?, "crossover selection hash")?;
        if rejection_tail > 0 {
            let mut distance = [0_u8; 32];
            for (slot, byte) in distance.iter_mut().zip(bytes) {
                *slot = 0xff_u8.wrapping_sub(byte);
            }
            let high_nonzero = distance[..16].iter().any(|byte| *byte != 0);
            let low = distance[16..]
                .iter()
                .fold(0_u128, |value, byte| (value << 8) | u128::from(*byte));
            if !high_nonzero && low < rejection_tail {
                continue;
            }
        }
        let remainder = bytes.iter().fold(0_u128, |value, byte| {
            (value * 256 + u128::from(*byte)) % modulus
        });
        return usize::try_from(remainder)
            .map_err(|_| invalid("crossover selection remainder does not fit usize"));
    }
    unreachable!("u64 crossover nonce stream cannot exhaust")
}

/// Exact Python `proposal_side`: route a proposal to a side from immutable
/// seed material, never process-local RNG state.  The source identity trims
/// the public proposal identifier before hashing.
pub fn proposal_side_for_seed(proposal_seed: &str) -> Result<&'static str> {
    let seed = text(&Value::String(proposal_seed.to_owned()), "proposal seed")?;
    let identity = sha(&object([
        (
            "schemaVersion",
            Value::String(BIDIRECTIONAL_GENOME_SCHEMA.to_owned()),
        ),
        ("proposalSeed", Value::String(seed)),
    ]))?;
    let nibble = identity
        .as_bytes()
        .last()
        .and_then(|byte| char::from(*byte).to_digit(16))
        .ok_or_else(|| invalid("proposal-side identity lacks a hexadecimal suffix"))?;
    Ok(if nibble % 2 == 0 { "long" } else { "short" })
}

/// Exact Python 14/5/1 mutation-depth draw.  The evolutionary scheduler owns
/// accepted-slot accounting, but it consumes this pure, replayable primitive
/// for every mutation attempt.
pub fn mutation_depth_for_seed(proposal_seed: &str) -> Result<u8> {
    // `_mutation_depth_for_seed` receives the raw `str(proposal_seed)` after
    // proposal-side routing, so preserve its bytes rather than normalizing
    // whitespace here.  Reject only the same invalid empty/oversized token
    // boundary used by the public proposal contract.
    let raw = proposal_seed.trim();
    if raw.is_empty() || raw.len() > 240 {
        return Err(invalid(
            "proposal seed must be a nonempty explicit identifier",
        ));
    }
    Ok(match unbiased_choice(proposal_seed, 20)? {
        0..=13 => 1,
        14..=18 => 2,
        19 => 3,
        _ => unreachable!("twenty-way unbiased choice is bounded"),
    })
}

fn selection_family(plan: &Value) -> Result<&'static str> {
    match object_get(plan, "operatorId").and_then(Value::as_str) {
        Some(V5_RESOURCE_OPERATOR_ID) | Some(V5_TEMPORAL_OPERATOR_ID) => Ok("indicator_learning"),
        Some(V5_TOPOLOGY_OPERATOR_ID) => Ok("typed_grammar"),
        Some(V5_HOLD_OPERATOR_ID) => Ok("hold"),
        Some(V5_INITIAL_PROTECTION_OPERATOR_ID) => Ok("initial_protection"),
        Some(V5_CROSSOVER_OPERATOR_ID) => Err(invalid(
            "same-side crossover is not a single-parent operation selection family",
        )),
        _ => Err(invalid("operator selection saw an unknown plan family")),
    }
}

fn choice_wrapper(plan: &Value) -> Result<Value> {
    Ok(object([
        ("kind", Value::String(selection_family(plan)?.to_owned())),
        ("plan", canonical_clone(plan)?),
    ]))
}

fn choice_wrapper_sha256(plan: &Value) -> Result<String> {
    sha(&choice_wrapper(plan)?)
}

fn sort_plans_by_choice_wrapper(mut plans: Vec<Value>) -> Result<Vec<Value>> {
    let mut keyed = plans
        .drain(..)
        .map(|plan| {
            let wrapper_sha = choice_wrapper_sha256(&plan)?;
            let plan_sha = text(
                required(&plan, "planSha256", "v5 operator plan")?,
                "plan SHA-256",
            )?;
            Ok((wrapper_sha, plan_sha, plan))
        })
        .collect::<Result<Vec<_>>>()?;
    keyed.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    for pair in keyed.windows(2) {
        if pair[0].0 == pair[1].0 {
            return Err(invalid(
                "duplicate canonical v5 operator choice wrapper identity",
            ));
        }
    }
    Ok(keyed.into_iter().map(|(_, _, plan)| plan).collect())
}

fn protection_class_weights(policy: &Value) -> Result<BTreeMap<String, u64>> {
    let rows = map_ref(
        required(policy, "mutationClassWeights", "initial protection policy")?,
        "initial protection mutation class weights",
    )?;
    let mut output = BTreeMap::new();
    for class in ["adjacent", "jump", "kind_switch"] {
        output.insert(
            class.to_owned(),
            as_u64(
                rows.get(class)
                    .ok_or_else(|| invalid("initial protection class is missing"))?,
                "initial protection class weight",
            )?,
        );
    }
    Ok(output)
}

/// Select one operator using the exact current Python hierarchy:
/// uniformly choose an available high-level family, then choose an ordinary
/// plan uniformly; for protection choose a 70/25/5-style class from the
/// sealed policy (renormalized to available classes), then a plan uniformly.
/// `parent_identity_sha256` is the enclosing pair's identity, not this side's
/// program hash, which preserves Python's cross-side deterministic stream.
pub fn select_operator_plan(
    proposal_seed: &str,
    parent_identity_sha256: &str,
    parent_program: &Value,
    authority: &V5OperatorAuthority,
) -> Result<V5OperatorSelection> {
    let parent_identity_sha256 = sha256_identifier(
        &Value::String(parent_identity_sha256.to_owned()),
        "operator selection parent identity SHA-256",
    )?;
    let choices = enumerate_operator_plans(parent_program, authority)?;
    if choices.is_empty() {
        return Err(invalid("operator selection has no applicable plans"));
    }
    let mut grouped = BTreeMap::<String, Vec<Value>>::new();
    for plan in choices {
        grouped
            .entry(selection_family(&plan)?.to_owned())
            .or_default()
            .push(plan);
    }
    let families = grouped.keys().cloned().collect::<Vec<_>>();
    let family_seed = sha(&object([
        ("seed", Value::String(proposal_seed.to_owned())),
        ("parent", Value::String(parent_identity_sha256.clone())),
        ("draw", Value::String("family".to_owned())),
    ]))?;
    let family = families[unbiased_choice(&family_seed, families.len())?].clone();
    let candidates = grouped
        .get(&family)
        .ok_or_else(|| invalid("selected operator family disappeared"))?;

    let (plan, selected_class) = if family != "initial_protection" {
        let plan_seed = sha(&object([
            ("seed", Value::String(proposal_seed.to_owned())),
            ("parent", Value::String(parent_identity_sha256.clone())),
            ("draw", Value::String("plan".to_owned())),
            ("family", Value::String(family.clone())),
        ]))?;
        (
            candidates[unbiased_choice(&plan_seed, candidates.len())?].clone(),
            Value::Null,
        )
    } else {
        let weights = protection_class_weights(authority.initial_protection_policy())?;
        let mut by_class = BTreeMap::<String, Vec<Value>>::new();
        for candidate in candidates {
            let class = object_get(
                required(candidate, "construction", "initial protection plan")?,
                "mutationClass",
            )
            .and_then(Value::as_str)
            .filter(|class| weights.contains_key(*class))
            .ok_or_else(|| invalid("initial protection plan lacks an admitted mutation class"))?;
            by_class
                .entry(class.to_owned())
                .or_default()
                .push(candidate.clone());
        }
        let total = by_class
            .keys()
            .map(|class| weights.get(class).copied().unwrap_or_default())
            .sum::<u64>();
        if total == 0 {
            return Err(invalid(
                "initial protection classes have no positive sealed weight",
            ));
        }
        let class_seed = sha(&object([
            ("seed", Value::String(proposal_seed.to_owned())),
            ("parent", Value::String(parent_identity_sha256.clone())),
            ("draw", Value::String("protection_class".to_owned())),
        ]))?;
        let bucket = unbiased_choice(
            &class_seed,
            usize::try_from(total)
                .map_err(|_| invalid("initial protection class weight total exceeds usize"))?,
        )? as u64;
        let mut cursor = 0_u64;
        let mut selected = None;
        for class in by_class.keys() {
            cursor += weights.get(class).copied().unwrap_or_default();
            if bucket < cursor {
                selected = Some(class.clone());
                break;
            }
        }
        let selected = selected.ok_or_else(|| invalid("protection class selection exhausted"))?;
        let class_candidates = by_class
            .get(&selected)
            .ok_or_else(|| invalid("selected protection class disappeared"))?;
        let plan_seed = sha(&object([
            ("seed", Value::String(proposal_seed.to_owned())),
            ("parent", Value::String(parent_identity_sha256.clone())),
            ("draw", Value::String("protection_plan".to_owned())),
            ("class", Value::String(selected.clone())),
        ]))?;
        (
            class_candidates[unbiased_choice(&plan_seed, class_candidates.len())?].clone(),
            Value::String(selected),
        )
    };
    let mut receipt = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_v5_operator_selection_v1".to_owned()),
        ),
        ("proposalSeed", Value::String(proposal_seed.to_owned())),
        (
            "parentIdentitySha256",
            Value::String(parent_identity_sha256),
        ),
        ("family", Value::String(family)),
        ("mutationClass", selected_class),
        (
            "planSha256",
            canonical_clone(required(&plan, "planSha256", "selected plan")?)?,
        ),
        (
            "choiceWrapperSha256",
            Value::String(choice_wrapper_sha256(&plan)?),
        ),
    ]);
    let receipt_sha = sha(&receipt)?;
    map_mut(&mut receipt, "operator selection receipt")?
        .insert("selectionSha256".to_owned(), Value::String(receipt_sha));
    Ok(V5OperatorSelection { plan, receipt })
}

fn evolved_choice_kind(choice: &V5LegacyOperatorChoice) -> Result<String> {
    text(
        required(&choice.legacy_choice, "kind", "legacy v5 operator choice")?,
        "legacy v5 operator choice kind",
    )
}

fn evolved_choice_mutation_class(choice: &V5LegacyOperatorChoice) -> Result<String> {
    let plan = required(
        &choice.legacy_choice,
        "plan",
        "legacy initial-protection choice",
    )?;
    text(
        required(plan, "mutationClass", "legacy initial-protection choice")?,
        "legacy initial-protection mutation class",
    )
}

/// Select from the exact legacy Python choice objects emitted for a compiled
/// side profile.  This is intentionally separate from `select_operator_plan`:
/// the latter is a narrow synthetic-support API whose native wrapper cannot
/// participate in historical pair/proposal identity material.
///
/// The returned native plan is only a replay transport.  All sampling happens
/// over `legacy_choice`, in canonical Python `_operation_choices` order.
pub(crate) fn select_evolved_operator_choice(
    proposal_seed: &str,
    parent_identity_sha256: &str,
    parent_program: &Value,
    authority: &V5OperatorAuthority,
    profile: &V5CompiledProfileView,
) -> Result<V5EvolvedOperatorSelection> {
    let admission = V5StructuralChildAdmission { authority };
    select_evolved_operator_choice_with_admission(
        proposal_seed,
        parent_identity_sha256,
        parent_program,
        authority,
        profile,
        &admission,
    )
}

/// Select from the exact Python legacy vocabulary after the transaction's
/// full compiled-child admission gate has removed ineligible constructions.
/// This is the production selection entry point for later generations.
pub(crate) fn select_evolved_operator_choice_with_admission(
    proposal_seed: &str,
    parent_identity_sha256: &str,
    parent_program: &Value,
    authority: &V5OperatorAuthority,
    profile: &V5CompiledProfileView,
    admission: &dyn V5EvolvedChildAdmission,
) -> Result<V5EvolvedOperatorSelection> {
    let parent_identity_sha256 = sha256_identifier(
        &Value::String(parent_identity_sha256.to_owned()),
        "evolved operator selection parent identity SHA-256",
    )?;
    let choices = enumerate_evolved_operator_choices_with_admission(
        parent_program,
        authority,
        profile,
        admission,
    )?;
    if choices.is_empty() {
        return Err(invalid(
            "evolved operator selection has no applicable legacy choices",
        ));
    }
    let mut grouped = BTreeMap::<String, Vec<V5LegacyOperatorChoice>>::new();
    for choice in choices {
        let kind = evolved_choice_kind(&choice)?;
        grouped.entry(kind).or_default().push(choice);
    }
    let families = grouped.keys().cloned().collect::<Vec<_>>();
    let family_seed = sha(&object([
        ("seed", Value::String(proposal_seed.to_owned())),
        ("parent", Value::String(parent_identity_sha256.clone())),
        ("draw", Value::String("family".to_owned())),
    ]))?;
    let family = families[unbiased_choice(&family_seed, families.len())?].clone();
    let candidates = grouped
        .get(&family)
        .ok_or_else(|| invalid("selected evolved operator family disappeared"))?;

    let (choice, selected_class) = if family != "initial_protection" {
        let plan_seed = sha(&object([
            ("seed", Value::String(proposal_seed.to_owned())),
            ("parent", Value::String(parent_identity_sha256.clone())),
            ("draw", Value::String("plan".to_owned())),
            ("family", Value::String(family.clone())),
        ]))?;
        (
            candidates[unbiased_choice(&plan_seed, candidates.len())?].clone(),
            Value::Null,
        )
    } else {
        let weights = protection_class_weights(authority.initial_protection_policy())?;
        let mut by_class = BTreeMap::<String, Vec<V5LegacyOperatorChoice>>::new();
        for candidate in candidates {
            let class = evolved_choice_mutation_class(candidate)?;
            if !weights.contains_key(&class) {
                return Err(invalid(
                    "legacy initial-protection choice has an unsealed mutation class",
                ));
            }
            by_class.entry(class).or_default().push(candidate.clone());
        }
        let total = by_class
            .keys()
            .map(|class| weights.get(class).copied().unwrap_or_default())
            .sum::<u64>();
        if total == 0 {
            return Err(invalid(
                "evolved initial-protection classes have no positive sealed weight",
            ));
        }
        let class_seed = sha(&object([
            ("seed", Value::String(proposal_seed.to_owned())),
            ("parent", Value::String(parent_identity_sha256.clone())),
            ("draw", Value::String("protection_class".to_owned())),
        ]))?;
        let bucket = unbiased_choice(
            &class_seed,
            usize::try_from(total).map_err(|_| {
                invalid("evolved initial-protection class weight total exceeds usize")
            })?,
        )? as u64;
        let mut cursor = 0_u64;
        let mut selected = None;
        for class in by_class.keys() {
            cursor += weights.get(class).copied().unwrap_or_default();
            if bucket < cursor {
                selected = Some(class.clone());
                break;
            }
        }
        let selected = selected
            .ok_or_else(|| invalid("evolved initial-protection class selection exhausted"))?;
        let class_candidates = by_class
            .get(&selected)
            .ok_or_else(|| invalid("selected evolved protection class disappeared"))?;
        let plan_seed = sha(&object([
            ("seed", Value::String(proposal_seed.to_owned())),
            ("parent", Value::String(parent_identity_sha256.clone())),
            ("draw", Value::String("protection_plan".to_owned())),
            ("class", Value::String(selected.clone())),
        ]))?;
        (
            class_candidates[unbiased_choice(&plan_seed, class_candidates.len())?].clone(),
            Value::String(selected),
        )
    };
    let mut receipt = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_v5_evolved_operator_selection_v1".to_owned()),
        ),
        ("proposalSeed", Value::String(proposal_seed.to_owned())),
        (
            "parentIdentitySha256",
            Value::String(parent_identity_sha256),
        ),
        ("family", Value::String(family)),
        ("mutationClass", selected_class),
        (
            "nativePlanSha256",
            canonical_clone(required(
                &choice.native_plan,
                "planSha256",
                "selected evolved native plan",
            )?)?,
        ),
        ("legacyChoice", canonical_clone(&choice.legacy_choice)?),
        (
            "legacyChoiceSha256",
            Value::String(choice.legacy_choice_sha256.clone()),
        ),
        (
            "legacyChoiceOrderingSha256",
            Value::String(choice.legacy_choice_ordering_sha256.clone()),
        ),
    ]);
    let receipt_sha = sha(&receipt)?;
    map_mut(&mut receipt, "evolved operator selection receipt")?
        .insert("selectionSha256".to_owned(), Value::String(receipt_sha));
    Ok(V5EvolvedOperatorSelection {
        native_plan: choice.native_plan,
        legacy_choice: choice.legacy_choice,
        receipt,
    })
}

/// Select from a pair-reidentified side state.  Later-generation callers
/// should prefer this over the raw `(pair identity, program, profile)` form:
/// its state invariant rejects reusing the prior step's compiled profile
/// before any deterministic draw is made.
pub(crate) fn select_evolved_operator_choice_from_state(
    proposal_seed: &str,
    state: &V5EvolvedSideState,
    authority: &V5OperatorAuthority,
    admission: &dyn V5EvolvedChildAdmission,
) -> Result<V5EvolvedOperatorSelection> {
    state.validate_for_authority(authority)?;
    select_evolved_operator_choice_with_admission(
        proposal_seed,
        &state.pair_identity_sha256,
        &state.program,
        authority,
        &state.compiled_profile,
        admission,
    )
}

fn current_evolved_operator_choice(
    parent: &Value,
    authority: &V5OperatorAuthority,
    profile: &V5CompiledProfileView,
    selection: &V5EvolvedOperatorSelection,
) -> Result<V5LegacyOperatorChoice> {
    let admission = V5StructuralChildAdmission { authority };
    current_evolved_operator_choice_with_admission(
        parent, authority, profile, selection, &admission,
    )
}

fn current_evolved_operator_choice_with_admission(
    parent: &Value,
    authority: &V5OperatorAuthority,
    profile: &V5CompiledProfileView,
    selection: &V5EvolvedOperatorSelection,
    admission: &dyn V5EvolvedChildAdmission,
) -> Result<V5LegacyOperatorChoice> {
    let expected_choice_sha256 = sha(&selection.legacy_choice)?;
    enumerate_evolved_operator_choices_with_admission(parent, authority, profile, admission)?
        .into_iter()
        .find(|candidate| {
            candidate.legacy_choice_sha256 == expected_choice_sha256
                && candidate.legacy_choice == selection.legacy_choice
                && candidate.native_plan == selection.native_plan
        })
        .ok_or_else(|| {
            invalid("evolved operator selection is stale, foreign, or not currently applicable")
        })
}

/// Apply a selected production choice only after rebuilding the exact current
/// Python vocabulary.  The pair transaction supplies the parent pair identity
/// because it, rather than this side-local engine, owns compilation of the
/// opposite side and assignment of the next pair identity.
pub(crate) fn execute_evolved_operator_selection(
    parent_pair_identity_sha256: &str,
    parent_program: &Value,
    authority: &V5OperatorAuthority,
    profile: &V5CompiledProfileView,
    selection: V5EvolvedOperatorSelection,
) -> Result<V5EvolvedOperatorExecution> {
    let admission = V5StructuralChildAdmission { authority };
    execute_evolved_operator_selection_with_admission(
        parent_pair_identity_sha256,
        parent_program,
        authority,
        profile,
        selection,
        &admission,
    )
}

/// Execute a previously selected production choice after rebuilding its full
/// compiler-admitted vocabulary.  A stale selection is recorded as a typed
/// rejected outcome; an accepted result exposes only side-local delta
/// material so the transaction must compile and re-identify the next pair.
pub(crate) fn execute_evolved_operator_selection_with_admission(
    parent_pair_identity_sha256: &str,
    parent_program: &Value,
    authority: &V5OperatorAuthority,
    profile: &V5CompiledProfileView,
    selection: V5EvolvedOperatorSelection,
    admission: &dyn V5EvolvedChildAdmission,
) -> Result<V5EvolvedOperatorExecution> {
    let parent_pair_identity_sha256 = sha256_identifier(
        &Value::String(parent_pair_identity_sha256.to_owned()),
        "evolved operator execution parent pair identity SHA-256",
    )?;
    let result_for = |disposition,
                      reason_code: &str,
                      detail: Value,
                      application: Option<&V5OperatorApplication>| {
        execution_result(
            disposition,
            authority,
            parent_program,
            &selection.native_plan,
            reason_code,
            detail,
            application,
        )
    };
    match current_evolved_operator_choice_with_admission(
        parent_program,
        authority,
        profile,
        &selection,
        admission,
    )
    .and_then(|choice| apply_verified_operator_plan(parent_program, authority, choice.native_plan))
    {
        Ok(application) => {
            let trace = canonical_clone(required(
                &application.audit,
                "mutationTrace",
                "evolved operator application",
            )?)?;
            let child_program_sha256 = sha(&application.child_program)?;
            let delta = V5EvolvedOperatorDelta {
                side: authority.side().to_owned(),
                parent_pair_identity_sha256,
                parent_program_sha256: sha(parent_program)?,
                child_program: canonical_clone(&application.child_program)?,
                child_program_sha256,
                native_plan: canonical_clone(&selection.native_plan)?,
                legacy_choice: canonical_clone(&selection.legacy_choice)?,
                trace,
            };
            Ok(V5EvolvedOperatorExecution {
                disposition: V5OperatorDisposition::Accepted,
                reason_code: "accepted".to_owned(),
                result: result_for(
                    V5OperatorDisposition::Accepted,
                    "accepted",
                    object([("kind", Value::String("applied".to_owned()))]),
                    Some(&application),
                )?,
                selection,
                application: Some(application),
                delta: Some(delta),
            })
        }
        Err(error) => {
            let no_op =
                matches!(&error, V5OperatorError::Invalid(message) if message.contains("no-op"));
            let disposition = if no_op {
                V5OperatorDisposition::NoOp
            } else {
                V5OperatorDisposition::Rejected
            };
            let reason_code = if no_op { "no_op" } else { "operator_rejected" };
            Ok(V5EvolvedOperatorExecution {
                disposition,
                reason_code: reason_code.to_owned(),
                result: result_for(
                    disposition,
                    reason_code,
                    Value::String(error.to_string()),
                    None,
                )?,
                selection,
                application: None,
                delta: None,
            })
        }
    }
}

/// Execute one selected operation and require a transaction-owned pair
/// recompilation before returning state that can drive another selection.
///
/// A structural child can be accepted while the bidirectional pair compiler
/// rejects it.  That is represented as a deterministic rejected pair-step,
/// with the evolved delta/trace retained for the journal but without a next
/// state, so callers cannot accidentally continue from the uncompiled child.
pub(crate) fn execute_evolved_operator_step_from_state(
    state: &V5EvolvedSideState,
    authority: &V5OperatorAuthority,
    selection: V5EvolvedOperatorSelection,
    admission: &dyn V5EvolvedChildAdmission,
    recompiler: &dyn V5EvolvedPairRecompiler,
) -> Result<V5EvolvedPairStepResult> {
    state.validate_for_authority(authority)?;
    let operator_execution = execute_evolved_operator_selection_with_admission(
        &state.pair_identity_sha256,
        &state.program,
        authority,
        &state.compiled_profile,
        selection,
        admission,
    )?;
    let delta = operator_execution.delta.clone();
    if operator_execution.disposition != V5OperatorDisposition::Accepted {
        return Ok(V5EvolvedPairStepResult {
            disposition: operator_execution.disposition,
            reason_code: operator_execution.reason_code.clone(),
            reason_detail: object([(
                "operatorResult",
                canonical_clone(&operator_execution.result.value)?,
            )]),
            operator_execution,
            delta,
            next_side_state: None,
        });
    }
    let delta_ref = delta.as_ref().ok_or_else(|| {
        invalid("accepted evolved operator execution omitted its side-local delta")
    })?;
    let next_side_state = match recompiler.recompile_evolved_pair(delta_ref) {
        Ok(recompiled) => {
            if recompiled.pair_identity_sha256 == state.pair_identity_sha256 {
                Err(invalid(
                    "pair recompiler reused the parent pair identity for an evolved child",
                ))
            } else {
                V5EvolvedSideState::from_recompiled_pair(
                    recompiled.pair_identity_sha256,
                    recompiled.module_identity_sha256,
                    authority,
                    canonical_clone(&delta_ref.child_program)?,
                    recompiled.compiled_profile,
                )
            }
        }
        Err(error) => Err(error),
    };
    match next_side_state {
        Ok(next_side_state) => Ok(V5EvolvedPairStepResult {
            disposition: V5OperatorDisposition::Accepted,
            reason_code: "accepted".to_owned(),
            reason_detail: object([
                ("kind", Value::String("pair_recompiled".to_owned())),
                (
                    "childPairIdentitySha256",
                    Value::String(next_side_state.pair_identity_sha256.clone()),
                ),
            ]),
            operator_execution,
            delta,
            next_side_state: Some(next_side_state),
        }),
        Err(error) => Ok(V5EvolvedPairStepResult {
            disposition: V5OperatorDisposition::Rejected,
            reason_code: "pair_recompile_rejected".to_owned(),
            reason_detail: Value::String(error.to_string()),
            operator_execution,
            delta,
            next_side_state: None,
        }),
    }
}

#[cfg(test)]
mod dashboard_schema_oracle_tests {
    use super::*;
    use serde_json::json;

    const OCCURRENCE: &str =
        "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn validate_one(guard: Value) -> Result<()> {
        let groups = BTreeSet::from(["group".to_owned()]);
        let events = BTreeSet::from(["event".to_owned()]);
        validate_guard(
            &guard,
            &groups,
            &events,
            4,
            1,
            &mut BTreeSet::new(),
            &mut BTreeSet::new(),
        )
    }

    fn sealed_temporal_domains() -> Value {
        let mut value = json!({
            "schemaVersion":"temporal_qd_v5_temporal_domains_v1",
            "eventAges":[0,1,2,3,5,8,13,21],
            "positionAges":[1,2,3,5,8,13,21,34],
            "utcSessionWindows":[[0,360],[360,720],[420,960],[720,1080],[780,1260],[1080,1439]],
            "eventAgeWindows":[[0,1],[0,3],[1,1],[1,3],[2,5]],
            "consecutiveCounts":[2,3,5],
            "cooldownCounts":[1,3,5]
        });
        let identity = sha(&value).expect("temporal domain identity");
        map_mut(&mut value, "temporal domains")
            .expect("domain object")
            .insert("temporalDomainsSha256".to_owned(), Value::String(identity));
        value
    }

    fn stopped_program() -> Value {
        let fixture: Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../../tests/fixtures/temporal_qd_v5_stopped_run_oracle.json"
        )))
        .expect("stopped-run fixture is JSON");
        canonical_clone(&fixture["construction"]["sides"]["long"]["program"])
            .expect("fixture program canonicalizes")
    }

    fn stopped_authority(program: &Value) -> V5OperatorAuthority {
        let indicators = resource_rows(program, "indicators").expect("program indicators");
        let catalog = json!({
            "indicators": indicators,
            "timeframes": {"M5": {}}
        });
        let policy = json!({
            "timeframePolicy":["M5"],
            "evidenceLookbackChoices":[1,2,3,5]
        });
        let temporal_domains = sealed_temporal_domains();
        let authority = V5OperatorAuthority::from_sealed_static_parts(
            &sha(&json!({"authority":"test"})).expect("authority SHA"),
            "long",
            "EURUSD",
            required(program, "budget", "program").expect("budget"),
            &catalog,
            &policy,
            &json!({"enabled":false}),
            &default_initial_protection_policy().expect("protection policy"),
            &temporal_domains,
        )
        .expect("sealed test authority");
        let catalog_sha256 = sha(&json!({
            "payload": catalog,
            "timeframePolicy":["M5"]
        }))
        .expect("catalog identity");
        let resource_spec_sha256 = sha(&json!({
            "schemaVersion":"evolvable_module_resource_operator_plan_v1",
            "operatorVersion": RESOURCE_OPERATOR_VERSION,
            "catalogSha256":catalog_sha256,
            "timeframePolicy":["M5"],
            "rawEvents":"fresh_only_v1",
            "weights":{"positive":true,"normalizedWithinExclusiveGroup":true,"minimum":0.25}
        }))
        .expect("resource specification identity");
        let compiler_sha = sha(&json!({"compiler":"test"})).expect("compiler identity");
        let temporal_domain_projection = temporal_domain_selection_projection(&temporal_domains)
            .expect("temporal selection projection");
        let mut temporal_specification = json!({
            "schemaVersion":"evolvable_module_temporal_operator_plan_v1",
            "operatorVersion": TEMPORAL_OPERATOR_VERSION,
            "domains":temporal_domain_projection,
            "guardFamilies":[
                "predicate_edge","consecutive_true","event_age_window",
                "fresh_event_absence","state_or_position_age","utc_session_window",
                "action_cooldown_elapsed"
            ],
            "compilerPolicySha256":compiler_sha,
            "nativeValidation":false
        });
        let temporal_spec_sha256 =
            sha(&temporal_specification).expect("temporal specification identity");
        map_mut(&mut temporal_specification, "temporal specification")
            .expect("temporal specification object")
            .insert(
                "operatorSpecSha256".to_owned(),
                Value::String(temporal_spec_sha256.clone()),
            );
        authority
            .with_legacy_selection_static(
                &catalog_sha256,
                &resource_spec_sha256,
                &compiler_sha,
                &temporal_specification,
                &temporal_spec_sha256,
            )
            .expect("sealed legacy selection authority")
    }

    fn two_member_exclusive_fuzzy_program() -> Value {
        let mut program = stopped_program();
        let indicators = required_mut(
            map_mut(
                required_mut(
                    map_mut(&mut program, "program").expect("program object"),
                    "resources",
                    "program",
                )
                .expect("resources"),
                "resources",
            )
            .expect("resources object"),
            "indicators",
            "resources",
        )
        .expect("indicators")
        .as_array_mut()
        .expect("indicator rows");
        let mut second = canonical_clone(indicators.first().expect("one stopped indicator"))
            .expect("clone stopped indicator");
        let meta = map_mut(
            required_mut(
                map_mut(&mut second, "indicator").expect("second indicator object"),
                "meta",
                "indicator",
            )
            .expect("second indicator metadata"),
            "indicator meta",
        )
        .expect("second indicator metadata");
        meta.insert(
            "id".to_owned(),
            Value::String("SYNTHETIC_SECOND_FUZZY".to_owned()),
        );
        meta.insert(
            "instanceId".to_owned(),
            Value::String("synthetic_second_fuzzy".to_owned()),
        );
        indicators.push(second);

        let groups = required_mut(
            map_mut(
                required_mut(
                    map_mut(&mut program, "program").expect("program object"),
                    "resources",
                    "program",
                )
                .expect("resources"),
                "resources",
            )
            .expect("resources object"),
            "evidenceGroups",
            "resources",
        )
        .expect("evidence groups")
        .as_array_mut()
        .expect("evidence group rows");
        let group = map_mut(
            groups.first_mut().expect("one stopped group"),
            "evidence group",
        )
        .expect("group object");
        let first_member = group
            .get("indicatorInstanceIds")
            .expect("first group members")
            .as_array()
            .expect("first group member array")
            .first()
            .and_then(Value::as_str)
            .expect("first group member")
            .to_owned();
        group.insert(
            "indicatorInstanceIds".to_owned(),
            array([
                Value::String(first_member),
                Value::String("synthetic_second_fuzzy".to_owned()),
            ]),
        );
        normalize_program(&program).expect("canonical synthetic fuzzy program")
    }

    #[test]
    fn generated_guard_kinds_match_dashboard_unions() {
        // One direct oracle fixture for every guard family emitted by the
        // temporal operator, plus the native execution guards it can retain.
        let cases = vec![
            json!({"kind":"always"}),
            json!({"kind":"all","guards":[{"kind":"state_age_at_least","events":0}]}),
            json!({"kind":"any","guards":[{"kind":"evidence_below","groupId":"group","thresholdPercent":40.0}]}),
            json!({"kind":"not","guard":{"kind":"fresh_event","eventId":"event"}}),
            json!({"kind":"evidence_at_least","groupId":"group","thresholdPercent":55.0}),
            json!({"kind":"evidence_below","groupId":"group","thresholdPercent":45.0}),
            json!({"kind":"fresh_event","eventId":"event"}),
            json!({"kind":"event_age_window","eventId":"event","minimumEvents":1,"maximumEvents":3}),
            json!({"kind":"event_age_at_most","eventId":"event","events":2}),
            json!({"kind":"condition_streak_at_least","groupId":"group","comparison":"at_least","thresholdPercent":65.0,"events":3}),
            // Dashboard permits weekdays:null and overnight sessions.
            json!({"kind":"utc_time_window","startMinute":1080,"endMinute":360,"weekdays":null}),
            json!({"kind":"state_age_at_least","events":5}),
            json!({"kind":"state_age_at_most","events":5}),
            json!({"kind":"position_exists","expected":false}),
            json!({"kind":"position_age_at_least","events":5}),
            json!({"kind":"unrealized_r_at_least","value":0.75}),
            json!({"kind":"unrealized_r_at_most","value":-0.75}),
            json!({"kind":"predicate_edge","operatorId":"edge_trigger_predicate_v1","operatorVersion":"1","occurrenceSha256":OCCURRENCE,"direction":"rising","predicate":{"kind":"state_age_at_least","events":2}}),
            json!({"kind":"consecutive_true","operatorId":"require_consecutive_true_v1","operatorVersion":"1","occurrenceSha256":OCCURRENCE,"predicate":{"kind":"position_exists","expected":true},"evaluations":5}),
            json!({"kind":"action_cooldown_elapsed","transitionId":"hub_manage","actionOrdinal":3,"evaluations":3}),
            json!({"kind":"execution_status_is","status":"applied"}),
            json!({"kind":"execution_reason_is","reasonCode":"stop_loss"}),
        ];
        for guard in cases {
            validate_one(guard).expect("Dashboard-valid guard fixture must be accepted");
        }
    }

    #[test]
    fn rejects_foreign_or_stale_guard_shapes() {
        let rejected = vec![
            json!({"kind":"event_age_at_most","events":2}),
            json!({"kind":"condition_streak_at_least","groupId":"group","comparison":"at_least","thresholdPercent":65.0,"events":0}),
            json!({"kind":"unrealized_r_at_least","multiple":0.75}),
            json!({"kind":"position_age_at_most","events":3}),
            json!({"kind":"initial_r_multiple","multiple":1.0}),
            json!({"kind":"utc_time_window","startMinute":360,"endMinute":360,"weekdays":null}),
            json!({"kind":"state_age_at_least","events":1_000_001}),
            json!({"kind":"position_age_at_least","events":10_000_001}),
            json!({"kind":"action_cooldown_elapsed","transitionId":"hub_manage","actionOrdinal":4,"evaluations":1}),
            json!({"kind":"predicate_edge","operatorId":"edge_trigger_predicate_v1","operatorVersion":"1","occurrenceSha256":"bad","direction":"rising","predicate":{"kind":"state_age_at_least","events":2}}),
            json!({"kind":"predicate_edge","operatorId":"edge_trigger_predicate_v1","operatorVersion":"1","occurrenceSha256":OCCURRENCE,"direction":"rising","predicate":{"kind":"fresh_event","eventId":"event"}}),
            json!({"kind":"execution_reason_is","reasonCode":"not valid"}),
        ];
        for guard in rejected {
            assert!(
                validate_one(guard).is_err(),
                "foreign/stale guard shape must fail closed"
            );
        }
    }

    #[test]
    fn management_locator_unions_match_dashboard_models() {
        let bindings = BTreeMap::from([
            (
                "level".to_owned(),
                json!({"id":"level","valueKind":"price_level"}),
            ),
            (
                "distance".to_owned(),
                json!({"id":"distance","valueKind":"price_distance"}),
            ),
        ]);
        for locator in [
            json!({"kind":"fixed_percent","percent":99.99}),
            json!({"kind":"indicator_price_level","bindingId":"level"}),
            json!({"kind":"indicator_distance_multiple","bindingId":"distance","multiple":100.0}),
        ] {
            validate_initial_stop_locator(&locator, &bindings)
                .expect("valid InitialStop union member must pass");
        }
        for locator in [
            json!({"kind":"reward_multiple","multiple":250.0}),
            json!({"kind":"fixed_percent","percent":99.99}),
            json!({"kind":"indicator_price_level","bindingId":"level"}),
            json!({"kind":"indicator_distance_multiple","bindingId":"distance","multiple":100.0}),
            json!({"kind":"none"}),
        ] {
            validate_initial_target_locator(&locator, &bindings, true, "initial target")
                .expect("valid InitialTarget union member must pass");
        }
        for anchor in [
            json!({"kind":"bar_close"}),
            json!({"kind":"favorable_bar_extreme"}),
            json!({"kind":"indicator_price_level","bindingId":"level"}),
        ] {
            validate_trailing_anchor(&anchor, &bindings)
                .expect("valid TrailingAnchor union member must pass");
        }
        for distance in [
            json!({"kind":"fixed_percent_of_entry","percent":99.99}),
            json!({"kind":"fixed_initial_r","multiple":100.0}),
            json!({"kind":"indicator_distance_multiple","bindingId":"distance","multiple":100.0}),
        ] {
            validate_trailing_distance(&distance, &bindings)
                .expect("valid TrailingDistance union member must pass");
        }
        for activation in [
            json!({"kind":"immediate"}),
            json!({"kind":"explicit"}),
            json!({"kind":"after_unrealized_r","value":100.0}),
            json!({"kind":"after_position_age","bars":1}),
            json!({"kind":"after_r_and_age","value":100.0,"bars":1}),
        ] {
            validate_trailing_activation(&activation)
                .expect("valid TrailingActivation union member must pass");
        }

        for locator in [
            json!({"kind":"fixed_percent","percent":100.0}),
            json!({"kind":"reward_multiple","multiple":1.0}),
            json!({"kind":"fixed_initial_r","multiple":1.0}),
        ] {
            assert!(validate_initial_stop_locator(&locator, &bindings).is_err());
        }
        assert!(
            validate_initial_target_locator(
                &json!({"kind":"bar_close"}),
                &bindings,
                true,
                "initial target"
            )
            .is_err()
        );
        assert!(validate_trailing_anchor(&json!({"kind":"immediate"}), &bindings).is_err());
        assert!(
            validate_trailing_distance(
                &json!({"kind":"fixed_percent_of_entry","percent":100.0}),
                &bindings
            )
            .is_err()
        );

        let indicators = BTreeSet::new();
        let management = json!({
            "id":"management","ownerSide":"long",
            "initialStop":{"kind":"fixed_percent","percent":1.0},
            "initialTarget":{"kind":"none"},
            "trailingStop":{
                "anchor":{"kind":"bar_close"},
                "distance":{"kind":"fixed_initial_r","multiple":100.0},
                "activation":{"kind":"immediate"},
                "minimumStepInitialR":100.0
            }
        });
        validate_management_ref(&management, "long", &indicators)
            .expect("inclusive trailing maxima must pass");
    }

    #[test]
    fn recovery_is_reachable_from_post_position_close_and_rearms_to_start() {
        let mut program = stopped_program();
        let nodes = required_mut(
            map_mut(&mut program, "program").expect("program is an object"),
            "nodes",
            "program",
        )
        .expect("nodes exist")
        .as_array_mut()
        .expect("nodes are an array");
        nodes.push(json!({"id":"recover","zone":"recovery","kind":"recovery","guard":{},"resources":[],"timeoutBars":1}));
        validate_v5_operator_graph(&program).expect(
            "bare bounded recovery is reachable from implicit post-position close and re-arms",
        );
        let edges = required_mut(
            map_mut(&mut program, "program").expect("program is an object"),
            "edges",
            "program",
        )
        .expect("edges exist")
        .as_array_mut()
        .expect("edges are an array");
        edges.push(json!({"id":"start_recover","source":"start","target":"recover","eventClass":"decision","priority":20,"guard":{},"effect":null}));
        validate_v5_operator_graph(&program)
            .expect("authored entry-to-recovery remains compatible with the implicit re-arm chain");
    }

    #[test]
    fn trusted_authority_closes_catalog_policy_and_cooldown_references() {
        let program = stopped_program();
        let authority = stopped_authority(&program);
        validate_program(&program, &authority).expect("real stopped program is catalog-bound");

        let mut forged = canonical_clone(&program).expect("clone program");
        let resources = required_mut(
            map_mut(&mut forged, "program").expect("program"),
            "resources",
            "program",
        )
        .expect("resources");
        let indicators = required_mut(
            map_mut(resources, "resources").expect("resources object"),
            "indicators",
            "resources",
        )
        .expect("indicators");
        let first = indicators
            .as_array_mut()
            .expect("indicator array")
            .first_mut()
            .expect("one indicator");
        let indicator = required_mut(
            map_mut(first, "indicator").expect("indicator object"),
            "meta",
            "indicator",
        )
        .expect("indicator meta");
        map_mut(indicator, "indicator meta")
            .expect("meta object")
            .insert(
                "name".to_owned(),
                Value::String("forged catalog metadata".to_owned()),
            );
        assert!(validate_program(&forged, &authority).is_err());

        let edge = json!({
            "id":"cooldown_probe","source":"hub","target":"manage","eventClass":"decision","priority":99,
            "guard":{"kind":"action_cooldown_elapsed","transitionId":"e_cooldown_probe","actionOrdinal":0,"evaluations":1},
            "effect":"tighten_stop_next_open"
        });
        // Directly exercise generated-subset closure without changing the
        // frozen graph's canonical edge ordering / priority contract.
        let mut cooldown_program = canonical_clone(&program).expect("clone program");
        required_mut(
            map_mut(&mut cooldown_program, "program").expect("program"),
            "edges",
            "program",
        )
        .expect("edges")
        .as_array_mut()
        .expect("edge array")
        .push(edge);
        assert!(validate_generated_cooldown_closure(&cooldown_program).is_ok());
        let set_probe_transition = |program: &mut Value, transition_id: &str| {
            let edges = required_mut(
                map_mut(program, "program").expect("program"),
                "edges",
                "program",
            )
            .expect("edges");
            let probe = edges
                .as_array_mut()
                .expect("edge array")
                .last_mut()
                .expect("probe edge");
            let guard = required_mut(
                map_mut(probe, "probe edge").expect("probe edge object"),
                "guard",
                "probe edge",
            )
            .expect("probe guard");
            map_mut(guard, "probe guard").expect("guard object").insert(
                "transitionId".to_owned(),
                Value::String(transition_id.to_owned()),
            );
        };
        set_probe_transition(&mut cooldown_program, "e_missing");
        assert!(validate_generated_cooldown_closure(&cooldown_program).is_err());
        set_probe_transition(&mut cooldown_program, "e_hub_manage");
        assert!(
            validate_generated_cooldown_closure(&cooldown_program).is_err(),
            "an existing cross-edge action must not satisfy containing-action closure",
        );
    }

    #[test]
    fn typed_plan_replay_has_authority_bound_outcomes() {
        let program = stopped_program();
        let authority = stopped_authority(&program);
        let plans = enumerate_operator_plans(&program, &authority)
            .expect("trusted program produces deterministic plans");
        assert!(
            !plans.is_empty(),
            "stopped program should expose at least one mutation"
        );
        let execution = execute_operator_plan(&program, &authority, &plans[0])
            .expect("journal-safe operator execution");
        assert_eq!(execution.disposition, V5OperatorDisposition::Accepted);
        let application = execution.application.expect("accepted child application");
        assert_eq!(
            object_get(&execution.result.value, "applicationSha256"),
            object_get(&application.audit, "applicationSha256"),
        );
        let mut stale = canonical_clone(&plans[0]).expect("clone plan");
        map_mut(&mut stale, "plan").expect("plan object").insert(
            "authoritySha256".to_owned(),
            Value::String(sha(&json!({"foreign":true})).expect("sha")),
        );
        let rejected = execute_operator_plan(&program, &authority, &stale)
            .expect("rejection remains journal data");
        assert_eq!(rejected.disposition, V5OperatorDisposition::Rejected);
        assert_eq!(
            object_get(&rejected.result.value, "reasonCode").and_then(Value::as_str),
            Some("operator_rejected"),
        );
    }

    #[test]
    fn evidence_weight_application_requires_its_normalized_postcondition() {
        // General parent validation intentionally accepts the factory shape:
        // both untouched catalog rows retain `weight: 1.0`.  The resource
        // operation is the narrower authority which turns that baseline into
        // a normalized allocation and must reject a tampered `afterWeights`.
        let program = two_member_exclusive_fuzzy_program();
        let authority = stopped_authority(&program);
        validate_program(&program, &authority)
            .expect("untouched multi-member catalog weights remain a valid parent");
        let construction = resource_constructions(&program, &authority)
            .expect("resource constructions")
            .into_iter()
            .find(|construction| {
                object_get(construction, "kind").and_then(Value::as_str)
                    == Some("evidence_weight_mutate")
            })
            .expect("exclusive two-member group exposes a weight construction");
        resource_transform(&program, &authority, &construction)
            .expect("enumerated normalized construction applies");

        let mut tampered = canonical_clone(&construction).expect("clone weight construction");
        let after = map_mut(
            required_mut(
                map_mut(&mut tampered, "weight construction").expect("weight construction object"),
                "afterWeights",
                "weight construction",
            )
            .expect("weight construction after map"),
            "after weights",
        )
        .expect("after weights object");
        for weight in after.values_mut() {
            *weight = value_number(0.25, "tampered weight").expect("finite tampered weight");
        }
        let error = resource_transform(&program, &authority, &tampered)
            .expect_err("wrong-sum resource application must fail");
        assert_eq!(
            error.to_string(),
            "v5 operator: evidence weights must remain normalized"
        );
    }

    #[test]
    fn topology_plans_cross_the_same_authority_bound_apply_gate() {
        let program = stopped_program();
        let authority = stopped_authority(&program);
        let topology = enumerate_operator_plans(&program, &authority)
            .expect("all trusted plans")
            .into_iter()
            .find(|plan| {
                object_get(plan, "operatorId").and_then(Value::as_str)
                    == Some(V5_TOPOLOGY_OPERATOR_ID)
            })
            .expect("stopped program has one admitted topology operation");
        let application = apply_operator_plan(&program, &authority, &topology)
            .expect("topology mutation crosses trusted pre/post validation");
        validate_program(&application.child_program, &authority)
            .expect("topology child remains catalog/policy/graph closed");
        assert_eq!(
            object_get(&application.audit, "operatorId").and_then(Value::as_str),
            Some(V5_TOPOLOGY_OPERATOR_ID)
        );
    }

    #[test]
    fn same_side_crossover_ports_are_closed_replayable_and_authority_bound() {
        let left = stopped_program();
        let right = stopped_program();
        let authority = stopped_authority(&left);
        let entry_setup = crossover_construction(&left, &right, "entry_setup", "start_setup")
            .expect("entry/setup construction");
        apply_same_side_crossover_plan(&left, &right, &authority, &entry_setup)
            .expect("entry/setup crossover must be an admitted typed port");
        let plans = enumerate_same_side_crossover_plans(&left, &right, &authority)
            .expect("same closed program exposes three compatible motif ports");
        let ports = plans
            .iter()
            .map(|plan| {
                let segments = map_ref(
                    required(plan, "segmentMap", "crossover plan").expect("segment map"),
                    "crossover segment map",
                )
                .expect("segment map object");
                assert_eq!(segments.len(), 1, "live Python authority selects one motif");
                segments.keys().next().cloned().expect("one motif port")
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(
            ports,
            BTreeSet::from([
                "entry_setup".to_owned(),
                "management_hub".to_owned(),
                "exit_hub".to_owned(),
            ])
        );
        for plan in &plans {
            let applied = apply_same_side_crossover_plan(&left, &right, &authority, plan)
                .expect("enumerated crossover plan replays through trusted gate");
            validate_program(&applied.child_program, &authority)
                .expect("crossover child remains authority-closed");
            let delta = required(&applied.audit, "semanticDelta", "crossover audit")
                .expect("Python-shaped semantic delta");
            assert_eq!(
                object_get(delta, "beforeTopologySha256"),
                Some(&Value::String(
                    v5_topology_operators::semantic_topology_sha256(&left)
                        .expect("before topology identity")
                ))
            );
            assert_eq!(
                object_get(delta, "childGenomeSha256"),
                Some(&Value::String(
                    sha(&applied.child_program).expect("child program identity")
                ))
            );
        }

        let selected =
            select_same_side_crossover_plan("crossover-selection-seed", &left, &right, &authority)
                .expect("exact factory-stream crossover selection");
        let replay =
            select_same_side_crossover_plan("crossover-selection-seed", &left, &right, &authority)
                .expect("crossover selection replay");
        assert_eq!(selected, replay, "crossover selection is seed-bound");
        apply_same_side_crossover_plan(&left, &right, &authority, &selected.plan)
            .expect("selected plan is immediately replayable");

        let mut stale = canonical_clone(&plans[0]).expect("clone crossover plan");
        map_mut(&mut stale, "crossover plan")
            .expect("crossover plan object")
            .insert(
                "leftGenomeSha256".to_owned(),
                Value::String(sha(&json!({"foreign":true})).expect("foreign SHA")),
            );
        assert!(
            apply_same_side_crossover_plan(&left, &right, &authority, &stale).is_err(),
            "self-consistent-looking stale ordered parent binding must fail closed"
        );

        let mut wrong_side = canonical_clone(&right).expect("clone right parent");
        map_mut(&mut wrong_side, "right parent")
            .expect("right parent object")
            .insert("direction".to_owned(), Value::String("short".to_owned()));
        assert!(
            enumerate_same_side_crossover_plans(&left, &wrong_side, &authority).is_err(),
            "opposite-side parent must fail before motif planning"
        );
    }

    #[test]
    fn operation_selection_uses_pair_identity_and_is_replayable() {
        let program = stopped_program();
        let authority = stopped_authority(&program);
        let ordered =
            enumerate_operator_plans(&program, &authority).expect("ordered operation choices");
        let wrapper_identities = ordered
            .iter()
            .map(choice_wrapper_sha256)
            .collect::<Result<Vec<_>>>()
            .expect("choice wrappers hash");
        let mut expected_order = wrapper_identities.clone();
        expected_order.sort();
        assert_eq!(
            wrapper_identities, expected_order,
            "public plans retain Python wrapper-hash operation-choice order"
        );
        let pair_identity = sha(&json!({"pair":"selection-test"})).expect("pair identity");
        let first = select_operator_plan("selection-seed", &pair_identity, &program, &authority)
            .expect("operator selection");
        let replay = select_operator_plan("selection-seed", &pair_identity, &program, &authority)
            .expect("operator selection replay");
        assert_eq!(first, replay, "selection must be entirely identity-bound");
        let family = object_get(&first.receipt, "family")
            .and_then(Value::as_str)
            .expect("family receipt");
        assert_eq!(
            selection_family(&first.plan).expect("selected family"),
            family
        );
        assert_eq!(
            object_get(&first.receipt, "planSha256"),
            object_get(&first.plan, "planSha256"),
        );
        let selected_wrapper = choice_wrapper_sha256(&first.plan).expect("selected wrapper hash");
        assert_eq!(
            object_get(&first.receipt, "choiceWrapperSha256").and_then(Value::as_str),
            Some(selected_wrapper.as_str()),
            "selection receipt binds the actual Python-equivalent wrapper identity"
        );
        if family == "initial_protection" {
            assert!(object_get(&first.receipt, "mutationClass").is_some_and(Value::is_string));
        } else {
            assert_eq!(
                object_get(&first.receipt, "mutationClass"),
                Some(&Value::Null)
            );
        }
    }

    #[test]
    fn proposal_side_and_mutation_depth_match_python_hash_routing() {
        // Goldens generated by `proposal_side` and
        // `_mutation_depth_for_seed` in temporal_bidirectional_genome.py /
        // temporal_qd_pair_generation.py.  Include a whitespace-bearing seed
        // because side routing trims its public identifier whereas the depth
        // helper receives the raw proposal string.
        for (seed, side, depth) in [
            ("selection-seed", "short", 1_u8),
            ("alpha", "short", 2_u8),
            ("beta", "short", 1_u8),
            ("0", "long", 2_u8),
            ("proposal-0001", "long", 1_u8),
            ("  trim-me  ", "short", 2_u8),
        ] {
            assert_eq!(proposal_side_for_seed(seed).expect("proposal side"), side);
            assert_eq!(
                mutation_depth_for_seed(seed).expect("mutation depth"),
                depth
            );
        }
        assert!(proposal_side_for_seed(" ").is_err());
        assert!(mutation_depth_for_seed(" ").is_err());
    }
}
