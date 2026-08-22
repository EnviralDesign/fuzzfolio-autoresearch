//! Opaque Dashboard-backed pair construction boundary.
//!
//! This module intentionally owns request shaping only.  The Dashboard keeps
//! ownership of profile hydration, native validation, pair compilation, graph
//! semantics, and descriptor derivation.  A generation must fail closed when
//! that authority cannot execute an admitted intent.

use temporal_qd_contract::{ContractError, Map, Value, canonical_sha256};

use crate::identity::{Side, immigrant_side_seed};

pub const NATIVE_PROPOSAL_AUTHORITY_SCHEMA: &str = "temporal_qd_native_pair_proposal_authority_v1";
pub const PROPOSAL_INTENT_SCHEMA: &str = "temporal_qd_pair_proposal_intent_v1";

#[derive(Debug, thiserror::Error)]
pub enum FactoryError {
    #[error("canonical contract failure: {0}")]
    Canonical(#[from] ContractError),
    #[error("pair factory contract failure: {0}")]
    Contract(String),
    #[error("Dashboard proposal authority failure: {0}")]
    Authority(String),
}

pub type Result<T> = std::result::Result<T, FactoryError>;

fn contract(message: impl Into<String>) -> FactoryError {
    FactoryError::Contract(message.into())
}

fn object(entries: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    let mut map = Map::new();
    for (key, value) in entries {
        map.insert(key.to_owned(), value);
    }
    Value::Object(map)
}

fn exact_sha256(value: &str, label: &str) -> Result<()> {
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

fn nonempty(value: &str, label: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(contract(format!("{label} must be nonempty")));
    }
    Ok(())
}

/// Opaque parent material.  The Rust scheduler owns its identity/order, but
/// only the native authority is allowed to interpret `pair_payload`.
#[derive(Clone, Debug, PartialEq)]
pub struct ParentReference {
    pub pair_identity_sha256: String,
    pub candidate_id: String,
    pub pair_payload: Value,
    pub selection_audit: Option<Value>,
}

impl ParentReference {
    pub fn validate(&self) -> Result<()> {
        exact_sha256(&self.pair_identity_sha256, "parent pair identity")?;
        nonempty(&self.candidate_id, "parent candidate ID")?;
        if !self.pair_payload.is_object() {
            return Err(contract("parent pair payload must be an opaque object"));
        }
        Ok(())
    }

    pub fn compact_value(&self) -> Value {
        object([
            (
                "pairIdentitySha256",
                Value::String(self.pair_identity_sha256.clone()),
            ),
            ("candidateId", Value::String(self.candidate_id.clone())),
            (
                "selectionAudit",
                self.selection_audit.clone().unwrap_or(Value::Null),
            ),
        ])
    }
}

#[derive(Clone, Debug)]
pub enum ProposalIntent {
    RichImmigrant {
        proposal_seed: String,
        long_seed: String,
        short_seed: String,
    },
    StructuralMutation {
        proposal_seed: String,
        parent: ParentReference,
        mutation_depth: u8,
        forced_operator_family: Option<String>,
    },
    SameSideCrossover {
        proposal_seed: String,
        side: Side,
        parent: ParentReference,
        mate: ParentReference,
        mate_selection_attempts: Vec<Value>,
    },
}

impl ProposalIntent {
    pub fn proposal_seed(&self) -> &str {
        match self {
            Self::RichImmigrant { proposal_seed, .. }
            | Self::StructuralMutation { proposal_seed, .. }
            | Self::SameSideCrossover { proposal_seed, .. } => proposal_seed,
        }
    }

    pub fn origin_kind(&self) -> &'static str {
        match self {
            Self::RichImmigrant { .. } => "random_immigrant",
            Self::StructuralMutation { .. } | Self::SameSideCrossover { .. } => {
                "structural_offspring"
            }
        }
    }

    pub fn forced_operator_family(&self) -> Option<&str> {
        match self {
            Self::StructuralMutation {
                forced_operator_family,
                ..
            } => forced_operator_family.as_deref(),
            _ => None,
        }
    }

    pub fn scheduled_kind(&self) -> &'static str {
        match self {
            Self::RichImmigrant { .. } => "random_immigrant",
            Self::StructuralMutation { .. } => "structural_offspring",
            Self::SameSideCrossover { .. } => "same_side_crossover",
        }
    }

    pub fn validate(&self) -> Result<()> {
        exact_sha256(self.proposal_seed(), "proposal seed")?;
        match self {
            Self::RichImmigrant {
                proposal_seed,
                long_seed,
                short_seed,
            } => {
                if long_seed != &immigrant_side_seed(proposal_seed, Side::Long)
                    || short_seed != &immigrant_side_seed(proposal_seed, Side::Short)
                {
                    return Err(contract(
                        "rich immigrant side seeds do not bind the proposal seed",
                    ));
                }
            }
            Self::StructuralMutation {
                parent,
                mutation_depth,
                forced_operator_family,
                ..
            } => {
                parent.validate()?;
                if !(1..=3).contains(mutation_depth) {
                    return Err(contract(
                        "structural mutation depth is outside the admitted 1..3 schedule",
                    ));
                }
                if let Some(family) = forced_operator_family {
                    if *mutation_depth != 1 {
                        return Err(contract(
                            "forced operator family requires mutation depth 1",
                        ));
                    }
                    if !crate::operator_family_matrix::is_matrix_family(family) {
                        return Err(contract(
                            "forced operator family is not a matrix family",
                        ));
                    }
                }
            }
            Self::SameSideCrossover {
                parent,
                mate,
                side: _,
                ..
            } => {
                parent.validate()?;
                mate.validate()?;
                if parent.pair_identity_sha256 == mate.pair_identity_sha256 {
                    return Err(contract(
                        "same-side crossover requires distinct parent pair identities",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Stable request payload for an external authority.  Parent payloads are
    /// deliberately omitted from compact state but included here, where the
    /// native implementation needs the exact immutable frozen pairs.
    pub fn native_request(&self) -> Value {
        let mut value = object([
            (
                "schemaVersion",
                Value::String(PROPOSAL_INTENT_SCHEMA.to_owned()),
            ),
            ("originKind", Value::String(self.origin_kind().to_owned())),
            (
                "proposalSeed",
                Value::String(self.proposal_seed().to_owned()),
            ),
        ]);
        let fields = value.as_object_mut().expect("proposal intent is an object");
        match self {
            Self::RichImmigrant {
                long_seed,
                short_seed,
                ..
            } => {
                fields.insert("longSeed".to_owned(), Value::String(long_seed.clone()));
                fields.insert("shortSeed".to_owned(), Value::String(short_seed.clone()));
            }
            Self::StructuralMutation {
                parent,
                mutation_depth,
                forced_operator_family,
                ..
            } => {
                fields.insert("mutationDepth".to_owned(), Value::from(*mutation_depth));
                fields.insert("parentPair".to_owned(), parent.pair_payload.clone());
                fields.insert(
                    "parentSelection".to_owned(),
                    parent.selection_audit.clone().unwrap_or(Value::Null),
                );
                if let Some(family) = forced_operator_family {
                    fields.insert(
                        "forcedOperatorFamily".to_owned(),
                        Value::String(family.clone()),
                    );
                }
            }
            Self::SameSideCrossover {
                side,
                parent,
                mate,
                mate_selection_attempts,
                ..
            } => {
                fields.insert("side".to_owned(), Value::String(side.as_str().to_owned()));
                fields.insert("parentPair".to_owned(), parent.pair_payload.clone());
                fields.insert("matePair".to_owned(), mate.pair_payload.clone());
                fields.insert(
                    "parentSelection".to_owned(),
                    parent.selection_audit.clone().unwrap_or(Value::Null),
                );
                fields.insert(
                    "mateSelection".to_owned(),
                    mate.selection_audit.clone().unwrap_or(Value::Null),
                );
                fields.insert(
                    "mateSelectionAttempts".to_owned(),
                    Value::Array(mate_selection_attempts.clone()),
                );
            }
        }
        value
    }

    pub fn compact_value(&self) -> Value {
        let mut request = self.native_request();
        let fields = request
            .as_object_mut()
            .expect("proposal intent is an object");
        fields.remove("parentPair");
        fields.remove("matePair");
        request
    }
}

/// The context whose values must bind exact candidate materialisation.  The
/// native authority owns all graph/validation semantics; Rust only freezes the
/// handoff values that affect the durable proposal bytes.
#[derive(Clone, Debug)]
pub struct NativeConstructionContext {
    pub generation_index: u64,
    pub birth_ordinal: u64,
    pub proposal_ordinal: u64,
    pub pair_policy: Value,
    pub evidence_identity_context: Option<Value>,
    pub frozen_construction_catalog: Option<Value>,
    pub g0_evaluation_width: Option<u64>,
    pub factory_construction_policy: Option<Value>,
}

impl NativeConstructionContext {
    pub fn validate(&self) -> Result<()> {
        if !self.pair_policy.is_object() {
            return Err(contract("pair policy must be an object"));
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
        Ok(())
    }
}

/// The only seam allowed to speak Dashboard-native construction semantics.
/// Implementations are expected to be ordered/fail-closed JSONL clients, but
/// the trait also makes a deterministic fake authority possible for the first
/// native parity and restart gates.
pub trait NativePairAuthority {
    fn authority_identity(&self) -> &Value;
    fn execute(
        &mut self,
        intent: &ProposalIntent,
        context: &NativeConstructionContext,
    ) -> Result<NativeProposal>;
}

/// Opaque authority output.  `proposal` and `candidate` are preserved in the
/// rich compatibility journal without local interpretation.  The scheduler
/// validates only closed identity bindings it owns.
#[derive(Clone, Debug)]
pub struct NativeProposal {
    pub proposal: Value,
    pub candidate: Option<Value>,
    pub executable_semantic_sha256: Option<String>,
    pub predeclared_lake_scope: Option<Value>,
    pub funnel_material: Option<NativeFunnelMaterial>,
}

/// Compact frozen-pair facts required to build the legacy funnel record after
/// an optional G0 persistence projection has rebound candidate identity.
#[derive(Clone, Debug)]
pub struct NativeFunnelMaterial {
    pub raw_source_profile_sha256: String,
    pub resolved_profile_sha256: String,
    pub program_sha256: String,
    pub validation_report_sha256: String,
}

impl NativeFunnelMaterial {
    pub fn validate(&self) -> Result<()> {
        for (value, label) in [
            (
                &self.raw_source_profile_sha256,
                "raw source profile SHA-256",
            ),
            (&self.resolved_profile_sha256, "resolved profile SHA-256"),
            (&self.program_sha256, "program SHA-256"),
            (&self.validation_report_sha256, "validation report SHA-256"),
        ] {
            exact_sha256(value, label)?;
        }
        Ok(())
    }
}

impl NativeProposal {
    pub fn rejected(proposal: Value) -> Self {
        Self {
            proposal,
            candidate: None,
            executable_semantic_sha256: None,
            predeclared_lake_scope: None,
            funnel_material: None,
        }
    }

    pub fn materialized(
        proposal: Value,
        candidate: Value,
        executable_semantic_sha256: String,
    ) -> Self {
        Self {
            proposal,
            candidate: Some(candidate),
            executable_semantic_sha256: Some(executable_semantic_sha256),
            predeclared_lake_scope: None,
            funnel_material: None,
        }
    }
}

pub struct PairFactory<'a> {
    authority: &'a mut dyn NativePairAuthority,
    expected_authority_sha256: String,
}

impl<'a> PairFactory<'a> {
    pub fn new(
        authority: &'a mut dyn NativePairAuthority,
        expected_authority_sha256: impl Into<String>,
    ) -> Result<Self> {
        let expected_authority_sha256 = expected_authority_sha256.into();
        exact_sha256(
            &expected_authority_sha256,
            "expected native authority identity",
        )?;
        let observed = canonical_sha256(authority.authority_identity())?;
        if observed != expected_authority_sha256 {
            return Err(contract(
                "native proposal authority identity mismatched the frozen request",
            ));
        }
        Ok(Self {
            authority,
            expected_authority_sha256,
        })
    }

    pub fn expected_authority_sha256(&self) -> &str {
        &self.expected_authority_sha256
    }

    pub fn execute(
        &mut self,
        intent: &ProposalIntent,
        context: &NativeConstructionContext,
    ) -> Result<NativeProposal> {
        intent.validate()?;
        context.validate()?;
        let observed = canonical_sha256(self.authority.authority_identity())?;
        if observed != self.expected_authority_sha256 {
            return Err(contract(
                "native proposal authority changed during a generation",
            ));
        }
        let output = self.authority.execute(intent, context)?;
        if !output.proposal.is_object() {
            return Err(contract("native proposal payload must be an object"));
        }
        match (&output.candidate, &output.executable_semantic_sha256) {
            (Some(candidate), Some(semantic)) => {
                if !candidate.is_object() {
                    return Err(contract("native candidate payload must be an object"));
                }
                exact_sha256(semantic, "native executable semantic identity")?;
                let material = output.funnel_material.as_ref().ok_or_else(|| {
                    contract("materialized native proposal lacks funnel material")
                })?;
                material.validate()?;
                Ok(output)
            }
            (None, None) if output.funnel_material.is_none() => Ok(output),
            _ => Err(contract(
                "native proposal must provide both candidate and executable semantic identity, or neither",
            )),
        }
    }
}
