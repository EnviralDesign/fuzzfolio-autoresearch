//! Verified, pre-economic G0 compact contracts.
//!
//! This module owns G0's closed compact-reference/pool/ledger contracts,
//! canonical identities, deterministic descriptor coverage selection, and the
//! projection of a rehydrated `FrozenPair` into its static descriptor.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque};

use crate::genome::{FrozenModule, FrozenPair, IdentitySnapshot, MODULE_SCHEMA, PAIR_SCHEMA};
use temporal_qd_contract::{ContractError, Map, Value, canonical_sha256};

pub const ACCEPTED_REFERENCE_SCHEMA: &str = "temporal_qd_g0_accepted_reference_v4";
pub const ACCEPTED_POOL_SCHEMA: &str = "temporal_qd_g0_accepted_pool_v4";
pub const DESCRIPTOR_PROJECTION_SCHEMA: &str = "temporal_qd_g0_descriptor_projection_v4";
pub const LEDGER_SCHEMA: &str = "temporal_qd_g0_campaign_ledger_v1";
pub const BOOTSTRAP_SELECTION_SCHEMA: &str = "temporal_qd_g0_bootstrap_selection_v3";
pub const POLICY_SCHEMA: &str = "temporal_qd_g0_bootstrap_policy_v3";

/// The order is part of the compatibility contract.  It determines vector
/// traversal, distributions, and the initial marginal coverage value.
pub const DESCRIPTOR_AXES: [&str; 19] = [
    "long.topology",
    "short.topology",
    "long.graphSize",
    "short.graphSize",
    "long.indicatorSemantics",
    "short.indicatorSemantics",
    "long.fuzzyMembershipShape",
    "short.fuzzyMembershipShape",
    "long.entryGuardEventEvidenceSemantics",
    "short.entryGuardEventEvidenceSemantics",
    "long.holdKindBucket",
    "short.holdKindBucket",
    "long.initialStopKindBucket",
    "short.initialStopKindBucket",
    "long.initialTargetKindBucket",
    "short.initialTargetKindBucket",
    "long.graphManagementTrailingModes",
    "short.graphManagementTrailingModes",
    "staticLongShortActivationPotential",
];

#[derive(Debug, thiserror::Error)]
pub enum G0Error {
    #[error("G0 contract error: {0}")]
    Contract(String),
    #[error("canonical JSON contract error: {0}")]
    Canonical(#[from] ContractError),
    #[error("frozen pair verification failed: {0}")]
    FrozenPair(String),
}

type Result<T> = std::result::Result<T, G0Error>;
type SelectionHeapEntry = Reverse<(i64, u64, CandidateKey, u64, usize)>;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CandidateKey {
    identity: String,
    id: String,
    ordinal: u64,
}

#[derive(Clone, Debug)]
pub struct AcceptedReference {
    pub candidate_id: String,
    pub candidate_identity_sha256: String,
    pub proposal_ordinal: u64,
    pub accepted_pair_entry_sha256: String,
    pub reference_sha256: String,
    pub construction_pool_identity_sha256: String,
    pub birth_ordinal: u64,
    descriptor: Vec<String>,
}

impl AcceptedReference {
    fn key(&self) -> CandidateKey {
        CandidateKey {
            identity: self.candidate_identity_sha256.clone(),
            id: self.candidate_id.clone(),
            ordinal: self.proposal_ordinal,
        }
    }
}

/// Closed outer-entry fields authenticated before the frozen-pair payload is
/// rehydrated and its descriptor is independently derived.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedEntrySurface {
    pub entry_sha256: String,
    pub candidate_id: String,
    pub candidate_identity_sha256: String,
    pub proposal_ordinal: u64,
    pub generation_index: u64,
    pub birth_ordinal: u64,
}

/// The compact, independently verified facts a streaming G0 admission needs
/// from one rich construction entry.  It deliberately exposes no rich graph
/// or Dashboard payload: callers can retain this across thousands of rows
/// without retaining a population-sized Python/Rust object graph.
#[derive(Clone, Debug)]
pub struct AdmittedAcceptedPairEntry {
    pub entry_sha256: String,
    pub proposal_ordinal: u64,
    pub generation_index: u64,
    pub birth_ordinal: u64,
    pub candidate_id: String,
    pub candidate_identity_sha256: String,
    pub executable_semantic_sha256: String,
    pub descriptor_projection: Value,
}

/// The descriptor layer is deliberately narrower than the construction
/// codec.  Both the historical typed-fragment pair and the v5 evolvable pair
/// authenticate their compiled profile, catalog snapshot, and native report
/// before reaching this shape; from here on descriptor semantics are exactly
/// shared.
struct DescriptorModule<'a> {
    profile: &'a Value,
    catalog_payload: &'a Value,
    catalog_sha256: &'a str,
}

struct DescriptorPair<'a> {
    profile: &'a Value,
    long: DescriptorModule<'a>,
    short: DescriptorModule<'a>,
    pair_identity_sha256: String,
    native_validation_report_sha256: &'a str,
}

fn contract(message: impl Into<String>) -> G0Error {
    G0Error::Contract(message.into())
}

fn object<K: Into<String>>(entries: impl IntoIterator<Item = (K, Value)>) -> Value {
    let mut value = Value::Object(Default::default());
    let Value::Object(values) = &mut value else {
        unreachable!("Value::Object must be an object")
    };
    for (key, item) in entries {
        values.insert(key.into(), item);
    }
    value
}

fn object_ref<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| contract(format!("{label} must be an object")))
}

fn array_ref<'a>(value: &'a Value, label: &str) -> Result<&'a Vec<Value>> {
    value
        .as_array()
        .ok_or_else(|| contract(format!("{label} must be an array")))
}

fn required<'a>(object: &'a Map<String, Value>, key: &str, label: &str) -> Result<&'a Value> {
    object
        .get(key)
        .ok_or_else(|| contract(format!("{label} is missing {key}")))
}

fn text(value: &Value, label: &str) -> Result<String> {
    value
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| contract(format!("{label} must be text")))
}

fn integer(value: &Value, label: &str, minimum: u64) -> Result<u64> {
    let number = value
        .as_u64()
        .ok_or_else(|| contract(format!("{label} must be an integer >= {minimum}")))?;
    if number < minimum {
        return Err(contract(format!("{label} must be an integer >= {minimum}")));
    }
    Ok(number)
}

fn sha(value: &Value, label: &str) -> Result<String> {
    let value = text(value, label)?;
    let hex = value
        .strip_prefix("sha256:")
        .ok_or_else(|| contract(format!("{label} must be a SHA-256 identity")))?;
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(contract(format!("{label} must be a SHA-256 identity")));
    }
    Ok(value)
}

fn exact_keys(
    object: &Map<String, Value>,
    required_keys: &[&str],
    optional_keys: &[&str],
    label: &str,
) -> Result<()> {
    let known: BTreeSet<&str> = required_keys
        .iter()
        .chain(optional_keys.iter())
        .copied()
        .collect();
    if !required_keys.iter().all(|key| object.contains_key(*key))
        || object.keys().any(|key| !known.contains(key.as_str()))
    {
        return Err(contract(format!("{label} has an unexpected schema")));
    }
    Ok(())
}

fn without(value: &Value, key: &str) -> Result<Value> {
    let mut result = value.clone();
    let object = result
        .as_object_mut()
        .ok_or_else(|| contract("self-hashed G0 value must be an object"))?;
    object.remove(key);
    Ok(result)
}

fn verify_self_hash(value: &Value, field: &str, label: &str) -> Result<String> {
    let object = object_ref(value, label)?;
    let claimed = sha(required(object, field, label)?, field)?;
    let expected = canonical_sha256(&without(value, field)?)?;
    if claimed != expected {
        return Err(contract(format!("{label} identity drift")));
    }
    Ok(claimed)
}

fn require_schema(value: &Value, expected: &str, label: &str) -> Result<()> {
    if text(value, &format!("{label} schemaVersion"))? != expected {
        return Err(contract(format!("{label} schema version is invalid")));
    }
    Ok(())
}

fn normal_journal_relative_path(value: &Value) -> Result<String> {
    let path = text(value, "journal relative path")?;
    if path.trim().is_empty()
        || path.contains('\\')
        || path.starts_with('/')
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        || path
            .split('/')
            .next()
            .is_some_and(|part| part.len() == 2 && part.as_bytes()[1] == b':')
    {
        return Err(contract(
            "journal relative path must stay below the journal root",
        ));
    }
    Ok(path)
}

fn key_value(reference: &AcceptedReference) -> Value {
    object([
        ("proposalOrdinal", Value::from(reference.proposal_ordinal)),
        ("candidateId", Value::from(reference.candidate_id.clone())),
        (
            "candidateIdentitySha256",
            Value::from(reference.candidate_identity_sha256.clone()),
        ),
        (
            "referenceSha256",
            Value::from(reference.reference_sha256.clone()),
        ),
    ])
}

/// The fixed no-market policy.  Any other value is a contract error rather
/// than a configurable selection heuristic.
pub fn default_policy() -> Value {
    object([
        ("schemaVersion", Value::from(POLICY_SCHEMA)),
        (
            "policyVersion",
            Value::from("temporal_qd_g0_verified_indexed_coverage_v3"),
        ),
        (
            "selectionMethod",
            Value::from("indexed_incremental_marginal_coverage"),
        ),
        (
            "secondaryTieBreak",
            Value::from("lower_global_bucket_frequency_then_canonical_identity"),
        ),
        (
            "descriptorAxes",
            Value::Array(
                DESCRIPTOR_AXES
                    .iter()
                    .map(|axis| Value::from(*axis))
                    .collect(),
            ),
        ),
        ("marketEvidenceRead", Value::Bool(false)),
    ])
}

fn validate_policy(policy: Option<&Value>) -> Result<Value> {
    let expected = default_policy();
    if let Some(policy) = policy {
        if policy != &expected {
            return Err(contract("G0 bootstrap policy is unknown or drifted"));
        }
    }
    Ok(expected)
}

/// Validate an already materialized compact reference.  This is intentionally
/// strict: it rejects future fields as well as economics-shaped additions.
pub fn validate_accepted_reference(value: &Value) -> Result<AcceptedReference> {
    let reference = object_ref(value, "G0 accepted reference")?;
    exact_keys(
        reference,
        &[
            "schemaVersion",
            "constructionPoolIdentitySha256",
            "proposalOrdinal",
            "journalReference",
            "acceptedPairEntrySha256",
            "candidateId",
            "candidateIdentitySha256",
            "constructionLineage",
            "descriptorProjection",
            "descriptorProjectionSha256",
            "referenceSha256",
        ],
        &[],
        "G0 accepted reference",
    )?;
    require_schema(
        required(reference, "schemaVersion", "G0 accepted reference")?,
        ACCEPTED_REFERENCE_SCHEMA,
        "G0 accepted reference",
    )?;
    let construction_pool_identity_sha256 = sha(
        required(
            reference,
            "constructionPoolIdentitySha256",
            "G0 accepted reference",
        )?,
        "constructionPoolIdentitySha256",
    )?;
    let proposal_ordinal = integer(
        required(reference, "proposalOrdinal", "G0 accepted reference")?,
        "proposalOrdinal",
        0,
    )?;
    let accepted_pair_entry_sha256 = sha(
        required(
            reference,
            "acceptedPairEntrySha256",
            "G0 accepted reference",
        )?,
        "acceptedPairEntrySha256",
    )?;
    let candidate_id = text(
        required(reference, "candidateId", "G0 accepted reference")?,
        "candidateId",
    )?;
    let candidate_identity_sha256 = sha(
        required(
            reference,
            "candidateIdentitySha256",
            "G0 accepted reference",
        )?,
        "candidateIdentitySha256",
    )?;

    let journal = object_ref(
        required(reference, "journalReference", "G0 accepted reference")?,
        "G0 accepted reference journalReference",
    )?;
    exact_keys(
        journal,
        &["schemaVersion", "journalRelativePath", "entrySha256"],
        &[],
        "G0 accepted reference journalReference",
    )?;
    require_schema(
        required(journal, "schemaVersion", "journalReference")?,
        "temporal_qd_g0_journal_reference_v1",
        "G0 accepted reference journalReference",
    )?;
    normal_journal_relative_path(required(
        journal,
        "journalRelativePath",
        "journalReference",
    )?)?;
    if sha(
        required(journal, "entrySha256", "journalReference")?,
        "journal entrySha256",
    )? != accepted_pair_entry_sha256
    {
        return Err(contract("G0 accepted reference entry binding drift"));
    }

    let lineage_value = required(reference, "constructionLineage", "G0 accepted reference")?;
    let lineage = object_ref(lineage_value, "G0 construction lineage")?;
    exact_keys(
        lineage,
        &[
            "schemaVersion",
            "entrySha256",
            "proposalOrdinal",
            "generationIndex",
            "birthOrdinal",
            "originKind",
            "candidateId",
            "candidateIdentitySha256",
            "constructionLineageSha256",
        ],
        &[],
        "G0 construction lineage",
    )?;
    require_schema(
        required(lineage, "schemaVersion", "G0 construction lineage")?,
        "temporal_qd_g0_construction_lineage_v1",
        "G0 construction lineage",
    )?;
    let birth_ordinal = integer(
        required(lineage, "birthOrdinal", "G0 construction lineage")?,
        "construction lineage birthOrdinal",
        0,
    )?;
    integer(
        required(lineage, "generationIndex", "G0 construction lineage")?,
        "construction lineage generationIndex",
        0,
    )?;
    if sha(
        required(lineage, "entrySha256", "G0 construction lineage")?,
        "construction lineage entrySha256",
    )? != accepted_pair_entry_sha256
        || integer(
            required(lineage, "proposalOrdinal", "G0 construction lineage")?,
            "construction lineage proposalOrdinal",
            0,
        )? != proposal_ordinal
        || text(
            required(lineage, "originKind", "G0 construction lineage")?,
            "construction lineage originKind",
        )? != "random_immigrant"
        || text(
            required(lineage, "candidateId", "G0 construction lineage")?,
            "construction lineage candidateId",
        )? != candidate_id
        || sha(
            required(
                lineage,
                "candidateIdentitySha256",
                "G0 construction lineage",
            )?,
            "construction lineage candidateIdentitySha256",
        )? != candidate_identity_sha256
    {
        return Err(contract("G0 construction lineage binding drift"));
    }
    verify_self_hash(
        lineage_value,
        "constructionLineageSha256",
        "G0 construction lineage",
    )?;

    let projection_value = required(reference, "descriptorProjection", "G0 accepted reference")?;
    let projection = object_ref(projection_value, "G0 descriptor projection")?;
    exact_keys(
        projection,
        &[
            "schemaVersion",
            "candidateId",
            "candidateIdentitySha256",
            "pairIdentitySha256",
            "longCatalogSha256",
            "shortCatalogSha256",
            "nativeValidationReportSha256",
            "staticReachabilityReportSha256",
            "perSideLivenessProof",
            "descriptorVector",
            "descriptorProjectionSha256",
        ],
        &[],
        "G0 descriptor projection",
    )?;
    require_schema(
        required(projection, "schemaVersion", "G0 descriptor projection")?,
        DESCRIPTOR_PROJECTION_SCHEMA,
        "G0 descriptor projection",
    )?;
    if text(
        required(projection, "candidateId", "G0 descriptor projection")?,
        "descriptor candidateId",
    )? != candidate_id
        || sha(
            required(
                projection,
                "candidateIdentitySha256",
                "G0 descriptor projection",
            )?,
            "descriptor candidateIdentitySha256",
        )? != candidate_identity_sha256
    {
        return Err(contract("G0 descriptor projection binding drift"));
    }
    for field in [
        "pairIdentitySha256",
        "longCatalogSha256",
        "shortCatalogSha256",
        "nativeValidationReportSha256",
        "staticReachabilityReportSha256",
    ] {
        sha(
            required(projection, field, "G0 descriptor projection")?,
            field,
        )?;
    }
    let liveness = object_ref(
        required(
            projection,
            "perSideLivenessProof",
            "G0 descriptor projection",
        )?,
        "G0 descriptor liveness proof",
    )?;
    exact_keys(
        liveness,
        &["long", "short"],
        &[],
        "G0 descriptor liveness proof",
    )?;
    for side in ["long", "short"] {
        let proof = object_ref(
            required(liveness, side, "G0 descriptor liveness proof")?,
            "G0 descriptor side liveness proof",
        )?;
        exact_keys(
            proof,
            &[
                "entryActionRouteCount",
                "reachableEntryActionRouteCount",
                "potential",
            ],
            &[],
            "G0 descriptor side liveness proof",
        )?;
        integer(
            required(
                proof,
                "entryActionRouteCount",
                "G0 descriptor side liveness proof",
            )?,
            "entryActionRouteCount",
            0,
        )?;
        integer(
            required(
                proof,
                "reachableEntryActionRouteCount",
                "G0 descriptor side liveness proof",
            )?,
            "reachableEntryActionRouteCount",
            0,
        )?;
        if required(proof, "potential", "G0 descriptor side liveness proof")? != &Value::Bool(true)
        {
            return Err(contract("G0 descriptor liveness proof is invalid"));
        }
    }
    let vector = object_ref(
        required(projection, "descriptorVector", "G0 descriptor projection")?,
        "G0 descriptor vector",
    )?;
    exact_keys(vector, &DESCRIPTOR_AXES, &[], "G0 descriptor vector")?;
    let descriptor = DESCRIPTOR_AXES
        .iter()
        .map(|axis| text(required(vector, axis, "G0 descriptor vector")?, axis))
        .collect::<Result<Vec<_>>>()?;
    let projection_sha = verify_self_hash(
        projection_value,
        "descriptorProjectionSha256",
        "G0 descriptor projection",
    )?;
    if sha(
        required(
            reference,
            "descriptorProjectionSha256",
            "G0 accepted reference",
        )?,
        "descriptorProjectionSha256",
    )? != projection_sha
    {
        return Err(contract("G0 descriptor projection drift"));
    }
    let reference_sha256 = verify_self_hash(value, "referenceSha256", "G0 accepted reference")?;

    Ok(AcceptedReference {
        candidate_id,
        candidate_identity_sha256,
        proposal_ordinal,
        accepted_pair_entry_sha256,
        reference_sha256,
        construction_pool_identity_sha256,
        birth_ordinal,
        descriptor,
    })
}

fn validate_reference_set(
    construction_pool_identity_sha256: &str,
    references: &[Value],
) -> Result<Vec<AcceptedReference>> {
    if references.is_empty() {
        return Err(contract("constructionPoolSize must be an integer >= 1"));
    }
    let mut candidate_identities = HashSet::new();
    let mut candidate_ids = HashSet::new();
    let mut ordinals = HashSet::new();
    let mut entries = HashSet::new();
    let mut references_seen = HashSet::new();
    let mut parsed = Vec::with_capacity(references.len());
    for value in references {
        let reference = validate_accepted_reference(value)?;
        if reference.construction_pool_identity_sha256 != construction_pool_identity_sha256 {
            return Err(contract("G0 accepted pool contains foreign reference"));
        }
        if !candidate_identities.insert(reference.candidate_identity_sha256.clone())
            || !candidate_ids.insert(reference.candidate_id.clone())
            || !ordinals.insert(reference.proposal_ordinal)
            || !entries.insert(reference.accepted_pair_entry_sha256.clone())
            || !references_seen.insert(reference.reference_sha256.clone())
        {
            return Err(contract(
                "G0 accepted pool duplicates a bound reference identity",
            ));
        }
        parsed.push(reference);
    }
    parsed.sort_by_key(AcceptedReference::key);
    let mut births: Vec<u64> = parsed
        .iter()
        .map(|reference| reference.birth_ordinal)
        .collect();
    births.sort_unstable();
    if births != (0..references.len() as u64).collect::<Vec<_>>() {
        return Err(contract(
            "G0 accepted pool birth ordinals must be unique and contiguous",
        ));
    }
    Ok(parsed)
}

fn pool_material(pool: &Value) -> Result<Value> {
    let pool_object = object_ref(pool, "G0 accepted pool")?;
    let references = array_ref(
        required(pool_object, "acceptedReferences", "G0 accepted pool")?,
        "G0 accepted pool references",
    )?;
    let mut sorted = references.clone();
    sorted.sort_by(|left, right| {
        let left_hash = left
            .as_object()
            .and_then(|object| object.get("referenceSha256"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        let right_hash = right
            .as_object()
            .and_then(|object| object.get("referenceSha256"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        left_hash.cmp(right_hash)
    });
    Ok(object([
        (
            "schemaVersion",
            required(pool_object, "schemaVersion", "G0 accepted pool")?.clone(),
        ),
        (
            "constructionPoolSize",
            required(pool_object, "constructionPoolSize", "G0 accepted pool")?.clone(),
        ),
        (
            "constructionPoolIdentitySha256",
            required(
                pool_object,
                "constructionPoolIdentitySha256",
                "G0 accepted pool",
            )?
            .clone(),
        ),
        ("acceptedReferences", Value::Array(sorted)),
    ]))
}

/// Build the public compact pool.  Rich proposal/candidate bytes are not
/// copied: a reference is validated and retained exactly as supplied.
pub fn build_accepted_pool(
    construction_pool_identity_sha256: &str,
    references: &[Value],
) -> Result<Value> {
    let identity = sha(
        &Value::from(construction_pool_identity_sha256),
        "constructionPoolIdentitySha256",
    )?;
    validate_reference_set(&identity, references)?;
    let mut pool = object([
        ("schemaVersion", Value::from(ACCEPTED_POOL_SCHEMA)),
        ("constructionPoolSize", Value::from(references.len() as u64)),
        ("constructionPoolIdentitySha256", Value::from(identity)),
        ("acceptedReferences", Value::Array(references.to_vec())),
    ]);
    let pool_sha = canonical_sha256(&pool_material(&pool)?)?;
    pool.as_object_mut()
        .expect("pool is object")
        .insert("acceptedPoolSha256".to_owned(), Value::from(pool_sha));
    Ok(pool)
}

/// Validate a self-hashed pool and return references in canonical identity
/// order.  Pool input order never affects selection.
pub fn validate_accepted_pool(pool: &Value) -> Result<Vec<AcceptedReference>> {
    let object = object_ref(pool, "G0 accepted pool")?;
    exact_keys(
        object,
        &[
            "schemaVersion",
            "constructionPoolSize",
            "constructionPoolIdentitySha256",
            "acceptedReferences",
            "acceptedPoolSha256",
        ],
        &[],
        "G0 accepted pool",
    )?;
    require_schema(
        required(object, "schemaVersion", "G0 accepted pool")?,
        ACCEPTED_POOL_SCHEMA,
        "G0 accepted pool",
    )?;
    let size = integer(
        required(object, "constructionPoolSize", "G0 accepted pool")?,
        "constructionPoolSize",
        1,
    )?;
    let identity = sha(
        required(object, "constructionPoolIdentitySha256", "G0 accepted pool")?,
        "constructionPoolIdentitySha256",
    )?;
    let references = array_ref(
        required(object, "acceptedReferences", "G0 accepted pool")?,
        "G0 accepted pool references",
    )?;
    if references.len() as u64 != size {
        return Err(contract("G0 accepted pool size does not bind references"));
    }
    let parsed = validate_reference_set(&identity, references)?;
    let claimed = sha(
        required(object, "acceptedPoolSha256", "G0 accepted pool")?,
        "acceptedPoolSha256",
    )?;
    if claimed != canonical_sha256(&pool_material(pool)?)? {
        return Err(contract("G0 accepted pool identity drift"));
    }
    Ok(parsed)
}

fn distribution(rows: &[&AcceptedReference]) -> Value {
    let mut distributions: Vec<BTreeMap<String, u64>> = (0..DESCRIPTOR_AXES.len())
        .map(|_| BTreeMap::new())
        .collect();
    for reference in rows {
        for (axis_index, value) in reference.descriptor.iter().enumerate() {
            *distributions[axis_index].entry(value.clone()).or_default() += 1;
        }
    }
    object(DESCRIPTOR_AXES.iter().enumerate().map(|(index, axis)| {
        let buckets = object(
            distributions[index]
                .iter()
                .map(|(value, count)| (value.clone(), Value::from(*count))),
        );
        (*axis, buckets)
    }))
}

fn ledger_rows(references: &[AcceptedReference], selected: &BTreeSet<String>) -> Value {
    Value::Array(
        references
            .iter()
            .map(|reference| {
                object([
                    ("proposalOrdinal", Value::from(reference.proposal_ordinal)),
                    ("candidateId", Value::from(reference.candidate_id.clone())),
                    (
                        "candidateIdentitySha256",
                        Value::from(reference.candidate_identity_sha256.clone()),
                    ),
                    (
                        "referenceSha256",
                        Value::from(reference.reference_sha256.clone()),
                    ),
                    ("marketEvidenceRead", Value::Bool(false)),
                    (
                        "evaluationDisposition",
                        Value::from(if selected.contains(&reference.reference_sha256) {
                            "selected_for_market_evaluation"
                        } else {
                            "bootstrap_diversity_not_selected"
                        }),
                    ),
                ])
            })
            .collect(),
    )
}

/// Materialize the ledger for every constructed identity, regardless of
/// evaluation selection.  Duplicate input hashes intentionally collapse just
/// as Python's `set(...)` implementation does.
pub fn materialize_campaign_ledger(
    accepted_pool: &Value,
    selected_reference_sha256s: &[String],
) -> Result<Value> {
    let references = validate_accepted_pool(accepted_pool)?;
    let selected: BTreeSet<String> = selected_reference_sha256s.iter().cloned().collect();
    let known: BTreeSet<String> = references
        .iter()
        .map(|reference| reference.reference_sha256.clone())
        .collect();
    if !selected.is_subset(&known) {
        return Err(contract(
            "G0 campaign ledger selection contains a foreign reference",
        ));
    }
    let pool = object_ref(accepted_pool, "G0 accepted pool")?;
    let mut ledger = object([
        ("schemaVersion", Value::from(LEDGER_SCHEMA)),
        (
            "constructionPoolIdentitySha256",
            required(pool, "constructionPoolIdentitySha256", "G0 accepted pool")?.clone(),
        ),
        (
            "acceptedPoolSha256",
            required(pool, "acceptedPoolSha256", "G0 accepted pool")?.clone(),
        ),
        (
            "constructionPoolSize",
            required(pool, "constructionPoolSize", "G0 accepted pool")?.clone(),
        ),
        ("rows", ledger_rows(&references, &selected)),
    ]);
    let hash = canonical_sha256(&ledger)?;
    ledger
        .as_object_mut()
        .expect("ledger is object")
        .insert("ledgerSha256".to_owned(), Value::from(hash));
    Ok(ledger)
}

pub fn verify_campaign_ledger(
    ledger: &Value,
    accepted_pool: &Value,
    selected_reference_sha256s: &[String],
) -> Result<Value> {
    let object = object_ref(ledger, "G0 campaign ledger")?;
    exact_keys(
        object,
        &[
            "schemaVersion",
            "constructionPoolIdentitySha256",
            "acceptedPoolSha256",
            "constructionPoolSize",
            "rows",
            "ledgerSha256",
        ],
        &[],
        "G0 campaign ledger",
    )?;
    require_schema(
        required(object, "schemaVersion", "G0 campaign ledger")?,
        LEDGER_SCHEMA,
        "G0 campaign ledger",
    )?;
    verify_self_hash(ledger, "ledgerSha256", "G0 campaign ledger")?;
    let expected = materialize_campaign_ledger(accepted_pool, selected_reference_sha256s)?;
    if ledger != &expected {
        return Err(contract("G0 campaign ledger diverged from accepted pool"));
    }
    Ok(expected)
}

/// Deterministic compact G0 selector.  It is an indexed incremental update:
/// selecting a new bucket decreases only the gain of references sharing that
/// bucket.  Heap order exactly matches Python: maximum gain, minimum global
/// frequency cost, then canonical identity/id/ordinal.
pub fn select_g0_bootstrap(
    accepted_pool: &Value,
    evaluation_width: u64,
    policy: Option<&Value>,
) -> Result<Value> {
    let references = validate_accepted_pool(accepted_pool)?;
    if evaluation_width == 0 || evaluation_width as usize > references.len() {
        return Err(contract("G0 evaluation width exceeds accepted pool"));
    }
    let policy = validate_policy(policy)?;
    let mut members: HashMap<(usize, String), Vec<usize>> = HashMap::new();
    let mut frequencies: HashMap<(usize, String), u64> = HashMap::new();
    for (index, reference) in references.iter().enumerate() {
        for (axis, value) in reference.descriptor.iter().enumerate() {
            let bucket = (axis, value.clone());
            members.entry(bucket.clone()).or_default().push(index);
            *frequencies.entry(bucket).or_default() += 1;
        }
    }
    let costs: Vec<u64> = references
        .iter()
        .map(|reference| {
            reference
                .descriptor
                .iter()
                .enumerate()
                .map(|(axis, value)| frequencies[&(axis, value.clone())])
                .sum()
        })
        .collect();
    let mut gains = vec![DESCRIPTOR_AXES.len() as i64; references.len()];
    let mut versions = vec![0_u64; references.len()];
    let mut heap: BinaryHeap<SelectionHeapEntry> = BinaryHeap::new();
    for (index, reference) in references.iter().enumerate() {
        heap.push(Reverse((
            -gains[index],
            costs[index],
            reference.key(),
            versions[index],
            index,
        )));
    }
    let mut selected_indexes = Vec::with_capacity(evaluation_width as usize);
    let mut selected_set = HashSet::new();
    let mut covered: HashSet<(usize, String)> = HashSet::new();
    let mut trace = Vec::with_capacity(evaluation_width as usize);
    while selected_indexes.len() < evaluation_width as usize {
        let (neg_gain, cost, _key, _version, index) = loop {
            let Reverse(item) = heap
                .pop()
                .ok_or_else(|| contract("G0 selector heap exhausted"))?;
            if versions[item.4] == item.3 && !selected_set.contains(&item.4) {
                break item;
            }
        };
        selected_indexes.push(index);
        selected_set.insert(index);
        for (axis, value) in references[index].descriptor.iter().enumerate() {
            let bucket = (axis, value.clone());
            if !covered.insert(bucket.clone()) {
                continue;
            }
            for member in &members[&bucket] {
                if selected_set.contains(member) {
                    continue;
                }
                gains[*member] -= 1;
                versions[*member] += 1;
                heap.push(Reverse((
                    -gains[*member],
                    costs[*member],
                    references[*member].key(),
                    versions[*member],
                    *member,
                )));
            }
        }
        trace.push(object([
            (
                "selectionIndex",
                Value::from(selected_indexes.len() as u64 - 1),
            ),
            (
                "candidateIdentitySha256",
                Value::from(references[index].candidate_identity_sha256.clone()),
            ),
            ("marginalCoverage", Value::from((-neg_gain) as u64)),
            ("globalBucketFrequencyCost", Value::from(cost)),
        ]));
    }
    let chosen: Vec<&AcceptedReference> = selected_indexes
        .iter()
        .map(|index| &references[*index])
        .collect();
    let selected_hashes: Vec<String> = chosen
        .iter()
        .map(|reference| reference.reference_sha256.clone())
        .collect();
    let ledger = materialize_campaign_ledger(accepted_pool, &selected_hashes)?;
    let pool = object_ref(accepted_pool, "G0 accepted pool")?;
    let mut result = object([
        ("schemaVersion", Value::from(BOOTSTRAP_SELECTION_SCHEMA)),
        ("policy", policy.clone()),
        ("policySha256", Value::from(canonical_sha256(&policy)?)),
        (
            "constructionPoolIdentitySha256",
            required(pool, "constructionPoolIdentitySha256", "G0 accepted pool")?.clone(),
        ),
        (
            "completePoolIdentitySha256",
            required(pool, "acceptedPoolSha256", "G0 accepted pool")?.clone(),
        ),
        (
            "constructionPoolSize",
            required(pool, "constructionPoolSize", "G0 accepted pool")?.clone(),
        ),
        ("completePoolCount", Value::from(references.len() as u64)),
        ("evaluationWidth", Value::from(evaluation_width)),
        ("marketEvidenceRead", Value::Bool(false)),
        (
            "campaignLedgerSha256",
            object_ref(&ledger, "G0 campaign ledger")
                .expect("ledger is object")
                .get("ledgerSha256")
                .expect("ledger hash exists")
                .clone(),
        ),
        (
            "campaignLedgerIntent",
            object([
                (
                    "allConstructedIdentitiesMustEnterCampaignLedger",
                    Value::Bool(true),
                ),
                ("selectedForMarketEvaluationOnly", Value::Bool(true)),
            ]),
        ),
        (
            "selected",
            Value::Array(
                chosen
                    .iter()
                    .map(|reference| key_value(reference))
                    .collect(),
            ),
        ),
        ("selectionTrace", Value::Array(trace)),
        (
            "poolDistribution",
            distribution(&references.iter().collect::<Vec<_>>()),
        ),
        ("selectedDistribution", distribution(&chosen)),
    ]);
    let hash = canonical_sha256(&result)?;
    result
        .as_object_mut()
        .expect("selection is object")
        .insert("selectionSha256".to_owned(), Value::from(hash));
    Ok(result)
}

pub fn verify_g0_bootstrap_selection(artifact: &Value, accepted_pool: &Value) -> Result<Value> {
    let object = object_ref(artifact, "G0 selection artifact")?;
    let claimed = sha(
        required(object, "selectionSha256", "G0 selection artifact")?,
        "selectionSha256",
    )?;
    if claimed != canonical_sha256(&without(artifact, "selectionSha256")?)? {
        return Err(contract("G0 selection artifact identity drift"));
    }
    let evaluation_width = integer(
        required(object, "evaluationWidth", "G0 selection artifact")?,
        "evaluationWidth",
        1,
    )?;
    let policy = required(object, "policy", "G0 selection artifact")?;
    let expected = select_g0_bootstrap(accepted_pool, evaluation_width, Some(policy))?;
    if artifact != &expected {
        return Err(contract(
            "G0 selection artifact diverged from accepted pool",
        ));
    }
    Ok(expected)
}

/// Validate the rich journal entry's closed outer surface and self-hashes
/// before `verify_accepted_entry` binds it to `FrozenPair` authority.
pub fn validate_accepted_entry_surface(entry: &Value) -> Result<AcceptedEntrySurface> {
    let row = object_ref(entry, "G0 accepted journal entry")?;
    exact_keys(
        row,
        &[
            "schemaVersion",
            "configSha256",
            "generationIndex",
            "proposalOrdinal",
            "originKind",
            "proposal",
            "operatorImplementationSha256",
            "disposition",
            "candidate",
            "entrySha256",
        ],
        &["funnelCandidate"],
        "G0 accepted journal entry",
    )?;
    require_schema(
        required(row, "schemaVersion", "G0 accepted journal entry")?,
        "temporal_qd_proposal_entry_v3",
        "G0 accepted journal entry",
    )?;
    if text(
        required(row, "originKind", "G0 accepted journal entry")?,
        "originKind",
    )? != "random_immigrant"
        || text(
            required(row, "disposition", "G0 accepted journal entry")?,
            "disposition",
        )? != "accepted"
    {
        return Err(contract(
            "G0 journal entry is not an accepted canonical v3 entry",
        ));
    }
    sha(
        required(row, "configSha256", "G0 accepted journal entry")?,
        "configSha256",
    )?;
    sha(
        required(
            row,
            "operatorImplementationSha256",
            "G0 accepted journal entry",
        )?,
        "operatorImplementationSha256",
    )?;
    let generation_index = integer(
        required(row, "generationIndex", "G0 accepted journal entry")?,
        "journal generationIndex",
        0,
    )?;
    let proposal_ordinal = integer(
        required(row, "proposalOrdinal", "G0 accepted journal entry")?,
        "journal proposalOrdinal",
        0,
    )?;
    let proposal_value = required(row, "proposal", "G0 accepted journal entry")?;
    let proposal = object_ref(proposal_value, "G0 immigrant proposal")?;
    exact_keys(
        proposal,
        &[
            "schemaVersion",
            "proposalSeed",
            "originKind",
            "side",
            "factoryPair",
            "pairIdentitySha256",
            "disposition",
            "proposalSha256",
        ],
        &["factoryConstructionAudit"],
        "G0 immigrant proposal",
    )?;
    require_schema(
        required(proposal, "schemaVersion", "G0 immigrant proposal")?,
        "temporal_qd_pair_proposal_v2",
        "G0 immigrant proposal",
    )?;
    if text(
        required(proposal, "originKind", "G0 immigrant proposal")?,
        "proposal originKind",
    )? != "random_immigrant"
        || text(
            required(proposal, "disposition", "G0 immigrant proposal")?,
            "proposal disposition",
        )? != "materialized"
    {
        return Err(contract("G0 immigrant proposal fields are invalid"));
    }
    text(
        required(proposal, "proposalSeed", "G0 immigrant proposal")?,
        "proposalSeed",
    )?;
    text(required(proposal, "side", "G0 immigrant proposal")?, "side")?;
    sha(
        required(proposal, "pairIdentitySha256", "G0 immigrant proposal")?,
        "proposal pairIdentitySha256",
    )?;
    let proposal_sha = verify_self_hash(proposal_value, "proposalSha256", "G0 immigrant proposal")?;
    object_ref(
        required(proposal, "factoryPair", "G0 immigrant proposal")?,
        "G0 immigrant factoryPair",
    )?;
    if let Some(audit_value) = proposal.get("factoryConstructionAudit") {
        let audit = object_ref(audit_value, "G0 factory construction audit")?;
        match text(
            required(audit, "schemaVersion", "G0 factory construction audit")?,
            "G0 factory construction audit schema",
        )?
        .as_str()
        {
            "temporal_qd_rich_immigrant_pair_construction_v1" => {
                exact_keys(
                    audit,
                    &[
                        "schemaVersion",
                        "pairIdentitySha256",
                        "sides",
                        "auditSha256",
                    ],
                    &[],
                    "G0 factory construction audit",
                )?;
                if sha(
                    required(audit, "pairIdentitySha256", "G0 factory construction audit")?,
                    "factory construction audit pairIdentitySha256",
                )? != sha(
                    required(proposal, "pairIdentitySha256", "G0 immigrant proposal")?,
                    "proposal pairIdentitySha256",
                )? {
                    return Err(contract("G0 factory construction audit identity drift"));
                }
                object_ref(
                    required(audit, "sides", "G0 factory construction audit")?,
                    "G0 factory construction audit sides",
                )?;
                verify_self_hash(audit_value, "auditSha256", "G0 factory construction audit")?;
            }
            "temporal_qd_evolvable_module_factory_audit_v1" => {
                exact_keys(
                    audit,
                    &[
                        "schemaVersion",
                        "authoritySha256",
                        "pairIdentitySha256",
                        "sides",
                        "auditSha256",
                    ],
                    &[],
                    "G0 evolvable factory construction audit",
                )?;
                sha(
                    required(
                        audit,
                        "authoritySha256",
                        "G0 evolvable factory construction audit",
                    )?,
                    "evolvable factory authoritySha256",
                )?;
                if sha(
                    required(
                        audit,
                        "pairIdentitySha256",
                        "G0 evolvable factory construction audit",
                    )?,
                    "evolvable factory audit pairIdentitySha256",
                )? != sha(
                    required(proposal, "pairIdentitySha256", "G0 immigrant proposal")?,
                    "proposal pairIdentitySha256",
                )? {
                    return Err(contract("G0 evolvable factory audit identity drift"));
                }
                object_ref(
                    required(audit, "sides", "G0 evolvable factory construction audit")?,
                    "G0 evolvable factory audit sides",
                )?;
                verify_self_hash(
                    audit_value,
                    "auditSha256",
                    "G0 evolvable factory construction audit",
                )?;
            }
            _ => {
                return Err(contract(
                    "G0 factory construction audit schema is not admitted",
                ));
            }
        }
    }

    let candidate_value = required(row, "candidate", "G0 accepted journal entry")?;
    let candidate = object_ref(candidate_value, "G0 accepted candidate")?;
    exact_keys(
        candidate,
        &[
            "candidateId",
            "sourceMode",
            "seedId",
            "generationIndex",
            "birthOrdinal",
            "proposalOrdinal",
            "sourceProfile",
            "sourceProfileSha256",
            "profileSnapshotSha256",
            "programSha256",
            "validationReportSha256",
            "candidateIdentityMaterial",
            "candidateIdentitySha256",
            "structuralDepth",
            "structuralOperatorHistory",
            "mutationTrace",
            "activationAwareRepairs",
            "constructionEvidenceScope",
            "bidirectionalGenome",
            "lineage",
            "pairProposal",
            "pairProposalSha256",
        ],
        &[],
        "G0 accepted candidate",
    )?;
    let candidate_id = text(
        required(candidate, "candidateId", "G0 accepted candidate")?,
        "candidateId",
    )?;
    let candidate_identity_sha256 = sha(
        required(
            candidate,
            "candidateIdentitySha256",
            "G0 accepted candidate",
        )?,
        "candidateIdentitySha256",
    )?;
    if candidate_id != format!("qd_{}", &candidate_identity_sha256[7..35]) {
        return Err(contract(
            "accepted candidate ID does not bind canonical identity",
        ));
    }
    if text(
        required(candidate, "sourceMode", "G0 accepted candidate")?,
        "sourceMode",
    )? != "qd_random_immigrant_bidirectional_pair"
        || text(
            required(candidate, "seedId", "G0 accepted candidate")?,
            "seedId",
        )? != "bidirectional_pair"
        || required(candidate, "mutationTrace", "G0 accepted candidate")?
            != &Value::Array(Vec::new())
        || required(candidate, "activationAwareRepairs", "G0 accepted candidate")?
            != &Value::Array(Vec::new())
    {
        return Err(contract("G0 random immigrant source semantics drifted"));
    }
    object_ref(
        required(candidate, "sourceProfile", "G0 accepted candidate")?,
        "G0 candidate sourceProfile",
    )?;
    for field in [
        "sourceProfileSha256",
        "profileSnapshotSha256",
        "programSha256",
        "validationReportSha256",
    ] {
        sha(required(candidate, field, "G0 accepted candidate")?, field)?;
    }
    integer(
        required(candidate, "structuralDepth", "G0 accepted candidate")?,
        "structuralDepth",
        0,
    )?;
    array_ref(
        required(
            candidate,
            "structuralOperatorHistory",
            "G0 accepted candidate",
        )?,
        "structuralOperatorHistory",
    )?;
    object_ref(
        required(candidate, "bidirectionalGenome", "G0 accepted candidate")?,
        "G0 candidate bidirectionalGenome",
    )?;
    let birth_ordinal = integer(
        required(candidate, "birthOrdinal", "G0 accepted candidate")?,
        "candidate birthOrdinal",
        0,
    )?;
    if integer(
        required(candidate, "generationIndex", "G0 accepted candidate")?,
        "candidate generationIndex",
        0,
    )? != generation_index
        || integer(
            required(candidate, "proposalOrdinal", "G0 accepted candidate")?,
            "candidate proposalOrdinal",
            0,
        )? != proposal_ordinal
        || required(candidate, "pairProposal", "G0 accepted candidate")? != proposal_value
        || sha(
            required(candidate, "pairProposalSha256", "G0 accepted candidate")?,
            "candidate pairProposalSha256",
        )? != proposal_sha
    {
        return Err(contract(
            "G0 accepted candidate proposal/generation binding drift",
        ));
    }
    let material_value = required(
        candidate,
        "candidateIdentityMaterial",
        "G0 accepted candidate",
    )?;
    let material = object_ref(material_value, "G0 candidate identity material")?;
    exact_keys(
        material,
        &[
            "schemaVersion",
            "qdEngineVersion",
            "originKind",
            "bidirectionalGenomeIdentitySha256",
            "pairPolicySha256",
            "longModuleIdentitySha256",
            "shortModuleIdentitySha256",
            "longGrammarContextSha256",
            "shortGrammarContextSha256",
            "longCatalogSha256",
            "shortCatalogSha256",
            "longPolicySha256",
            "shortPolicySha256",
            "longNativeAuthoritySha256",
            "shortNativeAuthoritySha256",
            "pairCompilerAuthoritySha256",
            "compiledRawPairSha256",
            "compiledProfileSha256",
            "compiledProgramSha256",
            "compiledValidationReportSha256",
            "orderedSideLineage",
            "materializedPairProposalSha256",
        ],
        &[],
        "G0 candidate identity material",
    )?;
    if text(
        required(material, "originKind", "G0 candidate identity material")?,
        "identity material originKind",
    )? != "random_immigrant"
        || sha(
            required(
                material,
                "materializedPairProposalSha256",
                "G0 candidate identity material",
            )?,
            "materializedPairProposalSha256",
        )? != proposal_sha
        || canonical_sha256(material_value)? != candidate_identity_sha256
    {
        return Err(contract("G0 candidate identity material drift"));
    }
    text(
        required(material, "schemaVersion", "G0 candidate identity material")?,
        "identity material schemaVersion",
    )?;
    text(
        required(
            material,
            "qdEngineVersion",
            "G0 candidate identity material",
        )?,
        "identity material qdEngineVersion",
    )?;
    for field in [
        "bidirectionalGenomeIdentitySha256",
        "pairPolicySha256",
        "longModuleIdentitySha256",
        "shortModuleIdentitySha256",
        "longGrammarContextSha256",
        "shortGrammarContextSha256",
        "longCatalogSha256",
        "shortCatalogSha256",
        "longPolicySha256",
        "shortPolicySha256",
        "longNativeAuthoritySha256",
        "shortNativeAuthoritySha256",
        "pairCompilerAuthoritySha256",
        "compiledRawPairSha256",
        "compiledProfileSha256",
        "compiledProgramSha256",
        "compiledValidationReportSha256",
    ] {
        sha(
            required(material, field, "G0 candidate identity material")?,
            field,
        )?;
    }
    let ordered_side_lineage = required(
        material,
        "orderedSideLineage",
        "G0 candidate identity material",
    )?;
    array_ref(ordered_side_lineage, "identity material orderedSideLineage")?;
    let lineage = object_ref(
        required(candidate, "lineage", "G0 accepted candidate")?,
        "G0 candidate lineage",
    )?;
    exact_keys(
        lineage,
        &[
            "schemaVersion",
            "candidateId",
            "candidateIdentitySha256",
            "pairIdentitySha256",
            "orderedSideLineage",
        ],
        &[],
        "G0 candidate lineage",
    )?;
    require_schema(
        required(lineage, "schemaVersion", "G0 candidate lineage")?,
        "temporal_qd_bidirectional_candidate_lineage_v1",
        "G0 candidate lineage",
    )?;
    if text(
        required(lineage, "candidateId", "G0 candidate lineage")?,
        "lineage candidateId",
    )? != candidate_id
        || sha(
            required(lineage, "candidateIdentitySha256", "G0 candidate lineage")?,
            "lineage candidateIdentitySha256",
        )? != candidate_identity_sha256
        || sha(
            required(lineage, "pairIdentitySha256", "G0 candidate lineage")?,
            "lineage pairIdentitySha256",
        )? != sha(
            required(proposal, "pairIdentitySha256", "G0 immigrant proposal")?,
            "proposal pairIdentitySha256",
        )?
        || required(lineage, "orderedSideLineage", "G0 candidate lineage")? != ordered_side_lineage
    {
        return Err(contract("G0 candidate lineage binding drift"));
    }
    let scope_value = required(
        candidate,
        "constructionEvidenceScope",
        "G0 accepted candidate",
    )?;
    let scope = object_ref(scope_value, "G0 construction evidence scope")?;
    exact_keys(
        scope,
        &[
            "schemaVersion",
            "evidencePlanRotationRequired",
            "lakeScopeRegenerationRequired",
            "reasons",
            "timeframeMutationTraceSha256s",
            "evidenceScopeSha256",
        ],
        &[],
        "G0 construction evidence scope",
    )?;
    require_schema(
        required(scope, "schemaVersion", "G0 construction evidence scope")?,
        "temporal_qd_construction_evidence_scope_v1",
        "G0 construction evidence scope",
    )?;
    if required(
        scope,
        "evidencePlanRotationRequired",
        "G0 construction evidence scope",
    )? != &Value::Bool(false)
        || required(
            scope,
            "lakeScopeRegenerationRequired",
            "G0 construction evidence scope",
        )? != &Value::Bool(false)
        || required(scope, "reasons", "G0 construction evidence scope")?
            != &Value::Array(Vec::new())
        || required(
            scope,
            "timeframeMutationTraceSha256s",
            "G0 construction evidence scope",
        )? != &Value::Array(Vec::new())
    {
        return Err(contract("G0 construction evidence scope is invalid"));
    }
    verify_self_hash(
        scope_value,
        "evidenceScopeSha256",
        "G0 construction evidence scope",
    )?;
    let entry_sha256 = verify_self_hash(entry, "entrySha256", "G0 accepted journal entry")?;
    if let Some(funnel_value) = row.get("funnelCandidate") {
        let funnel = object_ref(funnel_value, "G0 funnel candidate")?;
        exact_keys(
            funnel,
            &[
                "schemaVersion",
                "candidateId",
                "rawSourceProfileSha256",
                "staticReachability",
                "nativeValidation",
                "admission",
            ],
            &[],
            "G0 funnel candidate",
        )?;
        require_schema(
            required(funnel, "schemaVersion", "G0 funnel candidate")?,
            "temporal_qd_proposal_funnel_stage_v1",
            "G0 funnel candidate",
        )?;
        if text(
            required(funnel, "candidateId", "G0 funnel candidate")?,
            "funnel candidateId",
        )? != candidate_id
        {
            return Err(contract("G0 funnel candidate binding drift"));
        }
        sha(
            required(funnel, "rawSourceProfileSha256", "G0 funnel candidate")?,
            "funnel rawSourceProfileSha256",
        )?;
        let static_reachability = object_ref(
            required(funnel, "staticReachability", "G0 funnel candidate")?,
            "G0 funnel staticReachability",
        )?;
        exact_keys(
            static_reachability,
            &["outcome", "reasons"],
            &[],
            "G0 funnel staticReachability",
        )?;
        if text(
            required(
                static_reachability,
                "outcome",
                "G0 funnel staticReachability",
            )?,
            "funnel staticReachability outcome",
        )? != "reachable"
            || required(
                static_reachability,
                "reasons",
                "G0 funnel staticReachability",
            )? != &Value::Array(Vec::new())
        {
            return Err(contract("G0 funnel static reachability is invalid"));
        }
        let native_validation = object_ref(
            required(funnel, "nativeValidation", "G0 funnel candidate")?,
            "G0 funnel nativeValidation",
        )?;
        exact_keys(
            native_validation,
            &[
                "outcome",
                "reasons",
                "resolvedProfileSha256",
                "programSha256",
                "validationReportSha256",
            ],
            &[],
            "G0 funnel nativeValidation",
        )?;
        if text(
            required(native_validation, "outcome", "G0 funnel nativeValidation")?,
            "funnel nativeValidation outcome",
        )? != "valid"
            || required(native_validation, "reasons", "G0 funnel nativeValidation")?
                != &Value::Array(Vec::new())
        {
            return Err(contract("G0 funnel native validation is invalid"));
        }
        for field in [
            "resolvedProfileSha256",
            "programSha256",
            "validationReportSha256",
        ] {
            sha(
                required(native_validation, field, "G0 funnel nativeValidation")?,
                field,
            )?;
        }
        let admission = object_ref(
            required(funnel, "admission", "G0 funnel candidate")?,
            "G0 funnel admission",
        )?;
        exact_keys(
            admission,
            &["outcome", "reasons", "canonicalEvidenceIdentitySha256"],
            &[],
            "G0 funnel admission",
        )?;
        if text(
            required(admission, "outcome", "G0 funnel admission")?,
            "funnel admission outcome",
        )? != "admitted"
            || required(admission, "reasons", "G0 funnel admission")? != &Value::Array(Vec::new())
            || required(
                admission,
                "canonicalEvidenceIdentitySha256",
                "G0 funnel admission",
            )? != &Value::Null
        {
            return Err(contract("G0 funnel admission is invalid"));
        }
    }
    Ok(AcceptedEntrySurface {
        entry_sha256,
        candidate_id,
        candidate_identity_sha256,
        proposal_ordinal,
        generation_index,
        birth_ordinal,
    })
}

fn frozen<T>(value: std::result::Result<T, impl std::fmt::Display>) -> Result<T> {
    value.map_err(|error| G0Error::FrozenPair(error.to_string()))
}

fn expected_funnel(candidate: &Map<String, Value>, pair: &FrozenPair) -> Result<Value> {
    Ok(object([
        (
            "schemaVersion",
            Value::from("temporal_qd_proposal_funnel_stage_v1"),
        ),
        (
            "candidateId",
            required(candidate, "candidateId", "G0 accepted candidate")?.clone(),
        ),
        (
            "rawSourceProfileSha256",
            Value::from(pair.raw_pair_sha256.clone()),
        ),
        (
            "staticReachability",
            object([
                ("outcome", Value::from("reachable")),
                ("reasons", Value::Array(Vec::new())),
            ]),
        ),
        (
            "nativeValidation",
            object([
                ("outcome", Value::from("valid")),
                ("reasons", Value::Array(Vec::new())),
                (
                    "resolvedProfileSha256",
                    Value::from(pair.profile_sha256.clone()),
                ),
                (
                    "programSha256",
                    Value::from(pair.native_program_sha256.clone()),
                ),
                (
                    "validationReportSha256",
                    Value::from(pair.native_validation_report_sha256.clone()),
                ),
            ]),
        ),
        (
            "admission",
            object([
                ("outcome", Value::from("admitted")),
                ("reasons", Value::Array(Vec::new())),
                ("canonicalEvidenceIdentitySha256", Value::Null),
            ]),
        ),
    ]))
}

fn verify_accepted_entry(entry: &Value) -> Result<(AcceptedEntrySurface, FrozenPair)> {
    let surface = validate_accepted_entry_surface(entry)?;
    let row = object_ref(entry, "G0 accepted journal entry")?;
    let candidate_value = required(row, "candidate", "G0 accepted journal entry")?;
    let candidate = object_ref(candidate_value, "G0 accepted candidate")?;
    let proposal_value = required(row, "proposal", "G0 accepted journal entry")?;
    let proposal = object_ref(proposal_value, "G0 immigrant proposal")?;
    let pair = frozen(FrozenPair::from_payload(required(
        candidate,
        "bidirectionalGenome",
        "G0 accepted candidate",
    )?))?;
    let proposal_pair = frozen(FrozenPair::from_payload(required(
        proposal,
        "factoryPair",
        "G0 immigrant proposal",
    )?))?;
    let pair_identity = frozen(pair.identity_sha256())?;
    if sha(
        required(proposal, "pairIdentitySha256", "G0 immigrant proposal")?,
        "proposal pairIdentitySha256",
    )? != pair_identity
        || frozen(proposal_pair.identity_sha256())? != pair_identity
        || frozen(proposal_pair.canonical_payload())? != frozen(pair.canonical_payload())?
    {
        return Err(contract(
            "accepted proposal frozen pair does not bind candidate genome",
        ));
    }
    let material = object_ref(
        required(
            candidate,
            "candidateIdentityMaterial",
            "G0 accepted candidate",
        )?,
        "G0 candidate identity material",
    )?;
    if sha(
        required(
            material,
            "bidirectionalGenomeIdentitySha256",
            "G0 candidate identity material",
        )?,
        "bidirectionalGenomeIdentitySha256",
    )? != pair_identity
    {
        return Err(contract(
            "accepted candidate identity does not bind frozen pair",
        ));
    }
    let side_lineage = Value::Array(pair.side_targeted_lineage.clone());
    if required(
        material,
        "orderedSideLineage",
        "G0 candidate identity material",
    )? != &side_lineage
        || required(
            candidate,
            "structuralOperatorHistory",
            "G0 accepted candidate",
        )? != &side_lineage
        || integer(
            required(candidate, "structuralDepth", "G0 accepted candidate")?,
            "structuralDepth",
            0,
        )? != pair.side_targeted_lineage.len() as u64
    {
        return Err(contract("accepted candidate structural lineage drift"));
    }
    let lineage = object_ref(
        required(candidate, "lineage", "G0 accepted candidate")?,
        "G0 candidate lineage",
    )?;
    if text(
        required(lineage, "candidateId", "G0 candidate lineage")?,
        "lineage candidateId",
    )? != surface.candidate_id
        || sha(
            required(lineage, "candidateIdentitySha256", "G0 candidate lineage")?,
            "lineage candidateIdentitySha256",
        )? != surface.candidate_identity_sha256
        || sha(
            required(lineage, "pairIdentitySha256", "G0 candidate lineage")?,
            "lineage pairIdentitySha256",
        )? != pair_identity
        || required(lineage, "orderedSideLineage", "G0 candidate lineage")? != &side_lineage
    {
        return Err(contract(
            "accepted candidate lineage does not bind frozen pair",
        ));
    }
    let source_profile = required(candidate, "sourceProfile", "G0 accepted candidate")?;
    if canonical_sha256(source_profile)? != pair.raw_pair_sha256
        || sha(
            required(candidate, "sourceProfileSha256", "G0 accepted candidate")?,
            "sourceProfileSha256",
        )? != pair.raw_pair_sha256
        || sha(
            required(candidate, "profileSnapshotSha256", "G0 accepted candidate")?,
            "profileSnapshotSha256",
        )? != pair.profile_sha256
        || sha(
            required(candidate, "programSha256", "G0 accepted candidate")?,
            "programSha256",
        )? != pair.native_program_sha256
        || sha(
            required(candidate, "validationReportSha256", "G0 accepted candidate")?,
            "validationReportSha256",
        )? != pair.native_validation_report_sha256
    {
        return Err(contract(
            "accepted candidate compiled/native identities diverged",
        ));
    }
    let proposal_sha = sha(
        required(proposal, "proposalSha256", "G0 immigrant proposal")?,
        "proposalSha256",
    )?;
    if sha(
        required(candidate, "pairProposalSha256", "G0 accepted candidate")?,
        "pairProposalSha256",
    )? != proposal_sha
        || required(candidate, "pairProposal", "G0 accepted candidate")? != proposal_value
        || sha(
            required(
                material,
                "materializedPairProposalSha256",
                "G0 candidate identity material",
            )?,
            "materializedPairProposalSha256",
        )? != proposal_sha
    {
        return Err(contract(
            "accepted candidate does not bind exact pair proposal",
        ));
    }
    if proposal
        .get("factoryConstructionAudit")
        .and_then(Value::as_object)
        .and_then(|audit| audit.get("schemaVersion"))
        .and_then(Value::as_str)
        == Some("temporal_qd_rich_immigrant_pair_construction_v1")
    {
        let audit_value = proposal
            .get("factoryConstructionAudit")
            .expect("schema lookup requires audit");
        let audit = object_ref(audit_value, "G0 factory construction audit")?;
        let mut expected_sides = Value::Object(Default::default());
        for module in [&pair.long, &pair.short] {
            let construction = module
                .lineage
                .iter()
                .rev()
                .find_map(|item| {
                    let map = item.as_object()?;
                    (map.get("operation").and_then(Value::as_str)
                        == Some("rich_immigrant_construction"))
                    .then(|| map.get("audit").filter(|audit| audit.is_object()).cloned())
                    .flatten()
                })
                .ok_or_else(|| {
                    contract("G0 factory construction audit lacks frozen lineage authority")
                })?;
            expected_sides
                .as_object_mut()
                .expect("object")
                .insert(module.direction.clone(), construction);
        }
        if required(audit, "sides", "G0 factory construction audit")? != &expected_sides {
            return Err(contract(
                "G0 factory construction audit diverged from frozen lineage",
            ));
        }
    }
    if let Some(funnel) = row.get("funnelCandidate") {
        if funnel != &expected_funnel(candidate, &pair)? {
            return Err(contract(
                "G0 funnel candidate diverged from frozen authority",
            ));
        }
    }
    Ok((surface, pair))
}

#[derive(Clone, Debug)]
struct EvolvableModuleFacts {
    program_sha256: String,
    semantic_topology_sha256: String,
    resource_fingerprint_sha256: String,
    adapter: FrozenModule,
}

#[derive(Clone, Debug)]
struct EvolvablePairFacts {
    pair_identity_sha256: String,
    adapter: FrozenPair,
    long_program_sha256: String,
    short_program_sha256: String,
    long_semantic_topology_sha256: String,
    short_semantic_topology_sha256: String,
    long_resource_fingerprint_sha256: String,
    short_resource_fingerprint_sha256: String,
}

fn is_evolvable_pair_entry(entry: &Value) -> bool {
    entry
        .get("candidate")
        .and_then(Value::as_object)
        .and_then(|candidate| candidate.get("bidirectionalGenome"))
        .and_then(Value::as_object)
        .and_then(|pair| pair.get("long"))
        .and_then(Value::as_object)
        .and_then(|module| module.get("program"))
        .and_then(Value::as_object)
        .and_then(|program| program.get("programKind"))
        .and_then(Value::as_str)
        == Some("evolvable_module_genome_v1")
}

fn verify_evolvable_accepted_entry(
    entry: &Value,
    surface: AcceptedEntrySurface,
    expected_operator_implementation: Option<&Value>,
) -> Result<(AcceptedEntrySurface, EvolvablePairFacts)> {
    let row = object_ref(entry, "G0 evolvable accepted journal entry")?;
    let candidate_value = required(row, "candidate", "G0 evolvable accepted journal entry")?;
    let candidate = object_ref(candidate_value, "G0 evolvable accepted candidate")?;
    let proposal_value = required(row, "proposal", "G0 evolvable accepted journal entry")?;
    let proposal = object_ref(proposal_value, "G0 evolvable immigrant proposal")?;
    let pair_value = required(
        candidate,
        "bidirectionalGenome",
        "G0 evolvable accepted candidate",
    )?;
    let facts = validate_evolvable_pair(pair_value)?;
    if required(proposal, "factoryPair", "G0 evolvable immigrant proposal")? != pair_value
        || sha(
            required(
                proposal,
                "pairIdentitySha256",
                "G0 evolvable immigrant proposal",
            )?,
            "evolvable proposal pairIdentitySha256",
        )? != facts.pair_identity_sha256
    {
        return Err(contract(
            "evolvable accepted proposal frozen pair does not bind candidate genome",
        ));
    }
    let material = object_ref(
        required(
            candidate,
            "candidateIdentityMaterial",
            "G0 evolvable accepted candidate",
        )?,
        "G0 evolvable candidate identity material",
    )?;
    if sha(
        required(
            material,
            "bidirectionalGenomeIdentitySha256",
            "G0 evolvable candidate identity material",
        )?,
        "evolvable candidate pair identity",
    )? != facts.pair_identity_sha256
    {
        return Err(contract(
            "evolvable candidate identity does not bind frozen pair",
        ));
    }
    let side_lineage = facts.adapter.side_targeted_lineage.clone();
    if required(
        material,
        "orderedSideLineage",
        "G0 evolvable candidate identity material",
    )? != &Value::Array(side_lineage.clone())
        || required(
            candidate,
            "structuralOperatorHistory",
            "G0 evolvable accepted candidate",
        )? != &Value::Array(side_lineage.clone())
        || integer(
            required(
                candidate,
                "structuralDepth",
                "G0 evolvable accepted candidate",
            )?,
            "evolvable candidate structuralDepth",
            0,
        )? != side_lineage.len() as u64
    {
        return Err(contract("evolvable candidate structural lineage drift"));
    }
    let lineage = object_ref(
        required(candidate, "lineage", "G0 evolvable accepted candidate")?,
        "G0 evolvable candidate lineage",
    )?;
    if text(
        required(lineage, "candidateId", "G0 evolvable candidate lineage")?,
        "evolvable lineage candidateId",
    )? != surface.candidate_id
        || sha(
            required(
                lineage,
                "candidateIdentitySha256",
                "G0 evolvable candidate lineage",
            )?,
            "evolvable lineage candidate identity",
        )? != surface.candidate_identity_sha256
        || sha(
            required(
                lineage,
                "pairIdentitySha256",
                "G0 evolvable candidate lineage",
            )?,
            "evolvable lineage pair identity",
        )? != facts.pair_identity_sha256
        || required(
            lineage,
            "orderedSideLineage",
            "G0 evolvable candidate lineage",
        )? != &Value::Array(side_lineage.clone())
    {
        return Err(contract(
            "evolvable candidate lineage does not bind frozen pair",
        ));
    }
    let source_profile = required(
        candidate,
        "sourceProfile",
        "G0 evolvable accepted candidate",
    )?;
    if canonical_sha256(source_profile)? != facts.adapter.raw_pair_sha256
        || sha(
            required(
                candidate,
                "sourceProfileSha256",
                "G0 evolvable accepted candidate",
            )?,
            "evolvable candidate source profile identity",
        )? != facts.adapter.raw_pair_sha256
        || sha(
            required(
                candidate,
                "profileSnapshotSha256",
                "G0 evolvable accepted candidate",
            )?,
            "evolvable candidate profile snapshot identity",
        )? != facts.adapter.profile_sha256
        || sha(
            required(
                candidate,
                "programSha256",
                "G0 evolvable accepted candidate",
            )?,
            "evolvable candidate program identity",
        )? != facts.adapter.native_program_sha256
        || sha(
            required(
                candidate,
                "validationReportSha256",
                "G0 evolvable accepted candidate",
            )?,
            "evolvable candidate validation identity",
        )? != facts.adapter.native_validation_report_sha256
    {
        return Err(contract(
            "evolvable candidate compiled/native identities diverged",
        ));
    }
    let proposal_sha = sha(
        required(
            proposal,
            "proposalSha256",
            "G0 evolvable immigrant proposal",
        )?,
        "evolvable proposal SHA-256",
    )?;
    if sha(
        required(
            candidate,
            "pairProposalSha256",
            "G0 evolvable accepted candidate",
        )?,
        "evolvable candidate pair proposal identity",
    )? != proposal_sha
        || required(candidate, "pairProposal", "G0 evolvable accepted candidate")? != proposal_value
        || sha(
            required(
                material,
                "materializedPairProposalSha256",
                "G0 evolvable candidate identity material",
            )?,
            "evolvable materialized proposal identity",
        )? != proposal_sha
    {
        return Err(contract(
            "evolvable candidate does not bind exact pair proposal",
        ));
    }
    let audit = proposal
        .get("factoryConstructionAudit")
        .ok_or_else(|| contract("evolvable G0 entry lacks factory construction audit"))?;
    let proposal_seed = text(
        required(proposal, "proposalSeed", "G0 evolvable immigrant proposal")?,
        "evolvable proposal seed",
    )?;
    verify_evolvable_factory_audit(
        audit,
        &facts,
        &proposal_seed,
        expected_operator_implementation,
    )?;
    if let Some(funnel) = row.get("funnelCandidate") {
        if funnel != &expected_funnel(candidate, &facts.adapter)? {
            return Err(contract(
                "evolvable G0 funnel candidate diverged from frozen authority",
            ));
        }
    }
    Ok((surface, facts))
}

fn validate_evolvable_pair(pair_value: &Value) -> Result<EvolvablePairFacts> {
    let pair = object_ref(pair_value, "evolvable frozen pair")?;
    exact_keys(
        pair,
        &[
            "schemaVersion",
            "long",
            "short",
            "pairCompiler",
            "profile",
            "validation",
            "sideTargetedLineage",
            "identities",
        ],
        &[],
        "evolvable frozen pair",
    )?;
    require_schema(
        required(pair, "schemaVersion", "evolvable frozen pair")?,
        PAIR_SCHEMA,
        "evolvable frozen pair",
    )?;
    let long = validate_evolvable_module(required(pair, "long", "evolvable frozen pair")?, "long")?;
    let short =
        validate_evolvable_module(required(pair, "short", "evolvable frozen pair")?, "short")?;
    let pair_compiler = frozen(IdentitySnapshot::from_payload(
        required(pair, "pairCompiler", "evolvable frozen pair")?,
        Some("pairCompiler"),
    ))?;
    let profile = required(pair, "profile", "evolvable frozen pair")?.clone();
    let validation = required(pair, "validation", "evolvable frozen pair")?.clone();
    let lineage = array_ref(
        required(pair, "sideTargetedLineage", "evolvable frozen pair")?,
        "evolvable pair side-targeted lineage",
    )?
    .clone();
    let validation_fields = object_ref(&validation, "evolvable pair validation")?;
    let raw_pair_sha256 = canonical_sha256(&profile)?;
    if text(
        required(
            object_ref(&profile, "evolvable pair profile")?,
            "version",
            "evolvable pair profile",
        )?,
        "evolvable pair profile version",
    )? != "v3"
        || text(
            required(
                object_ref(&profile, "evolvable pair profile")?,
                "directionMode",
                "evolvable pair profile",
            )?,
            "evolvable pair profile direction",
        )? != "both"
        || object_ref(&profile, "evolvable pair profile")?.contains_key("hold")
        || text(
            required(
                validation_fields,
                "schemaVersion",
                "evolvable pair validation",
            )?,
            "evolvable pair validation schema",
        )? != "temporal_search_candidate_validation_v1"
        || required(
            validation_fields,
            "rawSourceProfileSha256",
            "evolvable pair validation",
        )? != &Value::String(raw_pair_sha256.clone())
        || text(
            required(validation_fields, "status", "evolvable pair validation")?,
            "evolvable pair validation status",
        )? != "valid_evaluable"
        || !matches!(
            validation_fields.get("candidateAcceptable"),
            Some(Value::Bool(true))
        )
    {
        return Err(contract("evolvable frozen pair validation is not exact"));
    }
    let adapter_profile_sha256 = sha(
        required(
            validation_fields,
            "profileSnapshotSha256",
            "evolvable pair validation",
        )?,
        "evolvable pair profile snapshot identity",
    )?;
    let adapter_native_program_sha256 = sha(
        required(
            validation_fields,
            "programSha256",
            "evolvable pair validation",
        )?,
        "evolvable pair program identity",
    )?;
    let adapter_native_validation_report_sha256 = sha(
        required(
            validation_fields,
            "validationReportSha256",
            "evolvable pair validation",
        )?,
        "evolvable pair validation report identity",
    )?;
    let adapter = FrozenPair {
        long: long.adapter.clone(),
        short: short.adapter.clone(),
        pair_compiler,
        profile,
        validation,
        side_targeted_lineage: lineage,
        raw_pair_sha256,
        profile_sha256: adapter_profile_sha256,
        native_program_sha256: adapter_native_program_sha256,
        native_validation_report_sha256: adapter_native_validation_report_sha256,
    };
    // Reopen the normalized adapter through the old strict pair parser.  It
    // validates the Dashboard v2/v3 compilation and native report without
    // pretending that the evolvable source program is a typed fragment.
    let adapter = frozen(FrozenPair::from_payload(&frozen(
        adapter.canonical_payload(),
    )?))?;
    let mut actual_pair_material = frozen(adapter.identity_material())?;
    for (side, program_sha256) in [
        ("longModule", long.program_sha256.as_str()),
        ("shortModule", short.program_sha256.as_str()),
    ] {
        actual_pair_material
            .get_mut(side)
            .and_then(Value::as_object_mut)
            .ok_or_else(|| contract("evolvable pair identity material is malformed"))?
            .insert(
                "programSha256".to_owned(),
                Value::String(program_sha256.to_owned()),
            );
    }
    let pair_identity_sha256 = canonical_sha256(&actual_pair_material)?;
    let mut expected_identities = frozen(adapter.canonical_payload())?
        .get("identities")
        .cloned()
        .expect("canonical frozen pair has identities");
    expected_identities
        .as_object_mut()
        .expect("canonical pair identities are object")
        .insert(
            "pairIdentitySha256".to_owned(),
            Value::String(pair_identity_sha256.clone()),
        );
    if required(pair, "identities", "evolvable frozen pair")? != &expected_identities {
        return Err(contract("evolvable frozen pair identity material drifted"));
    }
    Ok(EvolvablePairFacts {
        pair_identity_sha256,
        adapter,
        long_program_sha256: long.program_sha256,
        short_program_sha256: short.program_sha256,
        long_semantic_topology_sha256: long.semantic_topology_sha256,
        short_semantic_topology_sha256: short.semantic_topology_sha256,
        long_resource_fingerprint_sha256: long.resource_fingerprint_sha256,
        short_resource_fingerprint_sha256: short.resource_fingerprint_sha256,
    })
}

fn validate_evolvable_module(
    module_value: &Value,
    expected_direction: &str,
) -> Result<EvolvableModuleFacts> {
    let module = object_ref(module_value, "evolvable frozen module")?;
    exact_keys(
        module,
        &[
            "schemaVersion",
            "direction",
            "program",
            "profile",
            "grammarContext",
            "catalog",
            "policy",
            "nativeAuthority",
            "nativeReport",
            "lineage",
            "identities",
        ],
        &[],
        "evolvable frozen module",
    )?;
    require_schema(
        required(module, "schemaVersion", "evolvable frozen module")?,
        MODULE_SCHEMA,
        "evolvable frozen module",
    )?;
    if text(
        required(module, "direction", "evolvable frozen module")?,
        "evolvable module direction",
    )? != expected_direction
    {
        return Err(contract("evolvable frozen module direction drifted"));
    }
    let program = required(module, "program", "evolvable frozen module")?;
    let program_facts = validate_evolvable_program(program, expected_direction)?;
    let grammar_context = frozen(IdentitySnapshot::from_payload(
        required(module, "grammarContext", "evolvable frozen module")?,
        Some("grammarContext"),
    ))?;
    let catalog = frozen(IdentitySnapshot::from_payload(
        required(module, "catalog", "evolvable frozen module")?,
        Some("catalog"),
    ))?;
    let policy = frozen(IdentitySnapshot::from_payload(
        required(module, "policy", "evolvable frozen module")?,
        Some("policy"),
    ))?;
    let native_authority = frozen(IdentitySnapshot::from_payload(
        required(module, "nativeAuthority", "evolvable frozen module")?,
        Some("nativeAuthority"),
    ))?;
    let lineage = array_ref(
        required(module, "lineage", "evolvable frozen module")?,
        "evolvable module lineage",
    )?
    .clone();
    let adapter_program = object([
        (
            "schemaVersion",
            Value::String("temporal_typed_fragment_grammar_v2".to_owned()),
        ),
        ("grammarVersion", Value::String("3".to_owned())),
        ("direction", Value::String(expected_direction.to_owned())),
        ("fragments", Value::Array(Vec::new())),
    ]);
    let adapter = frozen(FrozenModule::freeze(
        &adapter_program,
        required(module, "profile", "evolvable frozen module")?,
        &grammar_context,
        &catalog,
        &policy,
        &native_authority,
        required(module, "nativeReport", "evolvable frozen module")?,
        &lineage,
    ))?;
    let mut identity_material = adapter.identity_material();
    identity_material
        .as_object_mut()
        .expect("frozen module identity material is object")
        .insert(
            "schemaVersion".to_owned(),
            Value::String(MODULE_SCHEMA.to_owned()),
        );
    identity_material
        .as_object_mut()
        .expect("frozen module identity material is object")
        .insert(
            "programSha256".to_owned(),
            Value::String(program_facts.program_sha256.clone()),
        );
    let module_identity_sha256 = canonical_sha256(&identity_material)?;
    let mut expected_identities = frozen(adapter.canonical_payload())?
        .get("identities")
        .cloned()
        .expect("canonical frozen module has identities");
    let expected_fields = expected_identities
        .as_object_mut()
        .expect("canonical frozen module identities are object");
    expected_fields.insert(
        "programSha256".to_owned(),
        Value::String(program_facts.program_sha256.clone()),
    );
    expected_fields.insert(
        "moduleIdentitySha256".to_owned(),
        Value::String(module_identity_sha256),
    );
    if required(module, "identities", "evolvable frozen module")? != &expected_identities {
        return Err(contract(
            "evolvable frozen module identity material drifted",
        ));
    }
    Ok(EvolvableModuleFacts {
        program_sha256: program_facts.program_sha256,
        semantic_topology_sha256: program_facts.semantic_topology_sha256,
        resource_fingerprint_sha256: program_facts.resource_fingerprint_sha256,
        adapter,
    })
}

#[derive(Clone, Debug)]
struct EvolvableProgramFacts {
    program_sha256: String,
    semantic_topology_sha256: String,
    resource_fingerprint_sha256: String,
}

fn validate_evolvable_program(
    program: &Value,
    expected_direction: &str,
) -> Result<EvolvableProgramFacts> {
    let fields = object_ref(program, "evolvable module program")?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "programKind",
            "codec",
            "direction",
            "instrument",
            "resources",
            "nodes",
            "edges",
            "budget",
        ],
        &[],
        "evolvable module program",
    )?;
    if text(
        required(fields, "schemaVersion", "evolvable module program")?,
        "evolvable module program schema",
    )? != "evolvable_module_genome_v1"
        || text(
            required(fields, "programKind", "evolvable module program")?,
            "evolvable module program kind",
        )? != "evolvable_module_genome_v1"
        || text(
            required(fields, "codec", "evolvable module program")?,
            "evolvable module program codec",
        )? != "evolvable_module_genome_json_v1"
        || text(
            required(fields, "direction", "evolvable module program")?,
            "evolvable module program direction",
        )? != expected_direction
        || text(
            required(fields, "instrument", "evolvable module program")?,
            "evolvable module instrument",
        )?
        .is_empty()
    {
        return Err(contract("evolvable module program identity is invalid"));
    }
    validate_evolvable_budget(required(fields, "budget", "evolvable module program")?)?;
    let resources = object_ref(
        required(fields, "resources", "evolvable module program")?,
        "evolvable module resources",
    )?;
    exact_keys(
        resources,
        &["indicators", "evidenceGroups", "events", "managementRefs"],
        &[],
        "evolvable module resources",
    )?;
    let indicators = array_ref(
        required(resources, "indicators", "evolvable module resources")?,
        "evolvable indicator resources",
    )?;
    let groups = array_ref(
        required(resources, "evidenceGroups", "evolvable module resources")?,
        "evolvable evidence group resources",
    )?;
    let events = array_ref(
        required(resources, "events", "evolvable module resources")?,
        "evolvable event resources",
    )?;
    let management = array_ref(
        required(resources, "managementRefs", "evolvable module resources")?,
        "evolvable management resources",
    )?;
    validate_sorted_resource_rows(indicators, "indicator", |row| {
        row.get("meta")
            .and_then(Value::as_object)
            .and_then(|meta| meta.get("instanceId"))
            .and_then(Value::as_str)
    })?;
    for (rows, label) in [
        (groups, "evidence group"),
        (events, "event"),
        (management, "management"),
    ] {
        validate_sorted_resource_rows(rows, label, |row| row.get("id").and_then(Value::as_str))?;
    }
    let nodes = array_ref(
        required(fields, "nodes", "evolvable module program")?,
        "evolvable module nodes",
    )?;
    let edges = array_ref(
        required(fields, "edges", "evolvable module program")?,
        "evolvable module edges",
    )?;
    let node_ids = validate_evolvable_nodes(nodes)?;
    validate_evolvable_edges(edges, &node_ids)?;
    let semantic_topology_sha256 = evolvable_semantic_topology(nodes, edges)?;
    let resource_fingerprint_sha256 = evolvable_resource_fingerprint(resources, edges)?;
    Ok(EvolvableProgramFacts {
        program_sha256: canonical_sha256(program)?,
        semantic_topology_sha256,
        resource_fingerprint_sha256,
    })
}

fn validate_evolvable_budget(value: &Value) -> Result<()> {
    let fields = object_ref(value, "evolvable module budget")?;
    let limits = [
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
    exact_keys(
        fields,
        &limits.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
        &[],
        "evolvable module budget",
    )?;
    for (name, maximum) in limits {
        let value = integer(
            required(fields, name, "evolvable module budget")?,
            "evolvable module budget value",
            1,
        )?;
        if value > maximum {
            return Err(contract("evolvable module budget exceeds the v1 ceiling"));
        }
    }
    Ok(())
}

fn validate_sorted_resource_rows<'a>(
    rows: &'a [Value],
    label: &str,
    identifier: impl Fn(&'a Map<String, Value>) -> Option<&'a str>,
) -> Result<()> {
    let mut prior = None::<String>;
    for value in rows {
        let fields = object_ref(value, &format!("evolvable {label} resource"))?;
        let id = identifier(fields)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| contract(format!("evolvable {label} resource lacks an ID")))?
            .to_owned();
        if prior.as_ref().is_some_and(|previous| previous >= &id) {
            return Err(contract(format!(
                "evolvable {label} resources are not canonically sorted and unique"
            )));
        }
        prior = Some(id);
    }
    Ok(())
}

fn validate_evolvable_nodes(nodes: &[Value]) -> Result<BTreeSet<String>> {
    if nodes.is_empty() {
        return Err(contract("evolvable module requires nodes"));
    }
    let mut ids = BTreeSet::new();
    let mut prior = None::<String>;
    let mut starts = 0_u64;
    let mut hubs = 0_u64;
    for node in nodes {
        let fields = object_ref(node, "evolvable module node")?;
        exact_keys(
            fields,
            &["id", "zone", "kind", "guard", "resources", "timeoutBars"],
            &[],
            "evolvable module node",
        )?;
        let id = text(
            required(fields, "id", "evolvable module node")?,
            "evolvable node ID",
        )?;
        if id.is_empty()
            || prior
                .as_ref()
                .is_some_and(|previous| previous.as_str() >= id.as_str())
            || !ids.insert(id.clone())
        {
            return Err(contract(
                "evolvable node IDs are not canonically sorted and unique",
            ));
        }
        prior = Some(id.clone());
        let zone = text(
            required(fields, "zone", "evolvable module node")?,
            "evolvable node zone",
        )?;
        let kind = text(
            required(fields, "kind", "evolvable module node")?,
            "evolvable node kind",
        )?;
        if ![
            "entry",
            "setup",
            "position",
            "management",
            "exit",
            "recovery",
        ]
        .contains(&zone.as_str())
            || kind.is_empty()
            || !required(fields, "guard", "evolvable module node")?.is_object()
            || !required(fields, "resources", "evolvable module node")?.is_array()
        {
            return Err(contract("evolvable module node shape is invalid"));
        }
        if zone == "entry" && kind == "start" {
            starts += 1;
        }
        if zone == "position" && kind == "position_hub" {
            hubs += 1;
        }
        if let Some(timeout) = fields.get("timeoutBars") {
            if !timeout.is_null() && integer(timeout, "evolvable node timeoutBars", 1)? > 64 {
                return Err(contract("evolvable node timeout exceeds v1 ceiling"));
            }
        }
    }
    if starts != 1 || hubs != 1 {
        return Err(contract(
            "evolvable module requires exactly one entry start and position hub",
        ));
    }
    Ok(ids)
}

fn validate_evolvable_edges(edges: &[Value], node_ids: &BTreeSet<String>) -> Result<()> {
    if edges.is_empty() {
        return Err(contract("evolvable module requires edges"));
    }
    let mut prior = None::<String>;
    let mut priorities = BTreeSet::new();
    for edge in edges {
        let fields = object_ref(edge, "evolvable module edge")?;
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
            &[],
            "evolvable module edge",
        )?;
        let id = text(
            required(fields, "id", "evolvable module edge")?,
            "evolvable edge ID",
        )?;
        if id.is_empty()
            || prior
                .as_ref()
                .is_some_and(|previous| previous.as_str() >= id.as_str())
        {
            return Err(contract(
                "evolvable edge IDs are not canonically sorted and unique",
            ));
        }
        prior = Some(id);
        let source = text(
            required(fields, "source", "evolvable module edge")?,
            "evolvable edge source",
        )?;
        let target = text(
            required(fields, "target", "evolvable module edge")?,
            "evolvable edge target",
        )?;
        let event_class = text(
            required(fields, "eventClass", "evolvable module edge")?,
            "evolvable edge class",
        )?;
        let priority = integer(
            required(fields, "priority", "evolvable module edge")?,
            "evolvable edge priority",
            0,
        )?;
        if !node_ids.contains(&source)
            || !node_ids.contains(&target)
            || event_class != "decision"
            || priority > 999
            || !required(fields, "guard", "evolvable module edge")?.is_object()
            || !required(fields, "effect", "evolvable module edge")?.is_null()
                && !required(fields, "effect", "evolvable module edge")?.is_string()
            || !priorities.insert((source, event_class, priority))
        {
            return Err(contract("evolvable module edge shape is invalid"));
        }
    }
    Ok(())
}

fn evolvable_resource_fingerprint(
    resources: &Map<String, Value>,
    edges: &[Value],
) -> Result<String> {
    let indicators = array_ref(
        required(resources, "indicators", "evolvable module resources")?,
        "evolvable indicator resources",
    )?;
    let groups = array_ref(
        required(resources, "evidenceGroups", "evolvable module resources")?,
        "evolvable evidence group resources",
    )?;
    let events = array_ref(
        required(resources, "events", "evolvable module resources")?,
        "evolvable event resources",
    )?;
    let mut indicator_values = Vec::with_capacity(indicators.len());
    for row in indicators {
        let fields = object_ref(row, "evolvable indicator resource")?;
        let meta = object_ref(
            required(fields, "meta", "evolvable indicator resource")?,
            "evolvable indicator meta",
        )?;
        let config = object_ref(
            required(fields, "config", "evolvable indicator resource")?,
            "evolvable indicator config",
        )?;
        indicator_values.push(Value::Array(vec![
            meta.get("id").cloned().unwrap_or(Value::Null),
            config.get("timeframe").cloned().unwrap_or(Value::Null),
        ]));
    }
    let group_values = groups
        .iter()
        .map(|row| {
            object_ref(row, "evolvable evidence group resource")?
                .get("indicatorInstanceIds")
                .cloned()
                .ok_or_else(|| contract("evolvable evidence group lacks indicator members"))
        })
        .collect::<Result<Vec<_>>>()?;
    let event_values = events
        .iter()
        .map(|row| {
            object_ref(row, "evolvable event resource")?
                .get("indicatorInstanceId")
                .cloned()
                .ok_or_else(|| contract("evolvable event lacks indicator member"))
        })
        .collect::<Result<Vec<_>>>()?;
    let management_effects = [
        "move_stop_to_break_even_next_open",
        "tighten_stop_next_open",
        "activate_trailing_stop_next_open",
        "deactivate_trailing_stop_next_open",
        "set_target_next_open",
        "cancel_target_next_open",
    ];
    let effects = edges
        .iter()
        .filter_map(|edge| edge.get("effect").and_then(Value::as_str))
        .filter(|effect| management_effects.contains(effect))
        .map(|effect| Value::String(effect.to_owned()))
        .collect::<Vec<_>>();
    let exits = edges
        .iter()
        .filter(|edge| edge.get("effect").and_then(Value::as_str) == Some("exit_next_open"))
        .count() as u64;
    canonical_sha256(&object([
        ("indicators", Value::Array(indicator_values)),
        ("groups", Value::Array(group_values)),
        ("events", Value::Array(event_values)),
        ("management", Value::Array(effects)),
        ("exits", Value::from(exits)),
    ]))
    .map_err(Into::into)
}

fn evolvable_semantic_topology(nodes: &[Value], edges: &[Value]) -> Result<String> {
    let mut labels = BTreeMap::new();
    for node in nodes {
        let fields = object_ref(node, "evolvable module node")?;
        let id = text(
            required(fields, "id", "evolvable module node")?,
            "evolvable node ID",
        )?;
        let mut shape = fields.clone();
        shape.remove("id");
        shape.insert(
            "guard".to_owned(),
            evolvable_guard_shape(required(fields, "guard", "evolvable module node")?)?,
        );
        let resources = array_ref(
            required(fields, "resources", "evolvable module node")?,
            "evolvable node resources",
        )?;
        let mut kinds = resources
            .iter()
            .map(|resource| {
                object_ref(resource, "evolvable node resource").and_then(|fields| {
                    text(
                        required(fields, "kind", "evolvable node resource")?,
                        "evolvable resource kind",
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        kinds.sort();
        shape.insert(
            "resources".to_owned(),
            Value::Array(kinds.into_iter().map(Value::String).collect()),
        );
        let timeout = fields
            .get("timeoutBars")
            .map(|value| Value::Bool(!value.is_null()))
            .unwrap_or(Value::Bool(false));
        shape.insert("timeoutBars".to_owned(), timeout);
        labels.insert(id, canonical_sha256(&Value::Object(shape))?);
    }
    for _ in 0..nodes.len().max(1) {
        let mut updated = BTreeMap::new();
        for node in nodes {
            let fields = object_ref(node, "evolvable module node")?;
            let id = text(
                required(fields, "id", "evolvable module node")?,
                "evolvable node ID",
            )?;
            let outgoing = evolvable_topology_edges(edges, &id, true, &labels)?;
            let incoming = evolvable_topology_edges(edges, &id, false, &labels)?;
            updated.insert(
                id.clone(),
                canonical_sha256(&object([
                    (
                        "node",
                        Value::String(
                            labels
                                .get(&id)
                                .cloned()
                                .ok_or_else(|| contract("evolvable topology lacks node label"))?,
                        ),
                    ),
                    ("out", Value::Array(outgoing)),
                    ("in", Value::Array(incoming)),
                ]))?,
            );
        }
        if updated == labels {
            break;
        }
        labels = updated;
    }
    let mut final_edges = Vec::new();
    for edge in edges {
        let fields = object_ref(edge, "evolvable module edge")?;
        let source = text(
            required(fields, "source", "evolvable module edge")?,
            "evolvable edge source",
        )?;
        let target = text(
            required(fields, "target", "evolvable module edge")?,
            "evolvable edge target",
        )?;
        final_edges.push(Value::Array(vec![
            Value::String(
                labels
                    .get(&source)
                    .cloned()
                    .ok_or_else(|| contract("evolvable topology source label is missing"))?,
            ),
            Value::String(
                labels
                    .get(&target)
                    .cloned()
                    .ok_or_else(|| contract("evolvable topology target label is missing"))?,
            ),
            required(fields, "eventClass", "evolvable module edge")?.clone(),
            required(fields, "priority", "evolvable module edge")?.clone(),
            required(fields, "effect", "evolvable module edge")?.clone(),
            evolvable_guard_shape(required(fields, "guard", "evolvable module edge")?)?,
        ]));
    }
    final_edges.sort_by_key(python_repr);
    let mut final_nodes = labels.into_values().collect::<Vec<_>>();
    final_nodes.sort();
    canonical_sha256(&object([
        (
            "schemaVersion",
            Value::String("evolvable_module_semantic_topology_v1".to_owned()),
        ),
        (
            "nodes",
            Value::Array(final_nodes.into_iter().map(Value::String).collect()),
        ),
        ("edges", Value::Array(final_edges)),
    ]))
    .map_err(Into::into)
}

fn evolvable_topology_edges(
    edges: &[Value],
    node_id: &str,
    outgoing: bool,
    labels: &BTreeMap<String, String>,
) -> Result<Vec<Value>> {
    let mut result = Vec::new();
    for edge in edges {
        let fields = object_ref(edge, "evolvable module edge")?;
        let source = text(
            required(fields, "source", "evolvable module edge")?,
            "evolvable edge source",
        )?;
        let target = text(
            required(fields, "target", "evolvable module edge")?,
            "evolvable edge target",
        )?;
        if (outgoing && source != node_id) || (!outgoing && target != node_id) {
            continue;
        }
        let other = if outgoing { target } else { source };
        result.push(Value::Array(vec![
            required(fields, "eventClass", "evolvable module edge")?.clone(),
            required(fields, "priority", "evolvable module edge")?.clone(),
            required(fields, "effect", "evolvable module edge")?.clone(),
            evolvable_guard_shape(required(fields, "guard", "evolvable module edge")?)?,
            Value::String(
                labels
                    .get(&other)
                    .cloned()
                    .ok_or_else(|| contract("evolvable topology adjacent label is missing"))?,
            ),
        ]));
    }
    result.sort_by_key(python_repr);
    Ok(result)
}

fn evolvable_guard_shape(value: &Value) -> Result<Value> {
    let fields = object_ref(value, "evolvable guard")?;
    let kind = fields
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match kind {
        "all" | "any" => {
            let mut children = fields
                .get("guards")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter(|item| item.is_object())
                .map(evolvable_guard_shape)
                .collect::<Result<Vec<_>>>()?;
            children.sort_by_key(python_repr);
            Ok(Value::Array(vec![
                Value::String(kind.to_owned()),
                Value::Array(children),
            ]))
        }
        "predicate_edge" | "consecutive_true" => Ok(Value::Array(vec![
            Value::String(kind.to_owned()),
            evolvable_guard_shape(
                fields
                    .get("predicate")
                    .unwrap_or(&Value::Object(Map::new())),
            )?,
        ])),
        _ => Ok(Value::String(kind.to_owned())),
    }
}

fn python_repr(value: &Value) -> String {
    match value {
        Value::Null => "None".to_owned(),
        Value::Bool(value) => {
            if *value {
                "True".to_owned()
            } else {
                "False".to_owned()
            }
        }
        Value::Number(value) => value.to_string(),
        Value::String(value) => format!("{:?}", value),
        Value::Array(values) => format!(
            "({}{})",
            values
                .iter()
                .map(python_repr)
                .collect::<Vec<_>>()
                .join(", "),
            if values.len() == 1 { "," } else { "" }
        ),
        Value::Object(values) => format!(
            "{{{}}}",
            values
                .iter()
                .map(|(key, value)| format!("{:?}: {}", key, python_repr(value)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn verify_evolvable_factory_audit(
    audit_value: &Value,
    facts: &EvolvablePairFacts,
    proposal_seed: &str,
    expected_operator_implementation: Option<&Value>,
) -> Result<()> {
    let audit = object_ref(audit_value, "G0 evolvable factory audit")?;
    exact_keys(
        audit,
        &[
            "schemaVersion",
            "authoritySha256",
            "pairIdentitySha256",
            "sides",
            "auditSha256",
        ],
        &[],
        "G0 evolvable factory audit",
    )?;
    require_schema(
        required(audit, "schemaVersion", "G0 evolvable factory audit")?,
        "temporal_qd_evolvable_module_factory_audit_v1",
        "G0 evolvable factory audit",
    )?;
    let authority_sha256 = sha(
        required(audit, "authoritySha256", "G0 evolvable factory audit")?,
        "evolvable factory authoritySha256",
    )?;
    let expected_operator = match expected_operator_implementation {
        Some(value) => Some(object_ref(
            value,
            "G0 expected evolvable operator implementation",
        )?),
        None => None,
    };
    let expected_authority = expected_operator
        .map(|operator| {
            sha(
                required(
                    operator,
                    "authoritySha256",
                    "G0 expected evolvable operator implementation",
                )?,
                "expected evolvable operator authoritySha256",
            )
        })
        .transpose()?;
    let expected_compiler_policy = expected_operator
        .map(|operator| {
            sha(
                required(
                    operator,
                    "compilerPolicySha256",
                    "G0 expected evolvable operator implementation",
                )?,
                "expected evolvable operator compilerPolicySha256",
            )
        })
        .transpose()?;
    if expected_authority
        .as_deref()
        .is_some_and(|expected| expected != authority_sha256)
    {
        return Err(contract(
            "evolvable factory audit authority does not bind publication operator authority",
        ));
    }
    if sha(
        required(audit, "pairIdentitySha256", "G0 evolvable factory audit")?,
        "evolvable audit pair identity",
    )? != facts.pair_identity_sha256
        || verify_self_hash(audit_value, "auditSha256", "G0 evolvable factory audit")?.is_empty()
    {
        return Err(contract("evolvable factory audit identity drifted"));
    }
    let sides = object_ref(
        required(audit, "sides", "G0 evolvable factory audit")?,
        "G0 evolvable factory audit sides",
    )?;
    exact_keys(
        sides,
        &["long", "short"],
        &[],
        "G0 evolvable factory audit sides",
    )?;
    let lineage = &facts.adapter.side_targeted_lineage;
    if lineage.len() != 2 {
        return Err(contract(
            "evolvable factory audit lacks exact side-targeted seed lineage",
        ));
    }
    let mut lineage_by_side = BTreeMap::new();
    for row in lineage {
        let fields = object_ref(row, "evolvable pair seed lineage")?;
        exact_keys(
            fields,
            &[
                "authoritySha256",
                "codec",
                "compilerPolicySha256",
                "genomeSha256",
                "operation",
                "programKind",
                "proposalSeed",
                "side",
            ],
            &[],
            "evolvable pair seed lineage",
        )?;
        let side = text(
            required(fields, "side", "evolvable pair seed lineage")?,
            "evolvable lineage side",
        )?;
        if !["long", "short"].contains(&side.as_str())
            || lineage_by_side.contains_key(&side)
            || text(
                required(fields, "operation", "evolvable pair seed lineage")?,
                "evolvable lineage operation",
            )? != "evolvable_module_pair_seed"
            || sha(
                required(fields, "authoritySha256", "evolvable pair seed lineage")?,
                "evolvable lineage authority",
            )? != authority_sha256
            || text(
                required(fields, "proposalSeed", "evolvable pair seed lineage")?,
                "evolvable lineage proposal seed",
            )? != proposal_seed
        {
            return Err(contract("evolvable factory side lineage drifted"));
        }
        for field in ["compilerPolicySha256", "genomeSha256"] {
            sha(
                required(fields, field, "evolvable pair seed lineage")?,
                field,
            )?;
        }
        if expected_compiler_policy.as_deref().is_some_and(|expected| {
            fields.get("compilerPolicySha256").and_then(Value::as_str) != Some(expected)
        }) {
            return Err(contract(
                "evolvable factory lineage compiler policy does not bind publication operator",
            ));
        }
        lineage_by_side.insert(side, fields);
    }
    for (side, program_sha256, topology_sha256, fingerprint_sha256) in [
        (
            "long",
            facts.long_program_sha256.as_str(),
            facts.long_semantic_topology_sha256.as_str(),
            facts.long_resource_fingerprint_sha256.as_str(),
        ),
        (
            "short",
            facts.short_program_sha256.as_str(),
            facts.short_semantic_topology_sha256.as_str(),
            facts.short_resource_fingerprint_sha256.as_str(),
        ),
    ] {
        let side_audit = object_ref(
            required(sides, side, "G0 evolvable factory audit sides")?,
            "G0 evolvable factory side audit",
        )?;
        exact_keys(
            side_audit,
            &[
                "programKind",
                "codec",
                "genomeSha256",
                "semanticTopologySha256",
                "resourceFingerprintSha256",
            ],
            &[],
            "G0 evolvable factory side audit",
        )?;
        if text(
            required(side_audit, "programKind", "G0 evolvable factory side audit")?,
            "evolvable side program kind",
        )? != "evolvable_module_genome_v1"
            || text(
                required(side_audit, "codec", "G0 evolvable factory side audit")?,
                "evolvable side codec",
            )? != "evolvable_module_genome_json_v1"
            || sha(
                required(
                    side_audit,
                    "genomeSha256",
                    "G0 evolvable factory side audit",
                )?,
                "evolvable side genome identity",
            )? != program_sha256
            || sha(
                required(
                    side_audit,
                    "semanticTopologySha256",
                    "G0 evolvable factory side audit",
                )?,
                "evolvable side topology identity",
            )? != topology_sha256
            || sha(
                required(
                    side_audit,
                    "resourceFingerprintSha256",
                    "G0 evolvable factory side audit",
                )?,
                "evolvable side resource fingerprint identity",
            )? != fingerprint_sha256
        {
            return Err(contract("evolvable factory side audit program drifted"));
        }
        let lineage = lineage_by_side
            .get(side)
            .ok_or_else(|| contract("evolvable factory audit lineage side is missing"))?;
        if text(
            required(lineage, "programKind", "evolvable pair seed lineage")?,
            "evolvable lineage program kind",
        )? != "evolvable_module_genome_v1"
            || text(
                required(lineage, "codec", "evolvable pair seed lineage")?,
                "evolvable lineage codec",
            )? != "evolvable_module_genome_json_v1"
            || sha(
                required(lineage, "genomeSha256", "evolvable pair seed lineage")?,
                "evolvable lineage genome identity",
            )? != program_sha256
        {
            return Err(contract("evolvable factory side lineage program drifted"));
        }
    }
    Ok(())
}

fn value_text(value: Option<&Value>) -> String {
    value.and_then(Value::as_str).unwrap_or_default().to_owned()
}

fn walk_guard<'a>(guard: &'a Value, output: &mut Vec<&'a Map<String, Value>>) {
    let Some(map) = guard.as_object() else { return };
    output.push(map);
    if let Some(child) = map.get("guard") {
        walk_guard(child, output);
    }
    if let Some(children) = map.get("guards").and_then(Value::as_array) {
        for child in children {
            walk_guard(child, output);
        }
    }
}

fn statically_true(guard: &Map<String, Value>) -> bool {
    match value_text(guard.get("kind")).as_str() {
        "state_age_at_least" => guard.get("events").and_then(Value::as_i64).unwrap_or(0) <= 0,
        "position_exists" => false,
        "all" => {
            let guards: Vec<&Map<String, Value>> = guard
                .get("guards")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_object)
                .collect();
            !guards.is_empty() && guards.iter().all(|item| statically_true(item))
        }
        "any" => guard
            .get("guards")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_object)
            .any(statically_true),
        _ => false,
    }
}

fn reachable(
    transitions: &[&Value],
    origins: impl IntoIterator<Item = String>,
) -> BTreeSet<String> {
    let mut reached: BTreeSet<String> = origins
        .into_iter()
        .filter(|item| !item.is_empty())
        .collect();
    let mut queue: VecDeque<String> = reached.iter().cloned().collect();
    while let Some(source) = queue.pop_front() {
        for transition in transitions {
            let Some(map) = transition.as_object() else {
                continue;
            };
            if value_text(map.get("sourceStateId")) != source {
                continue;
            }
            let destination = value_text(map.get("destinationStateId"));
            if !destination.is_empty() && reached.insert(destination.clone()) {
                queue.push_back(destination);
            }
        }
    }
    reached
}

fn static_reachability(profile: &Value) -> Result<Value> {
    let graph = object_ref(
        profile
            .get("graph")
            .ok_or_else(|| contract("candidate graph must be an object"))?,
        "candidate graph",
    )?;
    let transition_values = array_ref(
        required(graph, "transitions", "candidate graph")?,
        "candidate graph transitions",
    )?;
    let transitions: Vec<&Value> = transition_values
        .iter()
        .filter(|item| item.is_object())
        .collect();
    let initial = value_text(graph.get("initialStateId"));
    let all_reached = reachable(&transitions, [initial.clone()]);
    let mut filled = BTreeSet::new();
    for transition in &transitions {
        let map = object_ref(transition, "candidate graph transition")?;
        if value_text(map.get("eventClass")) != "execution" {
            continue;
        }
        let mut guards = Vec::new();
        if let Some(guard) = map.get("guard") {
            walk_guard(guard, &mut guards);
        }
        if guards.iter().any(|guard| {
            value_text(guard.get("kind")) == "execution_status_is"
                && value_text(guard.get("status")) == "filled"
        }) {
            let destination = value_text(map.get("destinationStateId"));
            if !destination.is_empty() {
                filled.insert(destination);
            }
        }
    }
    let post_entry = reachable(&transitions, filled.iter().cloned());
    let library = profile
        .get("executionConfig")
        .and_then(Value::as_object)
        .and_then(|value| value.get("managementLibrary"))
        .and_then(Value::as_object);
    let default_plan = library
        .and_then(|value| value.get("defaultPlanId"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let plans: Vec<&Map<String, Value>> = library
        .and_then(|value| value.get("plans"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|value| value.as_object())
        .collect();
    let plan_ids: BTreeSet<String> = plans
        .iter()
        .map(|plan| value_text(plan.get("id")))
        .collect();
    let mut referenced = BTreeSet::new();
    let mut issues: BTreeMap<String, u64> = BTreeMap::new();
    let mut issue = |name: &str| *issues.entry(name.to_owned()).or_default() += 1;
    let mut management_rows = Vec::new();
    let management_kinds: BTreeSet<&str> = [
        "move_stop_to_break_even_next_open",
        "tighten_stop_next_open",
        "set_target_next_open",
        "cancel_target_next_open",
        "activate_trailing_stop_next_open",
        "deactivate_trailing_stop_next_open",
    ]
    .into_iter()
    .collect();
    let mut entry_count = 0;
    for transition in &transitions {
        let row = object_ref(transition, "candidate graph transition")?;
        let actions = row
            .get("actions")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for action in actions.iter().filter_map(Value::as_object) {
            let kind = value_text(action.get("kind"));
            if kind == "enter_next_open" {
                entry_count += 1;
                let plan = value_text(action.get("managementPlanId"));
                let plan = if plan.is_empty() {
                    default_plan.clone().unwrap_or_default()
                } else {
                    plan
                };
                if plan.is_empty() || !plan_ids.contains(&plan) {
                    issue("entry_route_unknown_management_plan");
                } else {
                    referenced.insert(plan);
                }
                if !all_reached.contains(&value_text(row.get("sourceStateId"))) {
                    issue("entry_route_unreachable");
                }
            }
            if management_kinds.contains(kind.as_str()) {
                let source = value_text(row.get("sourceStateId"));
                let peers: Vec<&Map<String, Value>> = transitions
                    .iter()
                    .filter_map(|value| value.as_object())
                    .filter(|peer| {
                        value_text(peer.get("sourceStateId")) == source
                            && value_text(peer.get("eventClass"))
                                == value_text(row.get("eventClass"))
                    })
                    .collect();
                let mut ordered = peers;
                ordered.sort_by_key(|peer| {
                    (
                        peer.get("priority").and_then(Value::as_i64).unwrap_or(0),
                        value_text(peer.get("id")),
                    )
                });
                let dominated = ordered
                    .iter()
                    .take_while(|peer| value_text(peer.get("id")) != value_text(row.get("id")))
                    .any(|peer| {
                        peer.get("guard")
                            .and_then(Value::as_object)
                            .is_some_and(statically_true)
                    });
                let reachable_initial = all_reached.contains(&source);
                let reachable_after = post_entry.contains(&source);
                let action_row = object([
                    (
                        "transitionId",
                        row.get("id").cloned().unwrap_or(Value::Null),
                    ),
                    ("sourceStateId", Value::from(source.clone())),
                    (
                        "eventClass",
                        row.get("eventClass").cloned().unwrap_or(Value::Null),
                    ),
                    ("actionKind", Value::from(kind.clone())),
                    ("reachableFromInitial", Value::Bool(reachable_initial)),
                    ("reachableAfterEntry", Value::Bool(reachable_after)),
                    ("staticallyDominated", Value::Bool(dominated)),
                ]);
                if !reachable_initial {
                    issue("management_action_unreachable");
                }
                if kind != "activate_trailing_stop_next_open" && !reachable_after {
                    issue("management_action_not_post_entry");
                }
                if dominated {
                    issue("management_action_dominated");
                }
                management_rows.push(action_row);
            }
        }
    }
    if entry_count == 0 {
        issue("no_entry_route");
    }
    let orphan: Vec<String> = plan_ids.difference(&referenced).cloned().collect();
    if !orphan.is_empty() {
        issue("orphan_management_plan");
    }
    if filled.is_empty() {
        issue("no_entry_fill_transition");
    }
    let mut explicit: Vec<String> = plans
        .iter()
        .filter(|plan| {
            plan.get("trailingStop")
                .and_then(Value::as_object)
                .and_then(|trail| trail.get("activation"))
                .and_then(Value::as_object)
                .is_some_and(|activation| value_text(activation.get("kind")) == "explicit")
        })
        .map(|plan| value_text(plan.get("id")))
        .collect();
    explicit.sort();
    if !explicit.is_empty()
        && !management_rows.iter().any(|row| {
            row.get("actionKind").and_then(Value::as_str)
                == Some("activate_trailing_stop_next_open")
        })
    {
        issue("explicit_trailing_missing_activation_action");
    }
    if management_rows.iter().any(|row| {
        row.get("actionKind").and_then(Value::as_str) == Some("move_stop_to_break_even_next_open")
            && row.get("reachableAfterEntry") != Some(&Value::Bool(true))
    }) {
        issue("break_even_impossible_branch");
    }
    management_rows.sort_by_key(|row| {
        (
            value_text(row.get("transitionId")),
            value_text(row.get("actionKind")),
        )
    });
    let mut report = object([
        (
            "schemaVersion",
            Value::from("temporal_management_reachability_v1"),
        ),
        (
            "generatorVersion",
            Value::from("temporal_discovery_generator_v2_activation_aware"),
        ),
        ("acceptable", Value::Bool(issues.is_empty())),
        ("initialStateId", Value::from(initial)),
        (
            "reachableStates",
            Value::Array(all_reached.into_iter().map(Value::from).collect()),
        ),
        (
            "entryFillDestinationStates",
            Value::Array(filled.into_iter().map(Value::from).collect()),
        ),
        (
            "postEntryReachableStates",
            Value::Array(post_entry.into_iter().map(Value::from).collect()),
        ),
        (
            "managementPlanIds",
            Value::Array(plan_ids.into_iter().map(Value::from).collect()),
        ),
        (
            "referencedManagementPlanIds",
            Value::Array(referenced.into_iter().map(Value::from).collect()),
        ),
        (
            "orphanManagementPlanIds",
            Value::Array(orphan.into_iter().map(Value::from).collect()),
        ),
        (
            "explicitTrailingPlanIds",
            Value::Array(explicit.into_iter().map(Value::from).collect()),
        ),
        ("managementActions", Value::Array(management_rows)),
        (
            "issueCounts",
            object(
                issues
                    .into_iter()
                    .map(|(key, count)| (key, Value::from(count))),
            ),
        ),
    ]);
    let hash = canonical_sha256(&report)?;
    report
        .as_object_mut()
        .expect("report object")
        .insert("reachabilitySha256".to_owned(), Value::from(hash));
    Ok(report)
}

fn semantic(
    value: &Value,
    events: &HashMap<String, Value>,
    groups: &HashMap<String, Value>,
    indicators: &HashMap<String, Value>,
    plans: &HashMap<String, Value>,
    transitions: Option<&HashMap<String, String>>,
    states: Option<&HashMap<String, u64>>,
) -> Result<Value> {
    match value {
        Value::Array(values) => Ok(Value::Array(
            values
                .iter()
                .map(|item| semantic(item, events, groups, indicators, plans, transitions, states))
                .collect::<Result<Vec<_>>>()?,
        )),
        Value::Object(values) => {
            let mut result = Value::Object(Default::default());
            let output = result.as_object_mut().expect("object");
            for (key, item) in values {
                if ["id", "label", "description", "reasonCode"].contains(&key.as_str()) {
                    continue;
                }
                let mapped = match key.as_str() {
                    "sourceStateId" | "destinationStateId" | "stateId" => states
                        .and_then(|map| item.as_str().and_then(|id| map.get(id)))
                        .map(|value| Value::from(*value))
                        .unwrap_or_else(|| Value::from("unbound_state")),
                    "eventId" | "eventBindingId" => item
                        .as_str()
                        .and_then(|id| events.get(id))
                        .cloned()
                        .unwrap_or_else(|| Value::from("unbound_event")),
                    "groupId" | "evidenceGroupId" => item
                        .as_str()
                        .and_then(|id| groups.get(id))
                        .cloned()
                        .unwrap_or_else(|| Value::from("unbound_group")),
                    "indicatorInstanceId" | "indicatorId" => item
                        .as_str()
                        .and_then(|id| indicators.get(id))
                        .cloned()
                        .unwrap_or_else(|| Value::from("unbound_indicator")),
                    "indicatorInstanceIds" | "indicatorIds" => Value::Array(
                        array_ref(item, key)?
                            .iter()
                            .map(|part| -> Result<Value> {
                                let id = text(part, key)?;
                                Ok(indicators
                                    .get(&id)
                                    .cloned()
                                    .unwrap_or_else(|| Value::from("unbound_indicator")))
                            })
                            .collect::<Result<Vec<_>>>()?,
                    ),
                    "managementPlanId" | "planId" => item
                        .as_str()
                        .and_then(|id| plans.get(id))
                        .cloned()
                        .unwrap_or_else(|| Value::from("unbound_plan")),
                    "transitionId" => transitions
                        .and_then(|map| item.as_str().and_then(|id| map.get(id)))
                        .cloned()
                        .map(Value::from)
                        .unwrap_or_else(|| Value::from("unbound_transition")),
                    _ => semantic(item, events, groups, indicators, plans, transitions, states)?,
                };
                output.insert(key.clone(), mapped);
            }
            Ok(result)
        }
        _ => Ok(value.clone()),
    }
}

fn indicator_semantics(module: &DescriptorModule<'_>) -> Result<HashMap<String, Value>> {
    let profile = object_ref(module.profile, "frozen module profile")?;
    let indicators = array_ref(
        required(profile, "indicators", "frozen module profile")?,
        "frozen module indicators",
    )?;
    let catalog_payload = object_ref(module.catalog_payload, "frozen module catalog payload")?;
    let catalog = object_ref(
        required(catalog_payload, "catalog", "frozen module catalog payload")?,
        "frozen module catalog",
    )?;
    let primitives = array_ref(
        required(catalog, "indicators", "frozen module catalog")?,
        "frozen catalog indicators",
    )?;
    let mut primitive_by_id = HashMap::new();
    for primitive in primitives {
        let Some(primitive_map) = primitive.as_object() else {
            continue;
        };
        let nested = primitive_map
            .get("meta")
            .and_then(Value::as_object)
            .and_then(|meta| meta.get("id"))
            .and_then(Value::as_str);
        let flat = primitive_map.get("id").and_then(Value::as_str);
        if nested.is_some() && flat.is_some() && nested != flat {
            return Err(contract(
                "frozen module catalog indicator identity is ambiguous",
            ));
        }
        let Some(id) = nested.or(flat).filter(|value| !value.is_empty()) else {
            continue;
        };
        if primitive_by_id
            .insert(id.to_owned(), primitive.clone())
            .is_some()
        {
            return Err(contract(
                "frozen module catalog indicator identities duplicate",
            ));
        }
    }
    let mut result = HashMap::new();
    for indicator in indicators {
        let meta = object_ref(
            object_ref(indicator, "frozen module indicator")?
                .get("meta")
                .ok_or_else(|| contract("frozen module indicator is malformed"))?,
            "frozen module indicator meta",
        )?;
        let instance = text(
            required(meta, "instanceId", "frozen module indicator meta")?,
            "indicator instanceId",
        )?;
        let implementation = text(
            required(meta, "id", "frozen module indicator meta")?,
            "indicator id",
        )?;
        let base = match meta.get("baseIndicatorId") {
            None => implementation.clone(),
            Some(value) => text(value, "indicator baseIndicatorId")?,
        };
        if base.is_empty() {
            return Err(contract(
                "frozen module indicator base identity is malformed",
            ));
        }
        let primitive = primitive_by_id.get(&implementation).ok_or_else(|| {
            contract("module indicator implementation is absent from frozen catalog")
        })?;
        let semantic = object([
            ("baseIndicatorId", Value::from(base)),
            (
                "implementationIdentitySha256",
                Value::from(canonical_sha256(&object([
                    ("catalogSha256", Value::from(module.catalog_sha256)),
                    ("catalogPrimitive", primitive.clone()),
                ]))?),
            ),
        ]);
        if result.insert(instance, semantic).is_some() {
            return Err(contract("frozen module indicators duplicate an instance"));
        }
    }
    Ok(result)
}

fn bucket(value: &Map<String, Value>, fallback: &str) -> String {
    for key in ["percent", "multiple", "value", "bars", "hours"] {
        if let Some(number) = value.get(key).and_then(Value::as_f64) {
            if number.fract() == 0.0 {
                return format!("{key}:{}", number as i64);
            }
            return format!("{key}:{number}");
        }
    }
    fallback.to_owned()
}

fn module_descriptor(
    module: &DescriptorModule<'_>,
    transitions: &[&Value],
    states: &[&Value],
) -> Result<HashMap<String, String>> {
    let profile = object_ref(module.profile, "frozen module profile")?;
    let graph = object_ref(
        required(profile, "graph", "frozen module profile")?,
        "frozen module graph",
    )?;
    let indicators = indicator_semantics(module)?;
    let mut events = HashMap::new();
    for event in graph
        .get("eventBindings")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|item| item.get("id").and_then(Value::as_str).is_some())
    {
        let id = event.get("id").and_then(Value::as_str).unwrap().to_owned();
        events.insert(
            id,
            semantic(
                event,
                &HashMap::new(),
                &HashMap::new(),
                &indicators,
                &HashMap::new(),
                None,
                None,
            )?,
        );
    }
    let mut groups = HashMap::new();
    for group in graph
        .get("evidenceGroups")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|item| item.get("id").and_then(Value::as_str).is_some())
    {
        let id = group.get("id").and_then(Value::as_str).unwrap().to_owned();
        groups.insert(
            id,
            semantic(
                group,
                &events,
                &HashMap::new(),
                &indicators,
                &HashMap::new(),
                None,
                None,
            )?,
        );
    }
    let library = profile
        .get("executionConfig")
        .and_then(Value::as_object)
        .and_then(|value| value.get("managementLibrary"))
        .and_then(Value::as_object)
        .ok_or_else(|| contract("frozen module lacks closed management plan binding"))?;
    let plans_value = array_ref(
        required(library, "plans", "management library")?,
        "management plans",
    )?;
    let default_plan = text(
        required(library, "defaultPlanId", "management library")?,
        "defaultPlanId",
    )?;
    let mut raw_plans = HashMap::new();
    for plan in plans_value {
        let map = object_ref(plan, "management plan")?;
        let id = text(
            required(map, "id", "management plan")?,
            "management plan id",
        )?;
        if raw_plans.insert(id, plan.clone()).is_some() {
            return Err(contract("frozen module management plan binding is invalid"));
        }
    }
    if !raw_plans.contains_key(&default_plan) || raw_plans.len() != plans_value.len() {
        return Err(contract("frozen module management plan binding is invalid"));
    }
    let mut plans = HashMap::new();
    for (id, plan) in &raw_plans {
        plans.insert(
            id.clone(),
            semantic(
                plan,
                &events,
                &groups,
                &indicators,
                &HashMap::new(),
                None,
                None,
            )?,
        );
    }
    let mut state_ids = HashMap::new();
    for (index, state) in states.iter().enumerate() {
        let id = text(
            required(
                object_ref(state, "compiled graph state")?,
                "id",
                "compiled graph state",
            )?,
            "compiled state id",
        )?;
        if state_ids.insert(id, index as u64).is_some() {
            return Err(contract("compiled graph states are malformed"));
        }
    }
    if state_ids.is_empty() || states.len() > 32 || transitions.len() > 128 {
        return Err(contract(
            "compiled graph exceeds G0 canonical runtime bound",
        ));
    }
    let mut transition_ids = HashMap::new();
    for (index, transition) in transitions.iter().enumerate() {
        let map = object_ref(transition, "compiled graph transition")?;
        let id = text(
            required(map, "id", "compiled graph transition")?,
            "compiled transition id",
        )?;
        let source = text(
            required(map, "sourceStateId", "compiled graph transition")?,
            "compiled transition source",
        )?;
        let target = text(
            required(map, "destinationStateId", "compiled graph transition")?,
            "compiled transition destination",
        )?;
        if !state_ids.contains_key(&source)
            || !state_ids.contains_key(&target)
            || transition_ids.insert(id, index).is_some()
        {
            return Err(contract("compiled graph transition wiring is malformed"));
        }
    }
    let mut transition_semantics = HashMap::new();
    for transition in transitions {
        let mut stripped = (*transition).clone();
        stripped.as_object_mut().expect("object").remove("id");
        stripped
            .as_object_mut()
            .expect("object")
            .remove("transitionId");
        let id = transition
            .get("id")
            .and_then(Value::as_str)
            .unwrap()
            .to_owned();
        transition_semantics.insert(
            id,
            canonical_sha256(&semantic(
                &stripped,
                &events,
                &groups,
                &indicators,
                &plans,
                None,
                None,
            )?)?,
        );
    }
    let topology = canonical_sha256(&object([
        (
            "schemaVersion",
            Value::from("temporal_qd_g0_declared_graph_semantics_v1"),
        ),
        ("declarationOrderIsSemantic", Value::Bool(true)),
        (
            "states",
            Value::Array(
                states
                    .iter()
                    .map(|state| {
                        semantic(
                            state,
                            &events,
                            &groups,
                            &indicators,
                            &plans,
                            Some(&transition_semantics),
                            Some(&state_ids),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
        ),
        (
            "transitions",
            Value::Array(
                transitions
                    .iter()
                    .map(|transition| {
                        semantic(
                            transition,
                            &events,
                            &groups,
                            &indicators,
                            &plans,
                            Some(&transition_semantics),
                            Some(&state_ids),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
        ),
    ]))?;
    let edge_hashes = transitions
        .iter()
        .map(|transition| -> Result<String> {
            let semantic_value = semantic(
                transition,
                &events,
                &groups,
                &indicators,
                &plans,
                Some(&transition_semantics),
                None,
            )?;
            canonical_sha256(&semantic_value).map_err(G0Error::from)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut edge_hashes = edge_hashes;
    edge_hashes.sort();
    let plan = object_ref(
        raw_plans.get(&default_plan).expect("default exists"),
        "default management plan",
    )?;
    let stop = object_ref(
        required(plan, "initialStop", "default management plan")?,
        "initial stop",
    )?;
    let stop_kind = text(required(stop, "kind", "initial stop")?, "initial stop kind")?;
    let target_bucket = match plan.get("initialTarget") {
        None | Some(Value::Null) => "no_target|none".to_owned(),
        Some(value) => {
            let target = object_ref(value, "initial target")?;
            let kind = text(
                required(target, "kind", "initial target")?,
                "initial target kind",
            )?;
            let category = if kind == "reward_multiple" {
                "coupled"
            } else if kind == "fixed_percent" {
                "decoupled"
            } else {
                "dynamic"
            };
            format!("{category}:{kind}|{}", bucket(target, "dynamic"))
        }
    };
    let hold = plan.get("holdPolicy").and_then(Value::as_object);
    let hold_kind = hold
        .and_then(|value| value.get("kind"))
        .and_then(Value::as_str)
        .unwrap_or("none");
    let hold_bucket = format!(
        "{hold_kind}|{}",
        hold.map(|value| bucket(value, "default"))
            .unwrap_or_else(|| "default".to_owned())
    );
    let mut management = BTreeSet::new();
    if plan
        .get("trailingStop")
        .and_then(Value::as_object)
        .is_some()
    {
        management.insert("trailing".to_owned());
    }
    for transition in transitions {
        let actions = match transition.get("actions") {
            None | Some(Value::Null) => &[][..],
            Some(value) => array_ref(value, "native transition actions")?,
        };
        for action in actions {
            let action = object_ref(action, "native transition action")?;
            let kind = value_text(action.get("kind"));
            if [
                "move_stop_to_break_even_next_open",
                "tighten_stop_next_open",
                "set_target_next_open",
                "cancel_target_next_open",
                "activate_trailing_stop_next_open",
                "deactivate_trailing_stop_next_open",
            ]
            .contains(&kind.as_str())
            {
                management.insert(kind);
            }
        }
    }
    let management_mode = {
        let joined = management.into_iter().collect::<Vec<_>>().join(",");
        if joined.is_empty() {
            "none".to_owned()
        } else {
            joined
        }
    };
    let mut indicator_hashes = indicators
        .values()
        .map(canonical_sha256)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    indicator_hashes.sort();
    let mut group_hashes = groups
        .values()
        .map(canonical_sha256)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    group_hashes.sort();
    let mut event_hashes = events
        .values()
        .map(canonical_sha256)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    event_hashes.sort();
    let fuzzy_group_hashes = group_hashes.clone();
    Ok(HashMap::from([
        ("topology".to_owned(), topology),
        (
            "graphSize".to_owned(),
            format!("states:{}|transitions:{}", states.len(), transitions.len()),
        ),
        (
            "indicatorSemantics".to_owned(),
            canonical_sha256(&object([(
                "indicators",
                Value::Array(indicator_hashes.into_iter().map(Value::from).collect()),
            )]))?,
        ),
        (
            "fuzzyMembershipShape".to_owned(),
            canonical_sha256(&object([(
                "groups",
                Value::Array(fuzzy_group_hashes.into_iter().map(Value::from).collect()),
            )]))?,
        ),
        (
            "entryGuardEventEvidenceSemantics".to_owned(),
            canonical_sha256(&object([
                (
                    "events",
                    Value::Array(event_hashes.into_iter().map(Value::from).collect()),
                ),
                (
                    "groups",
                    Value::Array(group_hashes.into_iter().map(Value::from).collect()),
                ),
                (
                    "edges",
                    Value::Array(edge_hashes.into_iter().map(Value::from).collect()),
                ),
            ]))?,
        ),
        ("holdKindBucket".to_owned(), hold_bucket),
        (
            "initialStopKindBucket".to_owned(),
            format!("{stop_kind}|{}", bucket(stop, "dynamic")),
        ),
        ("initialTargetKindBucket".to_owned(), target_bucket),
        ("graphManagementTrailingModes".to_owned(), management_mode),
    ]))
}

fn per_side_liveness(profile: &Value, report: &Value) -> Result<Value> {
    let graph = object_ref(
        profile
            .get("graph")
            .ok_or_else(|| contract("frozen pair lacks graph"))?,
        "frozen pair graph",
    )?;
    let arbitration = object_ref(
        required(graph, "entryArbitration", "frozen pair graph")?,
        "entry arbitration",
    )?;
    let modules = array_ref(
        required(arbitration, "modules", "entry arbitration")?,
        "entry arbitration modules",
    )?;
    let transitions = array_ref(
        required(graph, "transitions", "frozen pair graph")?,
        "frozen pair transitions",
    )?;
    let reachable: BTreeSet<String> = array_ref(
        report
            .get("reachableStates")
            .ok_or_else(|| contract("static report lacks reachable states"))?,
        "reachable states",
    )?
    .iter()
    .filter_map(Value::as_str)
    .map(ToOwned::to_owned)
    .collect();
    let by_id: HashMap<String, &Value> = transitions
        .iter()
        .filter_map(|item| {
            item.get("id")
                .and_then(Value::as_str)
                .map(|id| (id.to_owned(), item))
        })
        .collect();
    let mut proof = Value::Object(Default::default());
    for module in modules {
        let module = object_ref(module, "frozen pair module")?;
        let side = text(
            required(module, "direction", "frozen pair module")?,
            "module direction",
        )?;
        if side != "long" && side != "short" {
            return Err(contract("frozen pair module is malformed"));
        }
        let mut entry_routes = 0_u64;
        let mut reachable_routes = 0_u64;
        for transition_id in array_ref(
            required(module, "transitionIds", "frozen pair module")?,
            "module transitionIds",
        )? {
            let transition = by_id
                .get(&text(transition_id, "module transitionId")?)
                .ok_or_else(|| contract("frozen pair module references missing transition"))?;
            let transition_map = object_ref(transition, "frozen pair transition")?;
            if transition_map
                .get("actions")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_object)
                .any(|action| value_text(action.get("kind")) == "enter_next_open")
            {
                entry_routes += 1;
                if reachable.contains(&value_text(transition_map.get("sourceStateId"))) {
                    reachable_routes += 1;
                }
            }
        }
        proof.as_object_mut().expect("proof object").insert(
            side,
            object([
                ("entryActionRouteCount", Value::from(entry_routes)),
                (
                    "reachableEntryActionRouteCount",
                    Value::from(reachable_routes),
                ),
                ("potential", Value::Bool(reachable_routes > 0)),
            ]),
        );
    }
    if proof.as_object().expect("proof object").len() != 2
        || proof["long"]["potential"] != Value::Bool(true)
        || proof["short"]["potential"] != Value::Bool(true)
    {
        return Err(contract("per-side entry liveness proof is incomplete"));
    }
    Ok(proof)
}

fn pair_descriptor(pair: &DescriptorPair<'_>, liveness: &Value) -> Result<Value> {
    let graph = object_ref(
        pair.profile
            .get("graph")
            .ok_or_else(|| contract("frozen pair lacks compiled graph"))?,
        "frozen pair graph",
    )?;
    let initial = text(
        required(graph, "initialStateId", "frozen pair graph")?,
        "initialStateId",
    )?;
    if initial.is_empty() {
        return Err(contract("frozen pair compiled graph lacks initialStateId"));
    }
    let compiled = array_ref(
        required(graph, "transitions", "frozen pair graph")?,
        "compiled transitions",
    )?;
    let states = array_ref(
        required(graph, "states", "frozen pair graph")?,
        "compiled states",
    )?;
    let arbitration = object_ref(
        required(graph, "entryArbitration", "frozen pair graph")?,
        "entry arbitration",
    )?;
    let modules = array_ref(
        required(arbitration, "modules", "entry arbitration")?,
        "entry arbitration modules",
    )?;
    let by_id: HashMap<String, &Value> = compiled
        .iter()
        .filter_map(|item| {
            item.get("id")
                .and_then(Value::as_str)
                .map(|id| (id.to_owned(), item))
        })
        .collect();
    let mut vector = Map::new();
    for (side, module) in [("long", &pair.long), ("short", &pair.short)] {
        let manifest = modules
            .iter()
            .find(|item| item.get("direction").and_then(Value::as_str) == Some(side))
            .ok_or_else(|| contract("frozen pair compiled module is missing"))?;
        let manifest = object_ref(manifest, "compiled module")?;
        let ids: Vec<String> = array_ref(
            required(manifest, "transitionIds", "compiled module")?,
            "compiled module transitionIds",
        )?
        .iter()
        .map(|id| text(id, "compiled module transitionId"))
        .collect::<Result<_>>()?;
        let requested: HashSet<String> = ids.iter().cloned().collect();
        if requested.len() != ids.len() || ids.iter().any(|id| !by_id.contains_key(id)) {
            return Err(contract(
                "frozen pair compiled module transition binding is incomplete",
            ));
        }
        let side_transitions: Vec<&Value> = compiled
            .iter()
            .filter(|item| {
                item.get("id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| requested.contains(id))
            })
            .collect();
        if side_transitions.len() != ids.len() {
            return Err(contract("frozen pair compiled module lacks transitions"));
        }
        let mut state_ids: HashSet<String> = array_ref(
            required(manifest, "stateIds", "compiled module")?,
            "compiled module stateIds",
        )?
        .iter()
        .map(|id| text(id, "compiled module stateId"))
        .collect::<Result<_>>()?;
        state_ids.insert(initial.clone());
        let side_states: Vec<&Value> = states
            .iter()
            .filter(|item| {
                item.get("id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| state_ids.contains(id))
            })
            .collect();
        let referenced: HashSet<String> = side_transitions
            .iter()
            .flat_map(|item| {
                [
                    value_text(item.get("sourceStateId")),
                    value_text(item.get("destinationStateId")),
                ]
            })
            .collect();
        let present: HashSet<String> = side_states
            .iter()
            .map(|item| value_text(item.get("id")))
            .collect();
        if referenced != present {
            return Err(contract(
                "frozen pair compiled module state binding is incomplete",
            ));
        }
        for (key, value) in module_descriptor(module, &side_transitions, &side_states)? {
            vector.insert(format!("{side}.{key}"), Value::from(value));
        }
    }
    vector.insert(
        "staticLongShortActivationPotential".to_owned(),
        Value::from(format!(
            "long:{}|short:{}",
            liveness["long"]["potential"].as_bool().unwrap_or(false),
            liveness["short"]["potential"].as_bool().unwrap_or(false)
        )),
    );
    let vector = Value::Object(vector);
    exact_keys(
        object_ref(&vector, "descriptor vector")?,
        &DESCRIPTOR_AXES,
        &[],
        "descriptor vector",
    )?;
    Ok(vector)
}

fn derive_descriptor_projection_from_verified(
    surface: &AcceptedEntrySurface,
    pair: &FrozenPair,
) -> Result<Value> {
    let descriptor_pair = DescriptorPair {
        profile: &pair.profile,
        long: DescriptorModule {
            profile: &pair.long.profile,
            catalog_payload: &pair.long.catalog.payload,
            catalog_sha256: &pair.long.catalog.sha256,
        },
        short: DescriptorModule {
            profile: &pair.short.profile,
            catalog_payload: &pair.short.catalog.payload,
            catalog_sha256: &pair.short.catalog.sha256,
        },
        pair_identity_sha256: frozen(pair.identity_sha256())?,
        native_validation_report_sha256: &pair.native_validation_report_sha256,
    };
    derive_descriptor_projection_from_descriptor_pair(surface, &descriptor_pair)
}

fn derive_descriptor_projection_from_descriptor_pair(
    surface: &AcceptedEntrySurface,
    pair: &DescriptorPair<'_>,
) -> Result<Value> {
    let report = static_reachability(pair.profile)?;
    let reachability_sha = verify_self_hash(
        &report,
        "reachabilitySha256",
        "canonical static reachability report",
    )?;
    if report.get("acceptable") != Some(&Value::Bool(true)) {
        return Err(contract(
            "canonical static reachability report is not acceptable",
        ));
    }
    let liveness = per_side_liveness(pair.profile, &report)?;
    let descriptor_vector = pair_descriptor(pair, &liveness)?;
    let mut projection = object([
        ("schemaVersion", Value::from(DESCRIPTOR_PROJECTION_SCHEMA)),
        ("candidateId", Value::from(surface.candidate_id.clone())),
        (
            "candidateIdentitySha256",
            Value::from(surface.candidate_identity_sha256.clone()),
        ),
        (
            "pairIdentitySha256",
            Value::from(pair.pair_identity_sha256.clone()),
        ),
        ("longCatalogSha256", Value::from(pair.long.catalog_sha256)),
        ("shortCatalogSha256", Value::from(pair.short.catalog_sha256)),
        (
            "nativeValidationReportSha256",
            Value::from(pair.native_validation_report_sha256),
        ),
        (
            "staticReachabilityReportSha256",
            Value::from(reachability_sha),
        ),
        ("perSideLivenessProof", liveness),
        ("descriptorVector", descriptor_vector),
    ]);
    let hash = canonical_sha256(&projection)?;
    projection
        .as_object_mut()
        .expect("projection object")
        .insert("descriptorProjectionSha256".to_owned(), Value::from(hash));
    Ok(projection)
}

/// Rehydrate one rich G0 entry exactly once, prove its native/static
/// semantics, derive the descriptor, and return only its compact admission
/// facts.  This is the streaming handoff used by the v5 post-construction
/// funnel; it avoids the historical derive-then-rederive path.
fn admit_accepted_pair_entry_with_operator(
    entry: &Value,
    expected_operator_implementation: Option<&Value>,
) -> Result<AdmittedAcceptedPairEntry> {
    if is_evolvable_pair_entry(entry) {
        let surface = validate_accepted_entry_surface(entry)?;
        let (surface, facts) =
            verify_evolvable_accepted_entry(entry, surface, expected_operator_implementation)?;
        let descriptor_pair = DescriptorPair {
            profile: &facts.adapter.profile,
            long: DescriptorModule {
                profile: &facts.adapter.long.profile,
                catalog_payload: &facts.adapter.long.catalog.payload,
                catalog_sha256: &facts.adapter.long.catalog.sha256,
            },
            short: DescriptorModule {
                profile: &facts.adapter.short.profile,
                catalog_payload: &facts.adapter.short.catalog.payload,
                catalog_sha256: &facts.adapter.short.catalog.sha256,
            },
            pair_identity_sha256: facts.pair_identity_sha256,
            native_validation_report_sha256: &facts.adapter.native_validation_report_sha256,
        };
        let descriptor_projection =
            derive_descriptor_projection_from_descriptor_pair(&surface, &descriptor_pair)?;
        let executable_semantic_sha256 = canonical_sha256(&object([
            (
                "schemaVersion",
                Value::from("temporal_qd_pair_genome_semantics_v1"),
            ),
            (
                "longProfileSha256",
                Value::from(facts.adapter.long.profile_sha256.clone()),
            ),
            (
                "shortProfileSha256",
                Value::from(facts.adapter.short.profile_sha256.clone()),
            ),
        ]))?;
        return Ok(AdmittedAcceptedPairEntry {
            entry_sha256: surface.entry_sha256,
            proposal_ordinal: surface.proposal_ordinal,
            generation_index: surface.generation_index,
            birth_ordinal: surface.birth_ordinal,
            candidate_id: surface.candidate_id,
            candidate_identity_sha256: surface.candidate_identity_sha256,
            executable_semantic_sha256,
            descriptor_projection,
        });
    }
    let (surface, pair) = verify_accepted_entry(entry)?;
    let descriptor_projection = derive_descriptor_projection_from_verified(&surface, &pair)?;
    let executable_semantic_sha256 = canonical_sha256(&object([
        (
            "schemaVersion",
            Value::from("temporal_qd_pair_genome_semantics_v1"),
        ),
        (
            "longProfileSha256",
            Value::from(pair.long.profile_sha256.clone()),
        ),
        (
            "shortProfileSha256",
            Value::from(pair.short.profile_sha256.clone()),
        ),
    ]))?;
    Ok(AdmittedAcceptedPairEntry {
        entry_sha256: surface.entry_sha256,
        proposal_ordinal: surface.proposal_ordinal,
        generation_index: surface.generation_index,
        birth_ordinal: surface.birth_ordinal,
        candidate_id: surface.candidate_id,
        candidate_identity_sha256: surface.candidate_identity_sha256,
        executable_semantic_sha256,
        descriptor_projection,
    })
}

pub fn admit_accepted_pair_entry(entry: &Value) -> Result<AdmittedAcceptedPairEntry> {
    admit_accepted_pair_entry_with_operator(entry, None)
}

/// Native G0 funnel admission has the closed publication operator authority
/// available.  Unlike compatibility callers, it must prove every evolvable
/// factory audit and side lineage derives from that exact authority/compiler.
pub fn admit_accepted_pair_entry_bound_to_operator(
    entry: &Value,
    expected_operator_implementation: &Value,
) -> Result<AdmittedAcceptedPairEntry> {
    admit_accepted_pair_entry_with_operator(entry, Some(expected_operator_implementation))
}

pub fn derive_descriptor_projection_from_rich_entry(entry: &Value) -> Result<Value> {
    Ok(admit_accepted_pair_entry(entry)?.descriptor_projection)
}

/// Materialize the public compact reference from a previously admitted rich
/// entry.  The admission object is intentionally sufficient to avoid a second
/// full FrozenPair/static-reachability reconstruction in the streaming path.
pub fn project_admitted_pair_entry(
    construction_pool_identity_sha256: &str,
    journal_relative_path: &str,
    admitted: &AdmittedAcceptedPairEntry,
) -> Result<Value> {
    let pool_identity = sha(
        &Value::from(construction_pool_identity_sha256),
        "constructionPoolIdentitySha256",
    )?;
    normal_journal_relative_path(&Value::from(journal_relative_path))?;
    let projection = object_ref(&admitted.descriptor_projection, "G0 descriptor projection")?;
    let lineage_without_hash = object([
        (
            "schemaVersion",
            Value::from("temporal_qd_g0_construction_lineage_v1"),
        ),
        ("entrySha256", Value::from(admitted.entry_sha256.clone())),
        ("proposalOrdinal", Value::from(admitted.proposal_ordinal)),
        ("generationIndex", Value::from(admitted.generation_index)),
        ("birthOrdinal", Value::from(admitted.birth_ordinal)),
        ("originKind", Value::from("random_immigrant")),
        ("candidateId", Value::from(admitted.candidate_id.clone())),
        (
            "candidateIdentitySha256",
            Value::from(admitted.candidate_identity_sha256.clone()),
        ),
    ]);
    let mut lineage = lineage_without_hash.clone();
    lineage.as_object_mut().expect("lineage is object").insert(
        "constructionLineageSha256".to_owned(),
        Value::from(canonical_sha256(&lineage_without_hash)?),
    );
    let descriptor_sha = sha(
        required(
            projection,
            "descriptorProjectionSha256",
            "G0 descriptor projection",
        )?,
        "descriptorProjectionSha256",
    )?;
    let reference_without_hash = object([
        ("schemaVersion", Value::from(ACCEPTED_REFERENCE_SCHEMA)),
        ("constructionPoolIdentitySha256", Value::from(pool_identity)),
        ("proposalOrdinal", Value::from(admitted.proposal_ordinal)),
        (
            "journalReference",
            object([
                (
                    "schemaVersion",
                    Value::from("temporal_qd_g0_journal_reference_v1"),
                ),
                ("journalRelativePath", Value::from(journal_relative_path)),
                ("entrySha256", Value::from(admitted.entry_sha256.clone())),
            ]),
        ),
        (
            "acceptedPairEntrySha256",
            Value::from(admitted.entry_sha256.clone()),
        ),
        ("candidateId", Value::from(admitted.candidate_id.clone())),
        (
            "candidateIdentitySha256",
            Value::from(admitted.candidate_identity_sha256.clone()),
        ),
        ("constructionLineage", lineage),
        (
            "descriptorProjection",
            admitted.descriptor_projection.clone(),
        ),
        ("descriptorProjectionSha256", Value::from(descriptor_sha)),
    ]);
    let mut reference = reference_without_hash.clone();
    reference
        .as_object_mut()
        .expect("reference is object")
        .insert(
            "referenceSha256".to_owned(),
            Value::from(canonical_sha256(&reference_without_hash)?),
        );
    validate_accepted_reference(&reference)?;
    Ok(reference)
}

/// Compatibility entry point for callers carrying a descriptor alongside an
/// entry.  The provided descriptor is accepted only if it exactly equals an
/// independently derived native projection.
pub fn project_accepted_pair_entry_with_descriptor(
    construction_pool_identity_sha256: &str,
    proposal_ordinal: u64,
    journal_relative_path: &str,
    entry: &Value,
    descriptor_projection: &Value,
) -> Result<Value> {
    let admitted = admit_accepted_pair_entry(entry)?;
    if admitted.proposal_ordinal != proposal_ordinal {
        return Err(contract(
            "G0 proposal ordinal does not bind journal/candidate",
        ));
    }
    normal_journal_relative_path(&Value::from(journal_relative_path))?;
    if descriptor_projection != &admitted.descriptor_projection {
        return Err(contract(
            "provided G0 descriptor projection diverges from rich frozen authority",
        ));
    }
    project_admitted_pair_entry(
        construction_pool_identity_sha256,
        journal_relative_path,
        &admitted,
    )
}

/// Rehydrate a closed accepted rich entry, derive its native descriptor, and
/// retain only the compact reference contract.
pub fn project_accepted_pair_entry(
    construction_pool_identity_sha256: &str,
    proposal_ordinal: u64,
    journal_relative_path: &str,
    entry: &Value,
) -> Result<Value> {
    sha(
        &Value::from(construction_pool_identity_sha256),
        "constructionPoolIdentitySha256",
    )?;
    normal_journal_relative_path(&Value::from(journal_relative_path))?;
    let admitted = admit_accepted_pair_entry(entry)?;
    if admitted.proposal_ordinal != proposal_ordinal {
        return Err(contract(
            "G0 proposal ordinal does not bind journal/candidate",
        ));
    }
    project_admitted_pair_entry(
        construction_pool_identity_sha256,
        journal_relative_path,
        &admitted,
    )
}
