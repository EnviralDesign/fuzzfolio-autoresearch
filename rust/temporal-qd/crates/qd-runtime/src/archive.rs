//! Exact parent selection over a frozen Temporal QD v3 archive.
//!
//! This module deliberately owns only the selection projection and its mutable
//! visit/offspring counters.  The archive remains immutable and self-authenticating;
//! proposal ordinals remain owned by the proposal planner.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use temporal_qd_contract::{Map, Value, canonical_sha256, canonical_sha256_without_object_field};
use temporal_qd_kernel::{
    factory::ParentReference,
    genome::{FrozenPair, IdentitySnapshot},
    proposal::{ParentSelector, ProposalError},
    schedule::PythonRandom,
};

pub const QD_ARCHIVE_SCHEMA: &str = "temporal_qd_archive_v3";
pub const QD_VERSION: &str = "temporal_qd_evolution_v3";
pub const QD_POLICY_NAME: &str = "stage5e7_v3_robust_quality_archive";
pub const QD_POLICY_SHA256: &str =
    "sha256:837c670a3cec80246a3231397d945b7cfd602035752eddbe0593dc9644579ca8";
pub const ARCHIVE_PARENT_STATE_SCHEMA: &str = "temporal_qd_archive_parent_state_v1";
pub const PAIR_PARENT_SELECTION_SCHEMA: &str = "temporal_qd_pair_parent_selection_v1";

const PAIR_POLICY_SCHEMA: &str = "temporal_qd_bidirectional_pair_policy_v1";
const ROTATING_PROJECTION_SCHEMA: &str = "temporal_qd_rotating_parent_projection_v1";
const DEFAULT_CAPPED_TRADES: u64 = 20;
const DESCRIPTOR_KEYS: [&str; 7] = [
    "operatorFamilies",
    "mutationDepth",
    "entryEvents",
    "managementActions",
    "graphNodes",
    "tradeFrequency",
    "medianHolding",
];

#[derive(Debug, thiserror::Error)]
pub enum ArchiveError {
    #[error("archive parent contract failure: {0}")]
    Contract(String),
}

pub type Result<T> = std::result::Result<T, ArchiveError>;

fn invalid(message: impl Into<String>) -> ArchiveError {
    ArchiveError::Contract(message.into())
}

fn object(entries: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    Value::Object(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect::<Map<_, _>>(),
    )
}

fn exact_keys(map: &Map<String, Value>, expected: &[&str], label: &str) -> Result<()> {
    if map.len() != expected.len() || expected.iter().any(|key| !map.contains_key(*key)) {
        return Err(invalid(format!("{label} fields are not exact")));
    }
    Ok(())
}

fn map<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| invalid(format!("{label} must be an object")))
}

fn field<'a>(map: &'a Map<String, Value>, key: &str, label: &str) -> Result<&'a Value> {
    map.get(key)
        .ok_or_else(|| invalid(format!("{label} lacks {key}")))
}

fn string(map: &Map<String, Value>, key: &str, label: &str) -> Result<String> {
    field(map, key, label)?
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| invalid(format!("{label} {key} must be a nonempty string")))
}

fn exact_sha256(value: &str, label: &str) -> Result<()> {
    if value.len() != 71
        || !value.starts_with("sha256:")
        || !value.as_bytes()[7..]
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(invalid(format!(
            "{label} must be a lowercase SHA-256 identity"
        )));
    }
    Ok(())
}

fn finite(value: &Value, label: &str) -> Result<f64> {
    let number = value
        .as_f64()
        .filter(|number| number.is_finite())
        .ok_or_else(|| invalid(format!("{label} must be a finite JSON number")))?;
    Ok(number)
}

fn nonnegative_count(value: Option<&Value>, label: &str) -> Result<u64> {
    match value {
        None | Some(Value::Null) => Ok(0),
        Some(value) => value
            .as_u64()
            .ok_or_else(|| invalid(format!("{label} must be a nonnegative integer"))),
    }
}

fn canonical_identity(payload: &Value, identity_key: &str, label: &str) -> Result<String> {
    let source = map(payload, label)?;
    let identity = string(source, identity_key, label)?;
    exact_sha256(&identity, &format!("{label} {identity_key}"))?;
    let actual = canonical_sha256_without_object_field(payload, identity_key)
        .map_err(|error| invalid(format!("{label} is not canonical: {error}")))?;
    if actual != identity {
        return Err(invalid(format!("{label} identity mismatch")));
    }
    Ok(identity)
}

#[derive(Clone, Debug)]
struct SelectionState {
    base_selection_visits: u64,
    base_offspring_attempts: u64,
    selection_visits: u64,
    offspring_attempts: u64,
}

#[derive(Clone, Debug)]
pub struct VerifiedArchiveMember {
    candidate_id: String,
    archive_lane: String,
    pair_identity_sha256: String,
    /// The exact frozen-pair bytes are needed only by a member which can be
    /// selected.  A real archive contains many observational members, so
    /// retaining every embedded pair would multiply the archive's footprint
    /// without changing either validation or ledger semantics.
    pair_payload: Option<Arc<Value>>,
    pareto_front_audit: Value,
    crowding_distance_audit: Value,
    order: MemberOrder,
    robust_return: f64,
    finite_data: bool,
    passes_support_gate: bool,
    valid_for_quality: bool,
    robust_breeder_eligible: bool,
    cumulative_evidence_archive_sha256: Option<String>,
    /// Exact ledger/evidence inputs retained after the full candidate JSON is
    /// released.  The identity ledger never consumes the candidate graph; it
    /// consumes these five identities and `executionConfig` only.
    candidate_identity_sha256: Option<String>,
    program_sha256: String,
    source_profile_sha256: String,
    profile_snapshot_sha256: Option<String>,
    execution_config: Value,
    long_profile_sha256: String,
    short_profile_sha256: String,
}

#[derive(Clone, Debug)]
struct MemberOrder {
    first: i64,
    rest: [OrderFloat; 4],
    candidate_id: String,
}

#[derive(Clone, Copy, Debug)]
enum OrderFloat {
    NegativeInfinity,
    Finite(f64),
}

impl OrderFloat {
    fn compare(self, other: Self) -> Ordering {
        match (self, other) {
            (Self::NegativeInfinity, Self::NegativeInfinity) => Ordering::Equal,
            (Self::NegativeInfinity, Self::Finite(_)) => Ordering::Less,
            (Self::Finite(_), Self::NegativeInfinity) => Ordering::Greater,
            (Self::Finite(left), Self::Finite(right)) => {
                left.partial_cmp(&right).expect("validated finite floats")
            }
        }
    }
}

impl MemberOrder {
    fn compare(&self, other: &Self) -> Ordering {
        self.first
            .cmp(&other.first)
            .then_with(|| self.rest[0].compare(other.rest[0]))
            .then_with(|| self.rest[1].compare(other.rest[1]))
            .then_with(|| self.rest[2].compare(other.rest[2]))
            .then_with(|| self.rest[3].compare(other.rest[3]))
            .then_with(|| self.candidate_id.cmp(&other.candidate_id))
    }
}

#[derive(Clone, Debug)]
struct ArchiveCell {
    cell_id: String,
    coordinates: [String; 7],
    members: Vec<Arc<VerifiedArchiveMember>>,
}

impl VerifiedArchiveMember {
    pub fn candidate_id(&self) -> &str {
        &self.candidate_id
    }

    pub fn pair_identity_sha256(&self) -> &str {
        &self.pair_identity_sha256
    }

    fn pair_payload(&self) -> Result<&Value> {
        self.pair_payload
            .as_deref()
            .ok_or_else(|| invalid("unselectable archive member lacks frozen pair payload"))
    }

    pub(crate) fn ledger_candidate(&self) -> Result<Value> {
        let mut source_profile = Map::new();
        source_profile.insert("executionConfig".to_owned(), self.execution_config.clone());
        let mut candidate = Map::new();
        candidate.insert(
            "candidateIdentitySha256".to_owned(),
            Value::String(self.candidate_identity_sha256.clone().ok_or_else(|| {
                invalid("archive ledger candidate lacks candidateIdentitySha256")
            })?),
        );
        candidate.insert(
            "programSha256".to_owned(),
            Value::String(self.program_sha256.clone()),
        );
        candidate.insert(
            "sourceProfileSha256".to_owned(),
            Value::String(self.source_profile_sha256.clone()),
        );
        candidate.insert("sourceProfile".to_owned(), Value::Object(source_profile));
        if let Some(profile_snapshot_sha256) = &self.profile_snapshot_sha256 {
            candidate.insert(
                "profileSnapshotSha256".to_owned(),
                Value::String(profile_snapshot_sha256.clone()),
            );
        }
        Ok(Value::Object(candidate))
    }

    pub(crate) fn executable_profile_sha256s(&self) -> (&str, &str) {
        (&self.long_profile_sha256, &self.short_profile_sha256)
    }
}

/// One-time strict validation of the complete signed parent archive.
///
/// The runtime uses this value as the single parent source: production parent
/// selection and pair-mode identity-ledger bootstrap both consume these exact
/// parsed cells and members, never a second raw archive read.
#[derive(Clone, Debug)]
pub struct VerifiedParentArchive {
    archive_sha256: String,
    source_cells: Vec<SourceCell>,
    rotating_evidence_archive_sha256: Option<String>,
}

/// Exact production selector corresponding to Python `_reproduction_cells`,
/// `_negative_novelty_cells`, and `_select_parent`.
#[derive(Clone, Debug)]
pub struct ArchiveParentSelector {
    generation_seed: String,
    archive_sha256: String,
    reproduction_cells: Vec<ArchiveCell>,
    negative_novelty_cells: Vec<ArchiveCell>,
    selection_state: BTreeMap<String, SelectionState>,
    eligible_parent_count: usize,
}

impl VerifiedParentArchive {
    /// Strictly validate and retain a complete production archive once.
    pub fn from_archive(archive: &Value) -> Result<Self> {
        let archive_sha256 = canonical_identity(archive, "archiveSha256", "QD parent archive")?;
        let archive_map = map(archive, "QD parent archive")?;
        if archive_map.get("schemaVersion").and_then(Value::as_str) != Some(QD_ARCHIVE_SCHEMA)
            || archive_map.get("qdVersion").and_then(Value::as_str) != Some(QD_VERSION)
            || archive_map.get("policyName").and_then(Value::as_str) != Some(QD_POLICY_NAME)
            || archive_map.get("policySha256").and_then(Value::as_str) != Some(QD_POLICY_SHA256)
        {
            return Err(invalid("unknown QD archive schema"));
        }
        let frozen_policy = field(archive_map, "frozenPolicy", "QD parent archive")?;
        let frozen_policy_sha256 = canonical_sha256(frozen_policy)
            .map_err(|error| invalid(format!("frozen QD policy is not canonical: {error}")))?;
        if frozen_policy_sha256 != QD_POLICY_SHA256 {
            return Err(invalid("unknown frozen QD archive policy"));
        }

        let pair_policy = map(
            field(archive_map, "bidirectionalPairPolicy", "QD parent archive")?,
            "QD bidirectional pair policy",
        )?;
        exact_keys(
            pair_policy,
            &["schemaVersion", "enabled", "compilerAuthority"],
            "QD bidirectional pair policy",
        )?;
        if pair_policy.get("schemaVersion").and_then(Value::as_str) != Some(PAIR_POLICY_SCHEMA)
            || pair_policy.get("enabled").and_then(Value::as_bool) != Some(true)
        {
            return Err(invalid(
                "QD bidirectional pair policy is not an enabled known version",
            ));
        }
        let compiler_authority = IdentitySnapshot::from_payload(
            field(
                pair_policy,
                "compilerAuthority",
                "QD bidirectional pair policy",
            )?,
            Some("pairCompiler"),
        )
        .map_err(|error| invalid(format!("QD pair compiler authority is invalid: {error}")))?;
        let pair_policy_sha256 = canonical_sha256(&Value::Object(pair_policy.clone()))
            .map_err(|error| invalid(format!("QD pair policy is not canonical: {error}")))?;

        let rotating_sha = match archive_map.get("rotatingEvidenceTransaction") {
            None | Some(Value::Null) => None,
            Some(value) => {
                let transaction = map(value, "rotating evidence transaction")?;
                if transaction.get("schemaVersion").and_then(Value::as_str)
                    != Some(ROTATING_PROJECTION_SCHEMA)
                {
                    return Err(invalid("unknown rotating evidence transaction schema"));
                }
                let value = string(
                    transaction,
                    "cumulativeArchiveSha256",
                    "rotating evidence transaction",
                )?;
                exact_sha256(&value, "rotating cumulative archive identity")?;
                Some(value)
            }
        };

        let cells = field(archive_map, "cells", "QD parent archive")?
            .as_array()
            .ok_or_else(|| invalid("QD parent archive cells must be an array"))?;
        let mut seen_cells = BTreeSet::new();
        let mut seen_candidates = BTreeSet::new();
        let mut source_cells = Vec::with_capacity(cells.len());
        for (cell_index, cell) in cells.iter().enumerate() {
            source_cells.push(parse_cell(
                cell,
                cell_index,
                &compiler_authority,
                &pair_policy_sha256,
                &mut seen_cells,
                &mut seen_candidates,
            )?);
        }

        for source in &source_cells {
            if source
                .members
                .iter()
                .filter(|member| negative_novelty_eligible(member))
                .count()
                > 1
            {
                return Err(invalid(
                    "QD negative-novelty lane exceeds one member per cell",
                ));
            }
        }
        Ok(Self {
            archive_sha256,
            source_cells,
            rotating_evidence_archive_sha256: rotating_sha,
        })
    }

    pub fn archive_sha256(&self) -> &str {
        &self.archive_sha256
    }

    pub fn members(&self) -> impl Iterator<Item = &VerifiedArchiveMember> {
        self.source_cells
            .iter()
            .flat_map(|cell| cell.members.iter().map(Arc::as_ref))
    }
}

impl ArchiveParentSelector {
    /// Compatibility constructor. New runtime code should validate once into
    /// [`VerifiedParentArchive`] and call [`Self::from_verified`].
    pub fn from_archive(
        archive: &Value,
        generation_seed: &str,
        allow_empty_quality_bootstrap: bool,
    ) -> Result<Self> {
        let verified = VerifiedParentArchive::from_archive(archive)?;
        Self::from_verified(&verified, generation_seed, allow_empty_quality_bootstrap)
    }

    /// Construct selection state from a once-validated archive projection.
    /// `generation_seed` is the pair-generation `configSha256`, not the
    /// separate pair-run authority identity.
    pub fn from_verified(
        verified: &VerifiedParentArchive,
        generation_seed: &str,
        allow_empty_quality_bootstrap: bool,
    ) -> Result<Self> {
        exact_sha256(generation_seed, "archive parent generation seed")?;
        let mut reproduction_cells = Vec::new();
        let mut negative_novelty_cells = Vec::new();
        let mut selection_state = BTreeMap::new();
        for source in &verified.source_cells {
            let base = SelectionState {
                base_selection_visits: source.selection_visits,
                base_offspring_attempts: source.offspring_attempts,
                selection_visits: source.selection_visits,
                offspring_attempts: source.offspring_attempts,
            };
            let reproduction_members = source
                .members
                .iter()
                .filter(|member| {
                    reproduction_eligible(
                        member,
                        verified.rotating_evidence_archive_sha256.as_deref(),
                    )
                })
                .cloned()
                .collect::<Vec<_>>();
            let negative_members = source
                .members
                .iter()
                .filter(|member| negative_novelty_eligible(member))
                .cloned()
                .collect::<Vec<_>>();
            if !reproduction_members.is_empty() || !negative_members.is_empty() {
                selection_state.insert(source.cell_id.clone(), base);
            }
            if !reproduction_members.is_empty() {
                reproduction_cells.push(ArchiveCell {
                    cell_id: source.cell_id.clone(),
                    coordinates: source.coordinates.clone(),
                    members: reproduction_members,
                });
            }
            if !negative_members.is_empty() {
                negative_novelty_cells.push(ArchiveCell {
                    cell_id: source.cell_id.clone(),
                    coordinates: source.coordinates.clone(),
                    members: negative_members,
                });
            }
        }
        reproduction_cells.sort_by(|left, right| left.cell_id.cmp(&right.cell_id));
        negative_novelty_cells.sort_by(|left, right| left.cell_id.cmp(&right.cell_id));
        if reproduction_cells.is_empty() && !allow_empty_quality_bootstrap {
            return Err(invalid(
                "QD archive has no quality-eligible reproduction members",
            ));
        }
        let eligible_parent_count = reproduction_cells
            .iter()
            .map(|cell| cell.members.len())
            .sum();
        Ok(Self {
            generation_seed: generation_seed.to_owned(),
            archive_sha256: verified.archive_sha256.clone(),
            reproduction_cells,
            negative_novelty_cells,
            selection_state,
            eligible_parent_count,
        })
    }

    pub fn archive_sha256(&self) -> &str {
        &self.archive_sha256
    }

    pub fn cell_counters(&self, cell_id: &str) -> Option<(u64, u64)> {
        self.selection_state
            .get(cell_id)
            .map(|state| (state.selection_visits, state.offspring_attempts))
    }

    fn select_exact(
        &mut self,
        label: &str,
        structural_selection_ordinal: u64,
    ) -> Result<ParentReference> {
        if self.reproduction_cells.is_empty() {
            return Err(invalid("QD archive has no selectable reproduction parent"));
        }
        let mut rng = PythonRandom::archive_parent_rng(
            &self.generation_seed,
            structural_selection_ordinal,
            label,
        );
        let negative_slot = structural_selection_ordinal % 10 == 9;
        let (cell, member, mode, lane, reason) =
            if negative_slot && !self.negative_novelty_cells.is_empty() {
                let index = rand_index(&mut rng, self.negative_novelty_cells.len())?;
                let cell = &self.negative_novelty_cells[index];
                let member = rank_aware_member(&cell.members, &mut rng)?;
                (
                    cell,
                    member,
                    "negative_novelty_exploration",
                    "negative_novelty".to_owned(),
                    "scheduled_every_tenth_structural_parent_selection",
                )
            } else {
                let draw = rng.random();
                let (mode, pool) = if draw < 0.50 {
                    (
                        "uniform_occupied_cell",
                        (0..self.reproduction_cells.len()).collect::<Vec<_>>(),
                    )
                } else if draw < 0.80 {
                    let minimum = self
                        .reproduction_cells
                        .iter()
                        .map(|cell| self.selection_state[&cell.cell_id].selection_visits)
                        .min()
                        .expect("nonempty reproduction cells");
                    (
                        "low_visit_cell",
                        self.reproduction_cells
                            .iter()
                            .enumerate()
                            .filter_map(|(index, cell)| {
                                (self.selection_state[&cell.cell_id].selection_visits == minimum)
                                    .then_some(index)
                            })
                            .collect::<Vec<_>>(),
                    )
                } else {
                    let boundary = boundary_cell_ids(&self.reproduction_cells);
                    (
                        "sparse_descriptor_boundary",
                        self.reproduction_cells
                            .iter()
                            .enumerate()
                            .filter_map(|(index, cell)| {
                                boundary.contains(&cell.cell_id).then_some(index)
                            })
                            .collect::<Vec<_>>(),
                    )
                };
                let pool_index = rand_index(&mut rng, pool.len())?;
                let cell = &self.reproduction_cells[pool[pool_index]];
                let member = rank_aware_member(&cell.members, &mut rng)?;
                let member_lane = member.archive_lane.clone();
                let reason = if member_lane == "rotating_frontier" {
                    "bounded_cumulative_frontier_fallback"
                } else if negative_slot {
                    "negative_novelty_slot_unavailable_quality_fallback"
                } else {
                    "quality_eligible_parent"
                };
                (cell, member, mode, member_lane, reason)
            };
        let state = self
            .selection_state
            .get_mut(&cell.cell_id)
            .expect("eligible cells have selection state");
        state.selection_visits = state
            .selection_visits
            .checked_add(1)
            .ok_or_else(|| invalid("selectionVisitCount overflow"))?;
        state.offspring_attempts = state
            .offspring_attempts
            .checked_add(1)
            .ok_or_else(|| invalid("offspringAttemptCount overflow"))?;
        let audit = object([
            (
                "schemaVersion",
                Value::String(PAIR_PARENT_SELECTION_SCHEMA.to_owned()),
            ),
            ("parentCellId", Value::String(cell.cell_id.clone())),
            (
                "parentCandidateId",
                Value::String(member.candidate_id.clone()),
            ),
            ("selectionMode", Value::String(mode.to_owned())),
            ("parentLane", Value::String(lane)),
            ("parentLaneReason", Value::String(reason.to_owned())),
            ("paretoFront", member.pareto_front_audit.clone()),
            ("crowdingDistance", member.crowding_distance_audit.clone()),
        ]);
        Ok(ParentReference {
            pair_identity_sha256: member.pair_identity_sha256.clone(),
            candidate_id: member.candidate_id.clone(),
            pair_payload: member.pair_payload()?.clone(),
            selection_audit: Some(audit),
        })
    }
}

impl ParentSelector for ArchiveParentSelector {
    fn has_parents(&self) -> bool {
        !self.reproduction_cells.is_empty()
    }

    fn eligible_parent_count(&self) -> usize {
        self.eligible_parent_count
    }

    fn archive_cell_count(&self) -> usize {
        self.reproduction_cells.len()
    }

    fn compact_state(&self) -> Value {
        object([
            (
                "schemaVersion",
                Value::String(ARCHIVE_PARENT_STATE_SCHEMA.to_owned()),
            ),
            ("archiveSha256", Value::String(self.archive_sha256.clone())),
            (
                "generationSeed",
                Value::String(self.generation_seed.clone()),
            ),
            (
                "cells",
                Value::Array(
                    self.selection_state
                        .iter()
                        .map(|(cell_id, state)| {
                            object([
                                ("cellId", Value::String(cell_id.clone())),
                                ("selectionVisitCount", Value::from(state.selection_visits)),
                                (
                                    "offspringAttemptCount",
                                    Value::from(state.offspring_attempts),
                                ),
                            ])
                        })
                        .collect(),
                ),
            ),
        ])
    }

    fn restore_compact_state(&mut self, state: &Value) -> std::result::Result<(), ProposalError> {
        self.restore_exact(state)
            .map_err(|error| ProposalError::Contract(error.to_string()))
    }

    fn select(
        &mut self,
        label: &str,
        structural_selection_ordinal: u64,
    ) -> std::result::Result<ParentReference, ProposalError> {
        self.select_exact(label, structural_selection_ordinal)
            .map_err(|error| ProposalError::Contract(error.to_string()))
    }
}

impl ArchiveParentSelector {
    fn restore_exact(&mut self, state: &Value) -> Result<()> {
        let state_map = map(state, "archive parent compact state")?;
        exact_keys(
            state_map,
            &["schemaVersion", "archiveSha256", "generationSeed", "cells"],
            "archive parent compact state",
        )?;
        if state_map.get("schemaVersion").and_then(Value::as_str)
            != Some(ARCHIVE_PARENT_STATE_SCHEMA)
            || state_map.get("archiveSha256").and_then(Value::as_str)
                != Some(self.archive_sha256.as_str())
            || state_map.get("generationSeed").and_then(Value::as_str)
                != Some(self.generation_seed.as_str())
        {
            return Err(invalid("archive parent compact state authority mismatch"));
        }
        let rows = field(state_map, "cells", "archive parent compact state")?
            .as_array()
            .ok_or_else(|| invalid("archive parent compact state cells must be an array"))?;
        if rows.len() != self.selection_state.len() {
            return Err(invalid("archive parent compact state cell set mismatch"));
        }
        let mut restored = BTreeMap::new();
        let mut previous: Option<String> = None;
        for row in rows {
            let row = map(row, "archive parent compact cell")?;
            exact_keys(
                row,
                &["cellId", "selectionVisitCount", "offspringAttemptCount"],
                "archive parent compact cell",
            )?;
            let cell_id = string(row, "cellId", "archive parent compact cell")?;
            if previous
                .as_deref()
                .is_some_and(|value| value >= cell_id.as_str())
            {
                return Err(invalid(
                    "archive parent compact state cells must be unique and sorted",
                ));
            }
            previous = Some(cell_id.clone());
            let selection_visits = nonnegative_count(
                row.get("selectionVisitCount"),
                "compact selectionVisitCount",
            )?;
            let offspring_attempts = nonnegative_count(
                row.get("offspringAttemptCount"),
                "compact offspringAttemptCount",
            )?;
            let base = self
                .selection_state
                .get(&cell_id)
                .ok_or_else(|| invalid("archive parent compact state cell set mismatch"))?;
            if selection_visits < base.base_selection_visits
                || offspring_attempts < base.base_offspring_attempts
                || selection_visits - base.base_selection_visits
                    != offspring_attempts - base.base_offspring_attempts
            {
                return Err(invalid(
                    "archive parent compact counters do not extend the frozen archive equally",
                ));
            }
            restored.insert(cell_id, (selection_visits, offspring_attempts));
        }
        for (cell_id, (selection_visits, offspring_attempts)) in restored {
            let target = self
                .selection_state
                .get_mut(&cell_id)
                .expect("validated compact cell");
            target.selection_visits = selection_visits;
            target.offspring_attempts = offspring_attempts;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct SourceCell {
    cell_id: String,
    coordinates: [String; 7],
    members: Vec<Arc<VerifiedArchiveMember>>,
    selection_visits: u64,
    offspring_attempts: u64,
}

fn parse_cell(
    value: &Value,
    index: usize,
    compiler_authority: &IdentitySnapshot,
    pair_policy_sha256: &str,
    seen_cells: &mut BTreeSet<String>,
    seen_candidates: &mut BTreeSet<String>,
) -> Result<SourceCell> {
    let label = format!("QD archive cell {index}");
    let cell = map(value, &label)?;
    let cell_id = string(cell, "cellId", &label)?;
    if !seen_cells.insert(cell_id.clone()) {
        return Err(invalid("QD archive cell IDs must be unique"));
    }
    let descriptor = map(field(cell, "descriptor", &label)?, "QD cell descriptor")?;
    let coordinates: [String; 7] = DESCRIPTOR_KEYS
        .map(|key| string(descriptor, key, "QD cell descriptor"))
        .into_iter()
        .collect::<Result<Vec<_>>>()?
        .try_into()
        .expect("descriptor has seven coordinates");
    if descriptor.get("cellId").and_then(Value::as_str) != Some(cell_id.as_str())
        || coordinates.join("|") != cell_id
    {
        return Err(invalid("QD cell descriptor identity mismatch"));
    }
    let members = field(cell, "members", &label)?
        .as_array()
        .ok_or_else(|| invalid(format!("{label} members must be an array")))?
        .iter()
        .enumerate()
        .map(|(member_index, member)| {
            parse_member(
                member,
                &format!("{label} member {member_index}"),
                compiler_authority,
                pair_policy_sha256,
                seen_candidates,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(SourceCell {
        cell_id,
        coordinates,
        members,
        selection_visits: nonnegative_count(
            cell.get("selectionVisitCount"),
            "cell selectionVisitCount",
        )?,
        offspring_attempts: nonnegative_count(
            cell.get("offspringAttemptCount"),
            "cell offspringAttemptCount",
        )?,
    })
}

fn parse_member(
    value: &Value,
    label: &str,
    compiler_authority: &IdentitySnapshot,
    pair_policy_sha256: &str,
    seen_candidates: &mut BTreeSet<String>,
) -> Result<Arc<VerifiedArchiveMember>> {
    let member = map(value, label)?;
    let candidate_id = string(member, "candidateId", label)?;
    if !seen_candidates.insert(candidate_id.clone()) {
        return Err(invalid("QD archive candidate IDs must be unique"));
    }
    let archive_lane = string(member, "archiveLane", label)?;
    if !matches!(
        archive_lane.as_str(),
        "quality" | "observational" | "negative_novelty" | "rotating_frontier"
    ) {
        return Err(invalid(format!("{label} has an unknown archiveLane")));
    }
    let validity = map(
        field(member, "finiteDataValidity", label)?,
        "finite data validity",
    )?;
    let finite_data = validity.get("isFiniteData").and_then(Value::as_bool) == Some(true);
    let passes_support_gate =
        validity.get("passesSupportGate").and_then(Value::as_bool) == Some(true);
    let valid_for_quality = validity.get("validForQuality").and_then(Value::as_bool) == Some(true);
    for key in ["isFiniteData", "passesSupportGate", "validForQuality"] {
        if validity.get(key).and_then(Value::as_bool).is_none() {
            return Err(invalid(format!("{label} validity {key} must be boolean")));
        }
    }
    let objectives = map(field(member, "objectives", label)?, "QD member objectives")?;
    let robust_return = finite(
        field(
            objectives,
            "worstWindowConservativeNetR",
            "QD member objectives",
        )?,
        "worstWindowConservativeNetR",
    )?;
    let complexity = finite(
        field(objectives, "structuralComplexity", "QD member objectives")?,
        "structuralComplexity",
    )?;
    finite(
        field(objectives, "maximumDrawdownR", "QD member objectives")?,
        "maximumDrawdownR",
    )?;
    let pareto_front_audit = member.get("paretoFront").cloned().unwrap_or(Value::Null);
    let pareto_front = match member.get("paretoFront") {
        None | Some(Value::Null) => 0,
        Some(value) => value
            .as_i64()
            .ok_or_else(|| invalid(format!("{label} paretoFront must be an integer or null")))?,
    };
    let crowding_distance_audit = member
        .get("crowdingDistance")
        .cloned()
        .unwrap_or(Value::Null);
    let crowding = match member.get("crowdingDistance") {
        None | Some(Value::Null) => OrderFloat::NegativeInfinity,
        Some(value) => OrderFloat::Finite(-finite(value, "crowdingDistance")?),
    };
    let capped_support = match member.get("cappedTradeSupport") {
        Some(value) if !value.is_null() => finite(value, "cappedTradeSupport")?,
        _ => {
            let cap = nonnegative_count(validity.get("capTrades"), "finiteDataValidity capTrades")?;
            let cap = if cap == 0 { DEFAULT_CAPPED_TRADES } else { cap };
            let trades = nonnegative_count(
                validity.get("totalTrades"),
                "finiteDataValidity totalTrades",
            )?;
            trades.min(cap) as f64
        }
    };
    let order = if let Some(robust) = member.get("robustObjectives") {
        let robust = map(robust, "robust parent objectives")?;
        let worst = finite(
            field(
                robust,
                "worstWindowConservativeNetR",
                "robust parent objectives",
            )?,
            "robust worstWindowConservativeNetR",
        )?;
        let drawdown = finite(
            field(robust, "drawdown", "robust parent objectives")?,
            "robust drawdown",
        )?;
        let cost_drag = finite(
            field(robust, "costDrag", "robust parent objectives")?,
            "robust costDrag",
        )?;
        let novelty = finite(
            field(robust, "novelty", "robust parent objectives")?,
            "robust novelty",
        )?;
        MemberOrder {
            first: i64::from(archive_lane != "quality"),
            rest: [
                OrderFloat::Finite(-worst),
                OrderFloat::Finite(drawdown),
                OrderFloat::Finite(cost_drag),
                OrderFloat::Finite(-novelty),
            ],
            candidate_id: candidate_id.clone(),
        }
    } else {
        MemberOrder {
            first: pareto_front,
            rest: [
                crowding,
                OrderFloat::Finite(-robust_return),
                OrderFloat::Finite(-capped_support),
                OrderFloat::Finite(complexity),
            ],
            candidate_id: candidate_id.clone(),
        }
    };

    let candidate = map(field(member, "candidate", label)?, "QD archive candidate")?;
    if candidate.get("candidateId").and_then(Value::as_str) != Some(candidate_id.as_str()) {
        return Err(invalid("QD archive member/candidate ID mismatch"));
    }
    let pair_payload = field(candidate, "bidirectionalGenome", "QD archive candidate")?;
    let pair = FrozenPair::from_payload(pair_payload).map_err(|error| {
        invalid(format!(
            "QD bidirectional pair material is invalid: {error}"
        ))
    })?;
    if pair.pair_compiler.canonical_payload() != compiler_authority.canonical_payload() {
        return Err(invalid("QD bidirectional pair compiler authority mismatch"));
    }
    if candidate.get("sourceProfile") != Some(&pair.profile)
        || candidate.get("sourceProfileSha256").and_then(Value::as_str)
            != Some(pair.raw_pair_sha256.as_str())
        || candidate.get("programSha256").and_then(Value::as_str)
            != Some(pair.native_program_sha256.as_str())
    {
        return Err(invalid(
            "QD economic candidate does not bind the exact frozen v3/both pair",
        ));
    }
    let pair_identity_sha256 = pair
        .identity_sha256()
        .map_err(|error| invalid(format!("QD pair identity is invalid: {error}")))?;
    let identity_material = map(
        field(
            candidate,
            "candidateIdentityMaterial",
            "QD archive candidate",
        )?,
        "QD candidate identity material",
    )?;
    if identity_material
        .get("bidirectionalGenomeIdentitySha256")
        .and_then(Value::as_str)
        != Some(pair_identity_sha256.as_str())
        || identity_material
            .get("pairPolicySha256")
            .and_then(Value::as_str)
            != Some(pair_policy_sha256)
    {
        return Err(invalid(
            "QD candidate identity does not bind frozen pair material",
        ));
    }
    if member
        .get("robustBreederEligible")
        .is_some_and(|value| value.as_bool().is_none())
    {
        return Err(invalid(format!(
            "{label} robustBreederEligible must be boolean when present"
        )));
    }
    let cumulative_evidence_archive_sha256 = match member.get("cumulativeEvidenceArchiveSha256") {
        None | Some(Value::Null) => None,
        Some(value) => Some(
            value
                .as_str()
                .filter(|identity| !identity.is_empty())
                .map(ToOwned::to_owned)
                .ok_or_else(|| {
                    invalid(format!(
                        "{label} cumulativeEvidenceArchiveSha256 must be a SHA-256 string"
                    ))
                })?,
        ),
    };
    if let Some(identity) = cumulative_evidence_archive_sha256.as_deref() {
        exact_sha256(identity, "member cumulative evidence archive identity")?;
    }
    let candidate_identity_sha256 = match candidate.get("candidateIdentitySha256") {
        None => None,
        Some(value) => {
            let value = value
                .as_str()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| invalid("QD archive candidate candidateIdentitySha256 is invalid"))?
                .to_owned();
            exact_sha256(&value, "QD archive candidate candidateIdentitySha256")?;
            Some(value)
        }
    };
    let program_sha256 = string(candidate, "programSha256", "QD archive candidate")?;
    exact_sha256(&program_sha256, "QD archive candidate programSha256")?;
    let source_profile_sha256 = string(candidate, "sourceProfileSha256", "QD archive candidate")?;
    exact_sha256(
        &source_profile_sha256,
        "QD archive candidate sourceProfileSha256",
    )?;
    let profile_snapshot_sha256 = match candidate.get("profileSnapshotSha256") {
        None => None,
        Some(value) => {
            let value = value
                .as_str()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| invalid("QD archive candidate profileSnapshotSha256 is invalid"))?
                .to_owned();
            exact_sha256(&value, "QD archive candidate profileSnapshotSha256")?;
            Some(value)
        }
    };
    let execution_config = candidate
        .get("sourceProfile")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("QD archive candidate sourceProfile must be an object"))?
        .get("executionConfig")
        .cloned()
        .unwrap_or_else(|| Value::Object(Map::new()));
    let mut result = VerifiedArchiveMember {
        candidate_id,
        archive_lane,
        pair_identity_sha256,
        pair_payload: None,
        pareto_front_audit,
        crowding_distance_audit,
        order,
        robust_return,
        finite_data,
        passes_support_gate,
        valid_for_quality,
        robust_breeder_eligible: member.get("robustBreederEligible").and_then(Value::as_bool)
            == Some(true),
        cumulative_evidence_archive_sha256,
        candidate_identity_sha256,
        program_sha256,
        source_profile_sha256,
        profile_snapshot_sha256,
        execution_config,
        long_profile_sha256: pair.long.profile_sha256,
        short_profile_sha256: pair.short.profile_sha256,
    };
    // Selection is the only long-lived consumer of a full frozen pair.  The
    // archive validator still reopened and authenticated every pair above,
    // including observational members, before discarding their raw graphs.
    if result.archive_lane == "quality"
        || result.archive_lane == "rotating_frontier"
        || negative_novelty_eligible(&result)
    {
        result.pair_payload = Some(Arc::new(pair_payload.clone()));
    }
    Ok(Arc::new(result))
}

fn quality_eligible(member: &VerifiedArchiveMember) -> bool {
    member.finite_data
        && member.passes_support_gate
        && member.valid_for_quality
        && member.robust_return >= 0.0
}

fn reproduction_eligible(member: &VerifiedArchiveMember, rotating_sha: Option<&str>) -> bool {
    let rotating_eligible = rotating_sha.is_some_and(|identity| {
        member.robust_breeder_eligible
            && member.cumulative_evidence_archive_sha256.as_deref() == Some(identity)
    });
    (member.archive_lane == "quality" && (quality_eligible(member) || rotating_eligible))
        || (member.archive_lane == "rotating_frontier" && rotating_eligible)
}

fn negative_novelty_eligible(member: &VerifiedArchiveMember) -> bool {
    member.archive_lane == "negative_novelty"
        && member.finite_data
        && member.passes_support_gate
        && member.valid_for_quality
        && member.robust_return < 0.0
}

fn rand_index(rng: &mut PythonRandom, len: usize) -> Result<usize> {
    let upper = u64::try_from(len).map_err(|_| invalid("selection pool is too large"))?;
    rng.randbelow(upper)
        .map_err(|error| invalid(format!("CPython parent draw failed: {error}")))
        .and_then(|index| {
            usize::try_from(index).map_err(|_| invalid("parent draw does not fit usize"))
        })
}

fn rank_aware_member<'a>(
    members: &'a [Arc<VerifiedArchiveMember>],
    rng: &mut PythonRandom,
) -> Result<&'a VerifiedArchiveMember> {
    let mut ordered = members.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.order.compare(&right.order));
    let count = u64::try_from(ordered.len()).map_err(|_| invalid("parent cell is too large"))?;
    let total = count
        .checked_mul(
            count
                .checked_add(1)
                .ok_or_else(|| invalid("rank weight overflow"))?,
        )
        .and_then(|value| value.checked_div(2))
        .ok_or_else(|| invalid("rank weight overflow"))?;
    let mut draw = rng
        .randbelow(total)
        .map_err(|error| invalid(format!("CPython rank draw failed: {error}")))?;
    for (index, member) in ordered.into_iter().enumerate() {
        let weight = count - u64::try_from(index).expect("index does not exceed count");
        if draw < weight {
            return Ok(member);
        }
        draw -= weight;
    }
    Err(invalid("rank-aware parent draw exhausted"))
}

fn boundary_cell_ids(cells: &[ArchiveCell]) -> BTreeSet<String> {
    let mut counts = Vec::with_capacity(cells.len());
    for left in cells {
        let count = cells
            .iter()
            .filter(|right| right.cell_id != left.cell_id)
            .filter(|right| {
                left.coordinates
                    .iter()
                    .zip(&right.coordinates)
                    .filter(|(left, right)| left != right)
                    .count()
                    == 1
            })
            .count();
        counts.push((left.cell_id.clone(), count));
    }
    let minimum = counts.iter().map(|(_, count)| *count).min().unwrap_or(0);
    counts
        .into_iter()
        .filter_map(|(cell_id, count)| (count == minimum).then_some(cell_id))
        .collect()
}
