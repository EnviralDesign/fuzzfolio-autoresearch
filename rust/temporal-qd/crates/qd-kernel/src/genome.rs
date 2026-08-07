//! Frozen v2-module / v3-pair identities for the Temporal QD boundary.
//!
//! This module deliberately owns persistence-shaped validation only.  It never
//! validates a Dashboard graph or compiles a bidirectional profile locally:
//! those decisions remain behind [`NativeModuleValidator`] and
//! [`CanonicalPairCompiler`].

use std::collections::{BTreeMap, BTreeSet};

use temporal_qd_contract::{
    ContractError, Value, canonical_json as contract_canonical_json,
    canonical_sha256 as contract_canonical_sha256,
};

pub const GENOME_SCHEMA: &str = "temporal_bidirectional_genome_v1";
pub const MODULE_SCHEMA: &str = "temporal_bidirectional_module_snapshot_v1";
pub const PAIR_SCHEMA: &str = "temporal_bidirectional_pair_snapshot_v1";
pub const HOLD_MUTATION_SCHEMA: &str = "temporal_management_plan_hold_mutation_v1";

const MUTABLE_ALIAS_KEYS: &[&str] = &[
    "alias",
    "catalogAlias",
    "catalogPath",
    "catalogRef",
    "currentCatalog",
    "mutableCatalog",
    "policyAlias",
    "policyPath",
    "authorityAlias",
    "authorityRef",
];

#[derive(Debug, thiserror::Error)]
pub enum GenomeError {
    #[error("{0}")]
    Contract(#[from] ContractError),
    #[error("{0}")]
    Invalid(String),
    #[error("entry decision route exceeds the distinct decision-indicator cap")]
    EntryRouteDecisionIndicatorCap,
}

impl GenomeError {
    fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }
}

/// The canonical JSON implementation shared with all native contracts.
pub fn canonical_json(value: &Value) -> Result<String, GenomeError> {
    Ok(contract_canonical_json(value)?)
}

/// The canonical hash implementation shared with all native contracts.
pub fn canonical_sha256(value: &Value) -> Result<String, GenomeError> {
    Ok(contract_canonical_sha256(value)?)
}

/// Dashboard-owned native admission for a hydrated v2 module.
pub trait NativeModuleValidator {
    fn validate_v2(&self, profile: &Value, candidate_id: &str) -> Result<Value, GenomeError>;
}

/// Dashboard-owned canonical v3 bidirectional compiler.
pub trait CanonicalPairCompiler {
    fn compile_pair(
        &self,
        long_profile: &Value,
        short_profile: &Value,
        candidate_id: &str,
    ) -> Result<Value, GenomeError>;
}

/// Grammar-owned same-side program crossover.  This layer checks only its
/// frozen construction inputs and the returned program envelope.
pub trait SameSideCrossover {
    fn crossover(
        &self,
        left_program: &Value,
        right_program: &Value,
        direction: &str,
        proposal_seed: &str,
    ) -> Result<Value, GenomeError>;
}

fn object(value: &Value, name: &str) -> Result<BTreeMap<String, Value>, GenomeError> {
    value
        .as_object()
        .map(|map| {
            map.iter()
                .map(|(key, item)| (key.clone(), item.clone()))
                .collect()
        })
        .ok_or_else(|| GenomeError::invalid(format!("{name} must be an object")))
}

fn array(value: &Value, name: &str) -> Result<Vec<Value>, GenomeError> {
    value
        .as_array()
        .cloned()
        .ok_or_else(|| GenomeError::invalid(format!("{name} must be an ordered list")))
}

fn required<'a>(
    map: &'a BTreeMap<String, Value>,
    key: &str,
    name: &str,
) -> Result<&'a Value, GenomeError> {
    map.get(key)
        .ok_or_else(|| GenomeError::invalid(format!("{name} is missing {key}")))
}

fn exact_keys(map: &BTreeMap<String, Value>, keys: &[&str], name: &str) -> Result<(), GenomeError> {
    let expected: BTreeSet<String> = keys.iter().map(|key| (*key).to_owned()).collect();
    let found: BTreeSet<String> = map.keys().cloned().collect();
    if found != expected {
        return Err(GenomeError::invalid(format!("{name} fields are not exact")));
    }
    Ok(())
}

fn value_object(items: impl IntoIterator<Item = (String, Value)>) -> Value {
    let mut value = Value::Object(Default::default());
    let map = value.as_object_mut().expect("object was just constructed");
    for (key, item) in items {
        map.insert(key, item);
    }
    value
}

fn obj(items: Vec<(&str, Value)>) -> Value {
    value_object(
        items
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value)),
    )
}

fn string(value: &Value, name: &str) -> Result<String, GenomeError> {
    value
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| GenomeError::invalid(format!("{name} must be a string")))
}

fn identifier(value: &Value, name: &str) -> Result<String, GenomeError> {
    let token = string(value, name)?.trim().to_owned();
    if token.is_empty() || token.len() > 240 {
        return Err(GenomeError::invalid(format!(
            "{name} must be a nonempty explicit identifier"
        )));
    }
    Ok(token)
}

fn sha(value: &Value, name: &str) -> Result<String, GenomeError> {
    let token = string(value, name)?.trim().to_owned();
    let valid = token.len() == 71
        && token.starts_with("sha256:")
        && token.as_bytes()[7..]
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase());
    if !valid {
        return Err(GenomeError::invalid(format!(
            "{name} must be an exact sha256 identity"
        )));
    }
    Ok(token)
}

fn side(value: &Value, name: &str) -> Result<String, GenomeError> {
    let token = string(value, name)?.trim().to_ascii_lowercase();
    if token != "long" && token != "short" {
        return Err(GenomeError::invalid(format!(
            "{name} must be long or short"
        )));
    }
    Ok(token)
}

fn assert_no_mutable_alias(value: &Value, name: &str) -> Result<(), GenomeError> {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if MUTABLE_ALIAS_KEYS.contains(&key.as_str()) {
                    return Err(GenomeError::invalid(format!(
                        "{name} must embed a frozen payload, not mutable alias {key:?}"
                    )));
                }
                assert_no_mutable_alias(child, name)?;
            }
        }
        Value::Array(items) => {
            for child in items {
                assert_no_mutable_alias(child, name)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn bool_true(value: Option<&Value>) -> bool {
    matches!(value, Some(Value::Bool(true)))
}

/// An authority/configuration payload embedded with its independently checked
/// canonical identity.  `Value` is owned, so snapshots cannot retain mutable
/// aliases from callers.
#[derive(Clone, Debug, PartialEq)]
pub struct IdentitySnapshot {
    pub kind: String,
    pub schema_version: String,
    pub payload: Value,
    pub sha256: String,
}

impl IdentitySnapshot {
    pub fn create(kind: &str, schema_version: &str, payload: &Value) -> Result<Self, GenomeError> {
        let kind = identifier(&Value::String(kind.to_owned()), "snapshot kind")?;
        let schema_version = identifier(
            &Value::String(schema_version.to_owned()),
            "snapshot schema version",
        )?;
        let payload = Value::Object(
            object(payload, &format!("{kind} snapshot payload"))?
                .into_iter()
                .collect(),
        );
        assert_no_mutable_alias(&payload, &format!("{kind} snapshot"))?;
        let sha256 = canonical_sha256(&payload)?;
        Ok(Self {
            kind,
            schema_version,
            payload,
            sha256,
        })
    }

    pub fn from_payload(payload: &Value, expected_kind: Option<&str>) -> Result<Self, GenomeError> {
        let value = object(payload, "identity snapshot")?;
        exact_keys(
            &value,
            &["kind", "schemaVersion", "payload", "sha256"],
            "identity snapshot",
        )?;
        let kind = identifier(
            required(&value, "kind", "identity snapshot")?,
            "snapshot kind",
        )?;
        let schema_version = identifier(
            required(&value, "schemaVersion", "identity snapshot")?,
            "snapshot schema version",
        )?;
        let body = required(&value, "payload", "identity snapshot")?;
        let result = Self::create(&kind, &schema_version, body)?;
        if let Some(expected_kind) = expected_kind {
            if result.kind != expected_kind {
                return Err(GenomeError::invalid(
                    "identity snapshot kind is incompatible",
                ));
            }
        }
        if result.sha256
            != sha(
                required(&value, "sha256", "identity snapshot")?,
                "snapshot SHA-256",
            )?
        {
            return Err(GenomeError::invalid(
                "identity snapshot payload hash mismatched",
            ));
        }
        Ok(result)
    }

    pub fn canonical_payload(&self) -> Value {
        obj(vec![
            ("kind", Value::String(self.kind.clone())),
            ("schemaVersion", Value::String(self.schema_version.clone())),
            ("payload", self.payload.clone()),
            ("sha256", Value::String(self.sha256.clone())),
        ])
    }
}

fn grammar_entry_cap(profile: &Value) -> Result<(), GenomeError> {
    // Kept here (rather than duplicated) so direct module factory output and
    // post-operator output cross the identical grammar policy boundary.
    crate::grammar::validate_entry_route_decision_indicator_cap(profile)
        .map(|_| ())
        .map_err(|error| match error {
            crate::grammar::GrammarError::EntryRouteDecisionIndicatorCap => {
                GenomeError::EntryRouteDecisionIndicatorCap
            }
            other => GenomeError::invalid(other.to_string()),
        })
}

#[derive(Clone, Debug, PartialEq)]
pub struct FrozenModule {
    pub direction: String,
    pub program: Value,
    pub profile: Value,
    pub grammar_context: IdentitySnapshot,
    pub catalog: IdentitySnapshot,
    pub policy: IdentitySnapshot,
    pub native_authority: IdentitySnapshot,
    pub native_report: Value,
    pub lineage: Vec<Value>,
    pub program_sha256: String,
    pub profile_sha256: String,
    pub native_snapshot_sha256: String,
    pub native_program_sha256: String,
    pub native_validation_report_sha256: String,
}

impl FrozenModule {
    #[allow(clippy::too_many_arguments)]
    pub fn freeze(
        program: &Value,
        profile: &Value,
        grammar_context: &IdentitySnapshot,
        catalog: &IdentitySnapshot,
        policy: &IdentitySnapshot,
        native_authority: &IdentitySnapshot,
        native_report: &Value,
        lineage: &[Value],
    ) -> Result<Self, GenomeError> {
        let grammar_context = IdentitySnapshot::from_payload(
            &grammar_context.canonical_payload(),
            Some("grammarContext"),
        )?;
        let catalog =
            IdentitySnapshot::from_payload(&catalog.canonical_payload(), Some("catalog"))?;
        let policy = IdentitySnapshot::from_payload(&policy.canonical_payload(), Some("policy"))?;
        let native_authority = IdentitySnapshot::from_payload(
            &native_authority.canonical_payload(),
            Some("nativeAuthority"),
        )?;
        let program_map = object(program, "v2 module program")?;
        let profile_map = object(profile, "hydrated v2 module profile")?;
        grammar_entry_cap(profile)?;

        exact_keys(
            &program_map,
            &["schemaVersion", "grammarVersion", "direction", "fragments"],
            "module program",
        )?;
        if string(
            required(&program_map, "schemaVersion", "module program")?,
            "module program schema",
        )? != "temporal_typed_fragment_grammar_v2"
            || string(
                required(&program_map, "grammarVersion", "module program")?,
                "module program grammar version",
            )? != "3"
            || required(&program_map, "fragments", "module program")?
                .as_array()
                .is_none()
        {
            return Err(GenomeError::invalid(
                "module program is not a canonical typed v2 program",
            ));
        }
        let direction = side(
            required(&program_map, "direction", "module program")?,
            "module program direction",
        )?;
        if string(
            required(&profile_map, "version", "v2 profile")?,
            "v2 profile version",
        )? != "v2"
            || side(
                required(&profile_map, "directionMode", "v2 profile")?,
                "v2 profile direction",
            )? != direction
        {
            return Err(GenomeError::invalid(
                "hydrated profile is not the matching v2 module",
            ));
        }
        let report = Value::Object(
            object(native_report, "native module validation report")?
                .into_iter()
                .collect(),
        );
        let report_map = object(&report, "native module validation report")?;
        let profile_sha256 = canonical_sha256(profile)?;
        if string(
            required(&report_map, "schemaVersion", "native module report")?,
            "native module report schema",
        )? != "temporal_search_candidate_validation_v1"
            || required(
                &report_map,
                "rawSourceProfileSha256",
                "native module report",
            )? != &Value::String(profile_sha256.clone())
            || string(
                required(&report_map, "status", "native module report")?,
                "native module report status",
            )? != "valid_evaluable"
            || !bool_true(report_map.get("candidateAcceptable"))
        {
            return Err(GenomeError::invalid(
                "native module report did not admit the exact v2 profile",
            ));
        }
        let native_snapshot_sha256 = sha(
            required(&report_map, "profileSnapshotSha256", "native module report")?,
            "native module snapshot SHA-256",
        )?;
        let native_program_sha256 = sha(
            required(&report_map, "programSha256", "native module report")?,
            "native module program SHA-256",
        )?;
        let native_validation_report_sha256 = sha(
            required(
                &report_map,
                "validationReportSha256",
                "native module report",
            )?,
            "native module validation report SHA-256",
        )?;
        let lineage = lineage
            .iter()
            .map(|item| {
                object(item, "module lineage item")
                    .map(|map| Value::Object(map.into_iter().collect()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let canonical_program = Value::Object(program_map.into_iter().collect());
        let canonical_profile = Value::Object(profile_map.into_iter().collect());
        let program_sha256 = canonical_sha256(&canonical_program)?;
        Ok(Self {
            direction,
            program: canonical_program,
            profile: canonical_profile,
            grammar_context,
            catalog,
            policy,
            native_authority,
            native_report: report,
            lineage,
            program_sha256,
            profile_sha256,
            native_snapshot_sha256,
            native_program_sha256,
            native_validation_report_sha256,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn validate_native(
        program: &Value,
        profile: &Value,
        grammar_context: &IdentitySnapshot,
        catalog: &IdentitySnapshot,
        policy: &IdentitySnapshot,
        native_authority_identity: &IdentitySnapshot,
        native_validator: &dyn NativeModuleValidator,
        candidate_id: &str,
        lineage: &[Value],
    ) -> Result<Self, GenomeError> {
        let candidate_id = identifier(
            &Value::String(candidate_id.to_owned()),
            "native module candidate id",
        )?;
        let profile = Value::Object(
            object(profile, "hydrated v2 module profile")?
                .into_iter()
                .collect(),
        );
        let report = native_validator.validate_v2(&profile, &candidate_id)?;
        Self::freeze(
            program,
            &profile,
            grammar_context,
            catalog,
            policy,
            native_authority_identity,
            &report,
            lineage,
        )
    }

    pub fn identity_material(&self) -> Value {
        obj(vec![
            ("direction", Value::String(self.direction.clone())),
            ("programSha256", Value::String(self.program_sha256.clone())),
            ("profileSha256", Value::String(self.profile_sha256.clone())),
            ("grammarContext", self.grammar_context.canonical_payload()),
            ("catalog", self.catalog.canonical_payload()),
            ("policy", self.policy.canonical_payload()),
            ("nativeAuthority", self.native_authority.canonical_payload()),
            (
                "nativeSnapshotSha256",
                Value::String(self.native_snapshot_sha256.clone()),
            ),
            (
                "nativeProgramSha256",
                Value::String(self.native_program_sha256.clone()),
            ),
            (
                "nativeValidationReportSha256",
                Value::String(self.native_validation_report_sha256.clone()),
            ),
            ("lineage", Value::Array(self.lineage.clone())),
        ])
    }

    pub fn identity_sha256(&self) -> Result<String, GenomeError> {
        canonical_sha256(&obj(vec![
            ("schemaVersion", Value::String(MODULE_SCHEMA.to_owned())),
            ("direction", Value::String(self.direction.clone())),
            ("programSha256", Value::String(self.program_sha256.clone())),
            ("profileSha256", Value::String(self.profile_sha256.clone())),
            ("grammarContext", self.grammar_context.canonical_payload()),
            ("catalog", self.catalog.canonical_payload()),
            ("policy", self.policy.canonical_payload()),
            ("nativeAuthority", self.native_authority.canonical_payload()),
            (
                "nativeSnapshotSha256",
                Value::String(self.native_snapshot_sha256.clone()),
            ),
            (
                "nativeProgramSha256",
                Value::String(self.native_program_sha256.clone()),
            ),
            (
                "nativeValidationReportSha256",
                Value::String(self.native_validation_report_sha256.clone()),
            ),
            ("lineage", Value::Array(self.lineage.clone())),
        ]))
    }

    pub fn canonical_payload(&self) -> Result<Value, GenomeError> {
        Ok(obj(vec![
            ("schemaVersion", Value::String(MODULE_SCHEMA.to_owned())),
            ("direction", Value::String(self.direction.clone())),
            ("program", self.program.clone()),
            ("profile", self.profile.clone()),
            ("grammarContext", self.grammar_context.canonical_payload()),
            ("catalog", self.catalog.canonical_payload()),
            ("policy", self.policy.canonical_payload()),
            ("nativeAuthority", self.native_authority.canonical_payload()),
            ("nativeReport", self.native_report.clone()),
            ("lineage", Value::Array(self.lineage.clone())),
            (
                "identities",
                obj(vec![
                    ("programSha256", Value::String(self.program_sha256.clone())),
                    ("profileSha256", Value::String(self.profile_sha256.clone())),
                    (
                        "nativeSnapshotSha256",
                        Value::String(self.native_snapshot_sha256.clone()),
                    ),
                    (
                        "nativeProgramSha256",
                        Value::String(self.native_program_sha256.clone()),
                    ),
                    (
                        "nativeValidationReportSha256",
                        Value::String(self.native_validation_report_sha256.clone()),
                    ),
                    (
                        "moduleIdentitySha256",
                        Value::String(self.identity_sha256()?),
                    ),
                ]),
            ),
        ]))
    }

    pub fn from_payload(payload: &Value) -> Result<Self, GenomeError> {
        let value = object(payload, "frozen module payload")?;
        exact_keys(
            &value,
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
            "frozen module payload",
        )?;
        if string(
            required(&value, "schemaVersion", "frozen module payload")?,
            "frozen module schema",
        )? != MODULE_SCHEMA
        {
            return Err(GenomeError::invalid(
                "frozen module payload fields are not exact",
            ));
        }
        let lineage = array(
            required(&value, "lineage", "frozen module payload")?,
            "module lineage",
        )?;
        let result = Self::freeze(
            required(&value, "program", "frozen module payload")?,
            required(&value, "profile", "frozen module payload")?,
            &IdentitySnapshot::from_payload(
                required(&value, "grammarContext", "frozen module payload")?,
                Some("grammarContext"),
            )?,
            &IdentitySnapshot::from_payload(
                required(&value, "catalog", "frozen module payload")?,
                Some("catalog"),
            )?,
            &IdentitySnapshot::from_payload(
                required(&value, "policy", "frozen module payload")?,
                Some("policy"),
            )?,
            &IdentitySnapshot::from_payload(
                required(&value, "nativeAuthority", "frozen module payload")?,
                Some("nativeAuthority"),
            )?,
            required(&value, "nativeReport", "frozen module payload")?,
            &lineage,
        )?;
        if result.direction
            != side(
                required(&value, "direction", "frozen module payload")?,
                "module direction",
            )?
        {
            return Err(GenomeError::invalid(
                "module direction mismatched its program",
            ));
        }
        if result
            .canonical_payload()?
            .as_object()
            .and_then(|item| item.get("identities"))
            != Some(required(&value, "identities", "frozen module payload")?)
        {
            return Err(GenomeError::invalid(
                "frozen module identity material mismatched payload",
            ));
        }
        Ok(result)
    }
}

pub fn proposal_side(proposal_seed: impl ToString) -> Result<String, GenomeError> {
    let proposal_seed = identifier(&Value::String(proposal_seed.to_string()), "proposal seed")?;
    let material = obj(vec![
        ("schemaVersion", Value::String(GENOME_SCHEMA.to_owned())),
        ("proposalSeed", Value::String(proposal_seed)),
    ]);
    let hash = canonical_sha256(&material)?;
    let last = hash.as_bytes().last().copied().expect("sha256 has content");
    Ok(if (last as char).to_digit(16).expect("hex hash") % 2 == 0 {
        "long"
    } else {
        "short"
    }
    .to_owned())
}

pub fn deterministic_same_side_crossover(
    left: &FrozenModule,
    right: &FrozenModule,
    proposal_seed: impl ToString,
    crossover: &dyn SameSideCrossover,
) -> Result<(Value, Value), GenomeError> {
    if left.direction != right.direction {
        return Err(GenomeError::invalid(
            "same-side crossover rejects opposite-side parents",
        ));
    }
    for (first, second) in [
        (&left.grammar_context, &right.grammar_context),
        (&left.catalog, &right.catalog),
        (&left.policy, &right.policy),
        (&left.native_authority, &right.native_authority),
    ] {
        if first.sha256 != second.sha256 {
            return Err(GenomeError::invalid(
                "same-side crossover requires identical frozen construction identities",
            ));
        }
    }
    let proposal_seed = identifier(&Value::String(proposal_seed.to_string()), "proposal seed")?;
    let mut parents = [left, right];
    parents.sort_by_key(|module| {
        canonical_sha256(&obj(vec![
            ("proposalSeed", Value::String(proposal_seed.clone())),
            (
                "moduleIdentitySha256",
                Value::String(module.identity_sha256().expect("valid frozen module")),
            ),
        ]))
        .expect("valid canonical JSON")
    });
    let child = crossover.crossover(
        &parents[0].program,
        &parents[1].program,
        &left.direction,
        &proposal_seed,
    )?;
    let child_map = object(&child, "same-side crossover child program")?;
    if side(
        required(&child_map, "direction", "same-side crossover child program")?,
        "crossover child direction",
    )? != left.direction
    {
        return Err(GenomeError::invalid(
            "same-side crossover emitted a cross-side child",
        ));
    }
    if string(
        required(
            &child_map,
            "schemaVersion",
            "same-side crossover child program",
        )?,
        "crossover schema",
    )? != "temporal_typed_fragment_grammar_v2"
        || string(
            required(
                &child_map,
                "grammarVersion",
                "same-side crossover child program",
            )?,
            "crossover grammar version",
        )? != "3"
    {
        return Err(GenomeError::invalid(
            "same-side crossover emitted a noncanonical v2 program",
        ));
    }
    let child = Value::Object(child_map.into_iter().collect());
    let record = obj(vec![
        ("operation", Value::String("same_side_crossover".to_owned())),
        ("side", Value::String(left.direction.clone())),
        ("proposalSeed", Value::String(proposal_seed)),
        (
            "orderedParentModuleIdentitySha256",
            Value::Array(
                parents
                    .iter()
                    .map(|module| {
                        Value::String(module.identity_sha256().expect("valid frozen module"))
                    })
                    .collect(),
            ),
        ),
        (
            "childProgramSha256",
            Value::String(canonical_sha256(&child)?),
        ),
    ]);
    Ok((child, record))
}

/// Canonical Dashboard-native `holdPolicy`; absent values become the explicit
/// mutation-vocabulary value `{ "kind": "none" }`.
pub fn canonical_hold(value: Option<&Value>) -> Result<Value, GenomeError> {
    let raw = value
        .cloned()
        .unwrap_or_else(|| obj(vec![("kind", Value::String("none".to_owned()))]));
    let map = object(&raw, "management-plan hold")?;
    let kind = string(
        required(&map, "kind", "management-plan hold")?,
        "management-plan hold kind",
    )?
    .trim()
    .to_owned();
    if kind == "none" && map.len() == 1 {
        return Ok(obj(vec![("kind", Value::String(kind))]));
    }
    if kind == "market_bars"
        && (map.len() == 3 || map.len() == 4)
        && map.contains_key("bars")
        && map.contains_key("timeframe")
    {
        if map.len() == 4
            && (!map.contains_key("onBreach")
                || required(&map, "onBreach", "market_bars hold")?
                    != &Value::String("exit_next_open".to_owned()))
        {
            return Err(GenomeError::invalid(
                "market_bars holdPolicy onBreach must be exit_next_open",
            ));
        }
        let bars = required(&map, "bars", "market_bars hold")?.as_i64();
        let timeframe = string(
            required(&map, "timeframe", "market_bars hold")?,
            "market_bars hold timeframe",
        )?
        .trim()
        .to_ascii_uppercase();
        if bars.is_none() || bars.unwrap() < 1 || timeframe.is_empty() || timeframe.len() > 32 {
            return Err(GenomeError::invalid(
                "market_bars holdPolicy requires positive bars and a timeframe",
            ));
        }
        return Ok(obj(vec![
            ("kind", Value::String(kind)),
            ("bars", Value::from(bars.unwrap())),
            ("timeframe", Value::String(timeframe)),
        ]));
    }
    if kind == "elapsed_calendar" && (map.len() == 2 || map.len() == 3) && map.contains_key("hours")
    {
        if map.len() == 3
            && (!map.contains_key("onBreach")
                || required(&map, "onBreach", "elapsed_calendar hold")?
                    != &Value::String("exit_next_open".to_owned()))
        {
            return Err(GenomeError::invalid(
                "elapsed_calendar holdPolicy onBreach must be exit_next_open",
            ));
        }
        let hours = required(&map, "hours", "elapsed_calendar hold")?.as_f64();
        if !hours.is_some_and(|item| item.is_finite() && item > 0.0) {
            return Err(GenomeError::invalid(
                "elapsed_calendar holdPolicy requires positive hours",
            ));
        }
        let number = Value::from(hours.expect("checked finite"));
        return Ok(obj(vec![("kind", Value::String(kind)), ("hours", number)]));
    }
    Err(GenomeError::invalid(
        "holdPolicy must be none, market_bars (bars/timeframe), or elapsed_calendar (hours)",
    ))
}

#[derive(Clone, Debug, PartialEq)]
pub struct HoldMutationPlan {
    pub side: String,
    pub plan_id: String,
    pub source_profile_sha256: String,
    pub old_hold: Value,
    pub new_hold: Value,
    pub old_hold_sha256: String,
    pub new_hold_sha256: String,
    pub plan_id_sha256: String,
}

impl HoldMutationPlan {
    pub fn create(
        module: &FrozenModule,
        plan_id: &str,
        new_hold: Option<&Value>,
    ) -> Result<Self, GenomeError> {
        let profile = object(&module.profile, "frozen module profile")?;
        let plans = management_plans(&profile)?;
        let selected = plans
            .iter()
            .filter(|item| {
                item.as_object().and_then(|plan| plan.get("id"))
                    == Some(&Value::String(plan_id.to_owned()))
            })
            .collect::<Vec<_>>();
        if selected.len() != 1 {
            return Err(GenomeError::invalid(
                "hold mutation requires one existing management plan id",
            ));
        }
        let selected_map = object(selected[0], "management plan")?;
        let old = canonical_hold(selected_map.get("holdPolicy"))?;
        let new = canonical_hold(new_hold)?;
        if old == new {
            return Err(GenomeError::invalid(
                "hold mutation must change the selected management plan",
            ));
        }
        let plan_id = identifier(&Value::String(plan_id.to_owned()), "management plan id")?;
        Ok(Self {
            side: module.direction.clone(),
            plan_id: plan_id.clone(),
            source_profile_sha256: module.profile_sha256.clone(),
            old_hold_sha256: canonical_sha256(&old)?,
            new_hold_sha256: canonical_sha256(&new)?,
            plan_id_sha256: canonical_sha256(&obj(vec![("planId", Value::String(plan_id))]))?,
            old_hold: old,
            new_hold: new,
        })
    }

    pub fn canonical_payload(&self) -> Value {
        obj(vec![
            (
                "schemaVersion",
                Value::String(HOLD_MUTATION_SCHEMA.to_owned()),
            ),
            ("side", Value::String(self.side.clone())),
            ("planId", Value::String(self.plan_id.clone())),
            (
                "sourceProfileSha256",
                Value::String(self.source_profile_sha256.clone()),
            ),
            ("oldHold", self.old_hold.clone()),
            ("newHold", self.new_hold.clone()),
            ("oldHoldSha256", Value::String(self.old_hold_sha256.clone())),
            ("newHoldSha256", Value::String(self.new_hold_sha256.clone())),
            ("planIdSha256", Value::String(self.plan_id_sha256.clone())),
        ])
    }

    pub fn from_payload(payload: &Value) -> Result<Self, GenomeError> {
        let value = object(payload, "hold mutation plan")?;
        exact_keys(
            &value,
            &[
                "schemaVersion",
                "side",
                "planId",
                "sourceProfileSha256",
                "oldHold",
                "newHold",
                "oldHoldSha256",
                "newHoldSha256",
                "planIdSha256",
            ],
            "hold mutation plan",
        )?;
        if string(
            required(&value, "schemaVersion", "hold mutation plan")?,
            "hold mutation schema",
        )? != HOLD_MUTATION_SCHEMA
        {
            return Err(GenomeError::invalid(
                "hold mutation plan fields are not exact",
            ));
        }
        let old_hold = canonical_hold(Some(required(&value, "oldHold", "hold mutation plan")?))?;
        let new_hold = canonical_hold(Some(required(&value, "newHold", "hold mutation plan")?))?;
        let plan_id = identifier(
            required(&value, "planId", "hold mutation plan")?,
            "management plan id",
        )?;
        let result = Self {
            side: side(
                required(&value, "side", "hold mutation plan")?,
                "hold mutation side",
            )?,
            source_profile_sha256: sha(
                required(&value, "sourceProfileSha256", "hold mutation plan")?,
                "hold mutation source profile SHA-256",
            )?,
            old_hold_sha256: canonical_sha256(&old_hold)?,
            new_hold_sha256: canonical_sha256(&new_hold)?,
            plan_id_sha256: canonical_sha256(&obj(vec![(
                "planId",
                Value::String(plan_id.clone()),
            )]))?,
            plan_id,
            old_hold,
            new_hold,
        };
        if result.canonical_payload() != *payload {
            return Err(GenomeError::invalid(
                "hold mutation plan hashes mismatched payload",
            ));
        }
        Ok(result)
    }

    pub fn plan_sha256(&self) -> Result<String, GenomeError> {
        canonical_sha256(&self.canonical_payload())
    }
}

fn management_plans(profile: &BTreeMap<String, Value>) -> Result<Vec<Value>, GenomeError> {
    let execution = object(
        required(profile, "executionConfig", "module profile")?,
        "execution config",
    )?;
    let library = object(
        required(&execution, "managementLibrary", "execution config")?,
        "management library",
    )?;
    array(
        required(&library, "plans", "management library")?,
        "management plans",
    )
}

pub fn apply_hold_mutation(
    module: &FrozenModule,
    plan: &HoldMutationPlan,
    native_validator: &dyn NativeModuleValidator,
    candidate_id: &str,
) -> Result<FrozenModule, GenomeError> {
    if module.direction != plan.side || module.profile_sha256 != plan.source_profile_sha256 {
        return Err(GenomeError::invalid(
            "hold mutation plan is not bound to this exact module",
        ));
    }
    let mut profile = module.profile.clone();
    let plans = profile
        .get_mut("executionConfig")
        .and_then(Value::as_object_mut)
        .and_then(|execution| execution.get_mut("managementLibrary"))
        .and_then(Value::as_object_mut)
        .and_then(|library| library.get_mut("plans"))
        .and_then(Value::as_array_mut)
        .ok_or_else(|| GenomeError::invalid("hold mutation requires management plans"))?;
    let indexes = plans
        .iter()
        .enumerate()
        .filter_map(|(index, item)| {
            (item.get("id") == Some(&Value::String(plan.plan_id.clone()))).then_some(index)
        })
        .collect::<Vec<_>>();
    if indexes.len() != 1 {
        return Err(GenomeError::invalid(
            "hold mutation source plan no longer matches its recorded hold",
        ));
    }
    let selected = plans[indexes[0]]
        .as_object_mut()
        .ok_or_else(|| GenomeError::invalid("hold mutation selected plan is malformed"))?;
    if canonical_sha256(&canonical_hold(selected.get("holdPolicy"))?)? != plan.old_hold_sha256 {
        return Err(GenomeError::invalid(
            "hold mutation source plan no longer matches its recorded hold",
        ));
    }
    if plan.new_hold.get("kind") == Some(&Value::String("none".to_owned())) {
        selected.remove("holdPolicy");
    } else {
        selected.insert("holdPolicy".to_owned(), plan.new_hold.clone());
    }
    let mut lineage = module.lineage.clone();
    lineage.push(obj(vec![
        ("operation", Value::String("hold_mutation".to_owned())),
        ("side", Value::String(plan.side.clone())),
        ("holdMutationPlan", plan.canonical_payload()),
        ("holdMutationPlanSha256", Value::String(plan.plan_sha256()?)),
    ]));
    FrozenModule::validate_native(
        &module.program,
        &profile,
        &module.grammar_context,
        &module.catalog,
        &module.policy,
        &module.native_authority,
        native_validator,
        candidate_id,
        &lineage,
    )
}

#[derive(Clone, Debug, PartialEq)]
pub struct FrozenPair {
    pub long: FrozenModule,
    pub short: FrozenModule,
    pub pair_compiler: IdentitySnapshot,
    pub profile: Value,
    pub validation: Value,
    pub side_targeted_lineage: Vec<Value>,
    pub raw_pair_sha256: String,
    pub profile_sha256: String,
    pub native_program_sha256: String,
    pub native_validation_report_sha256: String,
}

impl FrozenPair {
    pub fn compile(
        long: FrozenModule,
        short: FrozenModule,
        pair_compiler_identity: &IdentitySnapshot,
        pair_compiler: &dyn CanonicalPairCompiler,
        candidate_id: &str,
        side_targeted_lineage: &[Value],
    ) -> Result<Self, GenomeError> {
        if long.direction != "long" || short.direction != "short" {
            return Err(GenomeError::invalid(
                "economic candidates require exactly one long and one short v2 module",
            ));
        }
        let pair_compiler_identity = IdentitySnapshot::from_payload(
            &pair_compiler_identity.canonical_payload(),
            Some("pairCompiler"),
        )?;
        let candidate_id =
            identifier(&Value::String(candidate_id.to_owned()), "pair candidate id")?;
        let result = object(
            &pair_compiler.compile_pair(&long.profile, &short.profile, &candidate_id)?,
            "canonical pair compiler result",
        )?;
        exact_keys(
            &result,
            &["profile", "validation"],
            "canonical pair compiler result",
        )?;
        let profile = Value::Object(
            object(
                required(&result, "profile", "canonical pair compiler result")?,
                "compiled v3 profile",
            )?
            .into_iter()
            .collect(),
        );
        let validation = Value::Object(
            object(
                required(&result, "validation", "canonical pair compiler result")?,
                "compiled v3 validation",
            )?
            .into_iter()
            .collect(),
        );
        let profile_map = object(&profile, "compiled v3 profile")?;
        let validation_map = object(&validation, "compiled v3 validation")?;
        let profile_sha256 = canonical_sha256(&profile)?;
        if string(
            required(&profile_map, "version", "compiled v3 profile")?,
            "compiled v3 profile version",
        )? != "v3"
            || string(
                required(&profile_map, "directionMode", "compiled v3 profile")?,
                "compiled v3 profile direction",
            )? != "both"
            || profile_map.contains_key("hold")
            || string(
                required(&validation_map, "schemaVersion", "compiled v3 validation")?,
                "compiled v3 validation schema",
            )? != "temporal_search_candidate_validation_v1"
            || required(
                &validation_map,
                "rawSourceProfileSha256",
                "compiled v3 validation",
            )? != &Value::String(profile_sha256.clone())
            || string(
                required(&validation_map, "status", "compiled v3 validation")?,
                "compiled v3 validation status",
            )? != "valid_evaluable"
            || !bool_true(validation_map.get("candidateAcceptable"))
        {
            return Err(GenomeError::invalid(
                "canonical pair compiler did not admit the exact v3 profile",
            ));
        }
        let profile_snapshot_sha256 = sha(
            required(
                &validation_map,
                "profileSnapshotSha256",
                "compiled v3 validation",
            )?,
            "compiled v3 profile snapshot SHA-256",
        )?;
        let native_program_sha256 = sha(
            required(&validation_map, "programSha256", "compiled v3 validation")?,
            "compiled v3 program SHA-256",
        )?;
        let native_validation_report_sha256 = sha(
            required(
                &validation_map,
                "validationReportSha256",
                "compiled v3 validation",
            )?,
            "compiled v3 validation report SHA-256",
        )?;
        validate_pair_sources(&profile_map, &long, &short, "canonical compiler")?;
        let side_targeted_lineage = side_targeted_lineage
            .iter()
            .map(|item| {
                let map = object(item, "side-targeted lineage event")?;
                side(
                    required(&map, "side", "side-targeted lineage event")?,
                    "side-targeted lineage side",
                )?;
                Ok(Value::Object(map.into_iter().collect()))
            })
            .collect::<Result<Vec<_>, GenomeError>>()?;
        Ok(Self {
            long,
            short,
            pair_compiler: pair_compiler_identity,
            profile,
            validation,
            side_targeted_lineage,
            raw_pair_sha256: profile_sha256,
            profile_sha256: profile_snapshot_sha256,
            native_program_sha256,
            native_validation_report_sha256,
        })
    }

    pub fn identity_material(&self) -> Result<Value, GenomeError> {
        Ok(obj(vec![
            ("schemaVersion", Value::String(PAIR_SCHEMA.to_owned())),
            ("longModule", self.long.identity_material()),
            ("shortModule", self.short.identity_material()),
            ("pairCompiler", self.pair_compiler.canonical_payload()),
            (
                "compiledV3",
                obj(vec![
                    ("rawPairSha256", Value::String(self.raw_pair_sha256.clone())),
                    ("profileSha256", Value::String(self.profile_sha256.clone())),
                    (
                        "programSha256",
                        Value::String(self.native_program_sha256.clone()),
                    ),
                    (
                        "validationReportSha256",
                        Value::String(self.native_validation_report_sha256.clone()),
                    ),
                ]),
            ),
            (
                "sideTargetedLineage",
                Value::Array(self.side_targeted_lineage.clone()),
            ),
        ]))
    }

    pub fn identity_sha256(&self) -> Result<String, GenomeError> {
        canonical_sha256(&self.identity_material()?)
    }

    pub fn canonical_payload(&self) -> Result<Value, GenomeError> {
        let identities = self.identity_material()?;
        let compiled = identities
            .get("compiledV3")
            .cloned()
            .expect("identity material has compiledV3");
        let mut compiled_map = object(&compiled, "compiled identities")?;
        compiled_map.insert(
            "pairIdentitySha256".to_owned(),
            Value::String(self.identity_sha256()?),
        );
        Ok(obj(vec![
            ("schemaVersion", Value::String(PAIR_SCHEMA.to_owned())),
            ("long", self.long.canonical_payload()?),
            ("short", self.short.canonical_payload()?),
            ("pairCompiler", self.pair_compiler.canonical_payload()),
            ("profile", self.profile.clone()),
            ("validation", self.validation.clone()),
            (
                "sideTargetedLineage",
                Value::Array(self.side_targeted_lineage.clone()),
            ),
            (
                "identities",
                Value::Object(compiled_map.into_iter().collect()),
            ),
        ]))
    }

    pub fn from_payload(payload: &Value) -> Result<Self, GenomeError> {
        let value = object(payload, "frozen pair payload")?;
        exact_keys(
            &value,
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
            "frozen pair payload",
        )?;
        if string(
            required(&value, "schemaVersion", "frozen pair payload")?,
            "frozen pair schema",
        )? != PAIR_SCHEMA
        {
            return Err(GenomeError::invalid(
                "frozen pair payload fields are not exact",
            ));
        }
        let long = FrozenModule::from_payload(required(&value, "long", "frozen pair payload")?)?;
        let short = FrozenModule::from_payload(required(&value, "short", "frozen pair payload")?)?;
        let profile = Value::Object(
            object(
                required(&value, "profile", "frozen pair payload")?,
                "compiled v3 profile",
            )?
            .into_iter()
            .collect(),
        );
        let validation = Value::Object(
            object(
                required(&value, "validation", "frozen pair payload")?,
                "compiled v3 validation",
            )?
            .into_iter()
            .collect(),
        );
        let profile_map = object(&profile, "compiled v3 profile")?;
        let validation_map = object(&validation, "compiled v3 validation")?;
        let profile_sha256 = canonical_sha256(&profile)?;
        if long.direction != "long"
            || short.direction != "short"
            || string(
                required(&profile_map, "version", "compiled v3 profile")?,
                "compiled v3 profile version",
            )? != "v3"
            || string(
                required(&profile_map, "directionMode", "compiled v3 profile")?,
                "compiled v3 profile direction",
            )? != "both"
            || profile_map.contains_key("hold")
            || string(
                required(&validation_map, "schemaVersion", "compiled v3 validation")?,
                "compiled v3 validation schema",
            )? != "temporal_search_candidate_validation_v1"
            || required(
                &validation_map,
                "rawSourceProfileSha256",
                "compiled v3 validation",
            )? != &Value::String(profile_sha256.clone())
            || string(
                required(&validation_map, "status", "compiled v3 validation")?,
                "compiled v3 validation status",
            )? != "valid_evaluable"
            || !bool_true(validation_map.get("candidateAcceptable"))
        {
            return Err(GenomeError::invalid(
                "persisted pair is not an exact long/short v3 candidate",
            ));
        }
        validate_pair_sources(&profile_map, &long, &short, "persisted pair")?;
        let side_targeted_lineage = array(
            required(&value, "sideTargetedLineage", "frozen pair payload")?,
            "pair side-targeted lineage",
        )?
        .into_iter()
        .map(|item| {
            let map = object(&item, "side-targeted lineage item")?;
            side(
                required(&map, "side", "side-targeted lineage item")?,
                "side-targeted lineage side",
            )?;
            Ok(Value::Object(map.into_iter().collect()))
        })
        .collect::<Result<Vec<_>, GenomeError>>()?;
        let result = Self {
            long,
            short,
            pair_compiler: IdentitySnapshot::from_payload(
                required(&value, "pairCompiler", "frozen pair payload")?,
                Some("pairCompiler"),
            )?,
            profile_sha256: sha(
                required(
                    &validation_map,
                    "profileSnapshotSha256",
                    "compiled v3 validation",
                )?,
                "compiled v3 profile snapshot SHA-256",
            )?,
            native_program_sha256: sha(
                required(&validation_map, "programSha256", "compiled v3 validation")?,
                "compiled v3 program SHA-256",
            )?,
            native_validation_report_sha256: sha(
                required(
                    &validation_map,
                    "validationReportSha256",
                    "compiled v3 validation",
                )?,
                "compiled v3 validation report SHA-256",
            )?,
            profile,
            validation,
            side_targeted_lineage,
            raw_pair_sha256: profile_sha256,
        };
        let expected = result
            .canonical_payload()?
            .get("identities")
            .cloned()
            .expect("payload has identities");
        if required(&value, "identities", "frozen pair payload")? != &expected {
            return Err(GenomeError::invalid(
                "frozen pair identity material mismatched payload",
            ));
        }
        Ok(result)
    }
}

fn validate_pair_sources(
    profile: &BTreeMap<String, Value>,
    long: &FrozenModule,
    short: &FrozenModule,
    label: &str,
) -> Result<(), GenomeError> {
    let graph = object(
        required(profile, "graph", "compiled v3 profile")?,
        "compiled v3 graph",
    )?;
    let arbitration = object(
        required(&graph, "entryArbitration", "compiled v3 graph")?,
        "entry arbitration",
    )?;
    let modules = array(
        required(&arbitration, "modules", "entry arbitration")?,
        "entry arbitration modules",
    )?;
    let mut sources = BTreeMap::new();
    for module in modules {
        let module = object(&module, "entry arbitration module")?;
        let direction = string(
            required(&module, "direction", "entry arbitration module")?,
            "entry arbitration direction",
        )?;
        let snapshot = string(
            required(
                &module,
                "sourceProfileSnapshotSha256",
                "entry arbitration module",
            )?,
            "entry arbitration snapshot",
        )?;
        sources.insert(direction, snapshot);
    }
    let expected = BTreeMap::from([
        ("long".to_owned(), long.native_snapshot_sha256.clone()),
        ("short".to_owned(), short.native_snapshot_sha256.clone()),
    ]);
    if sources != expected {
        return Err(GenomeError::invalid(format!(
            "{label} native source snapshots mismatched"
        )));
    }
    Ok(())
}

pub fn apply_pair_hold_mutation(
    pair: &FrozenPair,
    plan: &HoldMutationPlan,
    native_validator: &dyn NativeModuleValidator,
    pair_compiler: &dyn CanonicalPairCompiler,
    candidate_id: &str,
) -> Result<FrozenPair, GenomeError> {
    let target = if plan.side == "long" {
        &pair.long
    } else {
        &pair.short
    };
    let changed = apply_hold_mutation(
        target,
        plan,
        native_validator,
        &format!("{candidate_id}_{}_hold", plan.side),
    )?;
    let (long, short) = if plan.side == "long" {
        (changed, pair.short.clone())
    } else {
        (pair.long.clone(), changed)
    };
    let mut lineage = pair.side_targeted_lineage.clone();
    lineage.push(obj(vec![
        ("operation", Value::String("pair_hold_mutation".to_owned())),
        ("side", Value::String(plan.side.clone())),
        ("holdMutationPlanSha256", Value::String(plan.plan_sha256()?)),
    ]));
    FrozenPair::compile(
        long,
        short,
        &pair.pair_compiler,
        pair_compiler,
        candidate_id,
        &lineage,
    )
}

/// Public domain name retained for the persisted payload schema.
pub type BidirectionalGenome = FrozenPair;
