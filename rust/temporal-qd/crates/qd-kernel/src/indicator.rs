//! Catalog-authorized indicator-learning profile construction.
//!
//! No evaluator, graph compiler, or management interpreter is implemented in
//! this crate.  This module only generates deterministic JSON transactions.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Arc,
};

use temporal_qd_contract::{ContractError, Value, canonical_sha256};

pub const INDICATOR_LEARNING_VERSION: &str = "temporal_indicator_learning_v1";
pub const INDICATOR_LEARNING_OPERATOR_VERSION: &str = "1";
pub const GRAPH_BOUND_TIMEFRAME: &str = "indicator_graph_bound_timeframe_v1";
pub const EVIDENCE_LOOKBACK: &str = "indicator_evidence_lookback_v1";
pub const TA_PERIOD: &str = "indicator_ta_period_v1";
pub const SEMANTIC_RANGE: &str = "indicator_semantic_range_v1";
pub const FAMILY_SUBSTITUTION: &str = "indicator_family_substitution_v1";
pub const EVIDENCE_WEIGHT: &str = "evidence_contribution_weight_v1";
pub const EVIDENCE_MEMBERSHIP: &str = "evidence_group_membership_v1";
pub const INDICATOR_INSTANCE: &str = "indicator_instance_structure_v1";
pub const ENTRY_ROUTE_DECISION_INDICATOR_CAP: usize = 3;
pub const ENTRY_ROUTE_DECISION_INDICATOR_POLICY_VERSION: &str =
    "temporal_entry_route_decision_indicator_cap_v1";

const TIMEFRAMES: &[&str] = &["M5", "M15", "H1"];
const LOOKBACKS: &[i64] = &[1, 2, 3, 5];
const MAX_GROUP_MEMBERS: usize = 3;

#[derive(Debug, thiserror::Error)]
pub enum IndicatorError {
    #[error("indicator contract error: {0}")]
    Contract(#[from] ContractError),
    #[error("indicator learning: {0}")]
    Invalid(String),
}
pub type IndicatorResult<T> = Result<T, IndicatorError>;

fn err(message: impl Into<String>) -> IndicatorError {
    IndicatorError::Invalid(message.into())
}
fn obj(rows: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    let mut output = Value::Object(Default::default());
    for (key, value) in rows {
        output
            .as_object_mut()
            .unwrap()
            .insert(key.to_owned(), value);
    }
    output
}
fn arr(values: impl IntoIterator<Item = Value>) -> Value {
    Value::Array(values.into_iter().collect())
}
fn at<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value.as_object()?.get(key)
}
fn at_mut<'a>(value: &'a mut Value, key: &str) -> Option<&'a mut Value> {
    value.as_object_mut()?.get_mut(key)
}
fn string(value: Option<&Value>) -> String {
    value.and_then(Value::as_str).unwrap_or_default().to_owned()
}
fn numeric(value: Option<&Value>) -> Option<f64> {
    value.and_then(Value::as_f64)
}
/// Match Python's JSON-value equality where numeric values compare by value,
/// not by their serialized integer/float representation.  The frozen
/// Dashboard model can materialize an authored `1` as `1.0`; that remains the
/// same construction parent in Python.
fn python_value_eq(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(_), Value::Number(_)) => numeric(Some(left)) == numeric(Some(right)),
        (Value::Array(left), Value::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| python_value_eq(left, right))
        }
        (Value::Object(left), Value::Object(right)) => {
            left.len() == right.len()
                && left.iter().all(|(key, left)| {
                    right
                        .get(key)
                        .is_some_and(|right| python_value_eq(left, right))
                })
        }
        _ => left == right,
    }
}
fn python_int(value: &Value) -> Option<i64> {
    if let Some(value) = value.as_i64() {
        return Some(value);
    }
    let value = numeric(Some(value))?;
    (value.is_finite() && value >= i64::MIN as f64 && value <= i64::MAX as f64)
        .then_some(value as i64)
}
fn int(value: Option<&Value>) -> Option<i64> {
    value.and_then(Value::as_i64)
}
fn cloned(value: &Value, key: &str) -> Option<Value> {
    at(value, key).cloned()
}
fn sha(value: &Value) -> IndicatorResult<String> {
    Ok(canonical_sha256(value)?)
}
fn strings(values: impl IntoIterator<Item = String>) -> Value {
    arr(values.into_iter().map(Value::String))
}
fn is_active_closed(item: &Value) -> bool {
    at(item, "config").is_some_and(|config| {
        at(config, "isActive").and_then(Value::as_bool) == Some(true)
            && at(config, "useFormingBar").and_then(Value::as_bool) == Some(false)
    })
}

/// Match Python's `_fuzzy_group_member_eligible`: membership may never use a
/// raw event or management scalar as a fuzzy-evidence member, even where that
/// instance also exposes a ranged state score.
fn membership_member_eligible(
    item: &Value,
    instance: &str,
    evidence: &BTreeSet<String>,
    events: &BTreeSet<String>,
    scalar: &BTreeSet<String>,
) -> bool {
    evidence.contains(instance)
        && !events.contains(instance)
        && !scalar.contains(instance)
        && at(item, "meta")
            .and_then(|meta| binding_contract(meta, true, false, false))
            .is_some()
        && is_active_closed(item)
}

/// Composite profiles must preserve Python's side-local evidence topology.
/// Directional module profiles have no ownership surface and remain eligible.
fn membership_owner_matches(profile: &Value, group_id: &str, instance_id: &str) -> bool {
    if string(at(profile, "directionMode")) != "both" {
        return true;
    }
    let Some(modules) = at(profile, "graph")
        .and_then(|graph| at(graph, "entryArbitration"))
        .and_then(|arbitration| at(arbitration, "modules"))
        .and_then(Value::as_array)
    else {
        return false;
    };
    let mut group_owners = BTreeSet::new();
    let mut instance_owners = BTreeSet::new();
    for module in modules {
        let direction = string(at(module, "direction"));
        let Some(indicator_ids) = at(module, "indicatorIds").and_then(Value::as_array) else {
            return false;
        };
        if !matches!(direction.as_str(), "long" | "short") {
            return false;
        }
        let owns_instance = indicator_ids
            .iter()
            .any(|value| string(Some(value)) == instance_id);
        let owns_group = at(module, "evidenceGroupIds")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .any(|value| string(Some(value)) == group_id);
        if owns_instance {
            instance_owners.insert(direction.clone());
        }
        if owns_group {
            group_owners.insert(direction);
        }
    }
    group_owners.len() == 1 && group_owners == instance_owners
}

#[derive(Clone, Debug)]
pub struct IndicatorCatalog {
    payload: Value,
    entries: BTreeMap<String, Value>,
    timeframe_policy: Vec<String>,
    catalog_sha256: String,
}

impl IndicatorCatalog {
    pub fn new(payload: &Value) -> IndicatorResult<Self> {
        Self::with_timeframes(
            payload,
            &TIMEFRAMES
                .iter()
                .map(|item| (*item).to_owned())
                .collect::<Vec<_>>(),
        )
    }

    pub fn with_timeframes(payload: &Value, requested: &[String]) -> IndicatorResult<Self> {
        let frames = at(payload, "timeframes")
            .and_then(Value::as_object)
            .ok_or_else(|| err("catalog requires timeframes"))?;
        let available = frames
            .keys()
            .map(|key| key.to_uppercase())
            .collect::<BTreeSet<_>>();
        let timeframe_policy = requested
            .iter()
            .map(|item| item.to_uppercase())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if timeframe_policy.is_empty()
            || timeframe_policy
                .iter()
                .any(|item| !available.contains(item))
        {
            return Err(err("timeframe policy is not catalog-backed"));
        }
        let mut entries = BTreeMap::new();
        for item in at(payload, "indicators")
            .and_then(Value::as_array)
            .ok_or_else(|| err("catalog requires indicators"))?
        {
            let Some(meta) = at(item, "meta") else {
                continue;
            };
            let id = string(at(meta, "id")).trim().to_owned();
            if id.is_empty() || entries.contains_key(&id) {
                return Err(err("catalog indicator IDs must be unique"));
            }
            entries.insert(
                id,
                obj([
                    ("meta", meta.clone()),
                    ("config", cloned(item, "config").unwrap_or_else(|| obj([]))),
                ]),
            );
        }
        if entries.is_empty() {
            return Err(err("catalog requires indicators"));
        }
        let identity = obj([
            ("payload", payload.clone()),
            ("timeframePolicy", strings(timeframe_policy.clone())),
        ]);
        Ok(Self {
            payload: payload.clone(),
            entries,
            timeframe_policy,
            catalog_sha256: sha(&identity)?,
        })
    }

    pub fn catalog_sha256(&self) -> &str {
        &self.catalog_sha256
    }
    pub fn payload(&self) -> &Value {
        &self.payload
    }
    pub fn timeframe_policy(&self) -> &[String] {
        &self.timeframe_policy
    }
    pub fn entry(&self, id: &str) -> Option<Value> {
        self.entries.get(id).cloned()
    }
    /// Report a deliberately unavailable substitution rather than guessing a
    /// replacement when the catalog omits a compatible technical capability.
    pub fn deferred_dispositions(&self, profile: &Value) -> IndicatorResult<Vec<Value>> {
        let (evidence, events, scalar) = bound(profile);
        let mut result = vec![];
        for (index, item) in at(profile, "indicators")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
        {
            let meta = at(item, "meta").unwrap_or(&Value::Null);
            let id = string(at(meta, "id"));
            let instance = string(at(meta, "instanceId"));
            let Some(source) = self.entry(&id) else {
                result.push(obj([
                    ("indicatorIndex", Value::from(index)),
                    ("indicatorId", Value::String(id)),
                    ("disposition", Value::String("deferred".into())),
                    (
                        "reason",
                        Value::String("source_catalog_metadata_missing".into()),
                    ),
                ]));
                continue;
            };
            let source_meta = at(&source, "meta").unwrap();
            let required = ["signalPersistence", "valueRange", "requiredPaddingBars"]
                .iter()
                .filter(|key| at(source_meta, key).is_none())
                .map(|key| (*key).to_owned())
                .collect::<Vec<_>>();
            if !required.is_empty() {
                result.push(obj([
                    ("indicatorIndex", Value::from(index)),
                    ("indicatorId", Value::String(id)),
                    ("disposition", Value::String("deferred".into())),
                    (
                        "reason",
                        Value::String("source_compatibility_metadata_missing".into()),
                    ),
                    ("missing", strings(required)),
                ]));
                continue;
            }
            let shape = (
                evidence.contains(&instance),
                events.contains(&instance),
                scalar.contains(&instance),
            );
            if !(shape.0 || shape.1 || shape.2) {
                continue;
            }
            let compatible =
                binding_contract(source_meta, shape.0, shape.1, shape.2).is_some_and(|contract| {
                    self.entries.iter().any(|(peer_id, peer)| {
                        peer_id != &id
                            && binding_contract(
                                at(peer, "meta").unwrap(),
                                shape.0,
                                shape.1,
                                shape.2,
                            ) == Some(contract.clone())
                    })
                });
            if compatible {
                continue;
            }
            let reason = if shape.1 {
                "event_output_schema_metadata_not_admitted"
            } else if shape.2 {
                "management_scalar_binding_replacement_not_admitted"
            } else {
                "fuzzy_evidence_capability_not_admitted"
            };
            result.push(obj([
                ("indicatorIndex", Value::from(index)),
                ("indicatorId", Value::String(id)),
                ("disposition", Value::String("deferred".into())),
                ("reason", Value::String(reason.into())),
            ]));
        }
        Ok(result)
    }
}

fn bound(profile: &Value) -> (BTreeSet<String>, BTreeSet<String>, BTreeSet<String>) {
    let mut evidence = BTreeSet::new();
    let mut events = BTreeSet::new();
    let mut scalar = BTreeSet::new();
    for group in at(profile, "graph")
        .and_then(|graph| at(graph, "evidenceGroups"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        for id in at(group, "indicatorInstanceIds")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let id = string(Some(id));
            if !id.is_empty() {
                evidence.insert(id);
            }
        }
    }
    for event in at(profile, "graph")
        .and_then(|graph| at(graph, "eventBindings"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let id = string(at(event, "indicatorInstanceId"));
        if !id.is_empty() {
            events.insert(id);
        }
    }
    for binding in at(profile, "executionConfig")
        .and_then(|config| at(config, "managementLibrary"))
        .and_then(|library| at(library, "scalarBindings"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let id = string(at(binding, "indicatorInstanceId"));
        if !id.is_empty() {
            scalar.insert(id);
        }
    }
    (evidence, events, scalar)
}
fn indic<'a>(profile: &'a Value, instance: &str) -> Option<(usize, &'a Value)> {
    at(profile, "indicators")
        .and_then(Value::as_array)?
        .iter()
        .enumerate()
        .find(|(_, item)| {
            string(at(item, "meta").and_then(|meta| at(meta, "instanceId"))) == instance
        })
}
fn reachable_groups(profile: &Value) -> BTreeSet<String> {
    let graph = at(profile, "graph");
    let transitions = graph
        .and_then(|item| at(item, "transitions"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut states = BTreeSet::new();
    let initial = string(graph.and_then(|item| at(item, "initialStateId")));
    if !initial.is_empty() {
        states.insert(initial);
    }
    while {
        let mut changed = false;
        for edge in &transitions {
            if states.contains(&string(at(edge, "sourceStateId"))) {
                let destination = string(at(edge, "destinationStateId"));
                if !destination.is_empty() {
                    changed |= states.insert(destination);
                }
            }
        }
        changed
    } {}
    fn visit(value: &Value, result: &mut BTreeSet<String>) {
        match value {
            Value::Object(rows) => {
                if ["evidence_at_least", "evidence_below"]
                    .contains(&string(rows.get("kind")).as_str())
                {
                    let id = string(rows.get("groupId"));
                    if !id.is_empty() {
                        result.insert(id);
                    }
                }
                for child in rows.values() {
                    visit(child, result);
                }
            }
            Value::Array(rows) => {
                for child in rows {
                    visit(child, result);
                }
            }
            _ => {}
        }
    }
    let mut result = BTreeSet::new();
    for edge in transitions {
        if states.contains(&string(at(&edge, "sourceStateId"))) {
            if let Some(guard) = at(&edge, "guard") {
                visit(guard, &mut result);
            }
        }
    }
    result
}
fn meta_catalog_matches(authored: &Value, catalog: &Value) -> IndicatorResult<bool> {
    fn normalized(value: &Value, catalog_shape: Option<&Value>) -> Value {
        match value {
            Value::Object(rows) => {
                let mut output = Value::Object(Default::default());
                for (key, item) in rows {
                    if key == "instanceId" || key == "docs" {
                        continue;
                    }
                    let shape = catalog_shape.and_then(|value| at(value, key));
                    if shape.is_some() || !item.is_null() {
                        output
                            .as_object_mut()
                            .unwrap()
                            .insert(key.clone(), normalized(item, shape));
                    }
                }
                output
            }
            Value::Array(rows) => arr(rows.iter().enumerate().map(|(index, item)| {
                normalized(
                    item,
                    catalog_shape
                        .and_then(Value::as_array)
                        .and_then(|items| items.get(index)),
                )
            })),
            Value::Number(item) => Value::from(item.as_f64().unwrap_or_default()),
            _ => value.clone(),
        }
    }
    Ok(sha(&normalized(authored, Some(catalog)))? == sha(&normalized(catalog, Some(catalog)))?)
}
fn talib(config: &Value) -> BTreeMap<String, Value> {
    at(config, "talibConfig")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|row| {
            let name = string(at(row, "name"));
            (!name.is_empty()).then(|| (name, cloned(row, "value").unwrap_or(Value::Null)))
        })
        .collect()
}
fn replace_talib(config: &mut Value, name: &str, value: Value) -> IndicatorResult<()> {
    let rows = at_mut(config, "talibConfig")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| err("indicator config requires talibConfig"))?;
    let row = rows
        .iter_mut()
        .find(|row| string(at(row, "name")) == name)
        .ok_or_else(|| err("catalog period descriptor absent"))?;
    row.as_object_mut().unwrap().insert("value".into(), value);
    Ok(())
}
fn period_order_valid(config: &Value) -> bool {
    let values = talib(config);
    let fast = values
        .iter()
        .filter(|(name, _)| name.to_lowercase().starts_with("fast"))
        .filter_map(|(_, value)| numeric(Some(value)))
        .fold(f64::NEG_INFINITY, f64::max);
    let slow = values
        .iter()
        .filter(|(name, _)| name.to_lowercase().starts_with("slow"))
        .filter_map(|(_, value)| numeric(Some(value)))
        .fold(f64::INFINITY, f64::min);
    fast == f64::NEG_INFINITY || slow == f64::INFINITY || fast < slow
}
fn compatibility_missing(meta: &Value) -> Vec<String> {
    ["signalPersistence", "valueRange", "requiredPaddingBars"]
        .iter()
        .filter(|key| at(meta, key).is_none())
        .map(|key| (*key).to_owned())
        .collect()
}
fn numeric_range(meta: &Value) -> Option<Value> {
    let range = at(meta, "valueRange")?;
    let min = numeric(at(range, "min"))?;
    let max = numeric(at(range, "max"))?;
    let step = numeric(at(range, "step"))?;
    let min_range = numeric(at(range, "minRange"))?;
    (min.is_finite()
        && max.is_finite()
        && step.is_finite()
        && min_range.is_finite()
        && step > 0.0
        && min_range > 0.0
        && max - min >= min_range)
        .then(|| {
            obj([
                ("min", Value::from(min)),
                ("max", Value::from(max)),
                ("step", Value::from(step)),
                ("minRange", Value::from(min_range)),
            ])
        })
}
/// The Python authority treats the numeric score domain as the causal
/// boundary for threshold inheritance during a family substitution.  The
/// catalog may change the indicator implementation, but ranges retain their
/// meaning only when the old and replacement score domains are identical.
fn range_signature(meta: &Value) -> String {
    numeric_range(meta)
        .and_then(|range| canonical_sha256(&range).ok())
        .unwrap_or_default()
}
fn substitution_contract(meta: &Value) -> Option<Value> {
    let contract = at(meta, "familySubstitution")?;
    let _rows = contract.as_object()?;
    for key in [
        "substitutionClass",
        "polarity",
        "scoreUnit",
        "rawUnit",
        "eventOutputSchema",
        "persistenceCompatibility",
    ] {
        at(contract, key)?;
    }
    if ![
        "substitutionClass",
        "polarity",
        "scoreUnit",
        "rawUnit",
        "persistenceCompatibility",
    ]
    .iter()
    .all(|key| !string(at(contract, key)).is_empty())
        || string(at(contract, "persistenceCompatibility")) != string(at(meta, "signalPersistence"))
    {
        return None;
    }
    Some(contract.clone())
}
fn scalar_output_contract(meta: &Value) -> Option<Value> {
    let rows = at(meta, "managementScalarOutputs")?.as_array()?;
    if rows.is_empty() {
        return None;
    }
    let mut outputs = rows
        .iter()
        .map(|row| {
            let row = row.as_object()?;
            let output_key = string(row.get("outputKey")).trim().to_owned();
            let value_kind = string(row.get("valueKind")).trim().to_owned();
            let unit = string(row.get("unit")).trim().to_owned();
            let expected = match value_kind.as_str() {
                "price_level" => "price",
                "price_distance" => "price_distance",
                _ => return None,
            };
            (!output_key.is_empty() && unit == expected).then(|| {
                (
                    (output_key.clone(), value_kind.clone(), unit.clone()),
                    obj([
                        ("outputKey", Value::String(output_key)),
                        ("valueKind", Value::String(value_kind)),
                        ("unit", Value::String(unit)),
                    ]),
                )
            })
        })
        .collect::<Option<Vec<_>>>()?;
    outputs.sort_by(|(left, _), (right, _)| left.cmp(right));
    outputs
        .windows(2)
        .all(|items| items[0].0 != items[1].0)
        .then(|| arr(outputs.into_iter().map(|(_, output)| output)))
}
fn fuzzy_evidence_contract(meta: &Value) -> Option<Value> {
    if string(at(meta, "signalPersistence")) != "state"
        || at(meta, "usesRangeConfiguration").and_then(Value::as_bool) != Some(true)
    {
        return None;
    }
    numeric_range(meta)?;
    let scalar_outputs = scalar_output_contract(meta).unwrap_or_else(|| arr([]));
    if at(meta, "familySubstitution").is_none_or(Value::is_null) {
        return Some(obj([
            ("kind", Value::String("fuzzy_evidence".into())),
            (
                "schema",
                Value::String("derived_ranged_state_score_v1".into()),
            ),
            ("scalarOutputs", scalar_outputs),
        ]));
    }
    let contract = substitution_contract(meta)?;
    at(&contract, "eventOutputSchema")?.as_object()?;
    Some(obj([
        ("kind", Value::String("fuzzy_evidence".into())),
        (
            "schema",
            Value::String("explicit_family_substitution_v1".into()),
        ),
        ("contract", contract),
        ("scalarOutputs", scalar_outputs),
    ]))
}
fn event_contract(meta: &Value) -> Option<Value> {
    let contract = substitution_contract(meta)?;
    let schema = at(&contract, "eventOutputSchema")?.as_object()?;
    let persistence = string(at(meta, "signalPersistence"));
    if !["event", "event-with-lookback"].contains(&persistence.as_str())
        || string(schema.get("kind")) != "directional_tokens"
    {
        return None;
    }
    let long_output = string(schema.get("longOutput")).trim().to_owned();
    let short_output = string(schema.get("shortOutput")).trim().to_owned();
    (!long_output.is_empty() && !short_output.is_empty() && long_output != short_output).then(
        || {
            obj([
                ("kind", Value::String("raw_event".into())),
                ("signalPersistence", Value::String(persistence)),
                (
                    "eventOutputSchema",
                    obj([
                        ("kind", Value::String("directional_tokens".into())),
                        ("longOutput", Value::String(long_output)),
                        ("shortOutput", Value::String(short_output)),
                    ]),
                ),
            ])
        },
    )
}
fn management_scalar_contract(meta: &Value) -> Option<Value> {
    let outputs = scalar_output_contract(meta)?;
    (string(at(meta, "signalPersistence")) == "state").then(|| {
        obj([
            ("kind", Value::String("scalar_management".into())),
            ("outputs", outputs),
        ])
    })
}
fn binding_contract(meta: &Value, fuzzy: bool, event: bool, scalar: bool) -> Option<Value> {
    if event && (fuzzy || scalar) {
        return None;
    }
    let mut capabilities = Vec::new();
    if fuzzy {
        capabilities.push(fuzzy_evidence_contract(meta)?);
    }
    if event {
        capabilities.push(event_contract(meta)?);
    }
    if scalar {
        capabilities.push(management_scalar_contract(meta)?);
    }
    (!capabilities.is_empty()).then(|| {
        obj([
            (
                "schemaVersion",
                Value::String("temporal_indicator_binding_contract_v1".into()),
            ),
            ("capabilities", arr(capabilities)),
        ])
    })
}
fn profile_invariants(profile: &Value) -> BTreeMap<String, bool> {
    let (evidence, events, scalar) = bound(profile);
    let indicators = at(profile, "indicators")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let by_id = indicators
        .iter()
        .filter_map(|item| {
            let id = string(at(item, "meta").and_then(|meta| at(meta, "instanceId")));
            (!id.is_empty()).then_some((id, item))
        })
        .collect::<BTreeMap<_, _>>();
    let groups = at(profile, "graph")
        .and_then(|graph| at(graph, "evidenceGroups"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let event_lookback = events.iter().all(|instance| {
        int(by_id
            .get(instance)
            .and_then(|item| at(item, "config"))
            .and_then(|config| at(config, "lookbackBars")))
        .unwrap_or(1)
            == 1
    });
    let groups_closed = groups.iter().all(|group| {
        at(group, "indicatorInstanceIds")
            .and_then(Value::as_array)
            .is_some_and(|members| {
                !members.is_empty()
                    && members.len() <= MAX_GROUP_MEMBERS
                    && members
                        .iter()
                        .map(|member| string(Some(member)))
                        .collect::<BTreeSet<_>>()
                        .len()
                        == members.len()
                    && members
                        .iter()
                        .all(|member| by_id.contains_key(&string(Some(member))))
            })
    });
    let direction = string(at(profile, "directionMode"));
    let cap = if ["long", "short"].contains(&direction.as_str()) {
        evidence.len() <= MAX_GROUP_MEMBERS
    } else {
        true
    };
    BTreeMap::from([
        ("event_bound_lookback_is_one".into(), event_lookback),
        (
            "bound_instances_exist".into(),
            evidence
                .union(&events)
                .chain(scalar.iter())
                .all(|id| by_id.contains_key(id)),
        ),
        (
            "scalar_bindings_do_not_overlap_event_persistence".into(),
            events.is_disjoint(&scalar),
        ),
        (
            "event_bindings_do_not_overlap_fuzzy_evidence".into(),
            events.is_disjoint(&evidence),
        ),
        ("evidence_group_membership_is_closed".into(), groups_closed),
        ("bound_indicator_instances_within_direction_cap".into(), cap),
        (
            "indicator_instance_ids_are_unique".into(),
            by_id.len() == indicators.len(),
        ),
    ])
}
fn static_audit(
    operator: &IndicatorOperator,
    parent: &Value,
    child: &Value,
    plan: &Value,
) -> IndicatorResult<Value> {
    let mut checks = BTreeMap::new();
    for (key, value) in profile_invariants(parent) {
        checks.insert(format!("parent_{key}"), value);
    }
    for (key, value) in profile_invariants(child) {
        checks.insert(format!("child_{key}"), value);
    }
    checks.insert(
        "construction_remains_behaviorally_relevant".into(),
        operator.relevant(parent, at(plan, "construction").unwrap_or(&Value::Null)),
    );
    checks.insert(
        "scalar_binding_sources_preserved".into(),
        bound(parent).2 == bound(child).2,
    );
    let mut check_values = Value::Object(Default::default());
    for (key, value) in checks {
        check_values
            .as_object_mut()
            .unwrap()
            .insert(key, Value::Bool(value));
    }
    let all = check_values
        .as_object()
        .unwrap()
        .values()
        .all(|value| value.as_bool() == Some(true));
    let mut report = obj([
        (
            "schemaVersion",
            Value::String("temporal_structural_operator_audit_v1".into()),
        ),
        ("operatorId", Value::String(operator.id.clone())),
        (
            "operatorVersion",
            Value::String(INDICATOR_LEARNING_OPERATOR_VERSION.into()),
        ),
        ("planSha256", cloned(plan, "planSha256").unwrap()),
        ("childSourceProfileSha256", Value::String(sha(child)?)),
        ("checks", check_values),
        ("allChecksPassed", Value::Bool(all)),
    ]);
    let digest = sha(&report)?;
    report
        .as_object_mut()
        .unwrap()
        .insert("auditSha256".into(), Value::String(digest));
    Ok(report)
}

#[derive(Clone, Debug)]
pub struct IndicatorOperator {
    id: String,
    /// Every operator in a registry observes the exact same frozen catalog.
    /// Sharing it avoids eight deep JSON/catalog clones per registry while
    /// retaining immutable, side-local construction semantics.
    catalog: Arc<IndicatorCatalog>,
    specification: Value,
}
impl IndicatorOperator {
    fn new(id: &str, catalog: Arc<IndicatorCatalog>) -> IndicatorResult<Self> {
        let mut specification = obj([
            (
                "schemaVersion",
                Value::String("temporal_indicator_learning_operator_spec_v1".into()),
            ),
            ("operatorId", Value::String(id.into())),
            (
                "operatorVersion",
                Value::String(INDICATOR_LEARNING_OPERATOR_VERSION.into()),
            ),
            (
                "learningVersion",
                Value::String(INDICATOR_LEARNING_VERSION.into()),
            ),
            (
                "catalogSha256",
                Value::String(catalog.catalog_sha256.clone()),
            ),
        ]);
        let digest = sha(&specification)?;
        specification
            .as_object_mut()
            .unwrap()
            .insert("operatorSpecSha256".into(), Value::String(digest));
        Ok(Self {
            id: id.into(),
            catalog,
            specification,
        })
    }
    pub fn operator_id(&self) -> &str {
        &self.id
    }
    pub fn specification(&self) -> &Value {
        &self.specification
    }
    pub fn enumerate_plans(&self, profile: &Value) -> IndicatorResult<Vec<Value>> {
        if !profile_invariants(profile).values().all(|value| *value) {
            return Ok(vec![]);
        }
        let parent = sha(profile)?;
        let mut result = BTreeMap::new();
        for construction in self.constructions(profile)? {
            if !self.relevant(profile, &construction) {
                continue;
            }
            let (child, _) = self.transform(profile, &construction)?;
            if !profile_invariants(&child).values().all(|value| *value) {
                continue;
            }
            let identity = obj([
                (
                    "schemaVersion",
                    Value::String("temporal_indicator_learning_identity_v1".into()),
                ),
                ("operatorId", Value::String(self.id.clone())),
                (
                    "operatorVersion",
                    Value::String(INDICATOR_LEARNING_OPERATOR_VERSION.into()),
                ),
                (
                    "learningVersion",
                    Value::String(INDICATOR_LEARNING_VERSION.into()),
                ),
                ("parentSourceProfileSha256", Value::String(parent.clone())),
                (
                    "catalogSha256",
                    Value::String(self.catalog.catalog_sha256.clone()),
                ),
                ("construction", construction.clone()),
            ]);
            let mut plan = obj([
                ("operatorId", Value::String(self.id.clone())),
                (
                    "operatorVersion",
                    Value::String(INDICATOR_LEARNING_OPERATOR_VERSION.into()),
                ),
                (
                    "operatorSpecSha256",
                    cloned(&self.specification, "operatorSpecSha256").unwrap(),
                ),
                ("parentSourceProfileSha256", Value::String(parent.clone())),
                (
                    "catalogSha256",
                    Value::String(self.catalog.catalog_sha256.clone()),
                ),
                ("construction", construction),
                ("constructionIdentitySha256", Value::String(sha(&identity)?)),
                (
                    "schemaVersion",
                    Value::String("temporal_structural_operator_plan_v1".into()),
                ),
            ]);
            let digest = sha(&plan)?;
            plan.as_object_mut()
                .unwrap()
                .insert("planSha256".into(), Value::String(digest.clone()));
            result.insert(digest, plan);
        }
        Ok(result.into_values().collect())
    }
    pub fn preview(&self, profile: &Value, plan: &Value) -> IndicatorResult<Value> {
        Ok(self.preview_trace(profile, plan)?.0)
    }
    pub fn apply(
        &self,
        profile: &Value,
        plan: &Value,
        parent_program_sha256: &str,
        child_program_sha256: &str,
    ) -> IndicatorResult<(Value, Value)> {
        let (child, trace) = self.preview_trace(profile, plan)?;
        let audit = static_audit(self, profile, &child, plan)?;
        if at(&audit, "allChecksPassed").and_then(Value::as_bool) != Some(true) {
            return Err(err("indicator invariant audit failed"));
        }
        let mut application = obj([
            (
                "schemaVersion",
                Value::String("temporal_structural_operator_application_v1".into()),
            ),
            ("operatorId", Value::String(self.id.clone())),
            (
                "operatorVersion",
                Value::String(INDICATOR_LEARNING_OPERATOR_VERSION.into()),
            ),
            (
                "operatorSpecSha256",
                cloned(&self.specification, "operatorSpecSha256").unwrap(),
            ),
            ("planSha256", cloned(plan, "planSha256").unwrap()),
            (
                "constructionIdentitySha256",
                cloned(plan, "constructionIdentitySha256").unwrap(),
            ),
            ("parentSourceProfileSha256", Value::String(sha(profile)?)),
            ("childSourceProfileSha256", Value::String(sha(&child)?)),
            (
                "parentValidatedProgramSha256",
                Value::String(parent_program_sha256.into()),
            ),
            (
                "childValidatedProgramSha256",
                Value::String(child_program_sha256.into()),
            ),
            ("mutationTrace", arr(trace)),
            ("staticInvariantReport", audit),
            (
                "evidenceScope",
                obj([
                    ("marketReplayRun", Value::Bool(false)),
                    ("firedEvidence", Value::String("unmeasured".into())),
                    ("activationEvidence", Value::String("unmeasured".into())),
                    ("evidencePlanRotationRequired", Value::Bool(true)),
                    ("lakeScopeRegenerationRequired", Value::Bool(true)),
                ]),
            ),
        ]);
        let application_sha = sha(&application)?;
        application
            .as_object_mut()
            .unwrap()
            .insert("applicationSha256".into(), Value::String(application_sha));
        Ok((child, application))
    }
    pub fn audit(
        &self,
        parent: &Value,
        transformed: &Value,
        application: &Value,
    ) -> IndicatorResult<Value> {
        let mut body = application.clone();
        let supplied = body
            .as_object_mut()
            .and_then(|rows| rows.remove("applicationSha256"));
        let plan = self
            .enumerate_plans(parent)?
            .into_iter()
            .find(|plan| cloned(plan, "planSha256") == cloned(&body, "planSha256"));
        let preview = plan
            .as_ref()
            .map(|plan| self.preview_trace(parent, plan))
            .transpose()?;
        let expected_static = plan
            .as_ref()
            .map(|plan| static_audit(self, parent, transformed, plan))
            .transpose()?;
        let checks = obj([
            (
                "application_identity_exact",
                Value::Bool(
                    supplied
                        .as_ref()
                        .and_then(Value::as_str)
                        .is_some_and(|digest| sha(&body).ok().as_deref() == Some(digest)),
                ),
            ),
            ("plan_is_currently_applicable", Value::Bool(plan.is_some())),
            (
                "transformed_profile_exact",
                Value::Bool(
                    preview
                        .as_ref()
                        .is_some_and(|(child, _)| child == transformed),
                ),
            ),
            (
                "mutation_trace_exact",
                Value::Bool(preview.as_ref().is_some_and(|(_, trace)| {
                    at(&body, "mutationTrace") == Some(&arr(trace.clone()))
                })),
            ),
            (
                "embedded_static_report_exact",
                Value::Bool(
                    expected_static
                        .as_ref()
                        .is_some_and(|report| at(&body, "staticInvariantReport") == Some(report)),
                ),
            ),
            (
                "embedded_static_report_passing",
                Value::Bool(
                    at(&body, "staticInvariantReport")
                        .and_then(|report| at(report, "allChecksPassed"))
                        .and_then(Value::as_bool)
                        == Some(true),
                ),
            ),
            (
                "recomputed_static_report_passing",
                Value::Bool(
                    expected_static
                        .as_ref()
                        .and_then(|report| at(report, "allChecksPassed"))
                        .and_then(Value::as_bool)
                        == Some(true),
                ),
            ),
        ]);
        let all = checks
            .as_object()
            .unwrap()
            .values()
            .all(|value| value.as_bool() == Some(true));
        let mut audit = obj([
            (
                "schemaVersion",
                Value::String("temporal_structural_operator_audit_v1".into()),
            ),
            ("operatorId", Value::String(self.id.clone())),
            ("applicationSha256", supplied.unwrap_or(Value::Null)),
            ("checks", checks),
            ("allChecksPassed", Value::Bool(all)),
        ]);
        let audit_sha = sha(&audit)?;
        audit
            .as_object_mut()
            .unwrap()
            .insert("auditSha256".into(), Value::String(audit_sha));
        Ok(audit)
    }
    fn preview_trace(&self, profile: &Value, plan: &Value) -> IndicatorResult<(Value, Vec<Value>)> {
        if !self
            .enumerate_plans(profile)?
            .iter()
            .any(|item| item == plan)
        {
            return Err(err("indicator plan is not canonical and applicable"));
        }
        self.transform(
            profile,
            at(plan, "construction").ok_or_else(|| err("plan construction missing"))?,
        )
    }
    fn relevant(&self, profile: &Value, construction: &Value) -> bool {
        let (evidence, events, scalar) = bound(profile);
        let instance = string(at(construction, "indicatorInstanceId"));
        match self.id.as_str() {
            GRAPH_BOUND_TIMEFRAME | TA_PERIOD | FAMILY_SUBSTITUTION => {
                evidence.contains(&instance)
                    || events.contains(&instance)
                    || scalar.contains(&instance)
            }
            EVIDENCE_LOOKBACK => {
                evidence.contains(&instance)
                    && !events.contains(&instance)
                    && !scalar.contains(&instance)
            }
            SEMANTIC_RANGE => {
                if !evidence.contains(&instance) {
                    return false;
                }
                let side = string(at(construction, "change").and_then(|change| at(change, "side")));
                match string(at(profile, "directionMode")).as_str() {
                    "long" => side == "buy",
                    "short" => side == "sell",
                    "both" => side == "buy" || side == "sell",
                    _ => false,
                }
            }
            EVIDENCE_WEIGHT => {
                evidence.contains(&instance)
                    && !events.contains(&instance)
                    && !scalar.contains(&instance)
            }
            EVIDENCE_MEMBERSHIP => {
                reachable_groups(profile).contains(&string(at(construction, "groupId")))
            }
            INDICATOR_INSTANCE => {
                if string(at(construction, "kind")) == "insert_fuzzy_indicator_instance" {
                    reachable_groups(profile).contains(&string(at(construction, "groupId")))
                } else {
                    at(construction, "affectedGroups")
                        .and_then(Value::as_array)
                        .is_some_and(|groups| {
                            groups.iter().any(|group| {
                                reachable_groups(profile).contains(&string(at(group, "groupId")))
                            })
                        })
                }
            }
            _ => false,
        }
    }
    fn constructions(&self, profile: &Value) -> IndicatorResult<Vec<Value>> {
        match self.id.as_str() {
            GRAPH_BOUND_TIMEFRAME => self.timeframes(profile),
            EVIDENCE_LOOKBACK => self.lookbacks(profile),
            TA_PERIOD => self.periods(profile),
            SEMANTIC_RANGE => self.ranges(profile),
            EVIDENCE_WEIGHT => self.weights(profile),
            EVIDENCE_MEMBERSHIP => self.membership(profile),
            INDICATOR_INSTANCE => self.instances(profile),
            FAMILY_SUBSTITUTION => self.family(profile),
            _ => Ok(vec![]),
        }
    }
    fn transform(
        &self,
        profile: &Value,
        construction: &Value,
    ) -> IndicatorResult<(Value, Vec<Value>)> {
        match self.id.as_str() {
            GRAPH_BOUND_TIMEFRAME | EVIDENCE_LOOKBACK | EVIDENCE_WEIGHT => {
                self.transform_simple(profile, construction)
            }
            TA_PERIOD => self.transform_period(profile, construction),
            SEMANTIC_RANGE => self.transform_range(profile, construction),
            EVIDENCE_MEMBERSHIP => self.transform_membership(profile, construction),
            INDICATOR_INSTANCE => self.transform_instance(profile, construction),
            FAMILY_SUBSTITUTION => self.transform_family(profile, construction),
            _ => Err(err("unknown indicator construction")),
        }
    }
    fn timeframes(&self, profile: &Value) -> IndicatorResult<Vec<Value>> {
        let (evidence, events, scalar) = bound(profile);
        let active = evidence
            .union(&events)
            .chain(scalar.iter())
            .collect::<BTreeSet<_>>();
        let mut out = vec![];
        for (index, item) in at(profile, "indicators")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
        {
            let instance = string(at(item, "meta").and_then(|meta| at(meta, "instanceId")));
            let id = string(at(item, "meta").and_then(|meta| at(meta, "id")));
            let before = string(at(item, "config").and_then(|config| at(config, "timeframe")))
                .to_uppercase();
            if !active.contains(&instance)
                || self.catalog.entry(&id).is_none()
                || !self.catalog.timeframe_policy.contains(&before)
            {
                continue;
            }
            for after in &self.catalog.timeframe_policy {
                if after != &before {
                    out.push(obj([
                        ("kind", Value::String("graph_bound_timeframe".into())),
                        ("indicatorIndex", Value::from(index)),
                        ("indicatorInstanceId", Value::String(instance.clone())),
                        ("before", Value::String(before.clone())),
                        ("after", Value::String(after.clone())),
                        ("policy", strings(self.catalog.timeframe_policy.clone())),
                    ]));
                }
            }
        }
        Ok(out)
    }
    fn lookbacks(&self, profile: &Value) -> IndicatorResult<Vec<Value>> {
        let (evidence, events, scalar) = bound(profile);
        let mut out = vec![];
        for (index, item) in at(profile, "indicators")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
        {
            let instance = string(at(item, "meta").and_then(|meta| at(meta, "instanceId")));
            let before = cloned(at(item, "config").unwrap_or(&Value::Null), "lookbackBars")
                .unwrap_or_else(|| Value::from(1));
            let before_integer = python_int(&before).unwrap_or(1);
            if !evidence.contains(&instance)
                || events.contains(&instance)
                || scalar.contains(&instance)
                || !LOOKBACKS.contains(&before_integer)
            {
                continue;
            }
            for after in LOOKBACKS {
                let after = Value::from(*after);
                if !python_value_eq(&before, &after) {
                    out.push(obj([
                        ("kind", Value::String("evidence_lookback".into())),
                        ("indicatorIndex", Value::from(index)),
                        ("indicatorInstanceId", Value::String(instance.clone())),
                        ("before", before.clone()),
                        ("after", after),
                        ("allowed", arr(LOOKBACKS.iter().copied().map(Value::from))),
                    ]));
                }
            }
        }
        Ok(out)
    }
    fn periods(&self, profile: &Value) -> IndicatorResult<Vec<Value>> {
        let (evidence, events, scalar) = bound(profile);
        let mut out = vec![];
        for (index, item) in at(profile, "indicators")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
        {
            let meta = at(item, "meta").unwrap_or(&Value::Null);
            let config = at(item, "config").unwrap_or(&Value::Null);
            let instance = string(at(meta, "instanceId"));
            let id = string(at(meta, "id"));
            if !(evidence.contains(&instance)
                || events.contains(&instance)
                || scalar.contains(&instance))
            {
                continue;
            }
            let Some(entry) = self.catalog.entry(&id) else {
                continue;
            };
            let catalog_meta = at(&entry, "meta").unwrap();
            if !meta_catalog_matches(meta, catalog_meta)? {
                continue;
            }
            let values = talib(config);
            for descriptor in at(catalog_meta, "talibMeta")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let name = string(at(descriptor, "name"));
                if !name.to_lowercase().contains("period")
                    || !values.contains_key(&name)
                    || !["integer_slider", "float_slider"]
                        .contains(&string(at(descriptor, "uiType")).as_str())
                {
                    continue;
                }
                let (Some(default), Some(min), Some(max)) = (
                    cloned(descriptor, "default"),
                    cloned(descriptor, "min"),
                    cloned(descriptor, "max"),
                ) else {
                    continue;
                };
                if [default.clone(), min.clone(), max.clone()]
                    .iter()
                    .any(|value| numeric(Some(value)).is_none())
                {
                    continue;
                }
                let nominal = numeric(Some(&default)).unwrap();
                let mut marks = at(descriptor, "marks")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|row| cloned(row, "value"))
                    .filter(|value| numeric(Some(value)).is_some())
                    .collect::<Vec<_>>();
                marks.sort_by(|left, right| {
                    numeric(Some(left))
                        .unwrap()
                        .total_cmp(&numeric(Some(right)).unwrap())
                });
                let fast = marks
                    .iter()
                    .rfind(|value| numeric(Some(value)).unwrap() < nominal)
                    .cloned()
                    .unwrap_or(min.clone());
                let slow = marks
                    .iter()
                    .find(|value| numeric(Some(value)).unwrap() > nominal)
                    .cloned()
                    .unwrap_or(max.clone());
                for (choice, after) in
                    [("fast", fast), ("nominal", default.clone()), ("slow", slow)]
                {
                    if !values
                        .get(&name)
                        .is_some_and(|before| python_value_eq(before, &after))
                    {
                        let mut candidate = config.clone();
                        replace_talib(&mut candidate, &name, after.clone())?;
                        if period_order_valid(&candidate) {
                            out.push(obj([
                                ("kind", Value::String("ta_period".into())),
                                ("indicatorIndex", Value::from(index)),
                                ("indicatorInstanceId", Value::String(instance.clone())),
                                ("indicatorId", Value::String(id.clone())),
                                (
                                    "change",
                                    obj([
                                        ("parameter", Value::String(name.clone())),
                                        ("choice", Value::String(choice.into())),
                                        ("before", values[&name].clone()),
                                        ("after", after),
                                        (
                                            "descriptor",
                                            obj([
                                                ("name", Value::String(name.clone())),
                                                ("default", default.clone()),
                                                ("min", min.clone()),
                                                ("max", max.clone()),
                                            ]),
                                        ),
                                    ]),
                                ),
                            ]));
                        }
                    }
                }
            }
        }
        Ok(out)
    }
    fn ranges(&self, profile: &Value) -> IndicatorResult<Vec<Value>> {
        let (evidence, _, _) = bound(profile);
        let direction = string(at(profile, "directionMode"));
        let sides: Vec<&str> = match direction.as_str() {
            "long" => vec!["buy"],
            "short" => vec!["sell"],
            "both" => vec!["buy", "sell"],
            _ => vec![],
        };
        let mut output = vec![];
        for (index, item) in at(profile, "indicators")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
        {
            let meta = at(item, "meta").unwrap_or(&Value::Null);
            let config = at(item, "config").unwrap_or(&Value::Null);
            let instance = string(at(meta, "instanceId"));
            if !evidence.contains(&instance) {
                continue;
            }
            let id = string(at(meta, "id"));
            let Some(catalog) = self.catalog.entry(&id) else {
                continue;
            };
            let catalog_meta = at(&catalog, "meta").unwrap();
            if !meta_catalog_matches(meta, catalog_meta)?
                || at(catalog_meta, "usesRangeConfiguration").and_then(Value::as_bool) != Some(true)
            {
                continue;
            }
            let Some(range) = at(catalog_meta, "valueRange") else {
                continue;
            };
            let (Some(min), Some(max), Some(step), Some(min_width)) = (
                numeric(at(range, "min")),
                numeric(at(range, "max")),
                numeric(at(range, "step")),
                numeric(at(range, "minRange")),
            ) else {
                continue;
            };
            if !step.is_finite() || step <= 0.0 || min_width <= 0.0 {
                continue;
            }
            for side in &sides {
                let Some(prior) = at(config, "ranges")
                    .and_then(|ranges| at(ranges, side))
                    .and_then(Value::as_array)
                else {
                    continue;
                };
                if prior.len() != 2 {
                    continue;
                }
                let (Some(lower), Some(upper)) = (numeric(prior.first()), numeric(prior.get(1)))
                else {
                    continue;
                };
                for (choice, candidate) in [
                    ("shift_lower", vec![lower - step, upper - step]),
                    ("shift_higher", vec![lower + step, upper + step]),
                    ("widen", vec![lower - step, upper + step]),
                    ("narrow", vec![lower + step, upper - step]),
                ] {
                    if candidate[0] < min
                        || candidate[1] > max
                        || candidate[1] - candidate[0] < min_width
                        || candidate == vec![lower, upper]
                    {
                        continue;
                    }
                    output.push(obj([
                        ("kind", Value::String("semantic_score_range".into())),
                        ("indicatorIndex", Value::from(index)),
                        ("indicatorInstanceId", Value::String(instance.clone())),
                        ("indicatorId", Value::String(id.clone())),
                        (
                            "change",
                            obj([
                                ("side", Value::String((*side).into())),
                                ("choice", Value::String(choice.into())),
                                ("before", arr(prior.clone())),
                                ("after", arr(candidate.into_iter().map(Value::from))),
                                (
                                    "catalogValueRange",
                                    obj([
                                        ("min", Value::from(min)),
                                        ("max", Value::from(max)),
                                        ("step", Value::from(step)),
                                        ("minRange", Value::from(min_width)),
                                    ]),
                                ),
                            ]),
                        ),
                    ]));
                }
            }
        }
        Ok(output)
    }
    fn weights(&self, profile: &Value) -> IndicatorResult<Vec<Value>> {
        let (evidence, events, scalar) = bound(profile);
        let reachable = reachable_groups(profile);
        let mut output = vec![];
        for (index, item) in at(profile, "indicators")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
        {
            let instance = string(at(item, "meta").and_then(|meta| at(meta, "instanceId")));
            let Some(weight) = numeric(at(item, "config").and_then(|config| at(config, "weight")))
            else {
                continue;
            };
            let used_in_group = at(profile, "graph")
                .and_then(|graph| at(graph, "evidenceGroups"))
                .and_then(Value::as_array)
                .is_some_and(|groups| {
                    groups.iter().any(|group| {
                        reachable.contains(&string(at(group, "id")))
                            && at(group, "indicatorInstanceIds")
                                .and_then(Value::as_array)
                                .is_some_and(|members| {
                                    members.len() > 1
                                        && members
                                            .iter()
                                            .any(|member| string(Some(member)) == instance)
                                })
                    })
                });
            if !evidence.contains(&instance)
                || events.contains(&instance)
                || scalar.contains(&instance)
                || !is_active_closed(item)
                || !weight.is_finite()
                || !(0.0..=10.0).contains(&weight)
                || !used_in_group
            {
                continue;
            }
            for multiplier in [0.5, 0.75, 1.25, 1.5] {
                let after = (weight * multiplier * 4.0).round_ties_even() / 4.0;
                if after > 0.0 && after <= 10.0 && after != weight {
                    output.push(obj([
                        ("kind", Value::String("evidence_contribution_weight".into())),
                        ("indicatorIndex", Value::from(index)),
                        ("indicatorInstanceId", Value::String(instance.clone())),
                        ("before", Value::from(weight)),
                        ("after", Value::from(after)),
                        ("multiplier", Value::from(multiplier)),
                        ("quantization", Value::from(0.25)),
                        (
                            "bounds",
                            obj([
                                ("exclusiveMinimum", Value::from(0.0)),
                                ("maximum", Value::from(10.0)),
                            ]),
                        ),
                    ]));
                }
            }
        }
        Ok(output)
    }
    fn membership(&self, profile: &Value) -> IndicatorResult<Vec<Value>> {
        let (evidence, events, scalar) = bound(profile);
        let reachable = reachable_groups(profile);
        // Python builds this lookup before iterating candidates, so duplicate
        // keys (already rejected by the profile invariants) would resolve in
        // the same last-value-wins way even before the invariant backstop.
        let candidates = at(profile, "indicators")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|item| {
                let instance = string(at(item, "meta").and_then(|meta| at(meta, "instanceId")));
                (!instance.is_empty()).then_some((instance, item))
            })
            .collect::<BTreeMap<_, _>>();
        let mut output = vec![];
        for (group_index, group) in at(profile, "graph")
            .and_then(|graph| at(graph, "evidenceGroups"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
        {
            let group_id = string(at(group, "id"));
            let members = at(group, "indicatorInstanceIds")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .map(|item| string(Some(item)))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if !reachable.contains(&group_id)
                || members.is_empty()
                || members.iter().collect::<BTreeSet<_>>().len() != members.len()
                || members.iter().any(|member| {
                    !indic(profile, member).is_some_and(|(_, item)| {
                        membership_member_eligible(item, member, &evidence, &events, &scalar)
                    })
                })
            {
                continue;
            }
            let member_contracts = members
                .iter()
                .map(|member| {
                    let (_, item) = indic(profile, member)
                        .expect("membership eligibility resolved this member");
                    let meta = at(item, "meta").expect("membership eligibility resolved metadata");
                    binding_contract(meta, true, false, false)
                        .expect("membership eligibility resolved contract")
                })
                .map(|contract| sha(&contract))
                .collect::<IndicatorResult<BTreeSet<_>>>()?;
            if member_contracts.len() != 1 {
                continue;
            }
            let member_contract = member_contracts
                .into_iter()
                .next()
                .expect("one membership contract");
            for member in &members {
                if members.len() > 1
                    && !events.contains(member)
                    && !scalar.contains(member)
                    && membership_owner_matches(profile, &group_id, member)
                {
                    output.push(obj([
                        ("kind", Value::String("remove_evidence_member".into())),
                        ("groupIndex", Value::from(group_index)),
                        ("groupId", Value::String(group_id.clone())),
                        ("indicatorInstanceId", Value::String(member.clone())),
                        ("beforeMembers", strings(members.clone())),
                    ]));
                }
            }
            if members.len() >= MAX_GROUP_MEMBERS {
                continue;
            }
            for (instance, candidate) in candidates.iter() {
                let Some(meta) = at(candidate, "meta") else {
                    continue;
                };
                if members.contains(instance)
                    || events.contains(instance)
                    || scalar.contains(instance)
                    || !is_active_closed(candidate)
                    || !membership_owner_matches(profile, &group_id, instance)
                {
                    continue;
                }
                let Some(candidate_contract) = binding_contract(meta, true, false, false) else {
                    continue;
                };
                if sha(&candidate_contract)? != member_contract {
                    continue;
                }
                output.push(obj([
                    ("kind", Value::String("add_evidence_member".into())),
                    ("groupIndex", Value::from(group_index)),
                    ("groupId", Value::String(group_id.clone())),
                    ("indicatorInstanceId", Value::String(instance.clone())),
                    ("beforeMembers", strings(members.clone())),
                ]));
            }
        }
        Ok(output)
    }
    fn instances(&self, profile: &Value) -> IndicatorResult<Vec<Value>> {
        let (evidence, events, scalar) = bound(profile);
        let direction = string(at(profile, "directionMode"));
        let reachable = reachable_groups(profile);
        let existing = at(profile, "indicators")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .map(|item| string(at(item, "meta").and_then(|meta| at(meta, "instanceId"))))
            .collect::<BTreeSet<_>>();
        let by_instance = at(profile, "indicators")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|item| {
                let instance = string(at(item, "meta").and_then(|meta| at(meta, "instanceId")));
                (!instance.is_empty()).then_some((instance, item))
            })
            .collect::<BTreeMap<_, _>>();
        // The corpus's v2 module profiles are directional.  A composite
        // profile without a complete ownership manifest has no safe
        // instance-insertion surface, matching Python's fail-closed count.
        let may_insert = match direction.as_str() {
            "long" | "short" => evidence.len() < MAX_GROUP_MEMBERS,
            _ => false,
        };
        let mut output = vec![];
        for (group_index, group) in at(profile, "graph")
            .and_then(|graph| at(graph, "evidenceGroups"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
        {
            let group_id = string(at(group, "id"));
            let members = at(group, "indicatorInstanceIds")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .map(|item| string(Some(item)))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if !may_insert
                || !reachable.contains(&group_id)
                || members.is_empty()
                || members.len() >= MAX_GROUP_MEMBERS
            {
                continue;
            }
            let mut member_contracts = BTreeSet::new();
            let mut members_eligible = true;
            for member in &members {
                let Some(item) = by_instance.get(member) else {
                    members_eligible = false;
                    break;
                };
                let Some(meta) = at(item, "meta") else {
                    members_eligible = false;
                    break;
                };
                if !evidence.contains(member)
                    || events.contains(member)
                    || scalar.contains(member)
                    || !is_active_closed(item)
                {
                    members_eligible = false;
                    break;
                }
                let Some(contract) = binding_contract(meta, true, false, false) else {
                    members_eligible = false;
                    break;
                };
                member_contracts.insert(sha(&contract)?);
            }
            if !members_eligible || member_contracts.len() != 1 {
                continue;
            }
            let member_contract = member_contracts.into_iter().next().unwrap();
            let seen_ids = members
                .iter()
                .filter_map(|member| {
                    indic(profile, member)
                        .map(|(_, item)| string(at(item, "meta").and_then(|meta| at(meta, "id"))))
                })
                .collect::<BTreeSet<_>>();
            for (id, entry) in &self.catalog.entries {
                let meta = at(entry, "meta").unwrap();
                let config = at(entry, "config").unwrap();
                if seen_ids.contains(id)
                    || binding_contract(meta, true, false, false)
                        .map(|contract| sha(&contract))
                        .transpose()?
                        .as_deref()
                        != Some(&member_contract)
                    || at(config, "isActive").and_then(Value::as_bool) != Some(true)
                {
                    continue;
                }
                let mut stem = id
                    .chars()
                    .map(|char| {
                        if char.is_ascii_alphanumeric() {
                            char.to_ascii_lowercase()
                        } else {
                            '_'
                        }
                    })
                    .collect::<String>()
                    .trim_matches('_')
                    .to_owned();
                stem.truncate(58);
                if stem.is_empty() {
                    stem = "indicator".into()
                }
                let mut ordinal = 1;
                let instance = loop {
                    let value = format!("fz_{stem}_{ordinal}");
                    if !existing.contains(&value) {
                        break value;
                    }
                    ordinal += 1
                };
                output.push(obj([
                    (
                        "kind",
                        Value::String("insert_fuzzy_indicator_instance".into()),
                    ),
                    ("groupIndex", Value::from(group_index)),
                    ("groupId", Value::String(group_id.clone())),
                    ("indicatorId", Value::String(id.clone())),
                    ("indicatorInstanceId", Value::String(instance)),
                    ("beforeMembers", strings(members.clone())),
                    (
                        "softRolePrior",
                        obj([
                            (
                                "strategyRole",
                                cloned(meta, "strategyRole").unwrap_or(Value::Null),
                            ),
                            (
                                "signalRole",
                                cloned(meta, "signalRole").unwrap_or(Value::Null),
                            ),
                        ]),
                    ),
                ]));
            }
        }
        for instance in evidence {
            if events.contains(&instance) || scalar.contains(&instance) {
                continue;
            }
            let Some((_, item)) = indic(profile, &instance) else {
                continue;
            };
            if binding_contract(at(item, "meta").unwrap_or(&Value::Null), true, false, false)
                .is_none()
            {
                continue;
            }
            let groups = at(profile, "graph")
                .and_then(|graph| at(graph, "evidenceGroups"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .enumerate()
                .filter(|(_, group)| {
                    at(group, "indicatorInstanceIds")
                        .and_then(Value::as_array)
                        .is_some_and(|members| {
                            members
                                .iter()
                                .any(|member| string(Some(member)) == instance)
                        })
                })
                .collect::<Vec<_>>();
            if groups.is_empty()
                || groups.iter().any(|(_, group)| {
                    at(group, "indicatorInstanceIds")
                        .and_then(Value::as_array)
                        .is_some_and(|members| members.len() <= 1)
                })
            {
                continue;
            }
            let affected = groups
                .into_iter()
                .map(|(index, group)| {
                    obj([
                        ("groupIndex", Value::from(index)),
                        ("groupId", Value::String(string(at(group, "id")))),
                        (
                            "beforeMembers",
                            cloned(group, "indicatorInstanceIds").unwrap(),
                        ),
                    ])
                })
                .collect::<Vec<_>>();
            output.push(obj([
                (
                    "kind",
                    Value::String("remove_fuzzy_indicator_instance".into()),
                ),
                ("indicatorInstanceId", Value::String(instance)),
                (
                    "indicatorId",
                    Value::String(string(at(item, "meta").and_then(|meta| at(meta, "id")))),
                ),
                ("affectedGroups", arr(affected)),
            ]));
        }
        Ok(output)
    }
    fn family(&self, profile: &Value) -> IndicatorResult<Vec<Value>> {
        let (evidence, events, scalar) = bound(profile);
        let mut output = vec![];
        for (index, item) in at(profile, "indicators")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
        {
            let meta = at(item, "meta").unwrap_or(&Value::Null);
            let instance = string(at(meta, "instanceId"));
            let source_id = string(at(meta, "id"));
            if !(evidence.contains(&instance)
                || events.contains(&instance)
                || scalar.contains(&instance))
            {
                continue;
            }
            let Some(source) = self.catalog.entry(&source_id) else {
                continue;
            };
            let source_meta = at(&source, "meta").unwrap();
            if !compatibility_missing(source_meta).is_empty()
                || !meta_catalog_matches(meta, source_meta)?
            {
                continue;
            }
            let shape = (
                evidence.contains(&instance),
                events.contains(&instance),
                scalar.contains(&instance),
            );
            let Some(contract) = binding_contract(source_meta, shape.0, shape.1, shape.2) else {
                continue;
            };
            for (replacement_id, replacement) in &self.catalog.entries {
                if replacement_id == &source_id {
                    continue;
                }
                let replacement_meta = at(replacement, "meta").unwrap();
                if !compatibility_missing(replacement_meta).is_empty()
                    || binding_contract(replacement_meta, shape.0, shape.1, shape.2)
                        != Some(contract.clone())
                {
                    continue;
                }
                output.push(obj([
                    ("kind", Value::String("family_substitution".into())),
                    ("indicatorIndex", Value::from(index)),
                    ("indicatorInstanceId", Value::String(instance.clone())),
                    ("beforeIndicatorId", Value::String(source_id.clone())),
                    ("afterIndicatorId", Value::String(replacement_id.clone())),
                    ("capabilityContract", contract.clone()),
                    ("eventBound", Value::Bool(shape.1)),
                    ("evidenceBound", Value::Bool(shape.0)),
                    ("scalarBound", Value::Bool(shape.2)),
                    (
                        "softRolePrior",
                        obj([
                            (
                                "beforeStrategyRole",
                                cloned(source_meta, "strategyRole").unwrap_or(Value::Null),
                            ),
                            (
                                "afterStrategyRole",
                                cloned(replacement_meta, "strategyRole").unwrap_or(Value::Null),
                            ),
                            (
                                "beforeSignalRole",
                                cloned(source_meta, "signalRole").unwrap_or(Value::Null),
                            ),
                            (
                                "afterSignalRole",
                                cloned(replacement_meta, "signalRole").unwrap_or(Value::Null),
                            ),
                        ]),
                    ),
                ]));
            }
        }
        Ok(output)
    }
    fn transform_simple(
        &self,
        profile: &Value,
        construction: &Value,
    ) -> IndicatorResult<(Value, Vec<Value>)> {
        let mut child = profile.clone();
        let index = int(at(construction, "indicatorIndex"))
            .ok_or_else(|| err("indicator index missing"))? as usize;
        let instance = string(at(construction, "indicatorInstanceId"));
        let (key, operation) = match self.id.as_str() {
            GRAPH_BOUND_TIMEFRAME => ("timeframe", "set_graph_bound_timeframe"),
            EVIDENCE_LOOKBACK => ("lookbackBars", "set_evidence_lookback"),
            _ => ("weight", "set_evidence_contribution_weight"),
        };
        let before = cloned(construction, "before").ok_or_else(|| err("before missing"))?;
        let after = cloned(construction, "after").ok_or_else(|| err("after missing"))?;
        let item = at_mut(&mut child, "indicators")
            .and_then(Value::as_array_mut)
            .and_then(|items| items.get_mut(index))
            .ok_or_else(|| err("indicator index stale"))?;
        let current = at(item, "config").and_then(|config| at(config, key));
        let parent_matches = match self.id.as_str() {
            GRAPH_BOUND_TIMEFRAME => current
                .map(|current| string(Some(current)))
                .is_some_and(|current| current.to_uppercase() == string(Some(&before))),
            _ => current.is_some_and(|current| python_value_eq(current, &before)),
        };
        if string(at(item, "meta").and_then(|meta| at(meta, "instanceId"))) != instance
            || !parent_matches
        {
            return Err(err("indicator parent drift"));
        }
        at_mut(item, "config")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert(key.into(), after.clone());
        Ok((
            child,
            vec![obj([
                ("operation", Value::String(operation.into())),
                ("indicatorInstanceId", Value::String(instance)),
                ("before", before),
                ("after", after),
            ])],
        ))
    }
    fn transform_period(
        &self,
        profile: &Value,
        construction: &Value,
    ) -> IndicatorResult<(Value, Vec<Value>)> {
        let mut child = profile.clone();
        let index = int(at(construction, "indicatorIndex"))
            .ok_or_else(|| err("period index missing"))? as usize;
        let instance = string(at(construction, "indicatorInstanceId"));
        let change = at(construction, "change").ok_or_else(|| err("period change missing"))?;
        let name = string(at(change, "parameter"));
        let before = cloned(change, "before").unwrap();
        let after = cloned(change, "after").unwrap();
        let item = at_mut(&mut child, "indicators")
            .and_then(Value::as_array_mut)
            .and_then(|items| items.get_mut(index))
            .ok_or_else(|| err("period index stale"))?;
        if string(at(item, "meta").and_then(|meta| at(meta, "instanceId"))) != instance
            || !talib(at(item, "config").unwrap())
                .get(&name)
                .is_some_and(|current| python_value_eq(current, &before))
        {
            return Err(err("period parent drift"));
        }
        replace_talib(at_mut(item, "config").unwrap(), &name, after.clone())?;
        if !period_order_valid(at(item, "config").unwrap()) {
            return Err(err("period order invalid"));
        }
        Ok((
            child,
            vec![obj([
                ("operation", Value::String("set_ta_period".into())),
                ("indicatorInstanceId", Value::String(instance)),
                ("parameter", Value::String(name)),
                ("choice", cloned(change, "choice").unwrap()),
                ("before", before),
                ("after", after),
            ])],
        ))
    }
    fn transform_range(
        &self,
        profile: &Value,
        construction: &Value,
    ) -> IndicatorResult<(Value, Vec<Value>)> {
        let mut child = profile.clone();
        let index = int(at(construction, "indicatorIndex"))
            .ok_or_else(|| err("range index missing"))? as usize;
        let instance = string(at(construction, "indicatorInstanceId"));
        let change = at(construction, "change").ok_or_else(|| err("range change missing"))?;
        let side = string(at(change, "side"));
        let before = cloned(change, "before").unwrap();
        let after = cloned(change, "after").unwrap();
        let item = at_mut(&mut child, "indicators")
            .and_then(Value::as_array_mut)
            .and_then(|items| items.get_mut(index))
            .ok_or_else(|| err("range index stale"))?;
        if string(at(item, "meta").and_then(|meta| at(meta, "instanceId"))) != instance
            || !at(item, "config")
                .and_then(|config| at(config, "ranges"))
                .and_then(|ranges| at(ranges, &side))
                .is_some_and(|current| python_value_eq(current, &before))
        {
            return Err(err("range parent drift"));
        }
        at_mut(at_mut(item, "config").unwrap(), "ranges")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| err("ranges missing"))?
            .insert(side.clone(), after.clone());
        Ok((
            child,
            vec![obj([
                (
                    "operation",
                    Value::String("set_semantic_score_range".into()),
                ),
                ("indicatorInstanceId", Value::String(instance)),
                ("side", Value::String(side)),
                ("choice", cloned(change, "choice").unwrap()),
                ("before", before),
                ("after", after),
                (
                    "catalogValueRange",
                    cloned(change, "catalogValueRange").unwrap(),
                ),
            ])],
        ))
    }
    fn transform_membership(
        &self,
        profile: &Value,
        construction: &Value,
    ) -> IndicatorResult<(Value, Vec<Value>)> {
        let mut child = profile.clone();
        let index =
            int(at(construction, "groupIndex")).ok_or_else(|| err("group index missing"))? as usize;
        let group_id = string(at(construction, "groupId"));
        let instance = string(at(construction, "indicatorInstanceId"));
        let before = at(construction, "beforeMembers")
            .and_then(Value::as_array)
            .ok_or_else(|| err("members missing"))?
            .iter()
            .map(|item| string(Some(item)))
            .collect::<Vec<_>>();
        let group = at_mut(&mut child, "graph")
            .and_then(|graph| at_mut(graph, "evidenceGroups"))
            .and_then(Value::as_array_mut)
            .and_then(|groups| groups.get_mut(index))
            .ok_or_else(|| err("group stale"))?;
        let actual = at(group, "indicatorInstanceIds")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .map(|item| string(Some(item)))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if string(at(group, "id")) != group_id || actual != before {
            return Err(err("membership parent drift"));
        }
        let mut after = before.clone();
        let operation = if string(at(construction, "kind")) == "add_evidence_member" {
            if after.contains(&instance) || after.len() >= MAX_GROUP_MEMBERS {
                return Err(err("membership add invalid"));
            }
            after.push(instance.clone());
            "add_evidence_group_member"
        } else {
            if !after.contains(&instance) || after.len() <= 1 {
                return Err(err("membership remove invalid"));
            }
            after.retain(|item| item != &instance);
            "remove_evidence_group_member"
        };
        after.sort();
        group
            .as_object_mut()
            .unwrap()
            .insert("indicatorInstanceIds".into(), strings(after.clone()));
        Ok((
            child,
            vec![obj([
                ("operation", Value::String(operation.into())),
                ("groupId", Value::String(group_id)),
                ("indicatorInstanceId", Value::String(instance)),
                ("beforeMembers", strings(before)),
                ("afterMembers", strings(after)),
            ])],
        ))
    }
    fn transform_instance(
        &self,
        profile: &Value,
        construction: &Value,
    ) -> IndicatorResult<(Value, Vec<Value>)> {
        let mut child = profile.clone();
        let kind = string(at(construction, "kind"));
        if kind == "insert_fuzzy_indicator_instance" {
            let index = int(at(construction, "groupIndex"))
                .ok_or_else(|| err("group index missing"))? as usize;
            let group_id = string(at(construction, "groupId"));
            let instance = string(at(construction, "indicatorInstanceId"));
            let indicator_id = string(at(construction, "indicatorId"));
            let before = at(construction, "beforeMembers")
                .and_then(Value::as_array)
                .unwrap()
                .iter()
                .map(|item| string(Some(item)))
                .collect::<Vec<_>>();
            let group_valid = at(&child, "graph")
                .and_then(|graph| at(graph, "evidenceGroups"))
                .and_then(Value::as_array)
                .and_then(|groups| groups.get(index))
                .is_some_and(|group| {
                    string(at(group, "id")) == group_id
                        && at(group, "indicatorInstanceIds")
                            .and_then(Value::as_array)
                            .is_some_and(|items| {
                                items
                                    .iter()
                                    .map(|item| string(Some(item)))
                                    .collect::<Vec<_>>()
                                    == before
                            })
                });
            if !group_valid || indic(&child, &instance).is_some() {
                return Err(err("instance insertion parent drift"));
            }
            let entry = self
                .catalog
                .entry(&indicator_id)
                .ok_or_else(|| err("catalog entry missing"))?;
            let mut created = obj([
                ("meta", cloned(&entry, "meta").unwrap()),
                ("config", cloned(&entry, "config").unwrap()),
            ]);
            at_mut(&mut created, "meta")
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert("instanceId".into(), Value::String(instance.clone()));
            at_mut(&mut created, "config")
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert("useFormingBar".into(), Value::Bool(false));
            at_mut(&mut child, "indicators")
                .and_then(Value::as_array_mut)
                .ok_or_else(|| err("indicators missing"))?
                .push(created);
            let mut after = before.clone();
            after.push(instance.clone());
            after.sort();
            at_mut(&mut child, "graph")
                .and_then(|graph| at_mut(graph, "evidenceGroups"))
                .and_then(Value::as_array_mut)
                .unwrap()[index]
                .as_object_mut()
                .unwrap()
                .insert("indicatorInstanceIds".into(), strings(after.clone()));
            return Ok((
                child,
                vec![obj([
                    (
                        "operation",
                        Value::String("insert_fuzzy_indicator_instance".into()),
                    ),
                    ("indicatorInstanceId", Value::String(instance)),
                    ("indicatorId", Value::String(indicator_id)),
                    ("groupId", Value::String(group_id)),
                    ("beforeMembers", strings(before)),
                    ("afterMembers", strings(after)),
                    (
                        "softRolePrior",
                        cloned(construction, "softRolePrior").unwrap(),
                    ),
                ])],
            ));
        }
        if kind == "remove_fuzzy_indicator_instance" {
            let instance = string(at(construction, "indicatorInstanceId"));
            let indicator_id = string(at(construction, "indicatorId"));
            let position = indic(&child, &instance)
                .filter(|(_, item)| {
                    string(at(item, "meta").and_then(|meta| at(meta, "id"))) == indicator_id
                })
                .map(|(index, _)| index)
                .ok_or_else(|| err("instance removal parent drift"))?;
            let mut traces = vec![];
            for affected in at(construction, "affectedGroups")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let index = int(at(affected, "groupIndex"))
                    .ok_or_else(|| err("affected group index"))?
                    as usize;
                let before = at(affected, "beforeMembers")
                    .and_then(Value::as_array)
                    .unwrap()
                    .iter()
                    .map(|item| string(Some(item)))
                    .collect::<Vec<_>>();
                let group = at_mut(&mut child, "graph")
                    .and_then(|graph| at_mut(graph, "evidenceGroups"))
                    .and_then(Value::as_array_mut)
                    .and_then(|groups| groups.get_mut(index))
                    .ok_or_else(|| err("affected group stale"))?;
                if string(at(group, "id")) != string(at(affected, "groupId"))
                    || at(group, "indicatorInstanceIds")
                        .and_then(Value::as_array)
                        .unwrap()
                        .iter()
                        .map(|item| string(Some(item)))
                        .collect::<Vec<_>>()
                        != before
                {
                    return Err(err("affected group parent drift"));
                }
                let after = before
                    .iter()
                    .filter(|item| *item != &instance)
                    .cloned()
                    .collect::<Vec<_>>();
                group
                    .as_object_mut()
                    .unwrap()
                    .insert("indicatorInstanceIds".into(), strings(after.clone()));
                traces.push(obj([
                    ("groupId", cloned(affected, "groupId").unwrap()),
                    ("beforeMembers", strings(before)),
                    ("afterMembers", strings(after)),
                ]));
            }
            at_mut(&mut child, "indicators")
                .and_then(Value::as_array_mut)
                .unwrap()
                .remove(position);
            return Ok((
                child,
                vec![obj([
                    (
                        "operation",
                        Value::String("remove_fuzzy_indicator_instance".into()),
                    ),
                    ("indicatorInstanceId", Value::String(instance)),
                    ("indicatorId", Value::String(indicator_id)),
                    ("affectedGroups", arr(traces)),
                ])],
            ));
        }
        Err(err("unknown instance construction"))
    }
    fn transform_family(
        &self,
        profile: &Value,
        construction: &Value,
    ) -> IndicatorResult<(Value, Vec<Value>)> {
        let mut child = profile.clone();
        let index = int(at(construction, "indicatorIndex"))
            .ok_or_else(|| err("family index missing"))? as usize;
        let instance = string(at(construction, "indicatorInstanceId"));
        let before_id = string(at(construction, "beforeIndicatorId"));
        let after_id = string(at(construction, "afterIndicatorId"));
        let entry = self
            .catalog
            .entry(&after_id)
            .ok_or_else(|| err("replacement catalog missing"))?;
        let item = at_mut(&mut child, "indicators")
            .and_then(Value::as_array_mut)
            .and_then(|items| items.get_mut(index))
            .ok_or_else(|| err("family index stale"))?;
        if string(at(item, "meta").and_then(|meta| at(meta, "instanceId"))) != instance
            || string(at(item, "meta").and_then(|meta| at(meta, "id"))) != before_id
        {
            return Err(err("family parent drift"));
        }
        let old_config = cloned(item, "config").unwrap();
        let old_meta = cloned(item, "meta").unwrap();
        let mut replacement_config =
            cloned(&entry, "config").ok_or_else(|| err("replacement config missing"))?;
        for key in [
            "isActive",
            "useFormingBar",
            "timeframe",
            "lookbackBars",
            "weight",
        ] {
            if let Some(value) = cloned(&old_config, key) {
                replacement_config
                    .as_object_mut()
                    .unwrap()
                    .insert(key.into(), value);
            }
        }
        // Keep thresholds only when their numeric interpretation has not
        // changed.  This is intentionally after the generic parent-owned
        // config transfer above: ranges are a separate semantic axis.
        if range_signature(at(&entry, "meta").unwrap_or(&Value::Null)) == range_signature(&old_meta)
        {
            if let Some(ranges) = cloned(&old_config, "ranges") {
                replacement_config
                    .as_object_mut()
                    .unwrap()
                    .insert("ranges".into(), ranges);
            }
        }
        let mut replacement_meta = cloned(&entry, "meta").unwrap();
        replacement_meta
            .as_object_mut()
            .unwrap()
            .insert("instanceId".into(), Value::String(instance.clone()));
        item.as_object_mut()
            .unwrap()
            .insert("meta".into(), replacement_meta);
        item.as_object_mut()
            .unwrap()
            .insert("config".into(), replacement_config);
        Ok((
            child,
            vec![obj([
                (
                    "operation",
                    Value::String("substitute_indicator_family".into()),
                ),
                ("indicatorInstanceId", Value::String(instance)),
                ("beforeIndicatorId", Value::String(before_id)),
                ("afterIndicatorId", Value::String(after_id)),
                (
                    "capabilityContract",
                    cloned(construction, "capabilityContract").unwrap(),
                ),
                (
                    "softRolePrior",
                    cloned(construction, "softRolePrior").unwrap(),
                ),
            ])],
        ))
    }
}

#[derive(Clone, Debug)]
pub struct IndicatorLearningRegistry {
    catalog: Arc<IndicatorCatalog>,
    operators: BTreeMap<String, IndicatorOperator>,
    policy: Value,
}
impl IndicatorLearningRegistry {
    pub fn new(catalog: IndicatorCatalog) -> IndicatorResult<Self> {
        let catalog = Arc::new(catalog);
        let mut operators = BTreeMap::new();
        for id in [
            GRAPH_BOUND_TIMEFRAME,
            EVIDENCE_LOOKBACK,
            TA_PERIOD,
            SEMANTIC_RANGE,
            EVIDENCE_WEIGHT,
            EVIDENCE_MEMBERSHIP,
            INDICATOR_INSTANCE,
            FAMILY_SUBSTITUTION,
        ] {
            operators.insert(id.to_owned(), IndicatorOperator::new(id, catalog.clone())?);
        }
        let mut policy = obj([
            (
                "schemaVersion",
                Value::String("temporal_indicator_learning_policy_v1".into()),
            ),
            (
                "learningVersion",
                Value::String(INDICATOR_LEARNING_VERSION.into()),
            ),
            (
                "catalogSha256",
                Value::String(catalog.catalog_sha256.clone()),
            ),
            ("timeframePolicy", strings(catalog.timeframe_policy.clone())),
            (
                "evidenceLookbackChoices",
                arr(LOOKBACKS.iter().copied().map(Value::from)),
            ),
            (
                "maxBoundFuzzyInstancesPerDirection",
                Value::from(MAX_GROUP_MEMBERS),
            ),
            ("maxEvidenceGroupMembers", Value::from(MAX_GROUP_MEMBERS)),
            ("operatorIds", strings(operators.keys().cloned())),
        ]);
        let policy_sha = sha(&policy)?;
        policy
            .as_object_mut()
            .unwrap()
            .insert("policySha256".into(), Value::String(policy_sha));
        Ok(Self {
            catalog,
            operators,
            policy,
        })
    }
    pub fn catalog(&self) -> &IndicatorCatalog {
        self.catalog.as_ref()
    }
    pub fn policy(&self) -> &Value {
        &self.policy
    }
    pub fn operator_ids(&self) -> Vec<String> {
        self.operators.keys().cloned().collect()
    }
    pub fn get(&self, id: &str) -> IndicatorResult<&IndicatorOperator> {
        self.operators
            .get(id)
            .ok_or_else(|| err("unknown indicator operator"))
    }
    pub fn enumerate_plans(&self, profile: &Value) -> IndicatorResult<Vec<Value>> {
        let mut output = vec![];
        for operator in self.operators.values() {
            output.extend(operator.enumerate_plans(profile)?);
        }
        output.sort_by_key(|plan| string(at(plan, "planSha256")));
        Ok(output)
    }
    pub fn deferred_dispositions(&self, profile: &Value) -> IndicatorResult<Vec<Value>> {
        self.catalog.deferred_dispositions(profile)
    }
}

fn guard_paths(
    guard: &Value,
    groups: &BTreeMap<String, BTreeSet<String>>,
    events: &BTreeMap<String, String>,
    known: &BTreeSet<String>,
    negated: bool,
) -> IndicatorResult<Vec<BTreeSet<String>>> {
    let kind = string(at(guard, "kind"));
    if kind == "not" {
        return guard_paths(
            at(guard, "guard").ok_or_else(|| err("not decision guard is not closed"))?,
            groups,
            events,
            known,
            !negated,
        );
    }
    if kind == "all" || kind == "any" {
        let children = at(guard, "guards")
            .and_then(Value::as_array)
            .ok_or_else(|| err("compound decision guard is not closed"))?;
        let choices = children
            .iter()
            .map(|child| guard_paths(child, groups, events, known, negated))
            .collect::<IndicatorResult<Vec<_>>>()?;
        let effective = if negated {
            if kind == "all" { "any" } else { "all" }
        } else {
            &kind
        };
        if effective == "any" {
            let mut result: Vec<BTreeSet<String>> = choices.into_iter().flatten().collect();
            if result.is_empty() {
                result.push(BTreeSet::new());
            }
            result.sort();
            result.dedup();
            return Ok(result);
        }
        let mut result = vec![BTreeSet::new()];
        for alternatives in choices {
            let mut next: Vec<BTreeSet<String>> = vec![];
            for left in &result {
                for right in &alternatives {
                    next.push(left.union(right).cloned().collect());
                }
            }
            next.sort();
            next.dedup();
            result = next;
        }
        return Ok(result);
    }
    if kind == "predicate_edge" || kind == "consecutive_true" {
        return guard_paths(
            at(guard, "predicate").ok_or_else(|| err("predicate edge is not closed"))?,
            groups,
            events,
            known,
            negated,
        );
    }
    let mut ids = BTreeSet::new();
    if at(guard, "groupId").is_some() {
        let id = string(at(guard, "groupId"));
        ids.extend(
            groups
                .get(&id)
                .ok_or_else(|| err("unknown evidence group"))?
                .clone(),
        );
    }
    if at(guard, "eventId").is_some() {
        let id = string(at(guard, "eventId"));
        ids.insert(
            events
                .get(&id)
                .ok_or_else(|| err("unknown event binding"))?
                .clone(),
        );
    }
    if !ids.is_subset(known) {
        return Err(err("decision guard indicator closure is incomplete"));
    }
    Ok(vec![ids])
}

/// Exact bounded fixed-point report for the three distinct decision-indicator cap.
pub fn entry_route_decision_indicator_report(profile: &Value) -> IndicatorResult<Value> {
    let graph = at(profile, "graph").ok_or_else(|| err("entry route cap requires graph"))?;
    let transitions = at(graph, "transitions")
        .and_then(Value::as_array)
        .ok_or_else(|| err("entry route transitions malformed"))?;
    let entries = transitions
        .iter()
        .enumerate()
        .filter(|(_, edge)| {
            string(at(edge, "eventClass")) == "decision"
                && at(edge, "actions")
                    .and_then(Value::as_array)
                    .is_some_and(|actions| {
                        actions
                            .iter()
                            .any(|action| string(at(action, "kind")) == "enter_next_open")
                    })
        })
        .map(|(index, _)| index)
        .collect::<BTreeSet<_>>();
    if entries.is_empty() {
        return Ok(obj([
            (
                "schemaVersion",
                Value::String("temporal_entry_route_decision_indicator_report_v1".into()),
            ),
            (
                "policyVersion",
                Value::String(ENTRY_ROUTE_DECISION_INDICATOR_POLICY_VERSION.into()),
            ),
            (
                "maxDistinctDecisionIndicatorInstances",
                Value::from(ENTRY_ROUTE_DECISION_INDICATOR_CAP),
            ),
            ("entryTransitions", arr([])),
            ("reachableStateIndicatorSetCount", Value::from(0)),
            (
                "observedMaximumDistinctDecisionIndicatorInstances",
                Value::from(0),
            ),
        ]));
    }
    let known = at(profile, "indicators")
        .and_then(Value::as_array)
        .ok_or_else(|| err("entry route resources missing"))?
        .iter()
        .map(|item| string(at(item, "meta").and_then(|meta| at(meta, "instanceId"))))
        .collect::<BTreeSet<_>>();
    let mut groups = BTreeMap::new();
    for group in at(graph, "evidenceGroups")
        .and_then(Value::as_array)
        .ok_or_else(|| err("entry route resources missing"))?
    {
        let id = string(at(group, "id"));
        let members = at(group, "indicatorInstanceIds")
            .and_then(Value::as_array)
            .ok_or_else(|| err("evidence group malformed"))?
            .iter()
            .map(|item| string(Some(item)))
            .collect::<BTreeSet<_>>();
        if id.is_empty() || members.is_empty() || groups.insert(id, members).is_some() {
            return Err(err("evidence group malformed"));
        }
    }
    let mut events = BTreeMap::new();
    for event in at(graph, "eventBindings")
        .and_then(Value::as_array)
        .ok_or_else(|| err("entry route resources missing"))?
    {
        let id = string(at(event, "id"));
        let member = string(at(event, "indicatorInstanceId"));
        if id.is_empty() || member.is_empty() || events.insert(id, member).is_some() {
            return Err(err("event binding malformed"));
        }
    }
    let initial = string(at(graph, "initialStateId"));
    if initial.is_empty() {
        return Err(err("entry route initial state missing"));
    }
    let mut outgoing: BTreeMap<String, Vec<(usize, &Value)>> = BTreeMap::new();
    for (index, edge) in transitions.iter().enumerate() {
        if string(at(edge, "eventClass")) == "decision" {
            let source = string(at(edge, "sourceStateId"));
            if !source.is_empty() {
                outgoing.entry(source).or_default().push((index, edge));
            }
        }
    }
    let mut work = VecDeque::from([(initial, BTreeSet::new())]);
    let mut seen = BTreeSet::from([(string(at(graph, "initialStateId")), BTreeSet::new())]);
    let mut routes: BTreeMap<usize, BTreeSet<BTreeSet<String>>> = BTreeMap::new();
    while let Some((state, prior)) = work.pop_front() {
        for (index, edge) in outgoing.get(&state).into_iter().flatten() {
            let is_entry = entries.contains(index);
            let destination = string(at(edge, "destinationStateId"));
            for required in guard_paths(
                at(edge, "guard").ok_or_else(|| err("entry guard missing"))?,
                &groups,
                &events,
                &known,
                false,
            )? {
                let combined = prior.union(&required).cloned().collect::<BTreeSet<_>>();
                if combined.len() > ENTRY_ROUTE_DECISION_INDICATOR_CAP {
                    return Err(err(
                        "entry decision route exceeds distinct decision-indicator cap",
                    ));
                }
                if is_entry {
                    routes.entry(*index).or_default().insert(combined);
                } else if !destination.is_empty()
                    && seen.insert((destination.clone(), combined.clone()))
                {
                    work.push_back((destination.clone(), combined));
                }
            }
        }
    }
    let reports = entries
        .into_iter()
        .map(|index| {
            let paths = routes.get(&index).cloned().unwrap_or_default();
            let counts = paths.iter().map(BTreeSet::len).collect::<BTreeSet<_>>();
            obj([
                (
                    "transitionId",
                    Value::String(string(at(&transitions[index], "id"))),
                ),
                ("routeCount", Value::from(paths.len())),
                (
                    "routeDistinctDecisionIndicatorCounts",
                    arr(counts.into_iter().map(Value::from)),
                ),
                (
                    "maxDistinctDecisionIndicatorInstances",
                    Value::from(paths.iter().map(BTreeSet::len).max().unwrap_or(0)),
                ),
            ])
        })
        .collect::<Vec<_>>();
    let observed = reports
        .iter()
        .filter_map(|item| int(at(item, "maxDistinctDecisionIndicatorInstances")))
        .max()
        .unwrap_or(0);
    Ok(obj([
        (
            "schemaVersion",
            Value::String("temporal_entry_route_decision_indicator_report_v1".into()),
        ),
        (
            "policyVersion",
            Value::String(ENTRY_ROUTE_DECISION_INDICATOR_POLICY_VERSION.into()),
        ),
        (
            "maxDistinctDecisionIndicatorInstances",
            Value::from(ENTRY_ROUTE_DECISION_INDICATOR_CAP),
        ),
        ("entryTransitions", arr(reports)),
        ("reachableStateIndicatorSetCount", Value::from(seen.len())),
        (
            "observedMaximumDistinctDecisionIndicatorInstances",
            Value::from(observed),
        ),
    ]))
}
pub fn validate_entry_route_decision_indicator_cap(profile: &Value) -> IndicatorResult<Value> {
    entry_route_decision_indicator_report(profile)
}
