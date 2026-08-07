//! Deterministic Generator-v3 source-profile construction transactions.
//!
//! This is a catalog-checked authoring surface only.  It does not interpret
//! indicators or management plans; native Dashboard validation remains the
//! execution authority after a proposed profile is produced.

use std::collections::{BTreeMap, BTreeSet};

use temporal_qd_contract::{ContractError, Value, canonical_sha256};

pub const GENERATOR_V3_VERSION: &str = "temporal_discovery_generator_v3_construction";
pub const CONSTRUCTION_OPERATOR_VERSION: &str = "1";
pub const CONSTRUCTION_REACHABILITY_SCHEMA: &str = "temporal_construction_reachability_v3";
pub const CONSTRUCTION_OPERATOR_SPEC_SCHEMA: &str = "temporal_construction_operator_spec_v1";
const V2_REACHABILITY_SCHEMA: &str = "temporal_management_reachability_v1";
const V2_GENERATOR_VERSION: &str = "temporal_discovery_generator_v2_activation_aware";
pub const MAX_MANAGEMENT_PLANS: usize = 16;
pub const MAX_SCALAR_BINDINGS: usize = 32;
pub const SCALAR_DYNAMIC_MANAGEMENT: &str = "scalar_dynamic_management_v3";
pub const MANAGEMENT_PLAN: &str = "management_plan_v3";
pub const DIRECTION_FLIP: &str = "direction_flip_v3";
pub const GRAPH_BOUND_TIMEFRAME: &str = "graph_bound_indicator_timeframe_v3";
pub const INDICATOR_FAMILY_SUBSTITUTION: &str = "indicator_family_substitution_v3";

#[derive(Debug, thiserror::Error)]
pub enum ConstructionError {
    #[error("construction contract error: {0}")]
    Contract(#[from] ContractError),
    #[error("construction: {0}")]
    Invalid(String),
}
pub type ConstructionResult<T> = Result<T, ConstructionError>;
fn invalid(message: impl Into<String>) -> ConstructionError {
    ConstructionError::Invalid(message.into())
}
fn object(rows: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    let mut value = Value::Object(Default::default());
    for (key, item) in rows {
        value
            .as_object_mut()
            .expect("object")
            .insert(key.to_owned(), item);
    }
    value
}
fn array(rows: impl IntoIterator<Item = Value>) -> Value {
    Value::Array(rows.into_iter().collect())
}
fn field<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value.as_object()?.get(key)
}
fn field_mut<'a>(value: &'a mut Value, key: &str) -> Option<&'a mut Value> {
    value.as_object_mut()?.get_mut(key)
}
fn text(value: Option<&Value>) -> String {
    value.and_then(Value::as_str).unwrap_or_default().to_owned()
}
fn integer(value: Option<&Value>) -> Option<i64> {
    value.and_then(Value::as_i64)
}
fn clone_field(value: &Value, key: &str) -> Option<Value> {
    field(value, key).cloned()
}
fn hash(value: &Value) -> ConstructionResult<String> {
    Ok(canonical_sha256(value)?)
}
fn value_strings(values: impl IntoIterator<Item = String>) -> Value {
    array(values.into_iter().map(Value::String))
}

#[derive(Clone, Debug)]
pub struct ConstructionCatalog {
    payload: Value,
    indicators: BTreeMap<String, Value>,
    timeframes: Vec<String>,
    catalog_sha256: String,
}
impl ConstructionCatalog {
    pub fn new(payload: &Value) -> ConstructionResult<Self> {
        let frames = field(payload, "timeframes")
            .and_then(Value::as_object)
            .ok_or_else(|| invalid("catalog requires timeframes"))?;
        let timeframes = frames
            .keys()
            .filter(|item| !item.trim().is_empty())
            .map(|item| item.to_uppercase())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if timeframes.is_empty() {
            return Err(invalid("catalog has no timeframes"));
        }
        let mut indicators = BTreeMap::new();
        for item in field(payload, "indicators")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("catalog requires indicators"))?
        {
            let Some(meta) = field(item, "meta") else {
                continue;
            };
            let id = text(field(meta, "id")).trim().to_owned();
            if id.is_empty() || indicators.insert(id, meta.clone()).is_some() {
                return Err(invalid("catalog IDs must be unique"));
            }
        }
        if indicators.is_empty() {
            return Err(invalid("catalog has no indicators"));
        }
        Ok(Self {
            payload: payload.clone(),
            indicators,
            timeframes,
            catalog_sha256: hash(payload)?,
        })
    }
    pub fn catalog_sha256(&self) -> &str {
        &self.catalog_sha256
    }
    pub fn payload(&self) -> &Value {
        &self.payload
    }
    pub fn timeframes(&self) -> &[String] {
        &self.timeframes
    }
    pub fn scalar_outputs(&self, id: &str) -> Vec<Value> {
        let Some(meta) = self.indicators.get(id) else {
            return vec![];
        };
        let mut output = field(meta, "managementScalarOutputs")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|item| {
                let key = text(field(item, "outputKey"));
                let kind = text(field(item, "valueKind"));
                let unit = text(field(item, "unit"));
                let expected = if kind == "price_level" {
                    "price"
                } else if kind == "price_distance" {
                    "price_distance"
                } else {
                    return None;
                };
                (!key.is_empty() && unit == expected).then(|| {
                    object([
                        ("outputKey", Value::String(key)),
                        ("valueKind", Value::String(kind)),
                        ("unit", Value::String(unit)),
                    ])
                })
            })
            .collect::<Vec<_>>();
        output.sort_by_key(|x| (text(field(x, "outputKey")), text(field(x, "valueKind"))));
        output
    }
}

fn library(profile: &Value) -> Option<&Value> {
    field(profile, "executionConfig").and_then(|item| field(item, "managementLibrary"))
}
fn plans(library: &Value) -> Vec<&Value> {
    field(library, "plans")
        .and_then(Value::as_array)
        .map(|items| items.iter().collect())
        .unwrap_or_default()
}
fn entry_actions(profile: &Value) -> Vec<(usize, usize, &Value)> {
    let mut result = vec![];
    for (ti, transition) in field(profile, "graph")
        .and_then(|item| field(item, "transitions"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
    {
        for (ai, action) in field(transition, "actions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
        {
            if text(field(action, "kind")) == "enter_next_open" {
                result.push((ti, ai, action));
            }
        }
    }
    result
}
fn binding_refs(profile: &Value) -> BTreeMap<String, usize> {
    let mut refs = BTreeMap::new();
    let mut add = |locator: Option<&Value>| {
        if let Some(locator) = locator {
            if ["indicator_price_level", "indicator_distance_multiple"]
                .contains(&text(field(locator, "kind")).as_str())
            {
                let id = text(field(locator, "bindingId"));
                if !id.is_empty() {
                    *refs.entry(id).or_default() += 1;
                }
            }
        }
    };
    if let Some(library) = library(profile) {
        for plan in plans(library) {
            add(field(plan, "initialStop"));
            add(field(plan, "initialTarget"));
            if let Some(trailing) = field(plan, "trailingStop") {
                add(field(trailing, "anchor"));
                add(field(trailing, "distance"));
            }
        }
    }
    for transition in field(profile, "graph")
        .and_then(|item| field(item, "transitions"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        for action in field(transition, "actions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if text(field(action, "kind")) == "set_target_next_open" {
                add(field(action, "targetLocator"));
            }
        }
    }
    refs
}

fn transition_rows(profile: &Value) -> ConstructionResult<Vec<&Value>> {
    field(profile, "graph")
        .and_then(|graph| field(graph, "transitions"))
        .and_then(Value::as_array)
        .map(|rows| rows.iter().filter(|row| row.is_object()).collect())
        .ok_or_else(|| invalid("candidate graph must be an object"))
}

fn walk_guard<'a>(guard: &'a Value, output: &mut Vec<&'a Value>) {
    output.push(guard);
    if let Some(child) = field(guard, "guard").filter(|child| child.is_object()) {
        walk_guard(child, output);
    }
    for child in field(guard, "guards")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|child| child.is_object())
    {
        walk_guard(child, output);
    }
}

fn guard_is_statically_true(guard: &Value) -> bool {
    match text(field(guard, "kind")).as_str() {
        "state_age_at_least" => integer(field(guard, "events")).unwrap_or(0) <= 0,
        // A position predicate is runtime-dependent even when its expected
        // value is true; only the v2 admitted structural tautologies count.
        "position_exists" => false,
        "all" => {
            let rows = field(guard, "guards")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter(|row| row.is_object())
                .collect::<Vec<_>>();
            !rows.is_empty() && rows.into_iter().all(guard_is_statically_true)
        }
        "any" => field(guard, "guards")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|row| row.is_object())
            .any(guard_is_statically_true),
        _ => false,
    }
}

fn reachable_from(
    profile: &Value,
    origins: impl IntoIterator<Item = String>,
) -> ConstructionResult<BTreeSet<String>> {
    let transitions = transition_rows(profile)?;
    let mut reached = origins
        .into_iter()
        .filter(|id| !id.is_empty())
        .collect::<BTreeSet<_>>();
    let mut queue = reached
        .iter()
        .cloned()
        .collect::<std::collections::VecDeque<_>>();
    while let Some(source) = queue.pop_front() {
        for transition in &transitions {
            if text(field(transition, "sourceStateId")) != source {
                continue;
            }
            let destination = text(field(transition, "destinationStateId"));
            if !destination.is_empty() && reached.insert(destination.clone()) {
                queue.push_back(destination);
            }
        }
    }
    Ok(reached)
}

fn filled_destinations(profile: &Value) -> ConstructionResult<BTreeSet<String>> {
    transition_rows(profile)?
        .into_iter()
        .try_fold(BTreeSet::new(), |mut result, transition| {
            if text(field(transition, "eventClass")) == "execution" {
                if let Some(guard) = field(transition, "guard").filter(|guard| guard.is_object()) {
                    let mut nodes = vec![];
                    walk_guard(guard, &mut nodes);
                    if nodes.iter().any(|node| {
                        text(field(node, "kind")) == "execution_status_is"
                            && text(field(node, "status")) == "filled"
                    }) {
                        let destination = text(field(transition, "destinationStateId"));
                        if !destination.is_empty() {
                            result.insert(destination);
                        }
                    }
                }
            }
            Ok(result)
        })
}

fn action_rows<'a>(
    profile: &'a Value,
    kinds: Option<&BTreeSet<&str>>,
) -> ConstructionResult<Vec<(&'a Value, &'a Value)>> {
    let mut output = vec![];
    for transition in transition_rows(profile)? {
        for action in field(transition, "actions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|action| action.is_object())
        {
            if kinds.is_none_or(|kinds| kinds.contains(text(field(action, "kind")).as_str())) {
                output.push((transition, action));
            }
        }
    }
    Ok(output)
}

fn transition_dominated(profile: &Value, target: &Value) -> ConstructionResult<bool> {
    let mut peers = transition_rows(profile)?
        .into_iter()
        .filter(|transition| {
            text(field(transition, "sourceStateId")) == text(field(target, "sourceStateId"))
                && text(field(transition, "eventClass")) == text(field(target, "eventClass"))
        })
        .collect::<Vec<_>>();
    peers.sort_by_key(|transition| {
        (
            integer(field(transition, "priority")).unwrap_or(0),
            text(field(transition, "id")),
        )
    });
    for peer in peers {
        if text(field(peer, "id")) == text(field(target, "id")) {
            return Ok(false);
        }
        if field(peer, "guard")
            .filter(|guard| guard.is_object())
            .is_some_and(guard_is_statically_true)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Exact static v2 management-reachability report used by the v3 construction
/// contract.  It traverses only declared graph topology and guards; it does
/// not evaluate market data or management geometry.
fn inspect_v2_management_reachability(profile: &Value) -> ConstructionResult<Value> {
    let graph =
        field(profile, "graph").ok_or_else(|| invalid("candidate graph must be an object"))?;
    let initial = text(field(graph, "initialStateId"));
    let all_reached = reachable_from(profile, [initial.clone()])?;
    let filled = filled_destinations(profile)?;
    let post_entry = reachable_from(profile, filled.iter().cloned())?;
    let lib = library(profile);
    let default_plan = lib
        .map(|value| text(field(value, "defaultPlanId")))
        .filter(|id| !id.is_empty());
    let plan_rows = lib.map(plans).unwrap_or_default();
    let plan_ids = plan_rows
        .iter()
        .map(|plan| text(field(plan, "id")))
        .collect::<BTreeSet<_>>();
    let entries = action_rows(profile, Some(&BTreeSet::from(["enter_next_open"])))?;
    let mut referenced = BTreeSet::new();
    let mut issues = BTreeMap::<String, usize>::new();
    let mut issue = |name: &str| *issues.entry(name.into()).or_default() += 1;
    if entries.is_empty() {
        issue("no_entry_route");
    }
    for (transition, action) in entries {
        let selected = text(field(action, "managementPlanId"));
        let selected = if selected.is_empty() {
            default_plan.clone().unwrap_or_default()
        } else {
            selected
        };
        if selected.is_empty() || !plan_ids.contains(&selected) {
            issue("entry_route_unknown_management_plan");
        } else {
            referenced.insert(selected);
        }
        if !all_reached.contains(&text(field(transition, "sourceStateId"))) {
            issue("entry_route_unreachable");
        }
    }
    let orphan = plan_ids
        .difference(&referenced)
        .cloned()
        .collect::<Vec<_>>();
    if !orphan.is_empty() {
        issue("orphan_management_plan");
    }
    if filled.is_empty() {
        issue("no_entry_fill_transition");
    }
    let management_kinds = BTreeSet::from([
        "move_stop_to_break_even_next_open",
        "tighten_stop_next_open",
        "set_target_next_open",
        "cancel_target_next_open",
        "activate_trailing_stop_next_open",
        "deactivate_trailing_stop_next_open",
    ]);
    let mut actions = vec![];
    for (transition, action) in action_rows(profile, Some(&management_kinds))? {
        let source = text(field(transition, "sourceStateId"));
        let row = object([
            (
                "transitionId",
                clone_field(transition, "id").unwrap_or(Value::Null),
            ),
            ("sourceStateId", Value::String(source.clone())),
            (
                "eventClass",
                clone_field(transition, "eventClass").unwrap_or(Value::Null),
            ),
            (
                "actionKind",
                clone_field(action, "kind").unwrap_or(Value::Null),
            ),
            (
                "reachableFromInitial",
                Value::Bool(all_reached.contains(&source)),
            ),
            (
                "reachableAfterEntry",
                Value::Bool(post_entry.contains(&source)),
            ),
            (
                "staticallyDominated",
                Value::Bool(transition_dominated(profile, transition)?),
            ),
        ]);
        if !all_reached.contains(&source) {
            issue("management_action_unreachable");
        }
        if text(field(action, "kind")) != "activate_trailing_stop_next_open"
            && !post_entry.contains(&source)
        {
            issue("management_action_not_post_entry");
        }
        if field(&row, "staticallyDominated").and_then(Value::as_bool) == Some(true) {
            issue("management_action_dominated");
        }
        actions.push(row);
    }
    let explicit_plans = plan_rows
        .iter()
        .filter(|plan| {
            field(plan, "trailingStop")
                .and_then(|trailing| field(trailing, "activation"))
                .is_some_and(|activation| text(field(activation, "kind")) == "explicit")
        })
        .map(|plan| text(field(plan, "id")))
        .collect::<Vec<_>>();
    let activation_rows = actions
        .iter()
        .filter(|row| text(field(row, "actionKind")) == "activate_trailing_stop_next_open")
        .count();
    if !explicit_plans.is_empty() && activation_rows == 0 {
        issue("explicit_trailing_missing_activation_action");
    }
    for row in actions
        .iter()
        .filter(|row| text(field(row, "actionKind")) == "move_stop_to_break_even_next_open")
    {
        if field(row, "reachableAfterEntry").and_then(Value::as_bool) != Some(true) {
            issue("break_even_impossible_branch");
        }
    }
    actions.sort_by_key(|row| {
        (
            text(field(row, "transitionId")),
            text(field(row, "actionKind")),
        )
    });
    let mut report = object([
        (
            "schemaVersion",
            Value::String(V2_REACHABILITY_SCHEMA.into()),
        ),
        (
            "generatorVersion",
            Value::String(V2_GENERATOR_VERSION.into()),
        ),
        ("acceptable", Value::Bool(issues.is_empty())),
        ("initialStateId", Value::String(initial)),
        ("reachableStates", value_strings(all_reached)),
        ("entryFillDestinationStates", value_strings(filled)),
        ("postEntryReachableStates", value_strings(post_entry)),
        ("managementPlanIds", value_strings(plan_ids)),
        ("referencedManagementPlanIds", value_strings(referenced)),
        ("orphanManagementPlanIds", value_strings(orphan)),
        ("explicitTrailingPlanIds", value_strings(explicit_plans)),
        ("managementActions", array(actions)),
        ("issueCounts", {
            let mut value = Value::Object(Default::default());
            for (name, count) in issues {
                value
                    .as_object_mut()
                    .unwrap()
                    .insert(name, Value::from(count));
            }
            value
        }),
    ]);
    let digest = hash(&report)?;
    report
        .as_object_mut()
        .unwrap()
        .insert("reachabilitySha256".into(), Value::String(digest));
    Ok(report)
}

/// Static closure checks for plans, entry references, and scalar locators.
/// This intentionally stops before any runtime-management semantics.
pub fn inspect_construction_reachability(profile: &Value) -> ConstructionResult<Value> {
    let mut issues = BTreeMap::<String, usize>::new();
    let mut issue = |name: &str| *issues.entry(name.into()).or_default() += 1;
    let mut plan_ids = BTreeSet::new();
    let mut referenced = BTreeSet::new();
    let mut binding_ids = BTreeSet::new();
    let refs = binding_refs(profile);
    if let Some(lib) = library(profile) {
        let plan_rows = plans(lib);
        if plan_rows.is_empty() || plan_rows.len() > MAX_MANAGEMENT_PLANS {
            issue("management_plan_count_out_of_bounds")
        }
        for plan in &plan_rows {
            let id = text(field(plan, "id"));
            if id.is_empty() || !plan_ids.insert(id) {
                issue("management_plan_ids_invalid")
            }
        }
        let default = text(field(lib, "defaultPlanId"));
        if !plan_ids.contains(&default) {
            issue("default_management_plan_unknown")
        } else {
            referenced.insert(default.clone());
        }
        let entries = entry_actions(profile);
        if entries.is_empty() {
            issue("no_entry_route")
        }
        for (_, _, action) in entries {
            let selected = {
                let id = text(field(action, "managementPlanId"));
                if id.is_empty() { default.clone() } else { id }
            };
            if plan_ids.contains(&selected) {
                referenced.insert(selected);
            } else {
                issue("entry_route_unknown_management_plan")
            }
        }
        if !plan_ids.is_subset(&referenced) {
            issue("orphan_management_plan")
        }
        let raw_bindings = field(lib, "scalarBindings").unwrap_or(&Value::Null);
        let mut bindings = raw_bindings.as_array().cloned().unwrap_or_default();
        if !raw_bindings.is_null() && !raw_bindings.is_array()
            || bindings.len() > MAX_SCALAR_BINDINGS
        {
            issue("management_scalar_binding_count_out_of_bounds");
            bindings.clear();
        }
        let mut sources = BTreeSet::new();
        for binding in bindings {
            let id = text(field(&binding, "id"));
            let source = (
                text(field(&binding, "indicatorInstanceId")),
                text(field(&binding, "outputKey")),
            );
            let kind = text(field(&binding, "valueKind"));
            if id.is_empty()
                || !binding_ids.insert(id)
                || source.0.is_empty()
                || source.1.is_empty()
                || !["price_level", "price_distance"].contains(&kind.as_str())
            {
                issue("management_scalar_binding_invalid");
                continue;
            }
            if !sources.insert(source) {
                issue("duplicate_management_scalar_source")
            }
            if !text(field(&binding, "availability")).is_empty()
                && text(field(&binding, "availability")) != "completed_bar"
            {
                issue("management_scalar_binding_not_completed_bar")
            }
        }
        if !refs.keys().all(|id| binding_ids.contains(id)) {
            issue("management_scalar_binding_missing")
        }
        if !binding_ids.iter().all(|id| refs.contains_key(id)) {
            issue("orphan_management_scalar_binding")
        }
    } else {
        issue("missing_management_library")
    }
    let v2 = inspect_v2_management_reachability(profile)?;
    for (code, count) in field(&v2, "issueCounts")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
    {
        if code != "orphan_management_plan" {
            *issues.entry(code.clone()).or_default() += count.as_u64().unwrap_or(0) as usize;
        }
    }
    let mut report = object([
        (
            "schemaVersion",
            Value::String(CONSTRUCTION_REACHABILITY_SCHEMA.into()),
        ),
        (
            "generatorVersion",
            Value::String(GENERATOR_V3_VERSION.into()),
        ),
        ("acceptable", Value::Bool(issues.is_empty())),
        ("managementPlanIds", value_strings(plan_ids.iter().cloned())),
        (
            "referencedManagementPlanIds",
            value_strings(referenced.iter().cloned()),
        ),
        (
            "orphanManagementPlanIds",
            value_strings(plan_ids.difference(&referenced).cloned()),
        ),
        (
            "managementScalarBindingIds",
            value_strings(binding_ids.iter().cloned()),
        ),
        (
            "referencedScalarBindingIds",
            value_strings(refs.keys().cloned()),
        ),
        (
            "orphanScalarBindingIds",
            value_strings(
                binding_ids
                    .iter()
                    .filter(|id| !refs.contains_key(*id))
                    .cloned(),
            ),
        ),
        ("issueCounts", {
            let mut value = Value::Object(Default::default());
            for (key, count) in issues {
                value
                    .as_object_mut()
                    .unwrap()
                    .insert(key, Value::from(count));
            }
            value
        }),
        (
            "v2ReachabilitySha256",
            clone_field(&v2, "reachabilitySha256").expect("v2 report has hash"),
        ),
    ]);
    let digest = hash(&report)?;
    report
        .as_object_mut()
        .unwrap()
        .insert("reachabilitySha256".into(), Value::String(digest));
    Ok(report)
}
fn graph_bound_instances(profile: &Value) -> BTreeSet<String> {
    let mut result = BTreeSet::new();
    for group in field(profile, "graph")
        .and_then(|item| field(item, "evidenceGroups"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        for id in field(group, "indicatorInstanceIds")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let id = text(Some(id));
            if !id.is_empty() {
                result.insert(id);
            }
        }
    }
    for binding in field(profile, "graph")
        .and_then(|item| field(item, "eventBindings"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let id = text(field(binding, "indicatorInstanceId"));
        if !id.is_empty() {
            result.insert(id);
        }
    }
    result
}
fn scalar_authorizations(profile: &Value, catalog: &ConstructionCatalog) -> Vec<Value> {
    let existing = library(profile)
        .and_then(|lib| field(lib, "scalarBindings"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|binding| {
            (
                text(field(binding, "indicatorInstanceId")),
                text(field(binding, "outputKey")),
            )
        })
        .collect::<BTreeSet<_>>();
    let existing_ids = library(profile)
        .and_then(|lib| field(lib, "scalarBindings"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|binding| text(field(binding, "id")))
        .collect::<BTreeSet<_>>();
    let mut out = vec![];
    for indicator in field(profile, "indicators")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let meta = field(indicator, "meta");
        let config = field(indicator, "config");
        let id = text(meta.and_then(|x| field(x, "id")));
        let instance = text(meta.and_then(|x| field(x, "instanceId")));
        if id.is_empty()
            || instance.is_empty()
            || field(config.unwrap_or(&Value::Null), "isActive").and_then(Value::as_bool)
                != Some(true)
            || field(config.unwrap_or(&Value::Null), "useFormingBar").and_then(Value::as_bool)
                != Some(false)
        {
            continue;
        }
        let authored = meta
            .and_then(|x| field(x, "managementScalarOutputs"))
            .and_then(Value::as_array)
            .map(|xs| {
                xs.iter()
                    .map(|x| {
                        (
                            text(field(x, "outputKey")),
                            text(field(x, "valueKind")),
                            text(field(x, "unit")),
                        )
                    })
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        for output in catalog.scalar_outputs(&id) {
            let key = text(field(&output, "outputKey"));
            let kind = text(field(&output, "valueKind"));
            let unit = text(field(&output, "unit"));
            let binding_id = format!("scalar_{instance}_{key}");
            if !authored.contains(&(key.clone(), kind.clone(), unit))
                || existing.contains(&(instance.clone(), key.clone()))
                || existing_ids.contains(&binding_id)
            {
                continue;
            }
            out.push(object([
                ("indicatorId", Value::String(id.clone())),
                ("indicatorInstanceId", Value::String(instance.clone())),
                ("outputKey", Value::String(key)),
                ("valueKind", Value::String(kind)),
                ("bindingId", Value::String(binding_id)),
            ]));
        }
    }
    out.sort_by_key(|x| {
        (
            text(field(x, "indicatorInstanceId")),
            text(field(x, "outputKey")),
            text(field(x, "valueKind")),
        )
    });
    out
}
fn locator(kind: &str, binding: &str, multiple: f64) -> Value {
    if kind == "price_level" {
        object([
            ("kind", Value::String("indicator_price_level".into())),
            ("bindingId", Value::String(binding.into())),
        ])
    } else {
        object([
            ("kind", Value::String("indicator_distance_multiple".into())),
            ("bindingId", Value::String(binding.into())),
            ("multiple", Value::from(multiple)),
        ])
    }
}
fn locator_sites(plan: &Value, kind: &str) -> Vec<Value> {
    let mut result = vec![
        object([
            ("path", array([Value::String("initialStop".into())])),
            ("multiple", Value::from(1.0)),
            ("site", Value::String("initial_stop".into())),
        ]),
        object([
            ("path", array([Value::String("initialTarget".into())])),
            ("multiple", Value::from(2.0)),
            ("site", Value::String("initial_target".into())),
        ]),
    ];
    if kind == "price_level" {
        result.push(if field(plan, "trailingStop").is_some() {
            object([
                (
                    "path",
                    array([
                        Value::String("trailingStop".into()),
                        Value::String("anchor".into()),
                    ]),
                ),
                ("multiple", Value::from(1.0)),
                ("site", Value::String("trailing_anchor".into())),
            ])
        } else {
            object([
                ("path", array([Value::String("trailingStop".into())])),
                ("multiple", Value::from(1.0)),
                ("site", Value::String("trailing_anchor_create".into())),
            ])
        });
    } else {
        result.push(if field(plan, "trailingStop").is_some() {
            object([
                (
                    "path",
                    array([
                        Value::String("trailingStop".into()),
                        Value::String("distance".into()),
                    ]),
                ),
                ("multiple", Value::from(1.0)),
                ("site", Value::String("trailing_distance".into())),
            ])
        } else {
            object([
                ("path", array([Value::String("trailingStop".into())])),
                ("multiple", Value::from(1.0)),
                ("site", Value::String("trailing_distance_create".into())),
            ])
        });
    }
    result
}
fn get_path<'a>(root: &'a Value, path: &[String]) -> Option<&'a Value> {
    let mut value = root;
    for part in path {
        value = field(value, part)?;
    }
    Some(value)
}
fn set_path(root: &mut Value, path: &[String], replacement: Value) -> ConstructionResult<()> {
    let mut target = root;
    for part in &path[..path.len().saturating_sub(1)] {
        target = field_mut(target, part).ok_or_else(|| invalid("locator parent path missing"))?;
    }
    target
        .as_object_mut()
        .ok_or_else(|| invalid("locator target is not object"))?
        .insert(
            path.last()
                .ok_or_else(|| invalid("locator path empty"))?
                .clone(),
            replacement,
        );
    Ok(())
}
fn remove_unreferenced_bindings(profile: &mut Value) -> Vec<Value> {
    let refs = binding_refs(profile);
    let bindings = library(profile)
        .and_then(|lib| field(lib, "scalarBindings"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut removed = bindings
        .iter()
        .filter(|item| !refs.contains_key(&text(field(item, "id"))))
        .cloned()
        .collect::<Vec<_>>();
    if !removed.is_empty() {
        let retained = bindings
            .into_iter()
            .filter(|item| refs.contains_key(&text(field(item, "id"))))
            .collect::<Vec<_>>();
        if let Some(rows) = field_mut(profile, "executionConfig")
            .and_then(|x| field_mut(x, "managementLibrary"))
            .and_then(Value::as_object_mut)
        {
            rows.insert("scalarBindings".into(), array(retained));
        }
    }
    removed.sort_by_key(|item| text(field(item, "id")));
    removed
}

#[derive(Clone, Debug)]
pub struct ConstructionOperator {
    id: String,
    catalog: ConstructionCatalog,
    specification: Value,
}
impl ConstructionOperator {
    fn new(id: &str, catalog: ConstructionCatalog) -> ConstructionResult<Self> {
        let mut spec = object([
            (
                "schemaVersion",
                Value::String(CONSTRUCTION_OPERATOR_SPEC_SCHEMA.into()),
            ),
            ("operatorId", Value::String(id.into())),
            (
                "operatorVersion",
                Value::String(CONSTRUCTION_OPERATOR_VERSION.into()),
            ),
            (
                "generatorVersion",
                Value::String(GENERATOR_V3_VERSION.into()),
            ),
            (
                "catalogSha256",
                Value::String(catalog.catalog_sha256.clone()),
            ),
        ]);
        let digest = hash(&spec)?;
        spec.as_object_mut()
            .unwrap()
            .insert("operatorSpecSha256".into(), Value::String(digest));
        Ok(Self {
            id: id.into(),
            catalog,
            specification: spec,
        })
    }
    pub fn operator_id(&self) -> &str {
        &self.id
    }
    pub fn specification(&self) -> &Value {
        &self.specification
    }
    pub fn enumerate_plans(&self, profile: &Value) -> ConstructionResult<Vec<Value>> {
        let parent_sha = hash(profile)?;
        let mut plans = BTreeMap::new();
        for construction in self.constructions(profile)? {
            let identity = object([
                (
                    "schemaVersion",
                    Value::String("temporal_construction_identity_v1".into()),
                ),
                (
                    "generatorVersion",
                    Value::String(GENERATOR_V3_VERSION.into()),
                ),
                ("operatorId", Value::String(self.id.clone())),
                (
                    "operatorVersion",
                    Value::String(CONSTRUCTION_OPERATOR_VERSION.into()),
                ),
                (
                    "parentSourceProfileSha256",
                    Value::String(parent_sha.clone()),
                ),
                (
                    "catalogSha256",
                    Value::String(self.catalog.catalog_sha256.clone()),
                ),
                ("construction", construction.clone()),
            ]);
            let mut plan = object([
                ("operatorId", Value::String(self.id.clone())),
                (
                    "operatorVersion",
                    Value::String(CONSTRUCTION_OPERATOR_VERSION.into()),
                ),
                (
                    "operatorSpecSha256",
                    clone_field(&self.specification, "operatorSpecSha256").unwrap(),
                ),
                (
                    "parentSourceProfileSha256",
                    Value::String(parent_sha.clone()),
                ),
                (
                    "catalogSha256",
                    Value::String(self.catalog.catalog_sha256.clone()),
                ),
                ("construction", construction),
                (
                    "constructionIdentitySha256",
                    Value::String(hash(&identity)?),
                ),
                (
                    "schemaVersion",
                    Value::String("temporal_structural_operator_plan_v1".into()),
                ),
            ]);
            let digest = hash(&plan)?;
            plan.as_object_mut()
                .unwrap()
                .insert("planSha256".into(), Value::String(digest.clone()));
            plans.insert(digest, plan);
        }
        Ok(plans.into_values().collect())
    }
    fn preview_trace(
        &self,
        profile: &Value,
        plan: &Value,
    ) -> ConstructionResult<(Value, Vec<Value>)> {
        if !self
            .enumerate_plans(profile)?
            .iter()
            .any(|item| item == plan)
        {
            return Err(invalid("construction plan is not canonical and applicable"));
        }
        self.transform(
            profile,
            field(plan, "construction").ok_or_else(|| invalid("construction missing"))?,
        )
    }
    pub fn preview(&self, profile: &Value, plan: &Value) -> ConstructionResult<Value> {
        Ok(self.preview_trace(profile, plan)?.0)
    }
    pub fn apply(
        &self,
        profile: &Value,
        plan: &Value,
        parent_program_sha256: &str,
        child_program_sha256: &str,
    ) -> ConstructionResult<(Value, Value)> {
        let (child, trace) = self.preview_trace(profile, plan)?;
        let reachability = inspect_construction_reachability(&child)?;
        let parent = hash(profile)?;
        let identity = object([
            (
                "schemaVersion",
                Value::String("temporal_construction_identity_v1".into()),
            ),
            (
                "generatorVersion",
                Value::String(GENERATOR_V3_VERSION.into()),
            ),
            ("operatorId", Value::String(self.id.clone())),
            (
                "operatorVersion",
                Value::String(CONSTRUCTION_OPERATOR_VERSION.into()),
            ),
            (
                "parentSourceProfileSha256",
                clone_field(plan, "parentSourceProfileSha256").unwrap(),
            ),
            (
                "catalogSha256",
                Value::String(self.catalog.catalog_sha256.clone()),
            ),
            ("construction", clone_field(plan, "construction").unwrap()),
        ]);
        let checks = BTreeMap::from([
            (
                "parent_identity_bound",
                clone_field(plan, "parentSourceProfileSha256")
                    == Some(Value::String(parent.clone())),
            ),
            (
                "catalog_identity_bound",
                clone_field(plan, "catalogSha256")
                    == Some(Value::String(self.catalog.catalog_sha256.clone())),
            ),
            (
                "construction_identity_bound",
                clone_field(plan, "constructionIdentitySha256")
                    == Some(Value::String(hash(&identity)?)),
            ),
            (
                "management_and_binding_reachability",
                field(&reachability, "acceptable").and_then(Value::as_bool) == Some(true),
            ),
        ]);
        let audit = finalize_audit(
            checks,
            vec![
                ("operatorId", Value::String(self.id.clone())),
                (
                    "operatorVersion",
                    Value::String(CONSTRUCTION_OPERATOR_VERSION.into()),
                ),
                (
                    "generatorVersion",
                    Value::String(GENERATOR_V3_VERSION.into()),
                ),
                ("planSha256", clone_field(plan, "planSha256").unwrap()),
                (
                    "constructionIdentitySha256",
                    clone_field(plan, "constructionIdentitySha256").unwrap(),
                ),
                ("childSourceProfileSha256", Value::String(hash(&child)?)),
                ("reachability", reachability),
            ],
        )?;
        let rotating = self.id == GRAPH_BOUND_TIMEFRAME;
        let mut application = object([
            ("operatorId", Value::String(self.id.clone())),
            (
                "operatorVersion",
                Value::String(CONSTRUCTION_OPERATOR_VERSION.into()),
            ),
            (
                "generatorVersion",
                Value::String(GENERATOR_V3_VERSION.into()),
            ),
            (
                "operatorSpecSha256",
                clone_field(&self.specification, "operatorSpecSha256").unwrap(),
            ),
            ("planSha256", clone_field(plan, "planSha256").unwrap()),
            (
                "constructionIdentitySha256",
                clone_field(plan, "constructionIdentitySha256").unwrap(),
            ),
            ("parentSourceProfileSha256", Value::String(parent)),
            ("childSourceProfileSha256", Value::String(hash(&child)?)),
            (
                "parentValidatedProgramSha256",
                Value::String(parent_program_sha256.into()),
            ),
            (
                "childValidatedProgramSha256",
                Value::String(child_program_sha256.into()),
            ),
            ("mutationTrace", array(trace)),
            ("staticInvariantReport", audit),
            (
                "evidenceScope",
                object([
                    ("marketReplayRun", Value::Bool(false)),
                    ("firedEvidence", Value::String("unmeasured".into())),
                    ("activationEvidence", Value::String("unmeasured".into())),
                    ("evidencePlanRotationRequired", Value::Bool(rotating)),
                    ("lakeScopeRegenerationRequired", Value::Bool(rotating)),
                ]),
            ),
            (
                "schemaVersion",
                Value::String("temporal_structural_operator_application_v1".into()),
            ),
        ]);
        let digest = hash(&application)?;
        application
            .as_object_mut()
            .unwrap()
            .insert("applicationSha256".into(), Value::String(digest));
        Ok((child, application))
    }
    pub fn audit(
        &self,
        parent: &Value,
        transformed: &Value,
        application: &Value,
    ) -> ConstructionResult<Value> {
        let mut body = application.clone();
        let supplied = body
            .as_object_mut()
            .and_then(|rows| rows.remove("applicationSha256"));
        let plan = self
            .enumerate_plans(parent)?
            .into_iter()
            .find(|item| clone_field(item, "planSha256") == clone_field(&body, "planSha256"));
        let (exact, trace) = match &plan {
            Some(plan) => self.preview_trace(parent, plan)?,
            None => (Value::Null, vec![]),
        };
        let checks = BTreeMap::from([
            (
                "application_identity_exact",
                supplied
                    .as_ref()
                    .and_then(Value::as_str)
                    .is_some_and(|id| hash(&body).ok().as_deref() == Some(id)),
            ),
            (
                "operator_identity_exact",
                text(field(&body, "operatorId")) == self.id
                    && text(field(&body, "operatorVersion")) == CONSTRUCTION_OPERATOR_VERSION,
            ),
            ("plan_is_currently_applicable", plan.is_some()),
            ("transformed_profile_exact", exact == *transformed),
            (
                "mutation_trace_exact",
                field(&body, "mutationTrace") == Some(&array(trace)),
            ),
            (
                "reachability_exact",
                field(
                    &inspect_construction_reachability(transformed)?,
                    "acceptable",
                )
                .and_then(Value::as_bool)
                    == Some(true),
            ),
        ]);
        finalize_audit(
            checks,
            vec![
                ("operatorId", Value::String(self.id.clone())),
                ("applicationSha256", supplied.unwrap_or(Value::Null)),
            ],
        )
    }
    fn constructions(&self, profile: &Value) -> ConstructionResult<Vec<Value>> {
        match self.id.as_str() {
            SCALAR_DYNAMIC_MANAGEMENT => self.scalar_constructions(profile),
            MANAGEMENT_PLAN => self.management_constructions(profile),
            DIRECTION_FLIP => Ok(match text(field(profile, "directionMode")).as_str() {
                "long" => vec![object([
                    ("kind", Value::String("direction_flip".into())),
                    ("before", Value::String("long".into())),
                    ("after", Value::String("short".into())),
                ])],
                "short" => vec![object([
                    ("kind", Value::String("direction_flip".into())),
                    ("before", Value::String("short".into())),
                    ("after", Value::String("long".into())),
                ])],
                _ => vec![],
            }),
            GRAPH_BOUND_TIMEFRAME => Ok(self.timeframe_constructions(profile)),
            _ => Ok(vec![]),
        }
    }
    fn transform(&self, profile: &Value, c: &Value) -> ConstructionResult<(Value, Vec<Value>)> {
        match self.id.as_str() {
            SCALAR_DYNAMIC_MANAGEMENT => self.transform_scalar(profile, c),
            MANAGEMENT_PLAN => self.transform_management(profile, c),
            DIRECTION_FLIP => {
                let mut child = profile.clone();
                let before = text(field(c, "before"));
                let after = text(field(c, "after"));
                if text(field(&child, "directionMode")) != before {
                    return Err(invalid("direction parent drift"));
                }
                child
                    .as_object_mut()
                    .unwrap()
                    .insert("directionMode".into(), Value::String(after.clone()));
                Ok((
                    child,
                    vec![object([
                        ("operation", Value::String("flip_direction".into())),
                        ("before", Value::String(before)),
                        ("after", Value::String(after)),
                    ])],
                ))
            }
            GRAPH_BOUND_TIMEFRAME => self.transform_timeframe(profile, c),
            _ => Err(invalid("unknown construction operator")),
        }
    }
    fn scalar_constructions(&self, profile: &Value) -> ConstructionResult<Vec<Value>> {
        if field(&inspect_construction_reachability(profile)?, "acceptable")
            .and_then(Value::as_bool)
            != Some(true)
        {
            return Ok(vec![]);
        }
        let bindings = library(profile)
            .and_then(|lib| field(lib, "scalarBindings"))
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0);
        if bindings >= MAX_SCALAR_BINDINGS {
            return Ok(vec![]);
        }
        let referenced = field(
            &inspect_construction_reachability(profile)?,
            "referencedManagementPlanIds",
        )
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
        let mut out = vec![];
        for scalar in scalar_authorizations(profile, &self.catalog) {
            for plan_id in &referenced {
                let id = text(Some(plan_id));
                let plan = plans(library(profile).unwrap())
                    .into_iter()
                    .find(|item| text(field(item, "id")) == id);
                let Some(plan) = plan else { continue };
                for site in locator_sites(plan, &text(field(&scalar, "valueKind"))) {
                    out.push(object([
                        ("kind", Value::String("scalar_dynamic_management".into())),
                        ("scalar", scalar.clone()),
                        ("planId", Value::String(id.clone())),
                        ("site", clone_field(&site, "site").unwrap()),
                        ("locatorPath", clone_field(&site, "path").unwrap()),
                        ("multiple", clone_field(&site, "multiple").unwrap()),
                    ]));
                }
            }
        }
        Ok(out)
    }
    fn transform_scalar(
        &self,
        profile: &Value,
        c: &Value,
    ) -> ConstructionResult<(Value, Vec<Value>)> {
        let mut child = profile.clone();
        let scalar = field(c, "scalar")
            .ok_or_else(|| invalid("scalar construction missing authorization"))?;
        let plan_id = text(field(c, "planId"));
        let plan = field_mut(&mut child, "executionConfig")
            .and_then(|x| field_mut(x, "managementLibrary"))
            .and_then(|x| field_mut(x, "plans"))
            .and_then(Value::as_array_mut)
            .and_then(|items| {
                items
                    .iter_mut()
                    .find(|item| text(field(item, "id")) == plan_id)
            })
            .ok_or_else(|| invalid("construction target plan disappeared"))?;
        let path = field(c, "locatorPath")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("locator path missing"))?
            .iter()
            .map(|x| text(Some(x)))
            .collect::<Vec<_>>();
        let site = text(field(c, "site"));
        let kind = text(field(scalar, "valueKind"));
        let binding = text(field(scalar, "bindingId"));
        let multiple = field(c, "multiple")
            .and_then(Value::as_f64)
            .ok_or_else(|| invalid("locator multiple missing"))?;
        let replacement = if site == "trailing_anchor_create" {
            object([
                ("anchor", locator(&kind, &binding, multiple)),
                (
                    "distance",
                    object([
                        ("kind", Value::String("fixed_initial_r".into())),
                        ("multiple", Value::from(1.0)),
                    ]),
                ),
                (
                    "activation",
                    object([("kind", Value::String("immediate".into()))]),
                ),
                ("minimumStepInitialR", Value::from(0.0)),
            ])
        } else if site == "trailing_distance_create" {
            object([
                (
                    "anchor",
                    object([("kind", Value::String("bar_close".into()))]),
                ),
                ("distance", locator(&kind, &binding, multiple)),
                (
                    "activation",
                    object([("kind", Value::String("immediate".into()))]),
                ),
                ("minimumStepInitialR", Value::from(0.0)),
            ])
        } else {
            locator(&kind, &binding, multiple)
        };
        let before = if site.ends_with("_create") {
            field(plan, "trailingStop")
                .cloned()
                .unwrap_or_else(|| object([("__absent__", Value::Bool(true))]))
        } else {
            get_path(plan, &path)
                .cloned()
                .ok_or_else(|| invalid("locator target missing"))?
        };
        set_path(plan, &path, replacement.clone())?;
        let binding_row = object([
            ("id", Value::String(binding)),
            (
                "indicatorInstanceId",
                clone_field(scalar, "indicatorInstanceId").unwrap(),
            ),
            ("outputKey", clone_field(scalar, "outputKey").unwrap()),
            ("valueKind", clone_field(scalar, "valueKind").unwrap()),
            ("availability", Value::String("completed_bar".into())),
        ]);
        let bindings = field_mut(&mut child, "executionConfig")
            .and_then(|x| field_mut(x, "managementLibrary"))
            .and_then(Value::as_object_mut)
            .expect("library")
            .entry("scalarBindings")
            .or_insert_with(|| array([]))
            .as_array_mut()
            .expect("bindings");
        bindings.push(binding_row.clone());
        bindings.sort_by_key(|item| text(field(item, "id")));
        let mut trace = vec![
            object([
                ("operation", Value::String("replace_locator".into())),
                ("planId", Value::String(plan_id)),
                ("path", Value::String(format!("/{}", path.join("/")))),
                ("before", before),
                ("after", replacement),
            ]),
            object([
                ("operation", Value::String("add_scalar_binding".into())),
                ("binding", binding_row),
            ]),
        ];
        for removed in remove_unreferenced_bindings(&mut child) {
            trace.push(object([
                (
                    "operation",
                    Value::String("delete_unreferenced_scalar_binding".into()),
                ),
                ("binding", removed),
            ]));
        }
        Ok((child, trace))
    }
    fn management_constructions(&self, profile: &Value) -> ConstructionResult<Vec<Value>> {
        if field(&inspect_construction_reachability(profile)?, "acceptable")
            .and_then(Value::as_bool)
            != Some(true)
        {
            return Ok(vec![]);
        }
        let lib = library(profile).unwrap();
        let current = plans(lib);
        let mut out = vec![];
        let entries = entry_actions(profile);
        if !entries.is_empty() && current.len() < MAX_MANAGEMENT_PLANS {
            let suffix = hash(&object([
                ("parent", Value::String(hash(profile)?)),
                ("operator", Value::String(self.id.clone())),
                ("kind", Value::String("create".into())),
            ]))?
            .trim_start_matches("sha256:")
            .chars()
            .take(12)
            .collect::<String>();
            let id = format!("constructed_plan_{suffix}");
            if !current.iter().any(|plan| text(field(plan, "id")) == id) {
                let (ti, ai, _) = entries[0];
                out.push(object([
                    ("kind", Value::String("create_plan".into())),
                    (
                        "plan",
                        object([
                            ("id", Value::String(id)),
                            (
                                "initialStop",
                                object([
                                    ("kind", Value::String("fixed_percent".into())),
                                    ("percent", Value::from(1.0)),
                                ]),
                            ),
                            (
                                "initialTarget",
                                object([
                                    ("kind", Value::String("reward_multiple".into())),
                                    ("multiple", Value::from(2.0)),
                                ]),
                            ),
                        ]),
                    ),
                    ("entryTransitionIndex", Value::from(ti)),
                    ("entryActionIndex", Value::from(ai)),
                ]));
            }
        }
        if current.len() > 1 {
            let ids = current
                .iter()
                .map(|item| text(field(item, "id")))
                .collect::<BTreeSet<_>>();
            for deleted in &ids {
                let replacement = ids
                    .iter()
                    .find(|item| *item != deleted)
                    .expect("more than one")
                    .clone();
                let candidate = object([
                    ("kind", Value::String("delete_plan".into())),
                    ("deletedPlanId", Value::String(deleted.clone())),
                    ("replacementPlanId", Value::String(replacement)),
                    (
                        "rewriteDefault",
                        Value::Bool(text(field(lib, "defaultPlanId")) == *deleted),
                    ),
                ]);
                if field(
                    &inspect_construction_reachability(
                        &self.transform_management(profile, &candidate)?.0,
                    )?,
                    "acceptable",
                )
                .and_then(Value::as_bool)
                    == Some(true)
                {
                    out.push(candidate)
                }
            }
        }
        Ok(out)
    }
    fn transform_management(
        &self,
        profile: &Value,
        c: &Value,
    ) -> ConstructionResult<(Value, Vec<Value>)> {
        let mut child = profile.clone();
        let kind = text(field(c, "kind"));
        let library = field_mut(&mut child, "executionConfig")
            .and_then(|x| field_mut(x, "managementLibrary"))
            .ok_or_else(|| invalid("management library missing"))?;
        let mut trace = vec![];
        if kind == "create_plan" {
            let plan = clone_field(c, "plan").ok_or_else(|| invalid("created plan missing"))?;
            field_mut(library, "plans")
                .and_then(Value::as_array_mut)
                .ok_or_else(|| invalid("plans missing"))?
                .push(plan.clone());
            field_mut(library, "plans")
                .and_then(Value::as_array_mut)
                .unwrap()
                .sort_by_key(|item| text(field(item, "id")));
            let preserved_default = clone_field(library, "defaultPlanId").unwrap();
            let ti = integer(field(c, "entryTransitionIndex"))
                .ok_or_else(|| invalid("transition index"))? as usize;
            let ai = integer(field(c, "entryActionIndex")).ok_or_else(|| invalid("action index"))?
                as usize;
            let action = field_mut(&mut child, "graph")
                .and_then(|g| field_mut(g, "transitions"))
                .and_then(Value::as_array_mut)
                .and_then(|ts| ts.get_mut(ti))
                .and_then(|t| field_mut(t, "actions"))
                .and_then(Value::as_array_mut)
                .and_then(|as_| as_.get_mut(ai))
                .ok_or_else(|| invalid("entry action missing"))?;
            let before = clone_field(action, "managementPlanId")
                .unwrap_or_else(|| object([("__absent__", Value::Bool(true))]));
            action
                .as_object_mut()
                .unwrap()
                .insert("managementPlanId".into(), clone_field(&plan, "id").unwrap());
            trace.extend([
                object([
                    ("operation", Value::String("create_management_plan".into())),
                    ("plan", plan.clone()),
                ]),
                object([
                    (
                        "operation",
                        Value::String("rewrite_enter_management_plan".into()),
                    ),
                    (
                        "transitionId",
                        clone_field(
                            field(&child, "graph")
                                .and_then(|g| field(g, "transitions"))
                                .and_then(Value::as_array)
                                .and_then(|ts| ts.get(ti))
                                .unwrap(),
                            "id",
                        )
                        .unwrap_or(Value::Null),
                    ),
                    ("actionIndex", Value::from(ai)),
                    ("before", before),
                    ("after", clone_field(&plan, "id").unwrap()),
                ]),
                object([
                    (
                        "operation",
                        Value::String("preserve_default_management_plan".into()),
                    ),
                    ("planId", preserved_default),
                ]),
            ]);
        } else if kind == "delete_plan" {
            let deleted = text(field(c, "deletedPlanId"));
            let replacement = text(field(c, "replacementPlanId"));
            let old = {
                let rows = field_mut(library, "plans")
                    .and_then(Value::as_array_mut)
                    .ok_or_else(|| invalid("plans missing"))?;
                let index = rows
                    .iter()
                    .position(|item| text(field(item, "id")) == deleted)
                    .ok_or_else(|| invalid("deleted plan missing"))?;
                rows.remove(index)
            };
            if text(field(library, "defaultPlanId")) == deleted {
                library
                    .as_object_mut()
                    .unwrap()
                    .insert("defaultPlanId".into(), Value::String(replacement.clone()));
                trace.push(object([
                    (
                        "operation",
                        Value::String("rewrite_default_management_plan".into()),
                    ),
                    ("before", Value::String(deleted.clone())),
                    ("after", Value::String(replacement.clone())),
                ]));
            }
            for transition in field_mut(&mut child, "graph")
                .and_then(|x| field_mut(x, "transitions"))
                .and_then(Value::as_array_mut)
                .into_iter()
                .flatten()
            {
                let transition_id = clone_field(transition, "id").unwrap_or(Value::Null);
                for (ai, action) in field_mut(transition, "actions")
                    .and_then(Value::as_array_mut)
                    .into_iter()
                    .flatten()
                    .enumerate()
                {
                    if text(field(action, "kind")) == "enter_next_open"
                        && text(field(action, "managementPlanId")) == deleted
                    {
                        action.as_object_mut().unwrap().insert(
                            "managementPlanId".into(),
                            Value::String(replacement.clone()),
                        );
                        trace.push(object([
                            (
                                "operation",
                                Value::String("rewrite_enter_management_plan".into()),
                            ),
                            ("transitionId", transition_id.clone()),
                            ("actionIndex", Value::from(ai)),
                            ("before", Value::String(deleted.clone())),
                            ("after", Value::String(replacement.clone())),
                        ]));
                    }
                }
            }
            trace.insert(
                0,
                object([
                    ("operation", Value::String("delete_management_plan".into())),
                    ("plan", old),
                ]),
            );
            for removed in remove_unreferenced_bindings(&mut child) {
                trace.push(object([
                    (
                        "operation",
                        Value::String("delete_unreferenced_scalar_binding".into()),
                    ),
                    ("binding", removed),
                ]));
            }
        } else {
            return Err(invalid("unknown management plan construction"));
        }
        Ok((child, trace))
    }
    fn timeframe_constructions(&self, profile: &Value) -> Vec<Value> {
        let bound = graph_bound_instances(profile);
        let mut out = vec![];
        for (index, item) in field(profile, "indicators")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
        {
            let instance = text(field(item, "meta").and_then(|x| field(x, "instanceId")));
            let id = text(field(item, "meta").and_then(|x| field(x, "id")));
            let before =
                text(field(item, "config").and_then(|x| field(x, "timeframe"))).to_uppercase();
            if !bound.contains(&instance)
                || !self.catalog.indicators.contains_key(&id)
                || !self.catalog.timeframes.contains(&before)
            {
                continue;
            }
            for after in &self.catalog.timeframes {
                if after != &before {
                    out.push(object([
                        (
                            "kind",
                            Value::String("graph_bound_timeframe_substitution".into()),
                        ),
                        ("indicatorIndex", Value::from(index)),
                        ("indicatorInstanceId", Value::String(instance.clone())),
                        ("indicatorId", Value::String(id.clone())),
                        ("before", Value::String(before.clone())),
                        ("after", Value::String(after.clone())),
                        (
                            "evidenceLakeScope",
                            object([
                                ("regenerationRequired", Value::Bool(true)),
                                (
                                    "reason",
                                    Value::String("graph_bound_indicator_timeframe_changed".into()),
                                ),
                            ]),
                        ),
                    ]));
                }
            }
        }
        out
    }
    fn transform_timeframe(
        &self,
        profile: &Value,
        c: &Value,
    ) -> ConstructionResult<(Value, Vec<Value>)> {
        let mut child = profile.clone();
        let index =
            integer(field(c, "indicatorIndex")).ok_or_else(|| invalid("timeframe index"))? as usize;
        let item = field_mut(&mut child, "indicators")
            .and_then(Value::as_array_mut)
            .and_then(|items| items.get_mut(index))
            .ok_or_else(|| invalid("timeframe item missing"))?;
        let instance = text(field(c, "indicatorInstanceId"));
        let before = text(field(c, "before"));
        let after = text(field(c, "after"));
        if text(field(item, "meta").and_then(|x| field(x, "instanceId"))) != instance
            || text(field(item, "config").and_then(|x| field(x, "timeframe"))).to_uppercase()
                != before
        {
            return Err(invalid("timeframe parent drift"));
        }
        field_mut(item, "config")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| invalid("indicator config missing"))?
            .insert("timeframe".into(), Value::String(after.clone()));
        Ok((
            child,
            vec![object([
                (
                    "operation",
                    Value::String("substitute_graph_bound_indicator_timeframe".into()),
                ),
                ("indicatorInstanceId", Value::String(instance)),
                ("before", Value::String(before)),
                ("after", Value::String(after)),
                (
                    "evidenceLakeScope",
                    clone_field(c, "evidenceLakeScope").unwrap(),
                ),
            ])],
        ))
    }
}
fn finalize_audit(
    checks: BTreeMap<&str, bool>,
    context: Vec<(&'static str, Value)>,
) -> ConstructionResult<Value> {
    let all = checks.values().all(|item| *item);
    let mut check_value = Value::Object(Default::default());
    for (key, value) in checks {
        check_value
            .as_object_mut()
            .unwrap()
            .insert(key.into(), Value::Bool(value));
    }
    let mut rows = context;
    rows.push((
        "schemaVersion",
        Value::String("temporal_structural_operator_audit_v1".into()),
    ));
    rows.push(("checks", check_value));
    rows.push(("allChecksPassed", Value::Bool(all)));
    let mut audit = object(rows);
    let digest = hash(&audit)?;
    audit
        .as_object_mut()
        .unwrap()
        .insert("auditSha256".into(), Value::String(digest));
    Ok(audit)
}

#[derive(Clone, Debug)]
pub struct DeferredIndicatorFamilySubstitutionOperator;
impl DeferredIndicatorFamilySubstitutionOperator {
    pub const OPERATOR_ID: &'static str = INDICATOR_FAMILY_SUBSTITUTION;
    pub const DEFERRED_REASON: &'static str =
        "strict_event_scalar_role_persistence_base_family_compatibility_map_not_admitted";
    pub fn enumerate_plans(&self, _: &Value) -> Vec<Value> {
        vec![]
    }
    pub fn audit(&self) -> ConstructionResult<Value> {
        finalize_audit(
            BTreeMap::from([("operator_is_deferred", true)]),
            vec![
                ("operatorId", Value::String(Self::OPERATOR_ID.into())),
                (
                    "deferredReason",
                    Value::String(Self::DEFERRED_REASON.into()),
                ),
            ],
        )
    }
}
#[derive(Clone, Debug)]
pub struct GeneratorV3ConstructionRegistry {
    catalog: ConstructionCatalog,
    operators: BTreeMap<String, ConstructionOperator>,
    policy: Value,
}
impl GeneratorV3ConstructionRegistry {
    pub fn new(catalog: ConstructionCatalog) -> ConstructionResult<Self> {
        let mut operators = BTreeMap::new();
        for id in [
            SCALAR_DYNAMIC_MANAGEMENT,
            MANAGEMENT_PLAN,
            DIRECTION_FLIP,
            GRAPH_BOUND_TIMEFRAME,
        ] {
            operators.insert(id.into(), ConstructionOperator::new(id, catalog.clone())?);
        }
        let deferred = obj_deferred_operator();
        let mut policy = object([
            (
                "schemaVersion",
                Value::String("temporal_generator_v3_construction_policy_v1".into()),
            ),
            (
                "generatorVersion",
                Value::String(GENERATOR_V3_VERSION.into()),
            ),
            (
                "catalogSha256",
                Value::String(catalog.catalog_sha256.clone()),
            ),
            (
                "enabledOperatorIds",
                value_strings(operators.keys().cloned()),
            ),
            ("deferredOperators", array([deferred])),
        ]);
        let digest = hash(&policy)?;
        policy
            .as_object_mut()
            .unwrap()
            .insert("policySha256".into(), Value::String(digest));
        Ok(Self {
            catalog,
            operators,
            policy,
        })
    }
    pub fn catalog(&self) -> &ConstructionCatalog {
        &self.catalog
    }
    pub fn policy(&self) -> &Value {
        &self.policy
    }
    pub fn enabled_operator_ids(&self) -> Vec<String> {
        self.operators.keys().cloned().collect()
    }
    pub fn get(&self, id: &str) -> ConstructionResult<&ConstructionOperator> {
        self.operators
            .get(id)
            .ok_or_else(|| invalid("unknown construction operator"))
    }
    pub fn enumerate_plans(&self, profile: &Value) -> ConstructionResult<Vec<Value>> {
        let mut plans = vec![];
        for op in self.operators.values() {
            plans.extend(op.enumerate_plans(profile)?);
        }
        plans.sort_by_key(|plan| text(field(plan, "planSha256")));
        Ok(plans)
    }
}

fn obj_deferred_operator() -> Value {
    object([
        (
            "operatorId",
            Value::String(INDICATOR_FAMILY_SUBSTITUTION.into()),
        ),
        (
            "reason",
            Value::String(DeferredIndicatorFamilySubstitutionOperator::DEFERRED_REASON.into()),
        ),
    ])
}
