//! Bounded typed-fragment grammar for reusable temporal v2 module genomes.
//!
//! The grammar is deliberately pure: it creates only resource-closed v2
//! modules.  Dashboard validation and v3 pair compilation are injected at the
//! boundary and are never reimplemented here.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use temporal_qd_contract::{ContractError, Value, canonical_sha256 as contract_canonical_sha256};

pub const GRAMMAR_SCHEMA: &str = "temporal_typed_fragment_grammar_v2";
pub const GRAMMAR_VERSION: &str = "3";
pub const MODULE_SCHEMA: &str = "temporal_typed_fragment_module_v2";
pub const WITNESS_SCHEMA: &str = "temporal_typed_fragment_activation_recipe_v1";
/// Read-only projection schema for auditing the sealed executable grammar.
/// It is intentionally separate from the compact frozen-authority registry,
/// which binds only the fields needed by construction.
pub const REGISTRY_PROJECTION_SCHEMA: &str = "temporal_typed_fragment_grammar_registry_projection_v1";
pub const ENTRY_ROUTE_DECISION_INDICATOR_CAP: usize = 3;
pub const ENTRY_ROUTE_DECISION_INDICATOR_POLICY_VERSION: &str =
    "temporal_entry_route_decision_indicator_cap_v1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Budgets {
    pub states: usize,
    pub transitions: usize,
    pub groups: usize,
    pub events: usize,
    pub indicators: usize,
    pub guard_depth: usize,
}

impl Default for Budgets {
    fn default() -> Self {
        Self {
            states: 16,
            transitions: 63,
            groups: 4,
            events: 8,
            indicators: 16,
            guard_depth: 4,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GrammarError {
    #[error("{0}")]
    Contract(#[from] ContractError),
    #[error("{0}")]
    Invalid(String),
    #[error("entry decision route exceeds the distinct decision-indicator cap")]
    EntryRouteDecisionIndicatorCap,
}

impl GrammarError {
    fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }
}

pub trait NativeValidator {
    fn validate_v2(&self, profile: &Value, candidate_id: &str) -> Result<Value, GrammarError>;
}

pub trait PairCompiler {
    fn compile_pair(
        &self,
        long_profile: &Value,
        short_profile: &Value,
        candidate_id: &str,
    ) -> Result<Value, GrammarError>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Port {
    Ready,
    Watch,
    EntryPending,
    PositionIdle,
    ManagementPending,
    ExitPending,
    Recovery,
}

impl Port {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Watch => "watch",
            Self::EntryPending => "entry_pending",
            Self::PositionIdle => "position_idle",
            Self::ManagementPending => "management_pending",
            Self::ExitPending => "exit_pending",
            Self::Recovery => "recovery",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FragmentSpec {
    pub production_id: String,
    pub family: String,
    pub consumes: Port,
    pub produces: Port,
    pub resource_slots: Vec<String>,
    pub choice_domains: BTreeMap<String, Vec<Value>>,
    pub max_instances: usize,
    pub activation_recipe: Value,
}

fn obj(items: Vec<(&str, Value)>) -> Value {
    let mut value = Value::Object(Default::default());
    let map = value.as_object_mut().expect("object was just constructed");
    for (key, item) in items {
        map.insert(key.to_owned(), item);
    }
    value
}

fn string(value: &Value, name: &str) -> Result<String, GrammarError> {
    value
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| GrammarError::invalid(format!("{name} must be a string")))
}

fn object(value: &Value, name: &str) -> Result<BTreeMap<String, Value>, GrammarError> {
    value
        .as_object()
        .map(|map| map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .ok_or_else(|| GrammarError::invalid(format!("{name} must be an object")))
}

fn array(value: &Value, name: &str) -> Result<Vec<Value>, GrammarError> {
    value
        .as_array()
        .cloned()
        .ok_or_else(|| GrammarError::invalid(format!("{name} must be an ordered list")))
}

fn required<'a>(
    map: &'a BTreeMap<String, Value>,
    key: &str,
    name: &str,
) -> Result<&'a Value, GrammarError> {
    map.get(key)
        .ok_or_else(|| GrammarError::invalid(format!("{name} is missing {key}")))
}

fn exact_keys(
    map: &BTreeMap<String, Value>,
    expected: &[&str],
    name: &str,
) -> Result<(), GrammarError> {
    let found = map.keys().cloned().collect::<BTreeSet<_>>();
    let expected = expected
        .iter()
        .map(|key| (*key).to_owned())
        .collect::<BTreeSet<_>>();
    if found != expected {
        return Err(GrammarError::invalid(format!(
            "{name} fields are not exact"
        )));
    }
    Ok(())
}

fn sha(value: &Value) -> Result<String, GrammarError> {
    Ok(contract_canonical_sha256(value)?)
}

fn choices(entries: Vec<(&str, Vec<Value>)>) -> BTreeMap<String, Vec<Value>> {
    entries
        .into_iter()
        .map(|(key, values)| (key.to_owned(), values))
        .collect()
}

fn recipe(facts: &[&str], outcome: &str) -> Value {
    obj(vec![
        (
            "facts",
            Value::Array(
                facts
                    .iter()
                    .map(|item| Value::String((*item).to_owned()))
                    .collect(),
            ),
        ),
        ("outcome", Value::String(outcome.to_owned())),
    ])
}

fn spec(
    production_id: &str,
    family: &str,
    consumes: Port,
    produces: Port,
    resources: &[&str],
    choice_domains: BTreeMap<String, Vec<Value>>,
    max_instances: usize,
    activation_recipe: Value,
) -> FragmentSpec {
    FragmentSpec {
        production_id: production_id.to_owned(),
        family: family.to_owned(),
        consumes,
        produces,
        resource_slots: resources.iter().map(|item| (*item).to_owned()).collect(),
        choice_domains,
        max_instances,
        activation_recipe,
    }
}

/// The sealed production table.  It is created afresh (and deterministically)
/// so callers cannot mutate global grammar state.
pub fn registry() -> BTreeMap<String, FragmentSpec> {
    let mut result = BTreeMap::new();
    let mut put = |item: FragmentSpec| {
        result.insert(item.production_id.clone(), item);
    };
    put(spec(
        "arm_level",
        "arm",
        Port::Ready,
        Port::Watch,
        &["group"],
        choices(vec![(
            "threshold",
            vec![35.0.into(), 50.0.into(), 65.0.into(), 75.0.into()],
        )]),
        1,
        recipe(&["position.absent", "evidence_at_least"], "watch"),
    ));
    put(spec(
        "arm_fresh_event",
        "arm",
        Port::Ready,
        Port::Watch,
        &["event"],
        BTreeMap::new(),
        1,
        recipe(&["position.absent", "fresh_event"], "watch"),
    ));
    put(spec(
        "gate_level",
        "gate",
        Port::Watch,
        Port::Watch,
        &["group"],
        choices(vec![(
            "threshold",
            vec![40.0.into(), 55.0.into(), 70.0.into(), 85.0.into()],
        )]),
        4,
        recipe(&["evidence_at_least"], "next_watch"),
    ));
    put(spec(
        "gate_below",
        "gate",
        Port::Watch,
        Port::Watch,
        &["group"],
        choices(vec![(
            "threshold",
            vec![20.0.into(), 35.0.into(), 50.0.into(), 65.0.into()],
        )]),
        4,
        recipe(&["evidence_below"], "next_watch"),
    ));
    put(spec(
        "gate_fresh_event",
        "gate",
        Port::Watch,
        Port::Watch,
        &["event"],
        BTreeMap::new(),
        4,
        recipe(&["fresh_event"], "next_watch"),
    ));
    put(spec(
        "gate_event_window",
        "gate",
        Port::Watch,
        Port::Watch,
        &["event"],
        choices(vec![("age", vec![0.into(), 1.into(), 2.into(), 3.into()])]),
        4,
        recipe(&["event_age"], "next_watch"),
    ));
    put(spec(
        "gate_delay",
        "gate",
        Port::Watch,
        Port::Watch,
        &[],
        choices(vec![("bars", vec![1.into(), 2.into(), 3.into(), 5.into()])]),
        4,
        recipe(&["state_age"], "next_watch"),
    ));
    put(spec(
        "gate_streak",
        "gate",
        Port::Watch,
        Port::Watch,
        &["group"],
        choices(vec![
            ("threshold", vec![45.0.into(), 60.0.into(), 75.0.into()]),
            ("bars", vec![2.into(), 3.into(), 5.into()]),
        ]),
        4,
        recipe(&["condition_streak"], "next_watch"),
    ));
    put(spec(
        "gate_predicate_edge",
        "gate",
        Port::Watch,
        Port::Watch,
        &["group"],
        choices(vec![(
            "threshold",
            vec![45.0.into(), 60.0.into(), 75.0.into()],
        )]),
        4,
        recipe(&["predicate_edge", "evidence_at_least"], "next_watch"),
    ));
    put(spec(
        "enter_on_level",
        "entry",
        Port::Watch,
        Port::EntryPending,
        &["group", "plan"],
        choices(vec![(
            "threshold",
            vec![45.0.into(), 60.0.into(), 75.0.into()],
        )]),
        1,
        recipe(&["position.absent", "evidence_at_least"], "entry_intent"),
    ));
    put(spec(
        "enter_on_event",
        "entry",
        Port::Watch,
        Port::EntryPending,
        &["event", "plan"],
        BTreeMap::new(),
        1,
        recipe(&["position.absent", "fresh_event"], "entry_intent"),
    ));
    put(spec(
        "enter_on_level_and_event",
        "entry",
        Port::Watch,
        Port::EntryPending,
        &["group", "event", "plan"],
        choices(vec![(
            "threshold",
            vec![45.0.into(), 60.0.into(), 75.0.into()],
        )]),
        1,
        recipe(
            &["position.absent", "evidence_at_least", "fresh_event"],
            "entry_intent",
        ),
    ));
    put(spec(
        "move_break_even",
        "management",
        Port::PositionIdle,
        Port::ManagementPending,
        &[],
        choices(vec![("r", vec![0.5.into(), 1.0.into(), 1.5.into()])]),
        4,
        recipe(&["position.present", "unrealized_r"], "management_intent"),
    ));
    put(spec(
        "tighten_stop",
        "management",
        Port::PositionIdle,
        Port::ManagementPending,
        &[],
        choices(vec![
            ("r", vec![0.5.into(), 1.0.into(), 1.5.into()]),
            ("multiple", vec![(-0.5).into(), 0.0.into(), 0.5.into()]),
        ]),
        4,
        recipe(&["position.present", "unrealized_r"], "management_intent"),
    ));
    put(spec(
        "set_target",
        "management",
        Port::PositionIdle,
        Port::ManagementPending,
        &[],
        choices(vec![
            ("r", vec![0.5.into(), 1.0.into(), 1.5.into()]),
            ("multiple", vec![1.0.into(), 1.5.into(), 2.0.into()]),
        ]),
        4,
        recipe(&["position.present", "unrealized_r"], "management_intent"),
    ));
    put(spec(
        "cancel_target",
        "management",
        Port::PositionIdle,
        Port::ManagementPending,
        &[],
        choices(vec![("r", vec![0.5.into(), 1.0.into(), 1.5.into()])]),
        4,
        recipe(&["position.present", "unrealized_r"], "management_intent"),
    ));
    put(spec(
        "activate_trailing",
        "management",
        Port::PositionIdle,
        Port::ManagementPending,
        &[],
        choices(vec![("r", vec![0.5.into(), 1.0.into(), 1.5.into()])]),
        4,
        recipe(&["position.present", "unrealized_r"], "management_intent"),
    ));
    put(spec(
        "deactivate_trailing",
        "management",
        Port::PositionIdle,
        Port::ManagementPending,
        &[],
        choices(vec![("bars", vec![2.into(), 3.into(), 5.into()])]),
        4,
        recipe(&["position.present", "position_age"], "management_intent"),
    ));
    put(spec(
        "exit_on_age",
        "exit",
        Port::PositionIdle,
        Port::ExitPending,
        &[],
        choices(vec![(
            "bars",
            vec![5.into(), 8.into(), 13.into(), 21.into()],
        )]),
        4,
        recipe(&["position.present", "position_age"], "exit_intent"),
    ));
    put(spec(
        "exit_on_loss",
        "exit",
        Port::PositionIdle,
        Port::ExitPending,
        &[],
        choices(vec![(
            "r",
            vec![(-1.5).into(), (-1.0).into(), (-0.5).into()],
        )]),
        4,
        recipe(&["position.present", "unrealized_r"], "exit_intent"),
    ));
    put(spec(
        "exit_on_profit",
        "exit",
        Port::PositionIdle,
        Port::ExitPending,
        &[],
        choices(vec![("r", vec![1.0.into(), 1.5.into(), 2.0.into()])]),
        4,
        recipe(&["position.present", "unrealized_r"], "exit_intent"),
    ));
    put(spec(
        "exit_on_signal",
        "exit",
        Port::PositionIdle,
        Port::ExitPending,
        &["event"],
        BTreeMap::new(),
        4,
        recipe(&["position.present", "fresh_event"], "exit_intent"),
    ));
    put(spec(
        "cooldown",
        "recovery",
        Port::Recovery,
        Port::Recovery,
        &[],
        choices(vec![("bars", vec![1.into(), 2.into(), 3.into(), 5.into()])]),
        4,
        recipe(&["state_age"], "recovery"),
    ));
    result
}

/// Export the complete executable registry without parsing Rust source text.
/// This is a research/audit projection only; production construction continues
/// to read [`registry`] directly.
pub fn registry_projection() -> Value {
    let productions = registry()
        .into_values()
        .map(|fragment| {
            obj(vec![
                ("productionId", Value::String(fragment.production_id)),
                ("family", Value::String(fragment.family)),
                (
                    "consumes",
                    Value::String(fragment.consumes.as_str().to_owned()),
                ),
                (
                    "produces",
                    Value::String(fragment.produces.as_str().to_owned()),
                ),
                (
                    "resourceSlots",
                    Value::Array(
                        fragment
                            .resource_slots
                            .into_iter()
                            .map(Value::String)
                            .collect(),
                    ),
                ),
                (
                    "choiceDomains",
                    Value::Object(
                        fragment
                            .choice_domains
                            .into_iter()
                            .map(|(key, values)| (key, Value::Array(values)))
                            .collect(),
                    ),
                ),
                ("maxInstances", Value::from(fragment.max_instances as u64)),
                ("activationRecipe", fragment.activation_recipe),
            ])
        })
        .collect();
    obj(vec![
        (
            "schemaVersion",
            Value::String(REGISTRY_PROJECTION_SCHEMA.to_owned()),
        ),
        ("grammarSchema", Value::String(GRAMMAR_SCHEMA.to_owned())),
        ("grammarVersion", Value::String(GRAMMAR_VERSION.to_owned())),
        ("productions", Value::Array(productions)),
    ])
}

#[derive(Clone, Debug, PartialEq)]
pub struct Fragment {
    pub uid: String,
    pub production_id: String,
    pub resources: BTreeMap<String, String>,
    pub choices: BTreeMap<String, Value>,
}

impl Fragment {
    pub fn new(
        uid: impl Into<String>,
        production_id: impl Into<String>,
        resources: BTreeMap<String, String>,
        choices: BTreeMap<String, Value>,
    ) -> Self {
        Self {
            uid: uid.into(),
            production_id: production_id.into(),
            resources,
            choices,
        }
    }

    pub fn canonical(&self) -> Value {
        obj(vec![
            ("productionId", Value::String(self.production_id.clone())),
            (
                "resources",
                Value::Object(
                    self.resources
                        .iter()
                        .map(|(key, value)| (key.clone(), Value::String(value.clone())))
                        .collect(),
                ),
            ),
            (
                "choices",
                Value::Object(self.choices.clone().into_iter().collect()),
            ),
        ])
    }

    fn from_canonical(value: &Value, uid: &str) -> Result<Self, GrammarError> {
        let value = object(value, "canonical fragment")?;
        exact_keys(
            &value,
            &["productionId", "resources", "choices"],
            "canonical fragment",
        )?;
        let resources = object(
            required(&value, "resources", "canonical fragment")?,
            "fragment resources",
        )?
        .into_iter()
        .map(|(key, value)| Ok((key, string(&value, "fragment resource")?)))
        .collect::<Result<_, GrammarError>>()?;
        let choices = object(
            required(&value, "choices", "canonical fragment")?,
            "fragment choices",
        )?;
        Ok(Self::new(
            uid,
            string(
                required(&value, "productionId", "canonical fragment")?,
                "fragment production",
            )?,
            resources,
            choices,
        ))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ModuleProgram {
    pub direction: String,
    pub fragments: Vec<Fragment>,
    pub lineage: Vec<Value>,
}

impl ModuleProgram {
    pub fn new(
        direction: impl Into<String>,
        fragments: Vec<Fragment>,
        lineage: Vec<Value>,
    ) -> Self {
        Self {
            direction: direction.into(),
            fragments,
            lineage,
        }
    }

    pub fn canonical(&self) -> Value {
        obj(vec![
            ("schemaVersion", Value::String(GRAMMAR_SCHEMA.to_owned())),
            ("grammarVersion", Value::String(GRAMMAR_VERSION.to_owned())),
            ("direction", Value::String(self.direction.clone())),
            (
                "fragments",
                Value::Array(self.fragments.iter().map(Fragment::canonical).collect()),
            ),
        ])
    }

    pub fn from_canonical(value: &Value) -> Result<Self, GrammarError> {
        let value = object(value, "typed module program")?;
        exact_keys(
            &value,
            &["schemaVersion", "grammarVersion", "direction", "fragments"],
            "typed module program",
        )?;
        if string(
            required(&value, "schemaVersion", "typed module program")?,
            "program schema",
        )? != GRAMMAR_SCHEMA
            || string(
                required(&value, "grammarVersion", "typed module program")?,
                "program grammar version",
            )? != GRAMMAR_VERSION
        {
            return Err(GrammarError::invalid(
                "module program is not canonical typed grammar v2",
            ));
        }
        let direction = string(
            required(&value, "direction", "typed module program")?,
            "program direction",
        )?;
        let fragments = array(
            required(&value, "fragments", "typed module program")?,
            "program fragments",
        )?
        .iter()
        .enumerate()
        .map(|(index, item)| Fragment::from_canonical(item, &format!("read_{index}")))
        .collect::<Result<Vec<_>, _>>()?;
        Ok(Self::new(direction, fragments, vec![]))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct GrammarContext {
    pub instrument: String,
    pub indicators: Vec<Value>,
    pub evidence_groups: Vec<Value>,
    pub event_bindings: Vec<Value>,
    pub execution_config: Value,
    pub budgets: Budgets,
}

impl GrammarContext {
    pub fn new(
        instrument: impl Into<String>,
        indicators: Vec<Value>,
        evidence_groups: Vec<Value>,
        event_bindings: Vec<Value>,
        execution_config: Value,
        budgets: Option<Budgets>,
    ) -> Self {
        Self {
            instrument: instrument.into(),
            indicators,
            evidence_groups,
            event_bindings,
            execution_config,
            budgets: budgets.unwrap_or_default(),
        }
    }

    pub fn normalized(&self) -> Result<Value, GrammarError> {
        let instrument = self.instrument.trim().to_ascii_uppercase();
        if instrument.is_empty() {
            return Err(GrammarError::invalid(
                "fragment context requires one instrument",
            ));
        }
        let indicators = self.indicators.clone();
        let mut groups = self.evidence_groups.clone();
        let mut events = self.event_bindings.clone();
        let execution = Value::Object(
            object(&self.execution_config, "execution config")?
                .into_iter()
                .collect(),
        );
        let indicator_ids = indicators
            .iter()
            .map(|item| {
                let item = object(item, "indicator")?;
                let meta = object(required(&item, "meta", "indicator")?, "indicator meta")?;
                string(
                    required(&meta, "instanceId", "indicator meta")?,
                    "indicator instance id",
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let group_ids = groups
            .iter()
            .map(|item| {
                string(
                    required(&object(item, "evidence group")?, "id", "evidence group")?,
                    "group id",
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let event_ids = events
            .iter()
            .map(|item| {
                string(
                    required(&object(item, "event binding")?, "id", "event binding")?,
                    "event id",
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let execution_map = object(&execution, "execution config")?;
        let library = object(
            required(&execution_map, "managementLibrary", "execution config")?,
            "management library",
        )?;
        let plans = array(
            required(&library, "plans", "management library")?,
            "management plans",
        )?;
        let mut plan_ids = plans
            .iter()
            .map(|item| {
                string(
                    required(&object(item, "management plan")?, "id", "management plan")?,
                    "plan id",
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        plan_ids.sort();
        for (name, values) in [
            ("indicator", &indicator_ids),
            ("group", &group_ids),
            ("event", &event_ids),
            ("plan", &plan_ids),
        ] {
            if values.is_empty()
                || values.iter().any(|item| item.is_empty())
                || values.iter().collect::<BTreeSet<_>>().len() != values.len()
            {
                return Err(GrammarError::invalid(format!(
                    "fragment context {name} identities are missing or duplicate"
                )));
            }
        }
        let known = indicator_ids.iter().collect::<BTreeSet<_>>();
        for group in &groups {
            let group = object(group, "evidence group")?;
            let members = array(
                required(&group, "indicatorInstanceIds", "evidence group")?,
                "group indicators",
            )?;
            if !members.iter().all(|member| {
                string(member, "group indicator")
                    .map(|id| known.contains(&id))
                    .unwrap_or(false)
            }) {
                return Err(GrammarError::invalid(
                    "evidence group has unknown indicator reference",
                ));
            }
        }
        for event in &events {
            let event = object(event, "event binding")?;
            let member = string(
                required(&event, "indicatorInstanceId", "event binding")?,
                "event indicator",
            )?;
            if !known.contains(&member) {
                return Err(GrammarError::invalid(
                    "event binding has unknown indicator reference",
                ));
            }
        }
        groups.sort_by_key(|item| {
            item.get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        });
        events.sort_by_key(|item| {
            item.get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        });
        Ok(obj(vec![
            ("instrument", Value::String(instrument)),
            ("indicators", Value::Array(indicators)),
            ("groups", Value::Array(groups)),
            ("events", Value::Array(events)),
            ("executionConfig", execution),
            (
                "plans",
                Value::Array(plan_ids.into_iter().map(Value::String).collect()),
            ),
            (
                "budgets",
                obj(vec![
                    ("states", self.budgets.states.into()),
                    ("transitions", self.budgets.transitions.into()),
                    ("groups", self.budgets.groups.into()),
                    ("events", self.budgets.events.into()),
                    ("indicators", self.budgets.indicators.into()),
                    ("guardDepth", self.budgets.guard_depth.into()),
                ]),
            ),
        ]))
    }

    pub fn from_normalized(value: &Value) -> Result<Self, GrammarError> {
        let value = object(value, "grammar context")?;
        exact_keys(
            &value,
            &[
                "instrument",
                "indicators",
                "groups",
                "events",
                "executionConfig",
                "plans",
                "budgets",
            ],
            "grammar context",
        )?;
        let budget_value = object(
            required(&value, "budgets", "grammar context")?,
            "grammar budgets",
        )?;
        let budget = |key: &str| -> Result<usize, GrammarError> {
            required(&budget_value, key, "grammar budgets")?
                .as_u64()
                .filter(|item| *item > 0)
                .map(|item| item as usize)
                .ok_or_else(|| GrammarError::invalid("invalid fragment budget"))
        };
        let context = Self::new(
            string(
                required(&value, "instrument", "grammar context")?,
                "instrument",
            )?,
            array(
                required(&value, "indicators", "grammar context")?,
                "indicators",
            )?,
            array(required(&value, "groups", "grammar context")?, "groups")?,
            array(required(&value, "events", "grammar context")?, "events")?,
            required(&value, "executionConfig", "grammar context")?.clone(),
            Some(Budgets {
                states: budget("states")?,
                transitions: budget("transitions")?,
                groups: budget("groups")?,
                events: budget("events")?,
                indicators: budget("indicators")?,
                guard_depth: budget("guardDepth")?,
            }),
        );
        // Require normalized ordering and that no user-controlled plans array
        // can disagree with the authoritative execution config.
        if context.normalized()? != Value::Object(value.clone().into_iter().collect()) {
            return Err(GrammarError::invalid("grammar context is not normalized"));
        }
        Ok(context)
    }
}

fn all(guards: Vec<Value>) -> Value {
    obj(vec![
        ("kind", Value::String("all".to_owned())),
        ("guards", Value::Array(guards)),
    ])
}

fn transition(
    id: String,
    source: String,
    destination: String,
    event_class: &str,
    guard: Value,
    actions: Vec<Value>,
    reason: String,
    priority: i64,
) -> Value {
    obj(vec![
        ("id", Value::String(id)),
        ("sourceStateId", Value::String(source)),
        ("destinationStateId", Value::String(destination)),
        ("eventClass", Value::String(event_class.to_owned())),
        ("priority", priority.into()),
        ("guard", guard),
        ("actions", Value::Array(actions)),
        ("reasonCode", Value::String(reason)),
    ])
}

struct GraphBuilder<'a> {
    fragments: &'a [Fragment],
    registry: BTreeMap<String, FragmentSpec>,
    states: Vec<Value>,
    transitions: Vec<Value>,
    current_watch: String,
    position_state: String,
    position_states: Vec<String>,
    expiring_watches: BTreeSet<String>,
    recovery_ids: Vec<String>,
}

impl<'a> GraphBuilder<'a> {
    fn new(fragments: &'a [Fragment]) -> Self {
        let registry = registry();
        let recovery_ordinals = fragments
            .iter()
            .enumerate()
            .filter_map(|(index, fragment)| {
                (registry
                    .get(&fragment.production_id)
                    .map(|item| item.family.as_str())
                    == Some("recovery"))
                .then_some(index)
            })
            .collect::<Vec<_>>();
        let recovery_ids = recovery_ordinals
            .iter()
            .map(|index| format!("recovery_{index}"))
            .collect();
        Self {
            fragments,
            registry,
            states: vec![
                obj(vec![("id", "ready".into())]),
                obj(vec![("id", "entry_pending".into())]),
                obj(vec![("id", "position_idle".into())]),
                obj(vec![("id", "exit_pending".into())]),
            ],
            transitions: vec![],
            current_watch: "ready".to_owned(),
            position_state: "position_idle".to_owned(),
            position_states: vec!["position_idle".to_owned()],
            expiring_watches: BTreeSet::new(),
            recovery_ids,
        }
    }

    fn recovery_entry(&self) -> String {
        self.recovery_ids
            .first()
            .cloned()
            .unwrap_or_else(|| "ready".to_owned())
    }

    fn predicate(&self, fragment: &Fragment) -> Result<Value, GrammarError> {
        let resource = |slot: &str| {
            fragment
                .resources
                .get(slot)
                .cloned()
                .ok_or_else(|| GrammarError::invalid("fragment resource closure is incomplete"))
        };
        let choice = |key: &str| {
            fragment.choices.get(key).cloned().ok_or_else(|| {
                GrammarError::invalid("fragment choices must use exact named domains")
            })
        };
        let id = fragment.production_id.as_str();
        if id.ends_with("level") || id == "gate_level" {
            return Ok(obj(vec![
                ("kind", "evidence_at_least".into()),
                ("groupId", Value::String(resource("group")?)),
                ("thresholdPercent", choice("threshold")?),
            ]));
        }
        if id == "gate_below" {
            return Ok(obj(vec![
                ("kind", "evidence_below".into()),
                ("groupId", Value::String(resource("group")?)),
                ("thresholdPercent", choice("threshold")?),
            ]));
        }
        if id.ends_with("fresh_event") || id == "enter_on_event" {
            return Ok(obj(vec![
                ("kind", "fresh_event".into()),
                ("eventId", Value::String(resource("event")?)),
            ]));
        }
        if id == "gate_event_window" {
            return Ok(obj(vec![
                ("kind", "event_age_at_most".into()),
                ("eventId", Value::String(resource("event")?)),
                ("events", choice("age")?),
            ]));
        }
        if id == "gate_delay" {
            return Ok(obj(vec![
                ("kind", "state_age_at_least".into()),
                ("events", choice("bars")?),
            ]));
        }
        if id == "gate_streak" {
            return Ok(obj(vec![
                ("kind", "condition_streak_at_least".into()),
                ("groupId", Value::String(resource("group")?)),
                ("comparison", "at_least".into()),
                ("thresholdPercent", choice("threshold")?),
                ("events", choice("bars")?),
            ]));
        }
        if id == "gate_predicate_edge" {
            return Ok(obj(vec![
                ("kind", "predicate_edge".into()),
                (
                    "occurrenceSha256",
                    Value::String(sha(&fragment.canonical())?),
                ),
                ("direction", "rising".into()),
                (
                    "predicate",
                    obj(vec![
                        ("kind", "evidence_at_least".into()),
                        ("groupId", Value::String(resource("group")?)),
                        ("thresholdPercent", choice("threshold")?),
                    ]),
                ),
            ]));
        }
        if id == "enter_on_level_and_event" {
            return Ok(all(vec![
                obj(vec![
                    ("kind", "evidence_at_least".into()),
                    ("groupId", Value::String(resource("group")?)),
                    ("thresholdPercent", choice("threshold")?),
                ]),
                obj(vec![
                    ("kind", "fresh_event".into()),
                    ("eventId", Value::String(resource("event")?)),
                ]),
            ]));
        }
        Err(GrammarError::invalid(
            "fragment predicate production is unimplemented",
        ))
    }

    fn attach(&mut self, ordinal: usize, fragment: &Fragment) -> Result<(), GrammarError> {
        let spec = self
            .registry
            .get(&fragment.production_id)
            .ok_or_else(|| GrammarError::invalid("unsealed fragment production"))?
            .clone();
        let tag = format!("f{ordinal}_{}", spec.production_id);
        match spec.family.as_str() {
            "arm" => {
                let target = format!("watch_{ordinal}");
                self.states
                    .push(obj(vec![("id", Value::String(target.clone()))]));
                self.transitions.push(transition(
                    format!("{tag}_arm"),
                    "ready".to_owned(),
                    target.clone(),
                    "decision",
                    all(vec![
                        obj(vec![
                            ("kind", "position_exists".into()),
                            ("expected", Value::Bool(false)),
                        ]),
                        self.predicate(fragment)?,
                    ]),
                    vec![],
                    format!("{}.armed", spec.production_id),
                    10,
                ));
                self.transitions.push(transition(
                    format!("{tag}_expire"),
                    target.clone(),
                    "ready".to_owned(),
                    "decision",
                    obj(vec![
                        ("kind", "state_age_at_least".into()),
                        ("events", 8.into()),
                    ]),
                    vec![],
                    format!("{}.expired", spec.production_id),
                    90,
                ));
                self.expiring_watches.insert(target.clone());
                self.current_watch = target;
            }
            "gate" => {
                let target = format!("watch_{ordinal}");
                self.states
                    .push(obj(vec![("id", Value::String(target.clone()))]));
                self.transitions.push(transition(
                    format!("{tag}_gate"),
                    self.current_watch.clone(),
                    target.clone(),
                    "decision",
                    self.predicate(fragment)?,
                    vec![],
                    format!("{}.passed", spec.production_id),
                    10,
                ));
                if !self.expiring_watches.contains(&self.current_watch) {
                    self.transitions.push(transition(
                        format!("{tag}_abort"),
                        self.current_watch.clone(),
                        "ready".to_owned(),
                        "decision",
                        obj(vec![
                            ("kind", "state_age_at_least".into()),
                            ("events", 8.into()),
                        ]),
                        vec![],
                        format!("{}.aborted", spec.production_id),
                        90,
                    ));
                    self.expiring_watches.insert(self.current_watch.clone());
                }
                self.current_watch = target;
            }
            "entry" => {
                let plan = fragment.resources.get("plan").cloned().ok_or_else(|| {
                    GrammarError::invalid("fragment resource closure is incomplete")
                })?;
                self.transitions.push(transition(
                    format!("{tag}_entry"),
                    self.current_watch.clone(),
                    "entry_pending".to_owned(),
                    "decision",
                    all(vec![
                        obj(vec![
                            ("kind", "position_exists".into()),
                            ("expected", Value::Bool(false)),
                        ]),
                        self.predicate(fragment)?,
                    ]),
                    vec![obj(vec![
                        ("kind", "enter_next_open".into()),
                        ("managementPlanId", Value::String(plan)),
                    ])],
                    format!("{}.entered", spec.production_id),
                    10,
                ));
                if !self.expiring_watches.contains(&self.current_watch) {
                    self.transitions.push(transition(
                        format!("{tag}_expire"),
                        self.current_watch.clone(),
                        "ready".to_owned(),
                        "decision",
                        obj(vec![
                            ("kind", "state_age_at_least".into()),
                            ("events", 8.into()),
                        ]),
                        vec![],
                        format!("{}.expired", spec.production_id),
                        90,
                    ));
                    self.expiring_watches.insert(self.current_watch.clone());
                }
            }
            "management" => {
                let state = format!("management_{ordinal}");
                let after = format!("position_after_management_{ordinal}");
                self.states
                    .push(obj(vec![("id", Value::String(state.clone()))]));
                self.states
                    .push(obj(vec![("id", Value::String(after.clone()))]));
                let base = if let Some(r) = fragment.choices.get("r") {
                    obj(vec![
                        ("kind", "unrealized_r_at_least".into()),
                        ("value", r.clone()),
                    ])
                } else {
                    obj(vec![
                        ("kind", "position_age_at_least".into()),
                        (
                            "events",
                            fragment
                                .choices
                                .get("bars")
                                .cloned()
                                .unwrap_or_else(|| 1.into()),
                        ),
                    ])
                };
                let action = match spec.production_id.as_str() {
                    "move_break_even" => {
                        obj(vec![("kind", "move_stop_to_break_even_next_open".into())])
                    }
                    "tighten_stop" => obj(vec![
                        ("kind", "tighten_stop_next_open".into()),
                        (
                            "stopLocator",
                            obj(vec![
                                ("kind", "initial_r_multiple".into()),
                                ("multiple", fragment.choices["multiple"].clone()),
                            ]),
                        ),
                    ]),
                    "set_target" => obj(vec![
                        ("kind", "set_target_next_open".into()),
                        (
                            "targetLocator",
                            obj(vec![
                                ("kind", "reward_multiple".into()),
                                ("multiple", fragment.choices["multiple"].clone()),
                            ]),
                        ),
                    ]),
                    "cancel_target" => obj(vec![("kind", "cancel_target_next_open".into())]),
                    "activate_trailing" => {
                        obj(vec![("kind", "activate_trailing_stop_next_open".into())])
                    }
                    _ => obj(vec![("kind", "deactivate_trailing_stop_next_open".into())]),
                };
                self.transitions.push(transition(
                    format!("{tag}_request"),
                    self.position_state.clone(),
                    state.clone(),
                    "decision",
                    all(vec![
                        obj(vec![
                            ("kind", "position_exists".into()),
                            ("expected", Value::Bool(true)),
                        ]),
                        base,
                    ]),
                    vec![action],
                    format!("{}.requested", spec.production_id),
                    10 + ordinal as i64,
                ));
                for (status, priority) in [("applied", 10), ("rejected", 20), ("canceled", 25)] {
                    self.transitions.push(transition(
                        format!("{tag}_{status}"),
                        state.clone(),
                        after.clone(),
                        "execution",
                        obj(vec![
                            ("kind", "execution_status_is".into()),
                            ("status", status.into()),
                        ]),
                        vec![],
                        format!("{}.{}", spec.production_id, status),
                        priority,
                    ));
                }
                self.transitions.push(transition(
                    format!("{tag}_closed"),
                    state,
                    self.recovery_entry(),
                    "execution",
                    obj(vec![
                        ("kind", "execution_status_is".into()),
                        ("status", "closed".into()),
                    ]),
                    vec![],
                    format!("{}.closed", spec.production_id),
                    30,
                ));
                self.position_state = after.clone();
                self.position_states.push(after);
            }
            "exit" => {
                let base = match spec.production_id.as_str() {
                    "exit_on_age" => obj(vec![
                        ("kind", "position_age_at_least".into()),
                        ("events", fragment.choices["bars"].clone()),
                    ]),
                    "exit_on_loss" => obj(vec![
                        ("kind", "unrealized_r_at_most".into()),
                        ("value", fragment.choices["r"].clone()),
                    ]),
                    "exit_on_profit" => obj(vec![
                        ("kind", "unrealized_r_at_least".into()),
                        ("value", fragment.choices["r"].clone()),
                    ]),
                    _ => obj(vec![
                        ("kind", "fresh_event".into()),
                        (
                            "eventId",
                            Value::String(fragment.resources["event"].clone()),
                        ),
                    ]),
                };
                self.transitions.push(transition(
                    format!("{tag}_exit"),
                    self.position_state.clone(),
                    "exit_pending".to_owned(),
                    "decision",
                    all(vec![
                        obj(vec![
                            ("kind", "position_exists".into()),
                            ("expected", Value::Bool(true)),
                        ]),
                        base,
                    ]),
                    vec![obj(vec![("kind", "exit_next_open".into())])],
                    format!("{}.requested", spec.production_id),
                    10 + ordinal as i64,
                ));
            }
            "recovery" => {
                let target = format!("recovery_{ordinal}");
                self.states
                    .push(obj(vec![("id", Value::String(target.clone()))]));
                let recovery_index = self
                    .recovery_ids
                    .iter()
                    .position(|item| item == &target)
                    .expect("recovery ordinal exists");
                let destination = self
                    .recovery_ids
                    .get(recovery_index + 1)
                    .cloned()
                    .unwrap_or_else(|| "ready".to_owned());
                self.transitions.push(transition(
                    format!("{tag}_cooldown"),
                    target,
                    destination,
                    "decision",
                    obj(vec![
                        ("kind", "state_age_at_least".into()),
                        ("events", fragment.choices["bars"].clone()),
                    ]),
                    vec![],
                    "cooldown.elapsed".to_owned(),
                    10,
                ));
            }
            _ => return Err(GrammarError::invalid("unknown sealed fragment family")),
        }
        Ok(())
    }

    fn build(mut self) -> Result<Value, GrammarError> {
        for index in 0..self.fragments.len() {
            let fragment = self.fragments[index].clone();
            self.attach(index, &fragment)?;
        }
        let recovery = self.recovery_entry();
        self.transitions.push(transition(
            "entry_filled".to_owned(),
            "entry_pending".to_owned(),
            "position_idle".to_owned(),
            "execution",
            obj(vec![
                ("kind", "execution_status_is".into()),
                ("status", "filled".into()),
            ]),
            vec![],
            "entry.filled".to_owned(),
            10,
        ));
        self.transitions.push(transition(
            "entry_rejected".to_owned(),
            "entry_pending".to_owned(),
            "ready".to_owned(),
            "execution",
            obj(vec![
                ("kind", "execution_status_is".into()),
                ("status", "rejected".into()),
            ]),
            vec![],
            "entry.rejected".to_owned(),
            20,
        ));
        self.transitions.push(transition(
            "entry_canceled".to_owned(),
            "entry_pending".to_owned(),
            "ready".to_owned(),
            "execution",
            obj(vec![
                ("kind", "execution_status_is".into()),
                ("status", "canceled".into()),
            ]),
            vec![],
            "entry.canceled".to_owned(),
            30,
        ));
        for position in &self.position_states {
            self.transitions.push(transition(
                if position == "position_idle" {
                    "protective_closed".to_owned()
                } else {
                    format!("protective_closed_{position}")
                },
                position.clone(),
                recovery.clone(),
                "execution",
                obj(vec![
                    ("kind", "execution_status_is".into()),
                    ("status", "closed".into()),
                ]),
                vec![],
                "position.protective_closed".to_owned(),
                10,
            ));
        }
        self.transitions.push(transition(
            "exit_closed".to_owned(),
            "exit_pending".to_owned(),
            recovery,
            "execution",
            obj(vec![
                ("kind", "execution_status_is".into()),
                ("status", "closed".into()),
            ]),
            vec![],
            "exit.closed".to_owned(),
            10,
        ));
        self.transitions.push(transition(
            "exit_rejected".to_owned(),
            "exit_pending".to_owned(),
            self.position_state.clone(),
            "execution",
            obj(vec![
                ("kind", "execution_status_is".into()),
                ("status", "rejected".into()),
            ]),
            vec![],
            "exit.rejected".to_owned(),
            20,
        ));
        self.transitions.push(transition(
            "exit_canceled".to_owned(),
            "exit_pending".to_owned(),
            self.position_state.clone(),
            "execution",
            obj(vec![
                ("kind", "execution_status_is".into()),
                ("status", "canceled".into()),
            ]),
            vec![],
            "exit.canceled".to_owned(),
            30,
        ));
        Ok(obj(vec![
            ("states", Value::Array(self.states)),
            ("transitions", Value::Array(self.transitions)),
        ]))
    }
}

pub struct TypedFragmentGrammar<'a> {
    pub context: Value,
    pub context_sha256: String,
    native_authority: &'a dyn NativeValidator,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CompiledModule {
    pub profile: Value,
    pub program: Value,
    pub lineage: Vec<Value>,
    pub identities: Value,
    pub activation_witnesses: Vec<Value>,
    pub native_report: Value,
}

impl<'a> TypedFragmentGrammar<'a> {
    pub fn new(
        context: GrammarContext,
        native_authority: &'a dyn NativeValidator,
    ) -> Result<Self, GrammarError> {
        let context = context.normalized()?;
        let context_sha256 = sha(&context)?;
        Ok(Self {
            context,
            context_sha256,
            native_authority,
        })
    }

    fn fragment(
        &self,
        production_id: &str,
        uid: &str,
        resources: BTreeMap<String, String>,
        choices: Option<BTreeMap<String, Value>>,
    ) -> Result<Fragment, GrammarError> {
        let spec = registry()
            .get(production_id)
            .cloned()
            .ok_or_else(|| GrammarError::invalid("unsealed fragment production"))?;
        let choices = choices.unwrap_or_else(|| {
            spec.choice_domains
                .iter()
                .map(|(key, values)| (key.clone(), values[0].clone()))
                .collect()
        });
        Ok(Fragment::new(uid, production_id, resources, choices))
    }

    pub fn seed(
        &self,
        direction: &str,
        name: &str,
        group_id: Option<&str>,
        event_id: Option<&str>,
        plan_id: Option<&str>,
    ) -> Result<ModuleProgram, GrammarError> {
        let recipes = BTreeMap::from([
            (
                "mean_reversion",
                vec![
                    "arm_level",
                    "gate_event_window",
                    "enter_on_level_and_event",
                    "exit_on_age",
                ],
            ),
            (
                "breakout",
                vec![
                    "arm_fresh_event",
                    "gate_delay",
                    "enter_on_level",
                    "exit_on_profit",
                ],
            ),
            (
                "trend",
                vec![
                    "arm_level",
                    "gate_below",
                    "enter_on_event",
                    "exit_on_signal",
                ],
            ),
        ]);
        let recipe = recipes
            .get(name)
            .ok_or_else(|| GrammarError::invalid("unknown registry seed"))?;
        let groups = array(self.context.get("groups").expect("normalized"), "groups")?;
        let events = array(self.context.get("events").expect("normalized"), "events")?;
        let plans = array(self.context.get("plans").expect("normalized"), "plans")?;
        let group_ids = groups
            .iter()
            .map(|item| string(item.get("id").expect("validated"), "group id"))
            .collect::<Result<Vec<_>, _>>()?;
        let event_ids = events
            .iter()
            .map(|item| string(item.get("id").expect("validated"), "event id"))
            .collect::<Result<Vec<_>, _>>()?;
        let plan_ids = plans
            .iter()
            .map(|item| string(item, "plan id"))
            .collect::<Result<Vec<_>, _>>()?;
        let group = group_id.unwrap_or(&group_ids[0]).to_owned();
        let event = event_id.unwrap_or(&event_ids[0]).to_owned();
        let plan = plan_id.unwrap_or(&plan_ids[0]).to_owned();
        if !group_ids.contains(&group) || !event_ids.contains(&event) || !plan_ids.contains(&plan) {
            return Err(GrammarError::invalid(
                "registry seed resource binding is outside the frozen context",
            ));
        }
        let table = registry();
        let mut fragments = vec![];
        for (index, production) in recipe.iter().enumerate() {
            let resources = table[*production]
                .resource_slots
                .iter()
                .map(|slot| {
                    (
                        slot.clone(),
                        match slot.as_str() {
                            "group" => group.clone(),
                            "event" => event.clone(),
                            "plan" => plan.clone(),
                            _ => unreachable!(),
                        },
                    )
                })
                .collect();
            fragments.push(self.fragment(production, &format!("seed_{index}"), resources, None)?);
        }
        let program = ModuleProgram::new(
            direction,
            fragments,
            vec![obj(vec![
                ("operation", "seed".into()),
                ("seed", Value::String(name.to_owned())),
            ])],
        );
        self.validate(&program)?;
        Ok(program)
    }

    pub fn canonical_program(&self, program: &ModuleProgram) -> Result<Value, GrammarError> {
        self.validate(program)?;
        Ok(program.canonical())
    }

    pub fn validate(&self, program: &ModuleProgram) -> Result<(), GrammarError> {
        if program.direction != "long" && program.direction != "short" {
            return Err(GrammarError::invalid(
                "module direction must be long or short",
            ));
        }
        if program.fragments.is_empty() {
            return Err(GrammarError::invalid("module has no fragments"));
        }
        let table = registry();
        let mut counts = BTreeMap::<String, usize>::new();
        let mut families = vec![];
        for fragment in &program.fragments {
            let spec = table.get(&fragment.production_id).ok_or_else(|| {
                GrammarError::invalid("fragment production exceeds sealed budget")
            })?;
            *counts.entry(fragment.production_id.clone()).or_default() += 1;
            families.push(spec.family.clone());
            let actual_resources = fragment.resources.keys().cloned().collect::<BTreeSet<_>>();
            let expected_resources = spec.resource_slots.iter().cloned().collect::<BTreeSet<_>>();
            if actual_resources != expected_resources {
                return Err(GrammarError::invalid(
                    "fragment resource closure is incomplete",
                ));
            }
            let actual_choices = fragment.choices.keys().cloned().collect::<BTreeSet<_>>();
            let expected_choices = spec.choice_domains.keys().cloned().collect::<BTreeSet<_>>();
            if actual_choices != expected_choices {
                return Err(GrammarError::invalid(
                    "fragment choices must use exact named domains",
                ));
            }
            for (key, domain) in &spec.choice_domains {
                if !domain.contains(&fragment.choices[key]) {
                    return Err(GrammarError::invalid(
                        "fragment choice is outside its named domain",
                    ));
                }
            }
        }
        if counts
            .iter()
            .any(|(production, count)| *count > table[production].max_instances)
        {
            return Err(GrammarError::invalid(
                "fragment production exceeds sealed budget",
            ));
        }
        if families
            .iter()
            .filter(|item| item.as_str() == "arm")
            .count()
            != 1
            || families
                .iter()
                .filter(|item| item.as_str() == "entry")
                .count()
                != 1
            || !families.iter().any(|item| item == "exit")
        {
            return Err(GrammarError::invalid(
                "module requires exactly one arm and entry plus at least one exit",
            ));
        }
        let rank = |family: &str| match family {
            "arm" => 0,
            "gate" => 1,
            "entry" => 2,
            "management" => 3,
            "exit" => 4,
            "recovery" => 5,
            _ => 6,
        };
        if families
            .windows(2)
            .any(|items| rank(&items[0]) > rank(&items[1]))
        {
            return Err(GrammarError::invalid(
                "fragment lifecycle order is incompatible",
            ));
        }
        let mut watch_port = Port::Ready;
        for fragment in &program.fragments {
            let spec = &table[&fragment.production_id];
            match spec.family.as_str() {
                "arm" | "gate" | "entry" => {
                    if spec.consumes != watch_port {
                        return Err(GrammarError::invalid(
                            "fragment port consume is incompatible with entry lifecycle",
                        ));
                    }
                    watch_port = spec.produces;
                }
                "management" | "exit"
                    if spec.consumes != Port::PositionIdle
                        || (spec.produces != Port::ManagementPending
                            && spec.produces != Port::ExitPending) =>
                {
                    return Err(GrammarError::invalid(
                        "fragment port consume is incompatible with position lifecycle",
                    ));
                }
                "recovery"
                    if spec.consumes != Port::Recovery || spec.produces != Port::Recovery =>
                {
                    return Err(GrammarError::invalid(
                        "fragment port consume is incompatible with recovery lifecycle",
                    ));
                }
                _ => {}
            }
        }
        if watch_port != Port::EntryPending {
            return Err(GrammarError::invalid(
                "entry lifecycle does not terminate in entry pending",
            ));
        }
        let resources = |key: &str| -> Result<BTreeSet<String>, GrammarError> {
            array(self.context.get(key).expect("normalized"), key)?
                .iter()
                .map(|item| match key {
                    "plans" => string(item, "plan id"),
                    _ => string(
                        item.get("id")
                            .ok_or_else(|| GrammarError::invalid("context resource missing id"))?,
                        "context resource",
                    ),
                })
                .collect()
        };
        let known = BTreeMap::from([
            ("group", resources("groups")?),
            ("event", resources("events")?),
            ("plan", resources("plans")?),
        ]);
        for fragment in &program.fragments {
            for (slot, value) in &fragment.resources {
                if !known
                    .get(slot.as_str())
                    .is_some_and(|known_values| known_values.contains(value))
                {
                    return Err(GrammarError::invalid(
                        "fragment resource reference is unknown or incompatible",
                    ));
                }
            }
        }
        let built = GraphBuilder::new(&program.fragments).build()?;
        let budgets = object(self.context.get("budgets").expect("normalized"), "budgets")?;
        let budget = |key: &str| budgets[key].as_u64().expect("normalized") as usize;
        if array(built.get("states").expect("built"), "states")?.len() > budget("states")
            || array(built.get("transitions").expect("built"), "transitions")?.len()
                > budget("transitions")
        {
            return Err(GrammarError::invalid(
                "module exceeds per-side graph budget",
            ));
        }
        if array(self.context.get("groups").expect("normalized"), "groups")?.len()
            > budget("groups")
            || array(self.context.get("events").expect("normalized"), "events")?.len()
                > budget("events")
            || array(
                self.context.get("indicators").expect("normalized"),
                "indicators",
            )?
            .len()
                > budget("indicators")
        {
            return Err(GrammarError::invalid(
                "context exceeds per-side resource budget",
            ));
        }
        if array(built.get("transitions").expect("built"), "transitions")?
            .iter()
            .any(|item| {
                guard_depth(item.get("guard").expect("transition guard")) > budget("guardDepth")
            })
        {
            return Err(GrammarError::invalid("fragment guard depth exceeds budget"));
        }
        validate_entry_route_decision_indicator_cap(&obj(vec![
            (
                "indicators",
                self.context.get("indicators").expect("normalized").clone(),
            ),
            (
                "graph",
                obj(vec![
                    ("initialStateId", "ready".into()),
                    (
                        "evidenceGroups",
                        self.context.get("groups").expect("normalized").clone(),
                    ),
                    (
                        "eventBindings",
                        self.context.get("events").expect("normalized").clone(),
                    ),
                    (
                        "transitions",
                        built.get("transitions").expect("built").clone(),
                    ),
                ]),
            ),
        ]))?;
        Ok(())
    }

    fn profile_payload(
        &self,
        program: &ModuleProgram,
    ) -> Result<(Value, Value, Value), GrammarError> {
        self.validate(program)?;
        let canonical = program.canonical();
        let built = GraphBuilder::new(&program.fragments).build()?;
        let closure = resource_closed_context(&self.context, &program.fragments)?;
        let profile = obj(vec![
            ("version", "v2".into()),
            (
                "name",
                Value::String(format!("typed fragment {} module", program.direction)),
            ),
            (
                "description",
                "sealed typed fragment module; not an economic candidate".into(),
            ),
            (
                "instruments",
                Value::Array(vec![
                    self.context.get("instrument").expect("normalized").clone(),
                ]),
            ),
            ("directionMode", Value::String(program.direction.clone())),
            ("isActive", Value::Bool(false)),
            (
                "indicators",
                closure.get("indicators").expect("closure").clone(),
            ),
            (
                "executionConfig",
                closure.get("executionConfig").expect("closure").clone(),
            ),
            (
                "graph",
                obj(vec![
                    ("kind", "temporal_graph_v1".into()),
                    ("semanticPolicy", "temporal_graph_semantics_v1".into()),
                    ("eventSchema", "temporal_event_v1".into()),
                    ("factLibrary", "temporal_market_facts_v1".into()),
                    ("guardLibrary", "temporal_guards_v1".into()),
                    ("actionLibrary", "temporal_market_actions_v1".into()),
                    ("clockRequirement", "clock.completed_bar".into()),
                    (
                        "fidelityRequirements",
                        Value::Array(vec!["data.completed_ohlc".into()]),
                    ),
                    ("initialStateId", "ready".into()),
                    ("states", built.get("states").expect("built").clone()),
                    (
                        "evidenceGroups",
                        closure.get("groups").expect("closure").clone(),
                    ),
                    (
                        "eventBindings",
                        closure.get("events").expect("closure").clone(),
                    ),
                    (
                        "transitions",
                        built.get("transitions").expect("built").clone(),
                    ),
                ]),
            ),
        ]);
        Ok((canonical, built, profile))
    }

    pub fn materialize_profile(&self, program: &ModuleProgram) -> Result<Value, GrammarError> {
        Ok(self.profile_payload(program)?.2)
    }

    fn compiled(
        &self,
        program: &ModuleProgram,
        canonical: Value,
        profile: Value,
        report: Value,
        candidate_id: &str,
    ) -> Result<CompiledModule, GrammarError> {
        let report_map = object(&report, "native validator report")?;
        let raw_sha = sha(&profile)?;
        if string(
            required(&report_map, "schemaVersion", "native validator report")?,
            "native validator report schema",
        )? != "temporal_search_candidate_validation_v1"
            || string(
                required(&report_map, "candidateId", "native validator report")?,
                "native candidate id",
            )? != candidate_id
            || required(
                &report_map,
                "rawSourceProfileSha256",
                "native validator report",
            )? != &Value::String(raw_sha.clone())
            || string(
                required(&report_map, "status", "native validator report")?,
                "native report status",
            )? != "valid_evaluable"
            || !matches!(
                report_map.get("candidateAcceptable"),
                Some(Value::Bool(true))
            )
            || report_map
                .get("evaluatorId")
                .and_then(Value::as_str)
                .filter(|item| !item.is_empty())
                .is_none()
            || !report_map
                .get("programSha256")
                .and_then(Value::as_str)
                .is_some_and(|item| item.starts_with("sha256:"))
            || !report_map
                .get("validationReportSha256")
                .and_then(Value::as_str)
                .is_some_and(|item| item.starts_with("sha256:"))
        {
            return Err(GrammarError::invalid(
                "Dashboard native validator rejected or failed to bind module identity",
            ));
        }
        let identities = obj(vec![
            ("contextSha256", Value::String(self.context_sha256.clone())),
            ("programSha256", Value::String(sha(&canonical)?)),
            ("rawModuleSha256", Value::String(raw_sha)),
            (
                "nativeProgramSha256",
                report_map.get("programSha256").expect("checked").clone(),
            ),
            (
                "nativeValidationReportSha256",
                report_map
                    .get("validationReportSha256")
                    .expect("checked")
                    .clone(),
            ),
            (
                "compiledGraphStructureSha256",
                Value::String(compiled_graph_signature(&profile)?),
            ),
        ]);
        let table = registry();
        let witnesses = program
            .fragments
            .iter()
            .map(|fragment| {
                obj(vec![
                    ("schemaVersion", Value::String(WITNESS_SCHEMA.to_owned())),
                    (
                        "productionId",
                        Value::String(fragment.production_id.clone()),
                    ),
                    (
                        "recipe",
                        table[&fragment.production_id].activation_recipe.clone(),
                    ),
                    ("fragment", fragment.canonical()),
                ])
            })
            .collect();
        Ok(CompiledModule {
            profile,
            program: canonical,
            lineage: program.lineage.clone(),
            identities,
            activation_witnesses: witnesses,
            native_report: report,
        })
    }

    pub fn compile_module(
        &self,
        program: &ModuleProgram,
        candidate_id: &str,
    ) -> Result<CompiledModule, GrammarError> {
        let (canonical, _, profile) = self.profile_payload(program)?;
        let report = self.native_authority.validate_v2(&profile, candidate_id)?;
        self.compiled(program, canonical, profile, report, candidate_id)
    }

    pub fn enumerate_operations(
        &self,
        program: &ModuleProgram,
    ) -> Result<Vec<Value>, GrammarError> {
        self.validate(program)?;
        let table = registry();
        let mut rows = vec![];
        let mut counts = BTreeMap::<String, usize>::new();
        for item in &program.fragments {
            *counts.entry(item.production_id.clone()).or_default() += 1;
        }
        for (index, item) in program.fragments.iter().enumerate() {
            let spec = &table[&item.production_id];
            for (id, target) in &table {
                if target.family == spec.family && id != &item.production_id {
                    rows.push(obj(vec![
                        ("operation", "substitute".into()),
                        ("index", index.into()),
                        ("productionId", Value::String(id.clone())),
                    ]));
                }
            }
            for slot in &spec.resource_slots {
                let values = self
                    .context
                    .get(match slot.as_str() {
                        "group" => "groups",
                        "event" => "events",
                        "plan" => "plans",
                        _ => unreachable!(),
                    })
                    .expect("normalized");
                for value in array(values, "context choices")? {
                    let id = if slot == "plan" {
                        string(&value, "plan id")?
                    } else {
                        string(value.get("id").expect("context id"), "context id")?
                    };
                    if item.resources.get(slot) != Some(&id) {
                        rows.push(obj(vec![
                            ("operation", "rebind".into()),
                            ("index", index.into()),
                            ("slot", Value::String(slot.clone())),
                            ("value", Value::String(id)),
                        ]));
                    }
                }
            }
            for (choice, domain) in &spec.choice_domains {
                for value in domain {
                    if item.choices.get(choice) != Some(value) {
                        rows.push(obj(vec![
                            ("operation", "mutate_choice".into()),
                            ("index", index.into()),
                            ("choice", Value::String(choice.clone())),
                            ("value", value.clone()),
                        ]));
                    }
                }
            }
            if ["gate", "management", "exit", "recovery"].contains(&spec.family.as_str())
                && counts[&item.production_id] < spec.max_instances
            {
                rows.push(obj(vec![
                    ("operation", "duplicate_specialize".into()),
                    ("index", index.into()),
                ]));
            }
        }
        for index in 0..program.fragments.len().saturating_sub(1) {
            let left = &program.fragments[index];
            let right = &program.fragments[index + 1];
            if table[&left.production_id].family == table[&right.production_id].family
                && left.canonical() != right.canonical()
            {
                rows.push(obj(vec![
                    ("operation", "move".into()),
                    ("from", index.into()),
                    ("to", (index + 1).into()),
                ]));
            }
        }
        for (production, spec) in &table {
            if ["gate", "management", "exit", "recovery"].contains(&spec.family.as_str())
                && counts.get(production).copied().unwrap_or_default() < spec.max_instances
            {
                rows.push(obj(vec![
                    (
                        "operation",
                        if ["management", "exit"].contains(&spec.family.as_str()) {
                            "add_branch".into()
                        } else {
                            "insert".into()
                        },
                    ),
                    ("productionId", Value::String(production.clone())),
                ]));
            }
        }
        for (index, item) in program.fragments.iter().enumerate() {
            if ["gate", "management", "exit", "recovery"]
                .contains(&table[&item.production_id].family.as_str())
            {
                rows.push(obj(vec![
                    (
                        "operation",
                        if ["management", "exit"]
                            .contains(&table[&item.production_id].family.as_str())
                        {
                            "remove_branch".into()
                        } else {
                            "remove".into()
                        },
                    ),
                    ("index", index.into()),
                ]));
            }
        }
        rows.sort_by_key(|row| sha(row).expect("finite generated operation"));
        Ok(rows)
    }

    pub fn apply(
        &self,
        program: &ModuleProgram,
        operation: &Value,
    ) -> Result<ModuleProgram, GrammarError> {
        if !self.enumerate_operations(program)?.contains(operation) {
            return Err(GrammarError::invalid(
                "operation is not canonical and applicable",
            ));
        }
        let operation_map = object(operation, "operation")?;
        let op = string(
            required(&operation_map, "operation", "operation")?,
            "operation kind",
        )?;
        let mut items = program.fragments.clone();
        let table = registry();
        if op == "move" {
            let source = required(&operation_map, "from", "move")?
                .as_u64()
                .ok_or_else(|| GrammarError::invalid("move index is invalid"))?
                as usize;
            let target = required(&operation_map, "to", "move")?
                .as_u64()
                .ok_or_else(|| GrammarError::invalid("move index is invalid"))?
                as usize;
            let item = items.remove(source);
            items.insert(target, item);
        } else if op == "insert" || op == "add_branch" {
            let production = string(
                required(&operation_map, "productionId", "insert")?,
                "production id",
            )?;
            let spec = &table[&production];
            let resources = spec
                .resource_slots
                .iter()
                .map(|slot| {
                    let value = self
                        .context
                        .get(match slot.as_str() {
                            "group" => "groups",
                            "event" => "events",
                            "plan" => "plans",
                            _ => unreachable!(),
                        })
                        .expect("normalized");
                    let first = array(value, "context resources")
                        .expect("normalized")
                        .first()
                        .expect("nonempty")
                        .clone();
                    let id = if slot == "plan" {
                        string(&first, "plan").expect("normalized")
                    } else {
                        string(first.get("id").expect("id"), "resource").expect("normalized")
                    };
                    (slot.clone(), id)
                })
                .collect();
            items.push(self.fragment(
                &production,
                &format!("edit_{}", items.len()),
                resources,
                None,
            )?);
        } else {
            let index = required(&operation_map, "index", "operation")?
                .as_u64()
                .ok_or_else(|| GrammarError::invalid("operation index is invalid"))?
                as usize;
            let item = items[index].clone();
            match op.as_str() {
                "remove" | "remove_branch" => {
                    items.remove(index);
                }
                "substitute" => {
                    let production = string(
                        required(&operation_map, "productionId", "substitute")?,
                        "production id",
                    )?;
                    let spec = &table[&production];
                    let resources = spec
                        .resource_slots
                        .iter()
                        .map(|slot| {
                            let value = item.resources.get(slot).cloned().unwrap_or_else(|| {
                                let options = self
                                    .context
                                    .get(match slot.as_str() {
                                        "group" => "groups",
                                        "event" => "events",
                                        "plan" => "plans",
                                        _ => unreachable!(),
                                    })
                                    .expect("normalized");
                                let first = array(options, "resource")
                                    .expect("normalized")
                                    .first()
                                    .expect("nonempty")
                                    .clone();
                                if slot == "plan" {
                                    string(&first, "plan").expect("valid")
                                } else {
                                    string(first.get("id").expect("id"), "resource").expect("valid")
                                }
                            });
                            (slot.clone(), value)
                        })
                        .collect();
                    items[index] = self.fragment(&production, &item.uid, resources, None)?;
                }
                "rebind" => {
                    let slot = string(required(&operation_map, "slot", "rebind")?, "rebind slot")?;
                    let value =
                        string(required(&operation_map, "value", "rebind")?, "rebind value")?;
                    items[index].resources.insert(slot, value);
                }
                "mutate_choice" => {
                    let choice = string(required(&operation_map, "choice", "mutation")?, "choice")?;
                    let value = required(&operation_map, "value", "mutation")?.clone();
                    items[index].choices.insert(choice, value);
                }
                "duplicate_specialize" => {
                    let spec = &table[&item.production_id];
                    let mut choices = item.choices.clone();
                    if let Some(key) = spec.choice_domains.keys().next() {
                        let domain = &spec.choice_domains[key];
                        let source = choices[key].clone();
                        let position = domain
                            .iter()
                            .position(|item| item == &source)
                            .expect("validated");
                        choices.insert(key.clone(), domain[(position + 1) % domain.len()].clone());
                    }
                    items.push(Fragment::new(
                        format!("duplicate_{}", items.len()),
                        item.production_id,
                        item.resources,
                        choices,
                    ));
                }
                _ => return Err(GrammarError::invalid("unsupported canonical operation")),
            }
        }
        let rank = |fragment: &Fragment| match table[&fragment.production_id].family.as_str() {
            "arm" => 0,
            "gate" => 1,
            "entry" => 2,
            "management" => 3,
            "exit" => 4,
            "recovery" => 5,
            _ => 6,
        };
        // `move` is the one operation whose caller-selected adjacent order is
        // semantic.  Every additive/replacement edit instead re-enters the
        // grammar's canonical lifecycle order, exactly as the Python oracle.
        if op != "move" {
            items.sort_by_key(|item| {
                (
                    rank(item),
                    item.production_id.clone(),
                    sha(&item.canonical()).expect("finite fragment"),
                )
            });
        }
        let mut lineage = program.lineage.clone();
        lineage.push(obj(vec![
            ("operation", Value::String(op)),
            ("details", operation.clone()),
            (
                "parentProgramSha256",
                Value::String(sha(&program.canonical())?),
            ),
        ]));
        let child = ModuleProgram::new(program.direction.clone(), items, lineage);
        self.validate(&child)?;
        Ok(child)
    }

    pub fn crossover(
        &self,
        left: &ModuleProgram,
        right: &ModuleProgram,
        direction: &str,
    ) -> Result<ModuleProgram, GrammarError> {
        self.validate(left)?;
        self.validate(right)?;
        let table = registry();
        let mut fragments = left
            .fragments
            .iter()
            .filter(|item| {
                ["arm", "gate", "entry"].contains(&table[&item.production_id].family.as_str())
            })
            .cloned()
            .collect::<Vec<_>>();
        fragments.extend(
            right
                .fragments
                .iter()
                .filter(|item| {
                    ["management", "exit", "recovery"]
                        .contains(&table[&item.production_id].family.as_str())
                })
                .cloned(),
        );
        let program = ModuleProgram::new(
            direction,
            fragments,
            vec![obj(vec![
                ("operation", "crossover".into()),
                ("leftProgramSha256", Value::String(sha(&left.canonical())?)),
                (
                    "rightProgramSha256",
                    Value::String(sha(&right.canonical())?),
                ),
            ])],
        );
        self.validate(&program)?;
        Ok(program)
    }

    /// Enumerate the finite grammar product in a seeded canonical order.
    /// This deliberately never retries after a native rejection: the caller
    /// receives only deterministic, resource-closed programs here.
    pub fn generate(&self, count: usize, seed: u64) -> Result<Vec<ModuleProgram>, GrammarError> {
        if !(1..=4096).contains(&count) {
            return Err(GrammarError::invalid("generation count must be 1..4096"));
        }
        let table = registry();
        let roots = [
            ("long", "mean_reversion"),
            ("long", "breakout"),
            ("long", "trend"),
            ("short", "mean_reversion"),
            ("short", "breakout"),
            ("short", "trend"),
        ];
        let gates = table
            .values()
            .filter(|item| item.family == "gate")
            .map(|item| item.production_id.clone())
            .collect::<Vec<_>>();
        let managements = table
            .values()
            .filter(|item| item.family == "management")
            .map(|item| item.production_id.clone())
            .collect::<Vec<_>>();
        let exits = table
            .values()
            .filter(|item| item.family == "exit")
            .map(|item| item.production_id.clone())
            .collect::<Vec<_>>();
        let mut variants = vec![];
        for root in roots {
            for gate in &gates {
                for management in &managements {
                    for exit in &exits {
                        for cooldown_count in 0..5 {
                            for choice_index in 0..4 {
                                let material = obj(vec![
                                    ("seed", seed.into()),
                                    (
                                        "variant",
                                        Value::Array(vec![
                                            Value::Array(vec![root.0.into(), root.1.into()]),
                                            Value::String(gate.clone()),
                                            Value::String(management.clone()),
                                            Value::String(exit.clone()),
                                            cooldown_count.into(),
                                            choice_index.into(),
                                        ]),
                                    ),
                                ]);
                                variants.push((
                                    sha(&material)?,
                                    root.0,
                                    root.1,
                                    gate.clone(),
                                    management.clone(),
                                    exit.clone(),
                                    cooldown_count,
                                    choice_index,
                                ));
                            }
                        }
                    }
                }
            }
        }
        variants.sort_by_key(|item| item.0.clone());
        let quotas = BTreeMap::from([("long", count / 2 + count % 2), ("short", count / 2)]);
        let mut used = BTreeMap::from([("long", 0usize), ("short", 0usize)]);
        let mut output = vec![];
        let mut seen = BTreeSet::new();
        for (_, direction, name, gate, management, exit, cooldown_count, choice_index) in variants {
            if output.len() >= count {
                break;
            }
            if used[direction] >= quotas[direction] {
                continue;
            }
            let mut program = self.seed(direction, name, None, None, None)?;
            for production in [gate, management, exit] {
                let operation = self
                    .enumerate_operations(&program)?
                    .into_iter()
                    .find(|item| {
                        item.get("productionId") == Some(&Value::String(production.clone()))
                            && matches!(
                                item.get("operation").and_then(Value::as_str),
                                Some("insert") | Some("add_branch")
                            )
                    })
                    .expect("finite product production is applicable");
                program = self.apply(&program, &operation)?;
            }
            for _ in 0..cooldown_count {
                let Some(operation) =
                    self.enumerate_operations(&program)?
                        .into_iter()
                        .find(|item| {
                            item.get("operation") == Some(&Value::String("insert".to_owned()))
                                && item.get("productionId")
                                    == Some(&Value::String("cooldown".to_owned()))
                        })
                else {
                    break;
                };
                program = self.apply(&program, &operation)?;
            }
            let mutations = self
                .enumerate_operations(&program)?
                .into_iter()
                .filter(|item| {
                    item.get("operation") == Some(&Value::String("mutate_choice".to_owned()))
                })
                .collect::<Vec<_>>();
            if !mutations.is_empty() {
                program = self.apply(&program, &mutations[choice_index % mutations.len()])?;
            }
            let program_sha256 = sha(&program.canonical())?;
            if !seen.insert(program_sha256) {
                continue;
            }
            *used.get_mut(direction).expect("known direction") += 1;
            output.push(program);
        }
        if output.len() != count {
            return Err(GrammarError::invalid(
                "finite generator has insufficient valid variants",
            ));
        }
        Ok(output)
    }

    /// Invoke the injected canonical Dashboard pair compiler after verifying
    /// that these are exactly one long and one short validated v2 modules.
    pub fn compile_pair(
        &self,
        long: &CompiledModule,
        short: &CompiledModule,
        candidate_id: &str,
        authority: &dyn PairCompiler,
    ) -> Result<Value, GrammarError> {
        if long.program.get("direction") != Some(&Value::String("long".to_owned()))
            || short.program.get("direction") != Some(&Value::String("short".to_owned()))
        {
            return Err(GrammarError::invalid(
                "economic candidates require one long and one short module",
            ));
        }
        let result = object(
            &authority.compile_pair(&long.profile, &short.profile, candidate_id)?,
            "pair authority result",
        )?;
        let profile = required(&result, "profile", "pair authority result")?.clone();
        let validation = required(&result, "validation", "pair authority result")?.clone();
        let validation_map = object(&validation, "pair validation")?;
        if profile.get("version") != Some(&Value::String("v3".to_owned()))
            || profile.get("directionMode") != Some(&Value::String("both".to_owned()))
            || validation_map.get("schemaVersion")
                != Some(&Value::String(
                    "temporal_search_candidate_validation_v1".to_owned(),
                ))
            || validation_map.get("candidateId") != Some(&Value::String(candidate_id.to_owned()))
            || validation_map.get("rawSourceProfileSha256") != Some(&Value::String(sha(&profile)?))
            || validation_map.get("status") != Some(&Value::String("valid_evaluable".to_owned()))
            || validation_map.get("candidateAcceptable") != Some(&Value::Bool(true))
            || validation_map
                .get("evaluatorId")
                .and_then(Value::as_str)
                .filter(|item| !item.is_empty())
                .is_none()
            || !validation_map
                .get("programSha256")
                .and_then(Value::as_str)
                .is_some_and(|item| item.starts_with("sha256:"))
            || !validation_map
                .get("validationReportSha256")
                .and_then(Value::as_str)
                .is_some_and(|item| item.starts_with("sha256:"))
        {
            return Err(GrammarError::invalid(
                "canonical Dashboard pair compiler rejected pair",
            ));
        }
        let manifests = profile
            .get("graph")
            .and_then(Value::as_object)
            .and_then(|graph| graph.get("entryArbitration"))
            .and_then(Value::as_object)
            .and_then(|arbitration| arbitration.get("modules"))
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let sources = manifests
            .into_iter()
            .filter_map(|item| {
                Some((
                    item.get("direction")?.as_str()?.to_owned(),
                    item.get("sourceProfileSnapshotSha256")?
                        .as_str()?
                        .to_owned(),
                ))
            })
            .collect::<BTreeMap<_, _>>();
        if sources.get("long").map(String::as_str)
            != long
                .native_report
                .get("profileSnapshotSha256")
                .and_then(Value::as_str)
            || sources.get("short").map(String::as_str)
                != short
                    .native_report
                    .get("profileSnapshotSha256")
                    .and_then(Value::as_str)
        {
            return Err(GrammarError::invalid(
                "canonical pair compiler did not bind both native module identities",
            ));
        }
        Ok(obj(vec![
            ("profile", profile.clone()),
            ("validation", validation.clone()),
            (
                "identities",
                obj(vec![
                    (
                        "longModuleSha256",
                        long.identities
                            .get("rawModuleSha256")
                            .expect("compiled identity")
                            .clone(),
                    ),
                    (
                        "shortModuleSha256",
                        short
                            .identities
                            .get("rawModuleSha256")
                            .expect("compiled identity")
                            .clone(),
                    ),
                    ("rawPairSha256", Value::String(sha(&profile)?)),
                    (
                        "nativeProgramSha256",
                        validation_map
                            .get("programSha256")
                            .expect("validated")
                            .clone(),
                    ),
                    (
                        "nativeValidationReportSha256",
                        validation_map
                            .get("validationReportSha256")
                            .expect("validated")
                            .clone(),
                    ),
                ]),
            ),
        ]))
    }
}

fn guard_depth(value: &Value) -> usize {
    let guard = value.as_object();
    let kind = guard
        .and_then(|item| item.get("kind"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    if ["all", "any"].contains(&kind) {
        1 + guard
            .and_then(|item| item.get("guards"))
            .and_then(Value::as_array)
            .map(|items| items.iter().map(guard_depth).max().unwrap_or(0))
            .unwrap_or(0)
    } else if ["predicate_edge", "consecutive_true"].contains(&kind) {
        1 + guard
            .and_then(|item| item.get("predicate"))
            .map(guard_depth)
            .unwrap_or(0)
    } else {
        1
    }
}

fn resource_closed_context(context: &Value, fragments: &[Fragment]) -> Result<Value, GrammarError> {
    let groups = fragments
        .iter()
        .filter_map(|item| item.resources.get("group").cloned())
        .collect::<BTreeSet<_>>();
    let events = fragments
        .iter()
        .filter_map(|item| item.resources.get("event").cloned())
        .collect::<BTreeSet<_>>();
    let plans = fragments
        .iter()
        .filter_map(|item| item.resources.get("plan").cloned())
        .collect::<BTreeSet<_>>();
    let selected_groups = array(context.get("groups").expect("normalized"), "groups")?
        .into_iter()
        .filter(|item| {
            item.get("id")
                .and_then(Value::as_str)
                .is_some_and(|id| groups.contains(id))
        })
        .collect::<Vec<_>>();
    let selected_events = array(context.get("events").expect("normalized"), "events")?
        .into_iter()
        .filter(|item| {
            item.get("id")
                .and_then(Value::as_str)
                .is_some_and(|id| events.contains(id))
        })
        .collect::<Vec<_>>();
    let mut indicator_ids = BTreeSet::new();
    for group in &selected_groups {
        for id in array(
            group
                .get("indicatorInstanceIds")
                .ok_or_else(|| GrammarError::invalid("selected group indicators missing"))?,
            "group indicator ids",
        )? {
            indicator_ids.insert(string(&id, "indicator id")?);
        }
    }
    for event in &selected_events {
        indicator_ids.insert(string(
            event
                .get("indicatorInstanceId")
                .ok_or_else(|| GrammarError::invalid("selected event indicator missing"))?,
            "event indicator",
        )?);
    }
    let execution = object(
        context.get("executionConfig").expect("normalized"),
        "execution config",
    )?;
    let library = object(
        execution
            .get("managementLibrary")
            .ok_or_else(|| GrammarError::invalid("management library missing"))?,
        "management library",
    )?;
    let selected_plans = array(
        library
            .get("plans")
            .ok_or_else(|| GrammarError::invalid("plans missing"))?,
        "plans",
    )?
    .into_iter()
    .filter(|item| {
        item.get("id")
            .and_then(Value::as_str)
            .is_some_and(|id| plans.contains(id))
    })
    .collect::<Vec<_>>();
    if selected_plans.len() != plans.len() {
        return Err(GrammarError::invalid("selected plan closure is incomplete"));
    }
    let bindings = library
        .get("scalarBindings")
        .map(|value| array(value, "scalar bindings"))
        .transpose()?
        .unwrap_or_default();
    let mut binding_ids = BTreeSet::new();
    for plan in &selected_plans {
        binding_ids.extend(binding_ids_in(plan));
    }
    let selected_bindings = bindings
        .into_iter()
        .filter(|item| {
            item.get("id")
                .and_then(Value::as_str)
                .is_some_and(|id| binding_ids.contains(id))
        })
        .collect::<Vec<_>>();
    let selected_binding_ids = selected_bindings
        .iter()
        .filter_map(|item| item.get("id").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();
    if selected_binding_ids != binding_ids {
        return Err(GrammarError::invalid(
            "selected scalar binding closure is incomplete",
        ));
    }
    for binding in &selected_bindings {
        indicator_ids.insert(string(
            binding
                .get("indicatorInstanceId")
                .ok_or_else(|| GrammarError::invalid("binding indicator missing"))?,
            "binding indicator",
        )?);
    }
    let selected_indicators = array(context.get("indicators").expect("normalized"), "indicators")?
        .into_iter()
        .filter(|item| {
            item.get("meta")
                .and_then(Value::as_object)
                .and_then(|meta| meta.get("instanceId"))
                .and_then(Value::as_str)
                .is_some_and(|id| indicator_ids.contains(id))
        })
        .collect::<Vec<_>>();
    if selected_indicators.len() != indicator_ids.len() {
        return Err(GrammarError::invalid("indicator closure is incomplete"));
    }
    let mut management = BTreeMap::new();
    management.insert(
        "version".to_owned(),
        library
            .get("version")
            .cloned()
            .unwrap_or_else(|| "temporal_management_v1".into()),
    );
    management.insert(
        "defaultPlanId".to_owned(),
        Value::String(plans.iter().next().expect("entry uses plan").clone()),
    );
    management.insert("plans".to_owned(), Value::Array(selected_plans));
    if !selected_bindings.is_empty() {
        management.insert("scalarBindings".to_owned(), Value::Array(selected_bindings));
    }
    let mut execution = execution;
    execution.remove("managementLibrary");
    execution.insert(
        "managementLibrary".to_owned(),
        Value::Object(management.into_iter().collect()),
    );
    Ok(obj(vec![
        ("indicators", Value::Array(selected_indicators)),
        ("groups", Value::Array(selected_groups)),
        ("events", Value::Array(selected_events)),
        (
            "executionConfig",
            Value::Object(execution.into_iter().collect()),
        ),
    ]))
}

fn binding_ids_in(value: &Value) -> BTreeSet<String> {
    let mut result = BTreeSet::new();
    match value {
        Value::Object(map) => {
            if let Some(id) = map.get("bindingId").and_then(Value::as_str) {
                result.insert(id.to_owned());
            }
            for item in map.values() {
                result.extend(binding_ids_in(item));
            }
        }
        Value::Array(items) => {
            for item in items {
                result.extend(binding_ids_in(item));
            }
        }
        _ => {}
    }
    result
}

fn guard_paths(
    guard: &Value,
    groups: &BTreeMap<String, BTreeSet<String>>,
    events: &BTreeMap<String, String>,
    known: &BTreeSet<String>,
    negated: bool,
) -> Result<BTreeSet<BTreeSet<String>>, GrammarError> {
    let map = object(guard, "decision guard")?;
    let kind = string(
        required(&map, "kind", "decision guard")?,
        "decision guard kind",
    )?;
    if kind == "not" {
        return guard_paths(
            required(&map, "guard", "not decision guard")?,
            groups,
            events,
            known,
            !negated,
        );
    }
    if kind == "all" || kind == "any" {
        let children = array(
            required(&map, "guards", "compound decision guard")?,
            "compound decision guard",
        )?;
        let paths = children
            .iter()
            .map(|item| guard_paths(item, groups, events, known, negated))
            .collect::<Result<Vec<_>, _>>()?;
        let effective_any = (kind == "any") != negated;
        if effective_any {
            return Ok(paths.into_iter().flatten().collect());
        }
        let mut current = BTreeSet::from([BTreeSet::new()]);
        for alternatives in paths {
            let mut next = BTreeSet::new();
            for left in &current {
                for right in &alternatives {
                    next.insert(left.union(right).cloned().collect());
                }
            }
            current = next;
        }
        return Ok(current);
    }
    if kind == "predicate_edge" || kind == "consecutive_true" {
        return guard_paths(
            required(&map, "predicate", "predicate-edge decision guard")?,
            groups,
            events,
            known,
            negated,
        );
    }
    let mut identifiers = BTreeSet::new();
    if let Some(group_id) = map.get("groupId") {
        let group_id = string(group_id, "group id")?;
        let members = groups.get(&group_id).ok_or_else(|| {
            GrammarError::invalid("decision guard references an unknown evidence group")
        })?;
        identifiers.extend(members.clone());
    }
    if let Some(event_id) = map.get("eventId") {
        let event_id = string(event_id, "event id")?;
        let member = events.get(&event_id).ok_or_else(|| {
            GrammarError::invalid("decision guard references an unknown event binding")
        })?;
        identifiers.insert(member.clone());
    }
    if !identifiers.is_subset(known) {
        return Err(GrammarError::invalid(
            "decision guard indicator closure is incomplete",
        ));
    }
    Ok(BTreeSet::from([identifiers]))
}

/// Exact finite-route cap enforced before every module becomes frozen.
pub fn entry_route_decision_indicator_report(profile: &Value) -> Result<Value, GrammarError> {
    let profile = object(profile, "profile")?;
    let graph = object(required(&profile, "graph", "profile")?, "graph")?;
    let transitions = array(
        graph.get("transitions").unwrap_or(&Value::Array(vec![])),
        "transition set",
    )?;
    if transitions.iter().any(|item| item.as_object().is_none()) {
        return Err(GrammarError::invalid(
            "entry route indicator cap transition is malformed",
        ));
    }
    let entries = transitions
        .iter()
        .enumerate()
        .filter(|(_, item)| {
            item.get("eventClass") == Some(&Value::String("decision".to_owned()))
                && item
                    .get("actions")
                    .and_then(Value::as_array)
                    .is_some_and(|actions| {
                        actions.iter().any(|action| {
                            action.get("kind") == Some(&Value::String("enter_next_open".to_owned()))
                        })
                    })
        })
        .map(|(index, _)| index)
        .collect::<BTreeSet<_>>();
    if entries.is_empty() {
        return Ok(obj(vec![
            (
                "schemaVersion",
                "temporal_entry_route_decision_indicator_report_v1".into(),
            ),
            (
                "policyVersion",
                Value::String(ENTRY_ROUTE_DECISION_INDICATOR_POLICY_VERSION.to_owned()),
            ),
            (
                "maxDistinctDecisionIndicatorInstances",
                ENTRY_ROUTE_DECISION_INDICATOR_CAP.into(),
            ),
            ("entryTransitions", Value::Array(vec![])),
            ("reachableStateIndicatorSetCount", 0.into()),
            (
                "observedMaximumDistinctDecisionIndicatorInstances",
                0.into(),
            ),
        ]));
    }
    let indicators = array(
        required(&profile, "indicators", "entry route indicator cap")?,
        "indicators",
    )?;
    let groups_raw = array(
        required(&graph, "evidenceGroups", "entry route indicator cap")?,
        "evidence groups",
    )?;
    let events_raw = array(
        required(&graph, "eventBindings", "entry route indicator cap")?,
        "event bindings",
    )?;
    let known = indicators
        .iter()
        .map(|item| {
            string(
                item.get("meta")
                    .and_then(Value::as_object)
                    .and_then(|meta| meta.get("instanceId"))
                    .ok_or_else(|| {
                        GrammarError::invalid(
                            "entry route indicator cap requires unique indicator instances",
                        )
                    })?,
                "indicator id",
            )
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if known.len() != indicators.len() || known.contains("") || known.len() > 16 {
        return Err(GrammarError::invalid(
            "entry route indicator cap requires unique indicator instances",
        ));
    }
    let groups = groups_raw
        .iter()
        .map(|item| {
            let item = object(item, "evidence group")?;
            let id = string(required(&item, "id", "evidence group")?, "group id")?;
            let members = array(
                required(&item, "indicatorInstanceIds", "evidence group")?,
                "group members",
            )?
            .iter()
            .map(|member| string(member, "group member"))
            .collect::<Result<BTreeSet<_>, _>>()?;
            if id.is_empty() || members.is_empty() {
                return Err(GrammarError::invalid(
                    "entry route indicator cap evidence group is malformed",
                ));
            }
            Ok::<(String, BTreeSet<String>), GrammarError>((id, members))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let events = events_raw
        .iter()
        .map(|item| {
            let item = object(item, "event binding")?;
            Ok::<(String, String), GrammarError>((
                string(required(&item, "id", "event binding")?, "event id")?,
                string(
                    required(&item, "indicatorInstanceId", "event binding")?,
                    "event indicator",
                )?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let initial = string(
        required(&graph, "initialStateId", "entry route indicator cap")?,
        "initial state",
    )?;
    let mut reverse = BTreeMap::<String, BTreeSet<String>>::new();
    let sources = entries
        .iter()
        .map(|index| {
            string(
                transitions[*index]
                    .get("sourceStateId")
                    .ok_or_else(|| GrammarError::invalid("entry decision route is not closed"))?,
                "entry source",
            )
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    for (index, edge) in transitions.iter().enumerate() {
        if entries.contains(&index)
            || edge.get("eventClass") != Some(&Value::String("decision".to_owned()))
        {
            continue;
        }
        if let (Some(source), Some(destination)) = (
            edge.get("sourceStateId").and_then(Value::as_str),
            edge.get("destinationStateId").and_then(Value::as_str),
        ) {
            reverse
                .entry(destination.to_owned())
                .or_default()
                .insert(source.to_owned());
        }
    }
    let mut relevant = sources.clone();
    let mut pending = sources.iter().cloned().collect::<VecDeque<_>>();
    while let Some(state) = pending.pop_front() {
        for predecessor in reverse.get(&state).into_iter().flatten() {
            if relevant.insert(predecessor.clone()) {
                pending.push_back(predecessor.clone());
            }
        }
    }
    let mut outgoing = BTreeMap::<String, Vec<usize>>::new();
    for (index, edge) in transitions.iter().enumerate() {
        if edge.get("eventClass") == Some(&Value::String("decision".to_owned())) {
            if let Some(source) = edge.get("sourceStateId").and_then(Value::as_str) {
                outgoing.entry(source.to_owned()).or_default().push(index);
            }
        }
    }
    let mut work = VecDeque::from([(initial, BTreeSet::new())]);
    let mut seen = BTreeSet::from([(work[0].0.clone(), work[0].1.clone())]);
    let mut entry_sets = entries
        .iter()
        .map(|index| (*index, BTreeSet::<BTreeSet<String>>::new()))
        .collect::<BTreeMap<_, _>>();
    while let Some((state, current)) = work.pop_front() {
        for index in outgoing.get(&state).into_iter().flatten() {
            let edge = &transitions[*index];
            let is_entry = entries.contains(index);
            let destination = edge
                .get("destinationStateId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if !is_entry && (destination.is_empty() || !relevant.contains(destination)) {
                continue;
            }
            for needed in guard_paths(
                edge.get("guard").ok_or_else(|| {
                    GrammarError::invalid("entry decision route guard is not closed")
                })?,
                &groups,
                &events,
                &known,
                false,
            )? {
                let combined = current.union(&needed).cloned().collect::<BTreeSet<_>>();
                if combined.len() > ENTRY_ROUTE_DECISION_INDICATOR_CAP {
                    return Err(GrammarError::EntryRouteDecisionIndicatorCap);
                }
                if is_entry {
                    entry_sets
                        .get_mut(index)
                        .expect("entry index")
                        .insert(combined);
                } else if seen.insert((destination.to_owned(), combined.clone())) {
                    work.push_back((destination.to_owned(), combined));
                }
            }
        }
    }
    let reports = entries
        .iter()
        .map(|index| {
            let sets = &entry_sets[index];
            let counts = sets.iter().map(BTreeSet::len).collect::<BTreeSet<_>>();
            obj(vec![
                (
                    "transitionId",
                    transitions[*index]
                        .get("id")
                        .cloned()
                        .unwrap_or_else(|| "".into()),
                ),
                ("routeCount", sets.len().into()),
                (
                    "routeDistinctDecisionIndicatorCounts",
                    Value::Array(counts.into_iter().map(Value::from).collect()),
                ),
                (
                    "maxDistinctDecisionIndicatorInstances",
                    sets.iter().map(BTreeSet::len).max().unwrap_or(0).into(),
                ),
            ])
        })
        .collect::<Vec<_>>();
    let observed = reports
        .iter()
        .filter_map(|item| {
            item.get("maxDistinctDecisionIndicatorInstances")
                .and_then(Value::as_u64)
        })
        .max()
        .unwrap_or(0);
    Ok(obj(vec![
        (
            "schemaVersion",
            "temporal_entry_route_decision_indicator_report_v1".into(),
        ),
        (
            "policyVersion",
            Value::String(ENTRY_ROUTE_DECISION_INDICATOR_POLICY_VERSION.to_owned()),
        ),
        (
            "maxDistinctDecisionIndicatorInstances",
            ENTRY_ROUTE_DECISION_INDICATOR_CAP.into(),
        ),
        ("entryTransitions", Value::Array(reports)),
        ("reachableStateIndicatorSetCount", seen.len().into()),
        (
            "observedMaximumDistinctDecisionIndicatorInstances",
            observed.into(),
        ),
    ]))
}

pub fn validate_entry_route_decision_indicator_cap(profile: &Value) -> Result<Value, GrammarError> {
    let report = entry_route_decision_indicator_report(profile)?;
    if report
        .get("observedMaximumDistinctDecisionIndicatorInstances")
        .and_then(Value::as_u64)
        .unwrap_or_default() as usize
        > ENTRY_ROUTE_DECISION_INDICATOR_CAP
    {
        return Err(GrammarError::EntryRouteDecisionIndicatorCap);
    }
    Ok(report)
}

fn python_tuple_repr(value: &Value) -> String {
    match value {
        Value::String(value) => format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'")),
        Value::Array(items) => {
            let body = items
                .iter()
                .map(python_tuple_repr)
                .collect::<Vec<_>>()
                .join(", ");
            if items.len() == 1 {
                format!("({body},)")
            } else {
                format!("({body})")
            }
        }
        _ => temporal_qd_contract::canonical_json(value).expect("finite graph shape"),
    }
}

fn guard_shape(guard: &Value) -> Value {
    let kind = guard
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if kind == "all" || kind == "any" {
        let mut children = guard
            .get("guards")
            .and_then(Value::as_array)
            .map(|items| items.iter().map(guard_shape).collect::<Vec<_>>())
            .unwrap_or_default();
        children.sort_by_key(python_tuple_repr);
        return Value::Array(vec![Value::String(kind.to_owned()), Value::Array(children)]);
    }
    if kind == "predicate_edge" || kind == "consecutive_true" {
        return Value::Array(vec![
            Value::String(kind.to_owned()),
            guard_shape(guard.get("predicate").unwrap_or(&Value::Null)),
        ]);
    }
    Value::String(kind.to_owned())
}

pub fn compiled_graph_signature(profile: &Value) -> Result<String, GrammarError> {
    let graph = profile.get("graph").and_then(Value::as_object);
    let transitions = graph
        .and_then(|item| item.get("transitions"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let state_count = graph
        .and_then(|item| item.get("states"))
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or_default();
    let mut degrees = BTreeMap::<String, usize>::new();
    let mut edges = vec![];
    for item in transitions {
        if let Some(item) = item.as_object() {
            *degrees
                .entry(
                    item.get("sourceStateId")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                )
                .or_default() += 1;
            let mut actions = item
                .get("actions")
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| {
                            item.get("kind")
                                .and_then(Value::as_str)
                                .map(|kind| Value::String(kind.to_owned()))
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            actions.sort_by_key(|value| value.as_str().unwrap_or_default().to_owned());
            edges.push(Value::Array(vec![
                item.get("eventClass").cloned().unwrap_or(Value::Null),
                guard_shape(item.get("guard").unwrap_or(&Value::Null)),
                Value::Array(actions),
            ]));
        }
    }
    edges.sort_by_key(python_tuple_repr);
    let histogram = degrees
        .values()
        .fold(BTreeMap::<usize, usize>::new(), |mut result, degree| {
            *result.entry(*degree).or_default() += 1;
            result
        })
        .into_iter()
        .map(|(degree, count)| Value::Array(vec![degree.into(), count.into()]))
        .collect();
    sha(&obj(vec![
        ("stateCount", state_count.into()),
        ("outDegreeHistogram", Value::Array(histogram)),
        ("edges", Value::Array(edges)),
    ]))
}

pub fn module_signatures(program: &ModuleProgram) -> Result<Value, GrammarError> {
    let table = registry();
    let fragments = program
        .fragments
        .iter()
        .map(Fragment::canonical)
        .collect::<Vec<_>>();
    let shape = program
        .fragments
        .iter()
        .map(|item| {
            let spec = &table[&item.production_id];
            obj(vec![
                ("family", Value::String(spec.family.clone())),
                ("productionId", Value::String(item.production_id.clone())),
                (
                    "ports",
                    Value::Array(vec![
                        Value::String(spec.consumes.as_str().to_owned()),
                        Value::String(spec.produces.as_str().to_owned()),
                    ]),
                ),
            ])
        })
        .collect();
    let parameters = program
        .fragments
        .iter()
        .map(|item| {
            obj(vec![
                ("productionId", Value::String(item.production_id.clone())),
                (
                    "choices",
                    Value::Object(item.choices.clone().into_iter().collect()),
                ),
            ])
        })
        .collect();
    let composition = fragments
        .iter()
        .map(|item| item.get("productionId").expect("fragment").clone())
        .collect();
    Ok(obj(vec![
        (
            "programShapeSha256",
            Value::String(sha(&Value::Array(shape))?),
        ),
        (
            "parameterSha256",
            Value::String(sha(&Value::Array(parameters))?),
        ),
        (
            "motifCompositionSha256",
            Value::String(sha(&Value::Array(composition))?),
        ),
        (
            "directionSha256",
            Value::String(sha(&obj(vec![(
                "direction",
                Value::String(program.direction.clone()),
            )]))?),
        ),
    ]))
}

pub fn inspect_module(profile: &Value) -> Result<Value, GrammarError> {
    let graph = profile.get("graph").and_then(Value::as_object);
    let states = graph
        .and_then(|item| item.get("states"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let transitions = graph
        .and_then(|item| item.get("transitions"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let ids = states
        .iter()
        .filter_map(|item| item.get("id").and_then(Value::as_str))
        .collect::<BTreeSet<_>>();
    let refs_ok = transitions.iter().all(|item| {
        item.as_object().is_some_and(|edge| {
            edge.get("sourceStateId")
                .and_then(Value::as_str)
                .is_some_and(|source| ids.contains(source))
                && edge
                    .get("destinationStateId")
                    .and_then(Value::as_str)
                    .is_some_and(|destination| ids.contains(destination))
        })
    });
    Ok(obj(vec![
        ("schemaVersion", Value::String(GRAMMAR_SCHEMA.to_owned())),
        ("diagnosticOnly", Value::Bool(true)),
        ("stateCount", states.len().into()),
        ("transitionCount", transitions.len().into()),
        ("referenceClosure", Value::Bool(refs_ok)),
        ("profileSha256", Value::String(sha(profile)?)),
    ]))
}
