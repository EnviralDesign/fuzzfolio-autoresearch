//! Closed initial stop/target construction vocabulary.
//!
//! The functions here only produce profile JSON mutations and audits.  They
//! never decide whether a locator is executable: Dashboard remains authority
//! for management-model parsing and runtime geometry.

use std::collections::BTreeMap;

use temporal_qd_contract::{ContractError, Value, canonical_sha256};

pub const INITIAL_PROTECTION_POLICY_SCHEMA: &str = "temporal_qd_initial_protection_policy_v2";

#[derive(Debug, thiserror::Error)]
pub enum ProtectionError {
    #[error("initial protection contract error: {0}")]
    Contract(#[from] ContractError),
    #[error("initial protection: {0}")]
    Invalid(String),
}
pub type ProtectionResult<T> = Result<T, ProtectionError>;

fn invalid(message: impl Into<String>) -> ProtectionError {
    ProtectionError::Invalid(message.into())
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
fn number(value: Option<&Value>) -> Option<f64> {
    value.and_then(Value::as_f64)
}
fn clone_field(value: &Value, key: &str) -> Option<Value> {
    field(value, key).cloned()
}
fn hash(value: &Value) -> ProtectionResult<String> {
    Ok(canonical_sha256(value)?)
}

pub fn default_initial_protection_policy() -> Value {
    object([
        (
            "schemaVersion",
            Value::String(INITIAL_PROTECTION_POLICY_SCHEMA.into()),
        ),
        (
            "stopPercentChoices",
            array(
                [0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0]
                    .into_iter()
                    .map(Value::from),
            ),
        ),
        (
            "rewardMultipleChoices",
            array(
                [0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 4.0, 6.0]
                    .into_iter()
                    .map(Value::from),
            ),
        ),
        (
            "targetPercentChoices",
            array(
                [0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0]
                    .into_iter()
                    .map(Value::from),
            ),
        ),
        (
            "distanceMultipleChoices",
            array(
                [0.5, 0.75, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0]
                    .into_iter()
                    .map(Value::from),
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
                .map(|item| Value::String(item.into())),
            ),
        ),
    ])
}

pub fn validate_initial_protection_policy(policy: &Value) -> ProtectionResult<Value> {
    (policy == &default_initial_protection_policy())
        .then(|| policy.clone())
        .ok_or_else(|| invalid("operator policy is not the closed admitted policy"))
}
fn library(profile: &Value) -> ProtectionResult<&Value> {
    field(profile, "executionConfig")
        .and_then(|item| field(item, "managementLibrary"))
        .filter(|item| field(item, "plans").and_then(Value::as_array).is_some())
        .ok_or_else(|| invalid("profile has no explicit management library"))
}
fn plan<'a>(profile: &'a Value, id: &str) -> ProtectionResult<&'a Value> {
    let matches = field(library(profile)?, "plans")
        .and_then(Value::as_array)
        .expect("plans")
        .iter()
        .filter(|item| text(field(item, "id")) == id)
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return Err(invalid("plan selector did not resolve one management plan"));
    }
    Ok(matches[0])
}
fn choice_rows(policy: &Value, key: &str) -> ProtectionResult<Vec<f64>> {
    field(policy, key)
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("closed policy malformed"))?
        .iter()
        .map(|item| number(Some(item)).ok_or_else(|| invalid("closed policy number malformed")))
        .collect()
}
fn classify_scalar(current: f64, choices: &[f64], candidate: f64) -> String {
    let Some(index) = choices.iter().position(|value| *value == current) else {
        return "jump".into();
    };
    let replacement = choices
        .iter()
        .position(|value| *value == candidate)
        .expect("candidate comes from choices");
    if replacement.abs_diff(index) == 1 {
        "adjacent".into()
    } else {
        "jump".into()
    }
}
fn binding_locators(profile: &Value, multiples: &[f64]) -> ProtectionResult<Vec<Value>> {
    let mut result = vec![];
    for binding in field(library(profile)?, "scalarBindings")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if text(field(binding, "availability")) != "completed_bar" {
            continue;
        }
        let id = text(field(binding, "id"));
        if id.is_empty() {
            continue;
        }
        match text(field(binding, "valueKind")).as_str() {
            "price_level" => result.push(object([
                ("kind", Value::String("indicator_price_level".into())),
                ("bindingId", Value::String(id)),
            ])),
            "price_distance" => {
                for multiple in multiples {
                    result.push(object([
                        ("kind", Value::String("indicator_distance_multiple".into())),
                        ("bindingId", Value::String(id.clone())),
                        ("multiple", Value::from(*multiple)),
                    ]))
                }
            }
            _ => {}
        }
    }
    Ok(result)
}
fn referenced_bindings(profile: &Value) -> ProtectionResult<BTreeMap<String, usize>> {
    let mut found = BTreeMap::new();
    let mut add = |locator: Option<&Value>| {
        if let Some(locator) = locator {
            if ["indicator_price_level", "indicator_distance_multiple"]
                .contains(&text(field(locator, "kind")).as_str())
            {
                let id = text(field(locator, "bindingId"));
                if !id.is_empty() {
                    *found.entry(id).or_default() += 1;
                }
            }
        }
    };
    let lib = library(profile)?;
    for item in field(lib, "plans")
        .and_then(Value::as_array)
        .expect("plans")
    {
        add(field(item, "initialStop"));
        add(field(item, "initialTarget"));
        if let Some(trailing) = field(item, "trailingStop") {
            add(field(trailing, "anchor"));
            add(field(trailing, "distance"));
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
            add(field(action, "stopLocator"));
            add(field(action, "targetLocator"));
        }
    }
    Ok(found)
}
fn remove_unreferenced_bindings(profile: &mut Value) -> ProtectionResult<Vec<Value>> {
    let refs = referenced_bindings(profile)?;
    let bindings = field(profile, "executionConfig")
        .and_then(|item| field(item, "managementLibrary"))
        .and_then(|item| field(item, "scalarBindings"))
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
        let lib = field_mut(profile, "executionConfig")
            .and_then(|item| field_mut(item, "managementLibrary"))
            .expect("library");
        if retained.is_empty() {
            lib.as_object_mut()
                .expect("library object")
                .remove("scalarBindings");
        } else {
            lib.as_object_mut()
                .expect("library object")
                .insert("scalarBindings".into(), array(retained));
        }
    }
    removed.sort_by_key(|item| text(field(item, "id")));
    Ok(removed)
}
fn replacement_rows(
    profile: &Value,
    plan_id: &str,
    site: &str,
    policy: &Value,
) -> ProtectionResult<Vec<Value>> {
    let current = clone_field(
        plan(profile, plan_id)?,
        if site == "stop" {
            "initialStop"
        } else {
            "initialTarget"
        },
    )
    .ok_or_else(|| invalid("initial protection locator missing"))?;
    let current_kind = text(field(&current, "kind"));
    let mut rows = vec![];
    if site == "stop" {
        let choices = choice_rows(policy, "stopPercentChoices")?;
        for value in &choices {
            let candidate = object([
                ("kind", Value::String("fixed_percent".into())),
                ("percent", Value::from(*value)),
            ]);
            if candidate != current {
                let class = if current_kind == "fixed_percent" {
                    classify_scalar(
                        number(field(&current, "percent")).unwrap_or(f64::NAN),
                        &choices,
                        *value,
                    )
                } else {
                    "kind_switch".into()
                };
                rows.push(object([
                    ("replacement", candidate),
                    ("mutationClass", Value::String(class)),
                ]));
            }
        }
    } else {
        let rewards = choice_rows(policy, "rewardMultipleChoices")?;
        for value in &rewards {
            let candidate = object([
                ("kind", Value::String("reward_multiple".into())),
                ("multiple", Value::from(*value)),
            ]);
            if candidate != current {
                let class = if current_kind == "reward_multiple" {
                    classify_scalar(
                        number(field(&current, "multiple")).unwrap_or(f64::NAN),
                        &rewards,
                        *value,
                    )
                } else {
                    "kind_switch".into()
                };
                rows.push(object([
                    ("replacement", candidate),
                    ("mutationClass", Value::String(class)),
                ]));
            }
        }
        let percents = choice_rows(policy, "targetPercentChoices")?;
        for value in &percents {
            let candidate = object([
                ("kind", Value::String("fixed_percent".into())),
                ("percent", Value::from(*value)),
            ]);
            if candidate != current {
                let class = if current_kind == "fixed_percent" {
                    classify_scalar(
                        number(field(&current, "percent")).unwrap_or(f64::NAN),
                        &percents,
                        *value,
                    )
                } else {
                    "kind_switch".into()
                };
                rows.push(object([
                    ("replacement", candidate),
                    ("mutationClass", Value::String(class)),
                ]));
            }
        }
        let candidate = object([("kind", Value::String("none".into()))]);
        if candidate != current {
            rows.push(object([
                ("replacement", candidate),
                ("mutationClass", Value::String("kind_switch".into())),
            ]));
        }
    }
    for candidate in binding_locators(profile, &choice_rows(policy, "distanceMultipleChoices")?)? {
        if candidate == current {
            continue;
        }
        let class = if current_kind == "indicator_distance_multiple"
            && text(field(&candidate, "kind")) == "indicator_distance_multiple"
            && text(field(&candidate, "bindingId")) == text(field(&current, "bindingId"))
        {
            classify_scalar(
                number(field(&current, "multiple")).unwrap_or(f64::NAN),
                &choice_rows(policy, "distanceMultipleChoices")?,
                number(field(&candidate, "multiple")).expect("policy multiple"),
            )
        } else {
            "kind_switch".into()
        };
        rows.push(object([
            ("replacement", candidate),
            ("mutationClass", Value::String(class)),
        ]));
    }
    Ok(rows)
}

pub fn enumerate_initial_protection_plans(
    profile: &Value,
    policy: &Value,
) -> ProtectionResult<Vec<Value>> {
    let policy = validate_initial_protection_policy(policy)?;
    let mut plans = BTreeMap::new();
    for item in field(library(profile)?, "plans")
        .and_then(Value::as_array)
        .expect("plans")
    {
        let id = text(field(item, "id"));
        if id.is_empty() {
            continue;
        }
        for site in ["stop", "target"] {
            for row in replacement_rows(profile, &id, site, &policy)? {
                let mut plan = object([
                    ("kind", Value::String("initial_protection".into())),
                    ("planId", Value::String(id.clone())),
                    ("site", Value::String(site.into())),
                    ("replacement", clone_field(&row, "replacement").unwrap()),
                    ("mutationClass", clone_field(&row, "mutationClass").unwrap()),
                ]);
                let digest = hash(&plan)?;
                plan.as_object_mut()
                    .unwrap()
                    .insert("planSha256".into(), Value::String(digest.clone()));
                plans.insert(digest, plan);
            }
        }
    }
    Ok(plans.into_values().collect())
}
pub fn apply_initial_protection_plan(
    profile: &Value,
    requested: &Value,
    policy: &Value,
) -> ProtectionResult<(Value, Value)> {
    let plan = enumerate_initial_protection_plans(profile, policy)?
        .into_iter()
        .find(|item| item == requested)
        .ok_or_else(|| invalid("plan is not canonical and applicable"))?;
    let mut child = profile.clone();
    let selected = field_mut(&mut child, "executionConfig")
        .and_then(|item| field_mut(item, "managementLibrary"))
        .and_then(|item| field_mut(item, "plans"))
        .and_then(Value::as_array_mut)
        .and_then(|plans| {
            plans
                .iter_mut()
                .find(|item| text(field(item, "id")) == text(field(&plan, "planId")))
        })
        .ok_or_else(|| invalid("canonical management plan disappeared"))?;
    let key = if text(field(&plan, "site")) == "stop" {
        "initialStop"
    } else {
        "initialTarget"
    };
    let before =
        clone_field(selected, key).ok_or_else(|| invalid("initial protection locator missing"))?;
    selected
        .as_object_mut()
        .unwrap()
        .insert(key.into(), clone_field(&plan, "replacement").unwrap());
    let removed = remove_unreferenced_bindings(&mut child)?;
    let mut audit = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_initial_protection_application_v1".into()),
        ),
        ("planSha256", clone_field(&plan, "planSha256").unwrap()),
        ("managementPlanId", clone_field(&plan, "planId").unwrap()),
        ("site", clone_field(&plan, "site").unwrap()),
        (
            "mutationClass",
            clone_field(&plan, "mutationClass").unwrap(),
        ),
        ("before", before),
        ("after", clone_field(&plan, "replacement").unwrap()),
        ("removedUnreferencedScalarBindings", array(removed)),
    ]);
    let digest = hash(&audit)?;
    audit
        .as_object_mut()
        .unwrap()
        .insert("applicationSha256".into(), Value::String(digest));
    Ok((child, audit))
}

/// The caller supplies deterministic choice selection; no RNG state is held
/// by the kernel.  Returning a choice not in `values` is rejected.
pub fn immigrant_initial_protection_selector<F>(
    policy: &Value,
    seed: &str,
    mut choose: F,
) -> ProtectionResult<Value>
where
    F: FnMut(&str, &str, &[Value]) -> ProtectionResult<Value>,
{
    let policy = validate_initial_protection_policy(policy)?;
    let modes = field(&policy, "immigrantModes")
        .and_then(Value::as_array)
        .unwrap();
    let mode = choose(seed, "initial_protection_mode", modes)?;
    if !modes.contains(&mode) {
        return Err(invalid(
            "immigrant chooser returned a value outside closed policy",
        ));
    }
    if text(Some(&mode)) == "dynamic_catalog_authorized" {
        let sites = array([
            Value::String("initial_stop".into()),
            Value::String("initial_target".into()),
        ]);
        let site = choose(
            seed,
            "initial_protection_dynamic_site",
            sites.as_array().unwrap(),
        )?;
        if !sites.as_array().unwrap().contains(&site) {
            return Err(invalid(
                "immigrant chooser returned a dynamic site outside closed policy",
            ));
        }
        return Ok(object([("mode", mode), ("dynamicSite", site)]));
    }
    let stops = field(&policy, "stopPercentChoices")
        .and_then(Value::as_array)
        .unwrap();
    let stop = choose(seed, "initial_protection_stop_percent", stops)?;
    if !stops.contains(&stop) {
        return Err(invalid(
            "immigrant chooser returned stop outside closed policy",
        ));
    }
    let target_values = if text(Some(&mode)) == "coupled_reward_multiple" {
        field(&policy, "rewardMultipleChoices")
            .and_then(Value::as_array)
            .unwrap()
    } else {
        field(&policy, "targetPercentChoices")
            .and_then(Value::as_array)
            .unwrap()
    };
    let target = choose(seed, "initial_protection_target", target_values)?;
    if !target_values.contains(&target) {
        return Err(invalid(
            "immigrant chooser returned target outside closed policy",
        ));
    }
    let mut output = object([("mode", mode.clone()), ("stopPercent", stop)]);
    if text(Some(&mode)) == "coupled_reward_multiple" {
        output
            .as_object_mut()
            .unwrap()
            .insert("rewardMultiple".into(), target);
    } else if text(Some(&mode)) == "decoupled_fixed_percent" {
        output
            .as_object_mut()
            .unwrap()
            .insert("targetPercent".into(), target);
    }
    Ok(output)
}
pub fn apply_immigrant_initial_protection(
    profile: &Value,
    plan_id: &str,
    selector: &Value,
    policy: &Value,
) -> ProtectionResult<(Value, Value)> {
    validate_initial_protection_policy(policy)?;
    let mode = text(field(selector, "mode"));
    let stop = number(field(selector, "stopPercent"))
        .ok_or_else(|| invalid("immigrant selector is invalid"))?;
    if ![
        "coupled_reward_multiple",
        "decoupled_fixed_percent",
        "no_fixed_target",
    ]
    .contains(&mode.as_str())
    {
        return Err(invalid("immigrant selector is invalid"));
    }
    let mut child = profile.clone();
    let selected = field_mut(&mut child, "executionConfig")
        .and_then(|item| field_mut(item, "managementLibrary"))
        .and_then(|item| field_mut(item, "plans"))
        .and_then(Value::as_array_mut)
        .and_then(|plans| {
            plans
                .iter_mut()
                .find(|item| text(field(item, "id")) == plan_id)
        })
        .ok_or_else(|| invalid("immigrant plan selector did not resolve one management plan"))?;
    let before = object([
        (
            "initialStop",
            clone_field(selected, "initialStop").unwrap_or(Value::Null),
        ),
        (
            "initialTarget",
            clone_field(selected, "initialTarget").unwrap_or(Value::Null),
        ),
    ]);
    selected.as_object_mut().unwrap().insert(
        "initialStop".into(),
        object([
            ("kind", Value::String("fixed_percent".into())),
            ("percent", Value::from(stop)),
        ]),
    );
    let target = match mode.as_str() {
        "coupled_reward_multiple" => object([
            ("kind", Value::String("reward_multiple".into())),
            (
                "multiple",
                Value::from(
                    number(field(selector, "rewardMultiple"))
                        .ok_or_else(|| invalid("coupled immigrant target selector invalid"))?,
                ),
            ),
        ]),
        "decoupled_fixed_percent" => object([
            ("kind", Value::String("fixed_percent".into())),
            (
                "percent",
                Value::from(
                    number(field(selector, "targetPercent"))
                        .ok_or_else(|| invalid("decoupled immigrant target selector invalid"))?,
                ),
            ),
        ]),
        _ => object([("kind", Value::String("none".into()))]),
    };
    selected
        .as_object_mut()
        .unwrap()
        .insert("initialTarget".into(), target);
    let after = object([
        ("initialStop", clone_field(selected, "initialStop").unwrap()),
        (
            "initialTarget",
            clone_field(selected, "initialTarget").unwrap(),
        ),
    ]);
    let mut audit = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_initial_protection_immigrant_application_v1".into()),
        ),
        ("managementPlanId", Value::String(plan_id.into())),
        ("selector", selector.clone()),
        ("before", before),
        ("after", after),
    ]);
    let digest = hash(&audit)?;
    audit
        .as_object_mut()
        .unwrap()
        .insert("applicationSha256".into(), Value::String(digest));
    Ok((child, audit))
}
