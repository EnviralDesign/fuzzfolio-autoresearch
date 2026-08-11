//! Standalone, value-only port of the later-generation topology operators.
//!
//! ## Source/support matrix
//! | Python surface | Native operation | status |
//! | --- | --- | --- |
//! | `make_plan` / `apply_plan` | [`make_plan`], [`apply_plan`] | exact plan binding/IDs |
//! | setup | insert/remove | implemented |
//! | entry branches | rewire/insert/remove | implemented |
//! | confirmation/rejection | insert | implemented |
//! | bounded re-arm | insert/remove | implemented |
//! | shared position hub | management insert/remove/rewire | implemented |
//! | shared position hub | exit insert/remove/rewire | implemented |
//!
//! This file deliberately has no `mod` declaration yet.  `v5_operators` owns
//! the integration boundary.  The public types are nevertheless intentionally
//! `Value`-based so that it can pass already sealed program/budget facts with
//! no Python, authority-tree traversal, or ambient configuration dependency.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use temporal_qd_contract::{
    ContractError, Map, Value, canonical_json, canonical_json_bytes, canonical_sha256,
};

pub const V5_TOPOLOGY_PLAN_SCHEMA: &str = "evolvable_module_topology_plan_v1";
pub const V5_TOPOLOGY_OPERATOR_SCHEMA: &str = "evolvable_module_topology_operator_v1";
pub const V5_TOPOLOGY_DELTA_SCHEMA: &str = "evolvable_module_topology_delta_v1";
pub const V5_TOPOLOGY_SEALED_FACTS_SCHEMA: &str = "temporal_qd_v5_topology_sealed_facts_v1";

const GENOME_SCHEMA: &str = "evolvable_module_genome_v1";
const PROGRAM_KIND: &str = "evolvable_module_genome_v1";
const CODEC: &str = "evolvable_module_genome_json_v1";
const MANAGEMENT: &[&str] = &[
    "move_stop_to_break_even_next_open",
    "tighten_stop_next_open",
    "set_target_next_open",
    "cancel_target_next_open",
    "activate_trailing_stop_next_open",
    "deactivate_trailing_stop_next_open",
];

#[derive(Debug, thiserror::Error)]
pub enum V5TopologyError {
    #[error("v5 topology canonical contract failure: {0}")]
    Canonical(#[from] ContractError),
    #[error("v5 topology: {0}")]
    Invalid(String),
}

pub type Result<T> = std::result::Result<T, V5TopologyError>;

/// Compact result data, ready for `v5_operators` to journal or envelope.
#[derive(Clone, Debug, PartialEq)]
pub struct V5TopologyApplication {
    pub plan: Value,
    pub child_program: Value,
    pub trace: Value,
}

fn invalid(message: impl Into<String>) -> V5TopologyError {
    V5TopologyError::Invalid(message.into())
}
fn object(rows: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    let mut out = Map::new();
    for (key, value) in rows {
        out.insert(key.to_owned(), value);
    }
    Value::Object(out)
}
fn array(rows: impl IntoIterator<Item = Value>) -> Value {
    Value::Array(rows.into_iter().collect())
}
fn field<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value.as_object()?.get(key)
}
fn required<'a>(value: &'a Value, key: &str, label: &str) -> Result<&'a Value> {
    field(value, key).ok_or_else(|| invalid(format!("{label} lacks {key}")))
}
fn obj<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| invalid(format!("{label} must be an object")))
}
fn rows<'a>(value: &'a Value, label: &str) -> Result<&'a [Value]> {
    value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| invalid(format!("{label} must be an ordered array")))
}
fn clone_value(value: &Value) -> Result<Value> {
    Ok(serde_json::from_slice(&canonical_json_bytes(value)?).map_err(ContractError::from)?)
}
fn token(value: &Value, label: &str) -> Result<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty() && s.len() <= 240)
        .map(str::to_owned)
        .ok_or_else(|| invalid(format!("{label} must be a nonempty explicit identifier")))
}
fn integer(value: &Value, label: &str) -> Result<u64> {
    value
        .as_u64()
        .filter(|n| *n <= 999)
        .ok_or_else(|| invalid(format!("{label} must be an integer from 0 to 999")))
}
fn positive(value: &Value, label: &str) -> Result<u64> {
    value
        .as_u64()
        .filter(|n| *n > 0)
        .ok_or_else(|| invalid(format!("{label} must be a positive integer")))
}
fn sha(value: &Value, label: &str) -> Result<String> {
    let value = token(value, label)?;
    if value.len() != 71
        || !value.starts_with("sha256:")
        || !value[7..]
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid(format!("{label} must be canonical SHA-256")));
    }
    Ok(value)
}
fn exact_keys(value: &Value, keys: &[&str], label: &str) -> Result<()> {
    let map = obj(value, label)?;
    if map.len() != keys.len() || keys.iter().any(|key| !map.contains_key(*key)) {
        return Err(invalid(format!("{label} has an unsupported shape")));
    }
    Ok(())
}

/// Build the exact content-bound plan shape used by Python.  The caller must
/// pass the same sealed facts later to [`apply_plan`].
pub fn make_plan(
    program: &Value,
    sealed_facts: &Value,
    operation: &str,
    arguments: &Value,
) -> Result<Value> {
    let program = canonical_program(program, sealed_facts)?;
    let operation = token(&Value::String(operation.to_owned()), "operation")?;
    let arguments = clone_value(arguments)?;
    obj(&arguments, "plan arguments")?;
    Ok(object([
        (
            "schemaVersion",
            Value::String(V5_TOPOLOGY_PLAN_SCHEMA.to_owned()),
        ),
        (
            "operatorSchema",
            Value::String(V5_TOPOLOGY_OPERATOR_SCHEMA.to_owned()),
        ),
        ("operation", Value::String(operation)),
        (
            "sourceGenomeSha256",
            Value::String(canonical_sha256(&program)?),
        ),
        ("arguments", arguments),
    ]))
}

/// Apply a data-only plan.  It rejects stale plans, malformed programs,
/// priority collisions, unknown topology shapes, and sealed-fact drift.
pub fn apply_plan(
    program: &Value,
    sealed_facts: &Value,
    plan: &Value,
) -> Result<V5TopologyApplication> {
    let before = canonical_program(program, sealed_facts)?;
    let parsed = Plan::parse(plan)?;
    if canonical_sha256(&before)? != parsed.source {
        return Err(invalid(
            "stale topology plan does not bind this exact parent genome",
        ));
    }
    let mut graph = Graph::parse(&before)?;
    let plan_hash = canonical_sha256(plan)?;
    graph.apply(&parsed.operation, &parsed.arguments, &plan_hash)?;
    let child = graph.canonical(&before)?;
    // Re-admit after transformation: all mutations fail closed before a
    // consuming operator can see a child.
    let child = canonical_program(&child, sealed_facts)?;
    let trace = semantic_trace(&before, &child, &parsed.operation, &plan_hash)?;
    Ok(V5TopologyApplication {
        plan: clone_value(plan)?,
        child_program: child,
        trace,
    })
}

/// Enumerate the exact neutral topology grammar candidates used by Python's
/// `EvolvableModulePairOperator.grammar_plans`.  The structural layer owns no
/// temporal grid: every generated placement guard is `{"kind":"always"}`.
/// Candidate identities are SHA-lexically ordered and deduplicated; candidates
/// rejected by this module's child admission are intentionally omitted.
pub fn enumerate_plans(program: &Value, sealed_facts: &Value) -> Result<Vec<Value>> {
    let canonical = canonical_program(program, sealed_facts)?;
    let graph = Graph::parse(&canonical)?;
    let always = object([("kind", Value::String("always".into()))]);
    let empty = object([]);
    let mut plans = BTreeMap::new();
    let nodes = graph.nodes.values().cloned().collect::<Vec<_>>();
    let edges = graph.edges.values().cloned().collect::<Vec<_>>();
    let management_ref = graph
        .resource_ids
        .get("management_ref")
        .and_then(|ids| ids.first())
        .cloned();

    for edge in &edges {
        let source = graph.node(&edge.source)?;
        if matches!(source.zone.as_str(), "entry" | "setup")
            && edge.effect.is_null()
            && edge.target != "hub"
        {
            enumerate_candidate(
                &canonical,
                sealed_facts,
                "insert_setup",
                object([
                    ("edgeId", Value::String(edge.id.clone())),
                    ("kind", Value::String("context".into())),
                    ("guard", always.clone()),
                ]),
                &mut plans,
            )?;
        }
    }
    let enter_edges = edges
        .iter()
        .filter(|edge| {
            graph
                .nodes
                .get(&edge.target)
                .is_some_and(|node| node.zone == "entry" && node.kind == "entry")
                && edge.effect.as_str() == Some("enter_next_open")
        })
        .cloned()
        .collect::<Vec<_>>();
    for edge in &enter_edges {
        enumerate_candidate(
            &canonical,
            sealed_facts,
            "rewire_entry_branch",
            object([
                ("edgeId", Value::String(edge.id.clone())),
                ("sourceId", Value::String(edge.source.clone())),
                ("priority", Value::from(edge.priority)),
                ("guard", always.clone()),
            ]),
            &mut plans,
        )?;
        enumerate_candidate(
            &canonical,
            sealed_facts,
            "insert_confirmation_rejection",
            object([
                ("edgeId", Value::String(edge.id.clone())),
                (
                    "rejectPriority",
                    Value::from(edge.priority.saturating_add(10)),
                ),
                ("rejectionTimeoutBars", Value::from(6)),
                ("confirmGuard", always.clone()),
                ("rejectGuard", empty.clone()),
                (
                    "sourceRejectGuard",
                    object([
                        ("kind", Value::String("not".into())),
                        ("guard", always.clone()),
                    ]),
                ),
            ]),
            &mut plans,
        )?;
    }
    if (enter_edges.len() as u64) < graph.cap("maxEntryBranches")? {
        if let Some(management_ref) = management_ref {
            for node in &nodes {
                if node.zone == "setup" || (node.zone == "entry" && node.kind == "start") {
                    enumerate_candidate(
                        &canonical,
                        sealed_facts,
                        "insert_entry_branch",
                        object([
                            ("sourceId", Value::String(node.id.clone())),
                            ("managementRefId", Value::String(management_ref.clone())),
                            ("priority", Value::from(90)),
                            ("hubPriority", Value::from(10)),
                            ("guard", always.clone()),
                        ]),
                        &mut plans,
                    )?;
                }
            }
        }
    }
    for node in &nodes {
        if node.zone == "setup" {
            enumerate_candidate(
                &canonical,
                sealed_facts,
                "remove_setup",
                object([("nodeId", Value::String(node.id.clone()))]),
                &mut plans,
            )?;
        } else if node.zone == "entry" && node.kind == "entry" {
            enumerate_candidate(
                &canonical,
                sealed_facts,
                "remove_entry_branch",
                object([("nodeId", Value::String(node.id.clone()))]),
                &mut plans,
            )?;
        }
    }
    for (effect, priority) in [
        ("move_stop_to_break_even_next_open", 30_u64),
        ("tighten_stop_next_open", 35),
        ("activate_trailing_stop_next_open", 40),
    ] {
        enumerate_candidate(
            &canonical,
            sealed_facts,
            "insert_management_region",
            object([
                ("effect", Value::String(effect.into())),
                ("priority", Value::from(priority)),
                ("kind", Value::String(effect.into())),
                ("guard", always.clone()),
            ]),
            &mut plans,
        )?;
    }
    enumerate_candidate(
        &canonical,
        sealed_facts,
        "insert_exit_region",
        object([
            ("priority", Value::from(50)),
            ("kind", Value::String("timed_exit".into())),
            ("guard", always.clone()),
        ]),
        &mut plans,
    )?;
    enumerate_candidate(
        &canonical,
        sealed_facts,
        "insert_timeout_rearm",
        object([("timeoutBars", Value::from(12)), ("guard", always.clone())]),
        &mut plans,
    )?;
    for node in &nodes {
        match node.zone.as_str() {
            "management" => {
                enumerate_candidate(
                    &canonical,
                    sealed_facts,
                    "rewire_management_region",
                    object([
                        ("nodeId", Value::String(node.id.clone())),
                        ("priority", Value::from(45)),
                        ("effect", Value::String("tighten_stop_next_open".into())),
                        ("guard", always.clone()),
                    ]),
                    &mut plans,
                )?;
                enumerate_candidate(
                    &canonical,
                    sealed_facts,
                    "remove_management_region",
                    object([("nodeId", Value::String(node.id.clone()))]),
                    &mut plans,
                )?;
            }
            "exit" => {
                enumerate_candidate(
                    &canonical,
                    sealed_facts,
                    "rewire_exit_region",
                    object([
                        ("nodeId", Value::String(node.id.clone())),
                        ("priority", Value::from(55)),
                        ("guard", always.clone()),
                    ]),
                    &mut plans,
                )?;
                enumerate_candidate(
                    &canonical,
                    sealed_facts,
                    "remove_exit_region",
                    object([("nodeId", Value::String(node.id.clone()))]),
                    &mut plans,
                )?;
            }
            "recovery" => enumerate_candidate(
                &canonical,
                sealed_facts,
                "remove_timeout_rearm",
                object([("nodeId", Value::String(node.id.clone()))]),
                &mut plans,
            )?,
            _ => {}
        }
    }
    Ok(plans.into_values().collect())
}

fn enumerate_candidate(
    program: &Value,
    sealed_facts: &Value,
    operation: &str,
    arguments: Value,
    output: &mut BTreeMap<String, Value>,
) -> Result<()> {
    let Ok(plan) = make_plan(program, sealed_facts, operation, &arguments) else {
        return Ok(());
    };
    if apply_plan(program, sealed_facts, &plan).is_ok() {
        output.insert(canonical_sha256(&plan)?, plan);
    }
    Ok(())
}

struct Plan {
    operation: String,
    source: String,
    arguments: Value,
}
impl Plan {
    fn parse(value: &Value) -> Result<Self> {
        exact_keys(
            value,
            &[
                "schemaVersion",
                "operatorSchema",
                "operation",
                "sourceGenomeSha256",
                "arguments",
            ],
            "topology plan",
        )?;
        if field(value, "schemaVersion").and_then(Value::as_str) != Some(V5_TOPOLOGY_PLAN_SCHEMA)
            || field(value, "operatorSchema").and_then(Value::as_str)
                != Some(V5_TOPOLOGY_OPERATOR_SCHEMA)
        {
            return Err(invalid("unsupported topology plan schema"));
        }
        let operation = token(required(value, "operation", "topology plan")?, "operation")?;
        if !OPERATIONS.contains(&operation.as_str()) {
            return Err(invalid("unsupported topology operation"));
        }
        let source = sha(
            required(value, "sourceGenomeSha256", "topology plan")?,
            "source genome SHA-256",
        )?;
        let arguments = clone_value(required(value, "arguments", "topology plan")?)?;
        obj(&arguments, "plan arguments")?;
        let canonical = object([
            (
                "schemaVersion",
                Value::String(V5_TOPOLOGY_PLAN_SCHEMA.to_owned()),
            ),
            (
                "operatorSchema",
                Value::String(V5_TOPOLOGY_OPERATOR_SCHEMA.to_owned()),
            ),
            ("operation", Value::String(operation.clone())),
            ("sourceGenomeSha256", Value::String(source.clone())),
            ("arguments", arguments.clone()),
        ]);
        if value != &canonical {
            return Err(invalid("topology plan must be canonical"));
        }
        Ok(Self {
            operation,
            source,
            arguments,
        })
    }
}
const OPERATIONS: &[&str] = &[
    "insert_setup",
    "remove_setup",
    "rewire_entry_branch",
    "insert_entry_branch",
    "remove_entry_branch",
    "insert_confirmation_rejection",
    "insert_timeout_rearm",
    "remove_timeout_rearm",
    "insert_management_region",
    "remove_management_region",
    "rewire_management_region",
    "insert_exit_region",
    "remove_exit_region",
    "rewire_exit_region",
];

/// The sealed-facts shape prevents a later caller from supplying a budget or
/// direction that differs from the canonical parent:
/// `{schemaVersion, authoritySha256, side, budget}`.
fn canonical_program(program: &Value, sealed: &Value) -> Result<Value> {
    exact_keys(
        sealed,
        &["schemaVersion", "authoritySha256", "side", "budget"],
        "sealed topology facts",
    )?;
    if field(sealed, "schemaVersion").and_then(Value::as_str)
        != Some(V5_TOPOLOGY_SEALED_FACTS_SCHEMA)
    {
        return Err(invalid("unsupported sealed topology facts schema"));
    }
    let side = match field(sealed, "side").and_then(Value::as_str) {
        Some("long") => "long",
        Some("short") => "short",
        _ => return Err(invalid("sealed topology side must be long or short")),
    };
    let _ = sha(
        required(sealed, "authoritySha256", "sealed topology facts")?,
        "sealed authority SHA-256",
    )?;
    let program = clone_value(program)?;
    exact_keys(
        &program,
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
        "v5 topology program",
    )?;
    if field(&program, "schemaVersion").and_then(Value::as_str) != Some(GENOME_SCHEMA)
        || field(&program, "programKind").and_then(Value::as_str) != Some(PROGRAM_KIND)
        || field(&program, "codec").and_then(Value::as_str) != Some(CODEC)
        || field(&program, "direction").and_then(Value::as_str) != Some(side)
    {
        return Err(invalid("v5 topology program envelope is invalid"));
    }
    let instrument = token(
        required(&program, "instrument", "v5 topology program")?,
        "instrument",
    )?;
    if instrument != instrument.to_ascii_uppercase() {
        return Err(invalid("instrument must be uppercase"));
    }
    validate_budget(required(&program, "budget", "v5 topology program")?)?;
    if required(sealed, "budget", "sealed topology facts")?
        != required(&program, "budget", "v5 topology program")?
    {
        return Err(invalid("sealed topology budget drifted from program"));
    }
    let graph = Graph::parse(&program)?;
    graph.validate()?;
    Ok(graph.canonical(&program)?)
}

fn validate_budget(value: &Value) -> Result<()> {
    const KEYS: &[&str] = &[
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
    exact_keys(value, KEYS, "budget")?;
    let caps = [14, 56, 4, 3, 4, 12, 3, 4, 3, 3, 3, 64, 4];
    for (key, cap) in KEYS.iter().zip(caps) {
        let n = positive(required(value, key, "budget")?, &format!("budget {key}"))?;
        if n > cap {
            return Err(invalid(format!("budget {key} may not exceed v1 cap")));
        }
    }
    Ok(())
}

#[derive(Clone)]
struct Node {
    id: String,
    zone: String,
    kind: String,
    guard: Value,
    resources: Vec<Value>,
    timeout: Value,
}
#[derive(Clone)]
struct Edge {
    id: String,
    source: String,
    target: String,
    event: String,
    priority: u64,
    guard: Value,
    effect: Value,
}
struct Graph {
    nodes: BTreeMap<String, Node>,
    edges: BTreeMap<String, Edge>,
    resource_ids: BTreeMap<String, BTreeSet<String>>,
    budget: BTreeMap<String, u64>,
}

impl Graph {
    fn parse(program: &Value) -> Result<Self> {
        let resources = required(program, "resources", "v5 topology program")?;
        exact_keys(
            resources,
            &["indicators", "evidenceGroups", "events", "managementRefs"],
            "topology resources",
        )?;
        let mut resource_ids = BTreeMap::new();
        for (field_name, kind) in [
            ("indicators", "indicator"),
            ("evidenceGroups", "evidence_group"),
            ("events", "event"),
            ("managementRefs", "management_ref"),
        ] {
            let mut ids = BTreeSet::new();
            for row in rows(
                required(resources, field_name, "topology resources")?,
                field_name,
            )? {
                let id = if kind == "indicator" {
                    token(
                        required(
                            required(row, "meta", "indicator")?,
                            "instanceId",
                            "indicator meta",
                        )?,
                        "indicator resource ID",
                    )?
                } else {
                    token(
                        required(row, "id", field_name)?,
                        &format!("{kind} resource ID"),
                    )?
                };
                if !ids.insert(id) {
                    return Err(invalid(format!("duplicate {kind} resource ID")));
                }
            }
            resource_ids.insert(kind.to_owned(), ids);
        }
        let mut nodes = BTreeMap::new();
        for raw in rows(
            required(program, "nodes", "v5 topology program")?,
            "topology nodes",
        )? {
            exact_keys(
                raw,
                &["id", "zone", "kind", "guard", "resources", "timeoutBars"],
                "topology node",
            )?;
            let node = Node {
                id: token(required(raw, "id", "topology node")?, "node ID")?,
                zone: token(required(raw, "zone", "topology node")?, "node zone")?,
                kind: token(required(raw, "kind", "topology node")?, "node kind")?,
                guard: clone_value(required(raw, "guard", "topology node")?)?,
                resources: clone_rows(
                    required(raw, "resources", "topology node")?,
                    "node resources",
                )?,
                timeout: clone_value(required(raw, "timeoutBars", "topology node")?)?,
            };
            if ![
                "entry",
                "setup",
                "position",
                "management",
                "exit",
                "recovery",
            ]
            .contains(&node.zone.as_str())
                || !node.guard.is_object()
            {
                return Err(invalid("topology node has unsupported fields"));
            }
            if !node.timeout.is_null() {
                let _ = positive(&node.timeout, "timeout bars")?;
            }
            if nodes.insert(node.id.clone(), node).is_some() {
                return Err(invalid("duplicate node ID"));
            }
        }
        let mut edges = BTreeMap::new();
        for raw in rows(
            required(program, "edges", "v5 topology program")?,
            "topology edges",
        )? {
            exact_keys(
                raw,
                &[
                    "id",
                    "source",
                    "target",
                    "eventClass",
                    "priority",
                    "guard",
                    "effect",
                ],
                "topology edge",
            )?;
            let edge = Edge {
                id: token(required(raw, "id", "topology edge")?, "edge ID")?,
                source: token(required(raw, "source", "topology edge")?, "edge source")?,
                target: token(required(raw, "target", "topology edge")?, "edge target")?,
                event: token(
                    required(raw, "eventClass", "topology edge")?,
                    "edge event class",
                )?,
                priority: integer(required(raw, "priority", "topology edge")?, "edge priority")?,
                guard: clone_value(required(raw, "guard", "topology edge")?)?,
                effect: clone_value(required(raw, "effect", "topology edge")?)?,
            };
            if edge.event != "decision"
                || !edge.guard.is_object()
                || (!edge.effect.is_null() && edge.effect.as_str().is_none())
            {
                return Err(invalid("topology edge has unsupported fields"));
            }
            if edges.insert(edge.id.clone(), edge).is_some() {
                return Err(invalid("duplicate edge ID"));
            }
        }
        let budget = obj(
            required(program, "budget", "v5 topology program")?,
            "budget",
        )?
        .iter()
        .map(|(k, v)| Ok((k.clone(), positive(v, &format!("budget {k}"))?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(Self {
            nodes,
            edges,
            resource_ids,
            budget,
        })
    }

    fn validate(&self) -> Result<()> {
        if self.nodes.is_empty() || self.edges.is_empty() {
            return Err(invalid("genome requires nodes and edges"));
        }
        if self.nodes.len() as u64 > self.cap("maxStates")?
            || self.edges.len() as u64 > self.cap("maxTransitions")?
        {
            return Err(invalid("authored graph exceeds state or transition budget"));
        }
        let starts = self
            .nodes
            .values()
            .filter(|n| n.zone == "entry" && n.kind == "start")
            .count();
        let hubs = self
            .nodes
            .values()
            .filter(|n| n.zone == "position" && n.kind == "position_hub")
            .count();
        if starts != 1
            || hubs != 1
            || self
                .nodes
                .values()
                .any(|n| n.zone == "position" && n.kind != "position_hub")
        {
            return Err(invalid(
                "genome requires exactly one entry start and shared position hub",
            ));
        }
        for (kind, max) in [
            ("management", "maxManagementRegions"),
            ("exit", "maxExitRegions"),
            ("recovery", "maxRecoveryRegions"),
        ] {
            if self.nodes.values().filter(|n| n.zone == kind).count() as u64 > self.cap(max)? {
                return Err(invalid("region count exceeds initial v1 budget"));
            }
        }
        let hub = self
            .nodes
            .values()
            .find(|n| n.zone == "position")
            .expect("validated hub");
        let mut priorities = BTreeSet::new();
        let mut entries = 0_u64;
        for edge in self.edges.values() {
            let source = self
                .nodes
                .get(&edge.source)
                .ok_or_else(|| invalid("dangling graph edge"))?;
            let target = self
                .nodes
                .get(&edge.target)
                .ok_or_else(|| invalid("dangling graph edge"))?;
            if !priorities.insert((edge.source.clone(), edge.event.clone(), edge.priority)) {
                return Err(invalid("priority conflict at one source/event class"));
            }
            match target.zone.as_str() {
                "entry" if target.kind == "entry" => {
                    entries += 1;
                    if !matches!(source.zone.as_str(), "entry" | "setup")
                        || edge.effect.as_str() != Some("enter_next_open")
                    {
                        return Err(invalid("entry branches require one enter_next_open effect"));
                    }
                }
                "setup" => {
                    if !matches!(source.zone.as_str(), "entry" | "setup") || !edge.effect.is_null()
                    {
                        return Err(invalid(
                            "setup graph must be a side-effect-free entry/setup DAG",
                        ));
                    }
                }
                "position" if target.id == hub.id => {
                    if source.zone != "entry" || source.kind != "entry" || !edge.effect.is_null() {
                        return Err(invalid(
                            "entry result may only connect conceptually to the position hub",
                        ));
                    }
                }
                "management" => {
                    if edge.source != hub.id
                        || !edge
                            .effect
                            .as_str()
                            .is_some_and(|e| MANAGEMENT.contains(&e))
                    {
                        return Err(invalid(
                            "management regions must dispatch from the shared position hub",
                        ));
                    }
                }
                "exit" => {
                    if edge.source != hub.id || edge.effect.as_str() != Some("exit_next_open") {
                        return Err(invalid(
                            "exit regions must dispatch from the shared position hub",
                        ));
                    }
                }
                "recovery" => {
                    if !matches!(source.zone.as_str(), "entry" | "setup")
                        || !edge.effect.is_null()
                        || target.timeout.is_null()
                    {
                        return Err(invalid(
                            "recovery routes must be side-effect-free and bounded",
                        ));
                    }
                }
                _ => return Err(invalid("unsupported authored edge topology")),
            }
        }
        if entries == 0 || entries > self.cap("maxEntryBranches")? {
            return Err(invalid("entry branch count violates budget"));
        }
        for node in self.nodes.values() {
            if node.zone == "recovery" && node.timeout.is_null() {
                return Err(invalid("recovery nodes require a bounded timeout"));
            }
            if !node.timeout.is_null()
                && positive(&node.timeout, "timeout bars")? > self.cap("maxTimeoutBars")?
            {
                return Err(invalid("timeout exceeds budget"));
            }
            self.validate_resources(node)?;
        }
        self.validate_reachability()?;
        self.validate_preposition_dag()?;
        Ok(())
    }
    fn validate_resources(&self, node: &Node) -> Result<()> {
        for use_ in &node.resources {
            exact_keys(use_, &["kind", "id"], "node resource use")?;
            let kind = token(
                required(use_, "kind", "node resource use")?,
                "resource kind",
            )?;
            let id = token(required(use_, "id", "node resource use")?, "resource ID")?;
            if !self
                .resource_ids
                .get(&kind)
                .is_some_and(|ids| ids.contains(&id))
            {
                return Err(invalid("node references a dangling resource"));
            }
        }
        Ok(())
    }
    fn validate_reachability(&self) -> Result<()> {
        let start = self
            .nodes
            .values()
            .find(|n| n.zone == "entry" && n.kind == "start")
            .unwrap()
            .id
            .clone();
        let hub = self
            .nodes
            .values()
            .find(|n| n.zone == "position")
            .unwrap()
            .id
            .clone();
        let mut next: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for e in self.edges.values() {
            next.entry(e.source.clone())
                .or_default()
                .push(e.target.clone());
        }
        let recovery = self
            .nodes
            .values()
            .filter(|n| n.zone == "recovery")
            .map(|n| n.id.clone())
            .collect::<Vec<_>>();
        for n in self.nodes.values() {
            if n.zone == "entry" && n.kind == "entry" {
                next.entry(n.id.clone()).or_default().push(hub.clone());
            }
        }
        // The compiler's protective-close, exit-close, and management-close
        // transitions all select the first lexically sorted recovery state.
        // Timed recovery transitions then form the same deterministic chain
        // before re-arming the entry start.  These runtime edges are not
        // authored decision edges, but are required for exact reachability.
        if let Some(first) = recovery.first() {
            next.entry(hub.clone()).or_default().push(first.clone());
        }
        for (index, node_id) in recovery.iter().enumerate() {
            let destination = recovery
                .get(index + 1)
                .cloned()
                .unwrap_or_else(|| start.clone());
            next.entry(node_id.clone()).or_default().push(destination);
        }
        let mut seen = BTreeSet::from([start.clone()]);
        let mut queue = VecDeque::from([start]);
        while let Some(id) = queue.pop_front() {
            for target in next.get(&id).into_iter().flatten() {
                if seen.insert(target.clone()) {
                    queue.push_back(target.clone());
                }
            }
        }
        if seen.len() != self.nodes.len() {
            return Err(invalid("orphan or unreachable graph node"));
        }
        Ok(())
    }
    fn validate_preposition_dag(&self) -> Result<()> {
        fn visit(
            id: &str,
            g: &Graph,
            active: &BTreeSet<String>,
            visiting: &mut BTreeSet<String>,
            done: &mut BTreeSet<String>,
        ) -> Result<()> {
            if !active.contains(id) || done.contains(id) {
                return Ok(());
            }
            if !visiting.insert(id.to_owned()) {
                return Err(invalid("entry/setup graph must be acyclic"));
            }
            for edge in g
                .edges
                .values()
                .filter(|e| e.source == id && active.contains(&e.target))
            {
                visit(&edge.target, g, active, visiting, done)?;
            }
            visiting.remove(id);
            done.insert(id.to_owned());
            Ok(())
        }
        let active = self
            .nodes
            .values()
            .filter(|n| n.zone == "entry" || n.zone == "setup")
            .map(|n| n.id.clone())
            .collect::<BTreeSet<_>>();
        let mut visiting = BTreeSet::new();
        let mut done = BTreeSet::new();
        for id in &active {
            visit(id, self, &active, &mut visiting, &mut done)?;
        }
        Ok(())
    }
    fn cap(&self, key: &str) -> Result<u64> {
        self.budget
            .get(key)
            .copied()
            .ok_or_else(|| invalid(format!("budget lacks {key}")))
    }
    fn unique_priority(&self, source: &str, priority: u64, except: Option<&str>) -> Result<()> {
        if self.edges.values().any(|e| {
            e.id != except.unwrap_or("")
                && e.source == source
                && e.event == "decision"
                && e.priority == priority
        }) {
            Err(invalid(
                "operator priority conflicts at one source/event class",
            ))
        } else {
            Ok(())
        }
    }
    fn id(plan_sha: &str, role: &str) -> String {
        format!("{role}_{}", &plan_sha[7..23])
    }
    fn node(&self, id: &str) -> Result<&Node> {
        self.nodes
            .get(id)
            .ok_or_else(|| invalid("operation names an unknown node"))
    }
    fn edge(&self, id: &str) -> Result<&Edge> {
        self.edges
            .get(id)
            .ok_or_else(|| invalid("operation names an unknown edge"))
    }
    fn hub(&self) -> Result<&Node> {
        self.nodes
            .values()
            .find(|n| n.zone == "position" && n.kind == "position_hub")
            .ok_or_else(|| invalid("genome lacks shared position hub"))
    }
    fn apply(&mut self, op: &str, args: &Value, plan_sha: &str) -> Result<()> {
        match op {
            "insert_setup" => self.insert_setup(args, plan_sha),
            "remove_setup" => self.remove_setup(args),
            "rewire_entry_branch" => self.rewire_entry(args),
            "insert_entry_branch" => self.insert_entry(args, plan_sha),
            "remove_entry_branch" => self.remove_entry(args),
            "insert_confirmation_rejection" => self.confirm_reject(args, plan_sha),
            "insert_timeout_rearm" => self.insert_rearm(args, plan_sha),
            "remove_timeout_rearm" => self.remove_rearm(args),
            "insert_management_region" => self.insert_region(args, plan_sha, "management"),
            "insert_exit_region" => self.insert_region(args, plan_sha, "exit"),
            "remove_management_region" => self.remove_region(args, "management"),
            "remove_exit_region" => self.remove_region(args, "exit"),
            "rewire_management_region" => self.rewire_region(args, "management"),
            "rewire_exit_region" => self.rewire_region(args, "exit"),
            _ => Err(invalid("unsupported topology operation")),
        }
    }
    fn insert_setup(&mut self, a: &Value, p: &str) -> Result<()> {
        let edge_id = arg_token(a, "edgeId")?;
        let old = self.edge(&edge_id)?.clone();
        let source = self.node(&old.source)?.clone();
        let target = self.node(&old.target)?.clone();
        if !matches!(source.zone.as_str(), "entry" | "setup")
            || !matches!(target.zone.as_str(), "entry" | "setup")
            || (!old.effect.is_null() && !(target.zone == "entry" && target.kind == "entry"))
        {
            return Err(invalid("setup insertion requires a pre-position edge"));
        }
        let node_id = Self::id(p, "setup");
        let inbound = Self::id(p, "setup_in");
        if self.nodes.contains_key(&node_id) || self.edges.contains_key(&inbound) {
            return Err(invalid("deterministic topology ID collision"));
        }
        let node = Node {
            id: node_id.clone(),
            zone: "setup".into(),
            kind: arg_token_default(a, "kind", "setup")?,
            guard: arg_object_default(a, "guard")?,
            resources: arg_resources_default(a)?,
            timeout: arg_optional(a, "timeoutBars")?,
        };
        let mut first = old.clone();
        first.id = inbound.clone();
        first.target = node_id.clone();
        first.effect = Value::Null;
        let mut second = old;
        second.source = node_id.clone();
        second.guard = object([]);
        self.edges.remove(&edge_id);
        self.nodes.insert(node_id, node);
        self.edges.insert(inbound, first);
        self.edges.insert(second.id.clone(), second);
        Ok(())
    }
    fn remove_setup(&mut self, a: &Value) -> Result<()> {
        let id = arg_token(a, "nodeId")?;
        let node = self.node(&id)?.clone();
        if node.zone != "setup" {
            return Err(invalid("operation requires a setup node"));
        }
        let incoming = self
            .edges
            .values()
            .filter(|e| e.target == id)
            .cloned()
            .collect::<Vec<_>>();
        let outgoing = self
            .edges
            .values()
            .filter(|e| e.source == id)
            .cloned()
            .collect::<Vec<_>>();
        if incoming.len() != 1 || outgoing.len() != 1 {
            return Err(invalid("only a linear setup motif may be removed"));
        }
        let before = &incoming[0];
        let after = &outgoing[0];
        if !before.effect.is_null() {
            return Err(invalid("setup motif has an invalid incoming effect"));
        }
        self.unique_priority(&before.source, after.priority, Some(&before.id))?;
        let mut replacement = after.clone();
        replacement.source = before.source.clone();
        replacement.guard = before.guard.clone();
        self.nodes.remove(&id);
        self.edges.remove(&before.id);
        self.edges.remove(&after.id);
        self.edges.insert(replacement.id.clone(), replacement);
        Ok(())
    }
    fn setup_source(&self, id: &str) -> Result<&Node> {
        let n = self.node(id)?;
        if !matches!(n.zone.as_str(), "entry" | "setup") || (n.zone == "entry" && n.kind != "start")
        {
            Err(invalid(
                "entry/setup operator source must be an entry or setup node",
            ))
        } else {
            Ok(n)
        }
    }
    fn rewire_entry(&mut self, a: &Value) -> Result<()> {
        let id = arg_token(a, "edgeId")?;
        let mut edge = self.edge(&id)?.clone();
        let target = self.node(&edge.target)?;
        if target.zone != "entry"
            || target.kind != "entry"
            || edge.effect.as_str() != Some("enter_next_open")
        {
            return Err(invalid("operation requires an enter branch"));
        }
        let source = arg_token(a, "sourceId")?;
        self.setup_source(&source)?;
        let priority = arg_integer_default(a, "priority", edge.priority)?;
        self.unique_priority(&source, priority, Some(&id))?;
        edge.source = source;
        edge.priority = priority;
        if field(a, "guard").is_some() {
            edge.guard = arg_object(a, "guard")?;
        }
        self.edges.insert(id, edge);
        Ok(())
    }
    fn insert_entry(&mut self, a: &Value, p: &str) -> Result<()> {
        let source = arg_token(a, "sourceId")?;
        self.setup_source(&source)?;
        let priority = arg_integer(a, "priority")?;
        self.unique_priority(&source, priority, None)?;
        let management = arg_token(a, "managementRefId")?;
        if !self
            .resource_ids
            .get("management_ref")
            .is_some_and(|x| x.contains(&management))
        {
            return Err(invalid(
                "entry branch references an unknown management plan",
            ));
        }
        let node_id = Self::id(p, "entry");
        let branch_id = Self::id(p, "entry_branch");
        let hub_id = Self::id(p, "entry_hub");
        let hub = self.hub()?.id.clone();
        for id in [&node_id, &branch_id, &hub_id] {
            if self.nodes.contains_key(id) || self.edges.contains_key(id) {
                return Err(invalid("deterministic topology ID collision"));
            }
        }
        self.nodes.insert(
            node_id.clone(),
            Node {
                id: node_id.clone(),
                zone: "entry".into(),
                kind: "entry".into(),
                guard: object([]),
                resources: vec![resource("management_ref", &management)],
                timeout: Value::Null,
            },
        );
        self.edges.insert(
            branch_id.clone(),
            Edge {
                id: branch_id,
                source,
                target: node_id.clone(),
                event: "decision".into(),
                priority,
                guard: arg_object_default(a, "guard")?,
                effect: Value::String("enter_next_open".into()),
            },
        );
        self.edges.insert(
            hub_id.clone(),
            Edge {
                id: hub_id,
                source: node_id,
                target: hub,
                event: "decision".into(),
                priority: arg_integer_default(a, "hubPriority", 10)?,
                guard: object([]),
                effect: Value::Null,
            },
        );
        Ok(())
    }
    fn remove_entry(&mut self, a: &Value) -> Result<()> {
        let id = arg_token(a, "nodeId")?;
        let entry = self.node(&id)?.clone();
        if entry.zone != "entry" || entry.kind != "entry" {
            return Err(invalid("operation requires an entry node"));
        }
        let incoming = self
            .edges
            .values()
            .filter(|e| e.target == id)
            .cloned()
            .collect::<Vec<_>>();
        let connector = self
            .edges
            .values()
            .filter(|e| {
                e.source == id
                    && self
                        .nodes
                        .get(&e.target)
                        .is_some_and(|n| n.zone == "position")
            })
            .cloned()
            .collect::<Vec<_>>();
        if incoming.len() != 1
            || incoming[0].effect.as_str() != Some("enter_next_open")
            || connector.len() != 1
        {
            return Err(invalid("only a single-port entry branch may be removed"));
        }
        if self
            .edges
            .values()
            .filter(|e| {
                self.nodes
                    .get(&e.target)
                    .is_some_and(|n| n.zone == "entry" && n.kind == "entry")
            })
            .count()
            <= 1
        {
            return Err(invalid("cannot remove the final entry branch"));
        }
        let refs = entry
            .resources
            .iter()
            .filter(|r| field(r, "kind").and_then(Value::as_str) == Some("management_ref"))
            .collect::<Vec<_>>();
        if refs.len() != 1 {
            return Err(invalid(
                "entry motif requires exactly one management reference",
            ));
        }
        if !self
            .nodes
            .values()
            .filter(|n| n.id != id)
            .any(|n| n.resources.iter().any(|r| r == refs[0]))
        {
            return Err(invalid(
                "cannot orphan a management plan while removing an entry branch",
            ));
        }
        self.nodes.remove(&id);
        self.edges.remove(&incoming[0].id);
        self.edges.remove(&connector[0].id);
        Ok(())
    }
    fn confirm_reject(&mut self, a: &Value, p: &str) -> Result<()> {
        let id = arg_token(a, "edgeId")?;
        let branch = self.edge(&id)?.clone();
        if branch.effect.as_str() != Some("enter_next_open") {
            return Err(invalid("confirmation motif requires an enter branch"));
        }
        let source = self.node(&branch.source)?.clone();
        let entry = self.node(&branch.target)?.clone();
        if !matches!(source.zone.as_str(), "entry" | "setup")
            || entry.zone != "entry"
            || entry.kind != "entry"
        {
            return Err(invalid(
                "confirmation motif requires a pre-position entry branch",
            ));
        }
        let priority = arg_integer(a, "rejectPriority")?;
        let timeout = positive(
            required(a, "rejectionTimeoutBars", "confirmation arguments")?,
            "rejection timeout",
        )?;
        self.unique_priority(&source.id, priority, None)?;
        let confirm_id = Self::id(p, "confirm");
        let reject_id = Self::id(p, "rejected_rearm");
        let confirm_edge_id = Self::id(p, "confirm_path");
        let reject_edge_id = Self::id(p, "reject_path");
        for x in [&confirm_id, &reject_id, &confirm_edge_id, &reject_edge_id] {
            if self.nodes.contains_key(x) || self.edges.contains_key(x) {
                return Err(invalid("deterministic topology ID collision"));
            }
        }
        let mut source_confirm = branch.clone();
        source_confirm.target = confirm_id.clone();
        source_confirm.effect = Value::Null;
        self.edges.remove(&id);
        self.nodes.insert(
            confirm_id.clone(),
            Node {
                id: confirm_id.clone(),
                zone: "setup".into(),
                kind: "confirmation".into(),
                guard: arg_object_default(a, "confirmGuard")?,
                resources: vec![],
                timeout: Value::Null,
            },
        );
        self.nodes.insert(
            reject_id.clone(),
            Node {
                id: reject_id.clone(),
                zone: "recovery".into(),
                kind: "rejection_rearm".into(),
                guard: arg_object_default(a, "rejectGuard")?,
                resources: vec![],
                timeout: Value::from(timeout),
            },
        );
        self.edges.insert(source_confirm.id.clone(), source_confirm);
        self.edges.insert(
            reject_edge_id.clone(),
            Edge {
                id: reject_edge_id,
                source: source.id,
                target: reject_id,
                event: "decision".into(),
                priority,
                guard: arg_object_default(a, "sourceRejectGuard")?,
                effect: Value::Null,
            },
        );
        self.edges.insert(
            confirm_edge_id.clone(),
            Edge {
                id: confirm_edge_id,
                source: confirm_id,
                target: entry.id,
                event: "decision".into(),
                priority: 10,
                guard: object([]),
                effect: Value::String("enter_next_open".into()),
            },
        );
        Ok(())
    }
    fn insert_rearm(&mut self, a: &Value, p: &str) -> Result<()> {
        let timeout = positive(
            required(a, "timeoutBars", "rearm arguments")?,
            "re-arm timeout",
        )?;
        let id = Self::id(p, "rearm");
        if self.nodes.contains_key(&id) {
            return Err(invalid("deterministic topology ID collision"));
        }
        self.nodes.insert(
            id.clone(),
            Node {
                id,
                zone: "recovery".into(),
                kind: "bounded_rearm".into(),
                guard: arg_object_default(a, "guard")?,
                resources: vec![],
                timeout: Value::from(timeout),
            },
        );
        Ok(())
    }
    fn remove_rearm(&mut self, a: &Value) -> Result<()> {
        let id = arg_token(a, "nodeId")?;
        let n = self.node(&id)?;
        if n.zone != "recovery" || n.kind != "bounded_rearm" {
            return Err(invalid("operation requires a bounded re-arm recovery node"));
        }
        self.nodes.remove(&id);
        Ok(())
    }
    fn insert_region(&mut self, a: &Value, p: &str, zone: &str) -> Result<()> {
        let hub = self.hub()?.id.clone();
        let priority = arg_integer(a, "priority")?;
        self.unique_priority(&hub, priority, None)?;
        let (role, dispatch, effect, default_kind): (&str, &str, String, &str) =
            if zone == "management" {
                let effect = arg_token(a, "effect")?;
                if !MANAGEMENT.contains(&effect.as_str()) {
                    return Err(invalid("management insertion requires a management effect"));
                }
                ("management", "management_dispatch", effect, "management")
            } else {
                ("exit", "exit_dispatch", "exit_next_open".to_owned(), "exit")
            };
        let node_id = Self::id(p, role);
        let edge_id = Self::id(p, dispatch);
        if self.nodes.contains_key(&node_id) || self.edges.contains_key(&edge_id) {
            return Err(invalid("deterministic topology ID collision"));
        }
        let kind_default = if zone == "management" {
            effect.as_str()
        } else {
            default_kind
        };
        self.nodes.insert(
            node_id.clone(),
            Node {
                id: node_id.clone(),
                zone: zone.into(),
                kind: arg_token_default(a, "kind", kind_default)?,
                guard: arg_object_default(a, "nodeGuard")?,
                resources: vec![],
                timeout: Value::Null,
            },
        );
        self.edges.insert(
            edge_id.clone(),
            Edge {
                id: edge_id,
                source: hub,
                target: node_id,
                event: "decision".into(),
                priority,
                guard: arg_object_default(a, "guard")?,
                effect: Value::String(effect),
            },
        );
        Ok(())
    }
    fn region(&self, id: &str, zone: &str) -> Result<(Node, Edge)> {
        let n = self.node(id)?.clone();
        if n.zone != zone {
            return Err(invalid(format!("operation requires a {zone} region")));
        }
        let incoming = self
            .edges
            .values()
            .filter(|e| {
                e.target == id
                    && self
                        .nodes
                        .get(&e.source)
                        .is_some_and(|s| s.zone == "position")
            })
            .cloned()
            .collect::<Vec<_>>();
        if incoming.len() != 1 {
            return Err(invalid("region must have exactly one shared-hub dispatch"));
        }
        Ok((n, incoming[0].clone()))
    }
    fn remove_region(&mut self, a: &Value, zone: &str) -> Result<()> {
        let id = arg_token(a, "nodeId")?;
        let (_, e) = self.region(&id, zone)?;
        self.nodes.remove(&id);
        self.edges.remove(&e.id);
        Ok(())
    }
    fn rewire_region(&mut self, a: &Value, zone: &str) -> Result<()> {
        let id = arg_token(a, "nodeId")?;
        let (mut node, mut edge) = self.region(&id, zone)?;
        let priority = arg_integer_default(a, "priority", edge.priority)?;
        self.unique_priority(&edge.source, priority, Some(&edge.id))?;
        edge.priority = priority;
        if field(a, "guard").is_some() {
            edge.guard = arg_object(a, "guard")?;
        }
        if field(a, "kind").is_some() {
            node.kind = arg_token(a, "kind")?;
        }
        if field(a, "nodeGuard").is_some() {
            node.guard = arg_object(a, "nodeGuard")?;
        }
        if zone == "management" && field(a, "effect").is_some() {
            let e = arg_token(a, "effect")?;
            if !MANAGEMENT.contains(&e.as_str()) {
                return Err(invalid(
                    "rewired management region requires a management effect",
                ));
            }
            edge.effect = Value::String(e);
        }
        self.nodes.insert(id, node);
        self.edges.insert(edge.id.clone(), edge);
        Ok(())
    }
    fn canonical(&self, template: &Value) -> Result<Value> {
        let mut output = clone_value(template)?;
        let map = output
            .as_object_mut()
            .ok_or_else(|| invalid("program drifted"))?;
        map.insert(
            "nodes".into(),
            array(self.nodes.values().map(Node::canonical)),
        );
        map.insert(
            "edges".into(),
            array(self.edges.values().map(Edge::canonical)),
        );
        Ok(output)
    }
}
impl Node {
    fn canonical(&self) -> Value {
        object([
            ("id", Value::String(self.id.clone())),
            ("zone", Value::String(self.zone.clone())),
            ("kind", Value::String(self.kind.clone())),
            ("guard", self.guard.clone()),
            ("resources", array(self.resources.clone())),
            ("timeoutBars", self.timeout.clone()),
        ])
    }
}
impl Edge {
    fn canonical(&self) -> Value {
        object([
            ("id", Value::String(self.id.clone())),
            ("source", Value::String(self.source.clone())),
            ("target", Value::String(self.target.clone())),
            ("eventClass", Value::String(self.event.clone())),
            ("priority", Value::from(self.priority)),
            ("guard", self.guard.clone()),
            ("effect", self.effect.clone()),
        ])
    }
}

fn clone_rows(value: &Value, label: &str) -> Result<Vec<Value>> {
    rows(value, label)?.iter().map(clone_value).collect()
}
fn arg_optional(args: &Value, key: &str) -> Result<Value> {
    match field(args, key) {
        Some(v) => clone_value(v),
        None => Ok(Value::Null),
    }
}
fn arg_token(args: &Value, key: &str) -> Result<String> {
    token(required(args, key, "topology operation arguments")?, key)
}
fn arg_token_default(args: &Value, key: &str, default: &str) -> Result<String> {
    match field(args, key) {
        Some(v) => token(v, key),
        None => Ok(default.into()),
    }
}
fn arg_integer(args: &Value, key: &str) -> Result<u64> {
    integer(required(args, key, "topology operation arguments")?, key)
}
fn arg_integer_default(args: &Value, key: &str, default: u64) -> Result<u64> {
    match field(args, key) {
        Some(v) => integer(v, key),
        None => Ok(default),
    }
}
fn arg_object(args: &Value, key: &str) -> Result<Value> {
    let v = clone_value(required(args, key, "topology operation arguments")?)?;
    obj(&v, key)?;
    Ok(v)
}
fn arg_object_default(args: &Value, key: &str) -> Result<Value> {
    match field(args, key) {
        Some(_) => arg_object(args, key),
        None => Ok(object([])),
    }
}
fn arg_resources_default(args: &Value) -> Result<Vec<Value>> {
    match field(args, "resources") {
        Some(v) => clone_rows(v, "setup resources"),
        None => Ok(vec![]),
    }
}
fn resource(kind: &str, id: &str) -> Value {
    object([
        ("kind", Value::String(kind.into())),
        ("id", Value::String(id.into())),
    ])
}

fn semantic_trace(before: &Value, after: &Value, operation: &str, plan_sha: &str) -> Result<Value> {
    let ids = |program: &Value, key: &str| -> Result<BTreeMap<String, Value>> {
        let mut out = BTreeMap::new();
        for row in rows(required(program, key, "topology program")?, key)? {
            out.insert(token(required(row, "id", key)?, "ID")?, clone_value(row)?);
        }
        Ok(out)
    };
    let before_nodes = ids(before, "nodes")?;
    let after_nodes = ids(after, "nodes")?;
    let before_edges = ids(before, "edges")?;
    let after_edges = ids(after, "edges")?;
    let delta = |left: &BTreeMap<String, Value>, right: &BTreeMap<String, Value>, which: bool| {
        array(
            (if which {
                right
                    .keys()
                    .filter(|k| !left.contains_key(*k))
                    .cloned()
                    .collect::<Vec<_>>()
            } else {
                left.keys()
                    .filter(|k| !right.contains_key(*k))
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .into_iter()
            .map(Value::String),
        )
    };
    let changed = array(
        before_edges
            .keys()
            .filter(|id| {
                after_edges
                    .get(*id)
                    .is_some_and(|other| other != before_edges.get(*id).unwrap())
            })
            .cloned()
            .map(Value::String),
    );
    Ok(object([
        (
            "schemaVersion",
            Value::String(V5_TOPOLOGY_DELTA_SCHEMA.into()),
        ),
        ("operation", Value::String(operation.into())),
        ("planSha256", Value::String(plan_sha.into())),
        (
            "beforeGenomeSha256",
            Value::String(canonical_sha256(before)?),
        ),
        ("afterGenomeSha256", Value::String(canonical_sha256(after)?)),
        (
            "beforeTopologySha256",
            Value::String(semantic_topology_sha256(before)?),
        ),
        (
            "afterTopologySha256",
            Value::String(semantic_topology_sha256(after)?),
        ),
        ("addedNodes", delta(&before_nodes, &after_nodes, true)),
        ("removedNodes", delta(&before_nodes, &after_nodes, false)),
        ("addedEdges", delta(&before_edges, &after_edges, true)),
        ("removedEdges", delta(&before_edges, &after_edges, false)),
        ("changedEdges", changed),
    ]))
}

/// Exact `EvolvableModuleGenomeV1.semantic_topology_signature` port.  It is
/// deliberately ID-independent: labels are refined from node/edge shape and
/// then lexically sorted by digest, never by generated identifiers.
pub fn semantic_topology_sha256(program: &Value) -> Result<String> {
    let nodes = rows(
        required(program, "nodes", "topology program")?,
        "topology nodes",
    )?;
    let edges = rows(
        required(program, "edges", "topology program")?,
        "topology edges",
    )?;
    let mut node_rows = BTreeMap::new();
    for node in nodes {
        let id = token(required(node, "id", "topology node")?, "node ID")?;
        if node_rows.insert(id, node).is_some() {
            return Err(invalid("topology nodes duplicate IDs"));
        }
    }
    if node_rows.is_empty() {
        return Err(invalid("topology has no nodes"));
    }
    let mut labels = BTreeMap::new();
    for (id, node) in &node_rows {
        let mut resource_kinds = rows(
            required(node, "resources", "topology node")?,
            "node resources",
        )?
        .iter()
        .map(|resource| {
            token(
                required(resource, "kind", "node resource")?,
                "resource kind",
            )
            .map(Value::String)
        })
        .collect::<Result<Vec<_>>>()?;
        // Resource-use insertion order is representation detail, not graph
        // topology.  Fresh v5 identity must match the corrected Python
        // semantic signature even when equivalent resource rows are authored
        // in a different order.
        sort_canonical_values(&mut resource_kinds);
        labels.insert(
            id.clone(),
            canonical_sha256(&object([
                (
                    "zone",
                    clone_value(required(node, "zone", "topology node")?)?,
                ),
                (
                    "kind",
                    clone_value(required(node, "kind", "topology node")?)?,
                ),
                (
                    "guard",
                    guard_shape(required(node, "guard", "topology node")?)?,
                ),
                ("resources", array(resource_kinds)),
                (
                    "timeoutBars",
                    Value::Bool(!required(node, "timeoutBars", "topology node")?.is_null()),
                ),
            ]))?,
        );
    }
    for _ in 0..nodes.len().max(1) {
        let mut updated = BTreeMap::new();
        for (id, label) in &labels {
            let mut outgoing = Vec::new();
            let mut incoming = Vec::new();
            for edge in edges {
                let source = token(required(edge, "source", "topology edge")?, "edge source")?;
                let target = token(required(edge, "target", "topology edge")?, "edge target")?;
                let other = if source == *id { &target } else { &source };
                let neighbor = labels
                    .get(other)
                    .ok_or_else(|| invalid("topology edge is dangling"))?
                    .clone();
                let row = array([
                    clone_value(required(edge, "eventClass", "topology edge")?)?,
                    clone_value(required(edge, "priority", "topology edge")?)?,
                    clone_value(required(edge, "effect", "topology edge")?)?,
                    guard_shape(required(edge, "guard", "topology edge")?)?,
                    Value::String(neighbor),
                ]);
                if source == *id {
                    outgoing.push(row.clone());
                }
                if target == *id {
                    incoming.push(row);
                }
            }
            sort_canonical_values(&mut outgoing);
            sort_canonical_values(&mut incoming);
            updated.insert(
                id.clone(),
                canonical_sha256(&object([
                    ("node", Value::String(label.clone())),
                    ("out", array(outgoing)),
                    ("in", array(incoming)),
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
        let source = token(required(edge, "source", "topology edge")?, "edge source")?;
        let target = token(required(edge, "target", "topology edge")?, "edge target")?;
        final_edges.push(array([
            Value::String(
                labels
                    .get(&source)
                    .ok_or_else(|| invalid("topology source disappeared"))?
                    .clone(),
            ),
            Value::String(
                labels
                    .get(&target)
                    .ok_or_else(|| invalid("topology target disappeared"))?
                    .clone(),
            ),
            clone_value(required(edge, "eventClass", "topology edge")?)?,
            clone_value(required(edge, "priority", "topology edge")?)?,
            clone_value(required(edge, "effect", "topology edge")?)?,
            guard_shape(required(edge, "guard", "topology edge")?)?,
        ]));
    }
    sort_canonical_values(&mut final_edges);
    let mut final_nodes = labels.into_values().map(Value::String).collect::<Vec<_>>();
    final_nodes.sort_by(|left, right| {
        left.as_str()
            .unwrap_or_default()
            .cmp(right.as_str().unwrap_or_default())
    });
    canonical_sha256(&object([
        (
            "schemaVersion",
            Value::String("evolvable_module_semantic_topology_v1".into()),
        ),
        ("nodes", array(final_nodes)),
        ("edges", array(final_edges)),
    ]))
    .map_err(Into::into)
}

fn sort_canonical_values(values: &mut [Value]) {
    values.sort_by_key(|value| canonical_json(value).unwrap_or_default());
}

fn guard_shape(value: &Value) -> Result<Value> {
    let fields = obj(value, "topology guard")?;
    let kind = fields
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    match kind.as_str() {
        "all" | "any" => {
            let mut children = rows(
                fields
                    .get("guards")
                    .ok_or_else(|| invalid("boolean guard lacks children"))?,
                "boolean guard children",
            )?
            .iter()
            .map(guard_shape)
            .collect::<Result<Vec<_>>>()?;
            sort_canonical_values(&mut children);
            Ok(array([Value::String(kind), array(children)]))
        }
        "predicate_edge" | "consecutive_true" => Ok(array([
            Value::String(kind),
            guard_shape(
                fields
                    .get("predicate")
                    .ok_or_else(|| invalid("predicate guard lacks predicate"))?,
            )?,
        ])),
        _ => Ok(Value::String(kind)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn plan_shape_is_content_bound() {
        let facts = facts();
        let program = program();
        let plan = make_plan(
            &program,
            &facts,
            "insert_timeout_rearm",
            &object([("timeoutBars", Value::from(2))]),
        )
        .unwrap();
        let result = apply_plan(&program, &facts, &plan).unwrap();
        assert!(
            rows(
                required(&result.child_program, "nodes", "program").unwrap(),
                "nodes"
            )
            .unwrap()
            .iter()
            .any(|n| field(n, "kind").and_then(Value::as_str) == Some("bounded_rearm"))
        );
        assert!(apply_plan(&result.child_program, &facts, &plan).is_err());
    }
    #[test]
    fn bare_recovery_is_reachable_from_post_position_close() {
        let mut value = program();
        let recovery = timed_recovery("orphan_rearm");
        required_mut(&mut value, "nodes")
            .as_array_mut()
            .unwrap()
            .push(recovery);
        assert!(canonical_program(&value, &facts()).is_ok());
    }
    #[test]
    fn authored_rejection_recovery_rearms_the_entry_start_after_timeout() {
        let mut value = program();
        required_mut(&mut value, "nodes")
            .as_array_mut()
            .unwrap()
            .push(timed_recovery("reject_rearm"));
        required_mut(&mut value, "edges")
            .as_array_mut()
            .unwrap()
            .push(edge(
                "start_reject",
                "start",
                "reject_rearm",
                20,
                Value::Null,
            ));
        assert!(canonical_program(&value, &facts()).is_ok());
    }
    #[test]
    fn plan_rejects_whitespace_or_uppercase_sha_rehashing() {
        let value = program();
        let facts = facts();
        let mut plan = make_plan(
            &value,
            &facts,
            "insert_timeout_rearm",
            &object([("timeoutBars", Value::from(2))]),
        )
        .unwrap();
        required_mut(&mut plan, "operation")
            .clone_from(&Value::String(" insert_timeout_rearm ".into()));
        assert!(apply_plan(&value, &facts, &plan).is_err());
        let mut plan = make_plan(
            &value,
            &facts,
            "insert_timeout_rearm",
            &object([("timeoutBars", Value::from(2))]),
        )
        .unwrap();
        let upper = required(&plan, "sourceGenomeSha256", "plan")
            .unwrap()
            .as_str()
            .unwrap()
            .to_ascii_uppercase();
        required_mut(&mut plan, "sourceGenomeSha256").clone_from(&Value::String(upper));
        assert!(apply_plan(&value, &facts, &plan).is_err());
    }
    #[test]
    fn enumeration_is_sha_sorted_and_contains_only_admitted_plans() {
        let value = program();
        let facts = facts();
        let plans = enumerate_plans(&value, &facts).unwrap();
        assert!(!plans.is_empty());
        let identities = plans
            .iter()
            .map(canonical_sha256)
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert!(identities.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(
            plans
                .iter()
                .all(|plan| apply_plan(&value, &facts, plan).is_ok())
        );
        assert!(
            plans
                .iter()
                .any(|plan| field(plan, "operation").and_then(Value::as_str)
                    == Some("insert_timeout_rearm"))
        );
    }
    #[test]
    fn semantic_topology_signature_is_id_independent_and_deterministic() {
        let original = program();
        let mut renamed = clone_value(&original).unwrap();
        for node in required_mut(&mut renamed, "nodes").as_array_mut().unwrap() {
            let id = required(node, "id", "node")
                .unwrap()
                .as_str()
                .unwrap()
                .to_owned();
            required_mut(node, "id").clone_from(&Value::String(format!("x_{id}")));
        }
        for edge in required_mut(&mut renamed, "edges").as_array_mut().unwrap() {
            for key in ["source", "target"] {
                let id = required(edge, key, "edge")
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_owned();
                required_mut(edge, key).clone_from(&Value::String(format!("x_{id}")));
            }
        }
        let expected = semantic_topology_sha256(&original).unwrap();
        assert_eq!(semantic_topology_sha256(&original).unwrap(), expected);
        assert_eq!(semantic_topology_sha256(&renamed).unwrap(), expected);
    }
    #[test]
    fn semantic_topology_signature_ignores_resource_use_order() {
        let original = program();
        let mut reordered = clone_value(&original).unwrap();
        let entry = required_mut(&mut reordered, "nodes")
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|node| field(node, "id").and_then(Value::as_str) == Some("entry"))
            .unwrap();
        {
            let resources = required_mut(entry, "resources").as_array_mut().unwrap();
            resources.push(resource("event", "event_alpha"));
            resources.push(resource("evidence_group", "group_alpha"));
        }
        let expected = semantic_topology_sha256(&reordered).unwrap();
        required_mut(
            required_mut(&mut reordered, "nodes")
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|node| field(node, "id").and_then(Value::as_str) == Some("entry"))
                .unwrap(),
            "resources",
        )
        .as_array_mut()
        .unwrap()
        .reverse();
        assert_eq!(semantic_topology_sha256(&reordered).unwrap(), expected);
    }
    fn facts() -> Value {
        object([
            (
                "schemaVersion",
                Value::String(V5_TOPOLOGY_SEALED_FACTS_SCHEMA.into()),
            ),
            (
                "authoritySha256",
                Value::String(format!("sha256:{}", "0".repeat(64))),
            ),
            ("side", Value::String("long".into())),
            ("budget", budget()),
        ])
    }
    fn budget() -> Value {
        object([
            ("maxStates", Value::from(14)),
            ("maxTransitions", Value::from(56)),
            ("maxEvidenceGroups", Value::from(4)),
            ("maxGroupMembers", Value::from(3)),
            ("maxEvents", Value::from(4)),
            ("maxIndicators", Value::from(12)),
            ("maxEntryBranches", Value::from(3)),
            ("maxManagementRegions", Value::from(4)),
            ("maxExitRegions", Value::from(3)),
            ("maxRecoveryRegions", Value::from(3)),
            ("maxSccNodes", Value::from(3)),
            ("maxTimeoutBars", Value::from(64)),
            ("maxGuardDepth", Value::from(4)),
        ])
    }
    fn program() -> Value {
        let indicator_meta = object([("instanceId", Value::String("rsi".into()))]);
        let indicator = object([("meta", indicator_meta)]);
        let resources = object([
            ("indicators", array([indicator])),
            ("evidenceGroups", array([])),
            ("events", array([])),
            (
                "managementRefs",
                array([object([("id", Value::String("base".into()))])]),
            ),
        ]);
        object([
            ("schemaVersion", Value::String(GENOME_SCHEMA.into())),
            ("programKind", Value::String(PROGRAM_KIND.into())),
            ("codec", Value::String(CODEC.into())),
            ("direction", Value::String("long".into())),
            ("instrument", Value::String("EURUSD".into())),
            ("resources", resources),
            (
                "nodes",
                array([
                    node(
                        "entry",
                        "entry",
                        "entry",
                        vec![resource("management_ref", "base")],
                    ),
                    node("hub", "position", "position_hub", vec![]),
                    node("start", "entry", "start", vec![]),
                ]),
            ),
            (
                "edges",
                array([
                    edge("entry_hub", "entry", "hub", 10, Value::Null),
                    edge(
                        "start_entry",
                        "start",
                        "entry",
                        10,
                        Value::String("enter_next_open".into()),
                    ),
                ]),
            ),
            ("budget", budget()),
        ])
    }
    fn node(id: &str, zone: &str, kind: &str, resources: Vec<Value>) -> Value {
        object([
            ("id", Value::String(id.into())),
            ("zone", Value::String(zone.into())),
            ("kind", Value::String(kind.into())),
            ("guard", object([])),
            ("resources", array(resources)),
            ("timeoutBars", Value::Null),
        ])
    }
    fn timed_recovery(id: &str) -> Value {
        let mut value = node(id, "recovery", "bounded_rearm", vec![]);
        required_mut(&mut value, "timeoutBars").clone_from(&Value::from(2));
        value
    }
    fn required_mut<'a>(value: &'a mut Value, key: &str) -> &'a mut Value {
        value.as_object_mut().unwrap().get_mut(key).unwrap()
    }
    fn edge(id: &str, source: &str, target: &str, priority: u64, effect: Value) -> Value {
        object([
            ("id", Value::String(id.into())),
            ("source", Value::String(source.into())),
            ("target", Value::String(target.into())),
            ("eventClass", Value::String("decision".into())),
            ("priority", Value::from(priority)),
            ("guard", object([])),
            ("effect", effect),
        ])
    }
}
