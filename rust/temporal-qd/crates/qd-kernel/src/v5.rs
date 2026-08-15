//! Native construction primitives for the evolvable-module v5 authority.
//!
//! The historical implementation builds each candidate by crossing Python
//! factory, compiler, and Dashboard JSONL-validator boundaries.  This module
//! owns the closed generated-profile subset in-process.  It intentionally
//! consumes only frozen JSON authority/context material and has no process,
//! Python, or Dashboard dependency.
//!
//! It is deliberately staged: the first public operation is exact immigrant
//! genome assembly.  The compact journal and later operator families build on
//! the exact same program envelope rather than adding a one-off G0 shortcut.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, OnceLock},
};

use temporal_qd_contract::{ContractError, Map, Value, canonical_json, canonical_sha256};

use crate::factory::ParentReference;

pub const V5_FACTORY_SCHEMA: &str = "temporal_qd_evolvable_module_factory_v1";
pub const V5_GENOME_SCHEMA: &str = "evolvable_module_genome_v1";
pub const V5_PROGRAM_KIND: &str = "evolvable_module_genome_v1";
pub const V5_CODEC: &str = "evolvable_module_genome_json_v1";
pub const V5_COMPACT_JOURNAL_SCHEMA: &str = "temporal_qd_v5_compact_proposal_journal_v1";
pub const V5_SHARED_AUTHORITY_SCHEMA: &str = "temporal_qd_v5_shared_authority_object_v1";
pub const V5_PROPOSAL_DELTA_SCHEMA: &str = "temporal_qd_v5_proposal_delta_v1";
pub const V5_COMPACT_ACCEPTED_RECORD_SCHEMA: &str = "temporal_qd_v5_compact_accepted_record_v2";
pub const V5_CONSTRUCTION_AUDIT_SCHEMA: &str = "temporal_qd_v5_construction_audit_v1";
pub const V5_EVOLVED_CONSTRUCTION_AUDIT_SCHEMA: &str =
    "temporal_qd_v5_evolved_construction_audit_v1";
pub const V5_SELECTED_PROJECTION_SCHEMA: &str = "temporal_qd_v5_selected_projection_v1";
pub const V5_ATTEMPT_LINEAGE_REFS_SCHEMA: &str = "temporal_qd_v5_attempt_lineage_refs_v1";
pub const V5_ATTEMPT_PARENT_REF_SCHEMA: &str = "temporal_qd_v5_attempt_parent_ref_v1";
pub const V5_PROPOSAL_ATTEMPT_SCHEMA: &str = "temporal_qd_v5_proposal_attempt_v1";
pub const V5_ATTEMPT_OUTCOME_AUDIT_SCHEMA: &str = "temporal_qd_v5_attempt_outcome_audit_v1";
pub const V5_ATTEMPT_JOURNAL_SCHEMA: &str = "temporal_qd_v5_attempt_journal_v1";
/// Immutable compact parent payload used by later-generation selection.  It
/// carries the accepted record and exact delta so a parent is reconstructed
/// from sealed authority before an operator sees it.
pub const V5_EVOLVED_PARENT_MATERIAL_SCHEMA: &str = "temporal_qd_v5_evolved_parent_material_v1";
/// Self-contained, content-addressed archive witness for an evolved parent.
/// It seals the source archive identities alongside the exact compact parent
/// material so offline replay never has to reopen a previous-generation file.
pub const V5_EVOLVED_PARENT_SNAPSHOT_SCHEMA: &str = "temporal_qd_v5_evolved_parent_snapshot_v1";

#[derive(Debug, thiserror::Error)]
pub enum V5Error {
    #[error("v5 canonical contract failure: {0}")]
    Canonical(#[from] ContractError),
    #[error("v5 construction: {0}")]
    Invalid(String),
}

pub type Result<T> = std::result::Result<T, V5Error>;

fn invalid(message: impl Into<String>) -> V5Error {
    V5Error::Invalid(message.into())
}

fn object(rows: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    let mut map = Map::new();
    for (key, value) in rows {
        map.insert(key.to_owned(), value);
    }
    Value::Object(map)
}

fn field<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value.as_object()?.get(key)
}

fn object_ref<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| invalid(format!("{label} must be an object")))
}

fn array_ref<'a>(value: &'a Value, label: &str) -> Result<&'a [Value]> {
    value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| invalid(format!("{label} must be an ordered array")))
}

fn required<'a>(value: &'a Value, key: &str, label: &str) -> Result<&'a Value> {
    field(value, key).ok_or_else(|| invalid(format!("{label} lacks {key}")))
}

fn text(value: &Value, label: &str) -> Result<String> {
    let token = value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.len() <= 240)
        .ok_or_else(|| invalid(format!("{label} must be a nonempty explicit identifier")))?;
    Ok(token.to_owned())
}

/// `text` intentionally normalizes user-facing identifiers so the older
/// authority parsers can give useful errors.  Durable compact artifacts have
/// a stricter contract: a writer must never emit a spelling which a reader
/// silently trims into a different identity-bearing value.
fn exact_text_string(value: &str, label: &str) -> Result<()> {
    let parsed = text(&Value::String(value.to_owned()), label)?;
    if parsed != value {
        return Err(invalid(format!("{label} is not canonically spelled")));
    }
    Ok(())
}

fn exact_sha256_string(value: &str, label: &str) -> Result<()> {
    let parsed = sha256_text(&Value::String(value.to_owned()), label)?;
    if parsed != value {
        return Err(invalid(format!("{label} is not canonically spelled")));
    }
    Ok(())
}

fn exact_sha256_value(value: &Value, label: &str) -> Result<String> {
    let parsed = sha256_text(value, label)?;
    exact_sha256_string(&parsed, label)?;
    if value.as_str() != Some(parsed.as_str()) {
        return Err(invalid(format!("{label} is not canonically spelled")));
    }
    Ok(parsed)
}

/// Canonical location shared by every immutable native-v5 object.  Kernel
/// callers decide when/how to write it; this helper merely prevents object
/// families from inventing divergent paths for the same content identity.
pub(crate) fn v5_native_object_relative_path(object_sha256: &str) -> Result<String> {
    exact_sha256_string(object_sha256, "v5 native object SHA-256")?;
    Ok(format!(
        "v5-native/objects/sha256/{}.json",
        &object_sha256[7..]
    ))
}

fn exact_text_value(value: &Value, label: &str) -> Result<String> {
    let parsed = text(value, label)?;
    exact_text_string(&parsed, label)?;
    if value.as_str() != Some(parsed.as_str()) {
        return Err(invalid(format!("{label} is not canonically spelled")));
    }
    Ok(parsed)
}

fn exact_side(side: &str) -> Result<&'static str> {
    match side {
        "long" => Ok("long"),
        "short" => Ok("short"),
        _ => Err(invalid("v5 module direction must be long or short")),
    }
}

fn array(values: impl IntoIterator<Item = Value>) -> Value {
    Value::Array(values.into_iter().collect())
}

fn clone_value(value: &Value) -> Result<Value> {
    // Reparse canonical bytes rather than retaining caller-owned maps.  This
    // also rejects non-finite numeric values at the authority boundary.
    let bytes = temporal_qd_contract::canonical_json_bytes(value)?;
    Ok(serde_json::from_slice(&bytes).map_err(ContractError::from)?)
}

fn ordered_rows(value: Option<&Value>, label: &str) -> Result<Vec<Value>> {
    match value {
        Some(Value::Array(rows)) => Ok(rows.to_vec()),
        _ => Err(invalid(format!("{label} must be an ordered array"))),
    }
}

fn indicator_id(value: &Value) -> Result<String> {
    let meta = required(value, "meta", "indicator")?;
    text(
        required(meta, "instanceId", "indicator meta")?,
        "indicator instance ID",
    )
}

fn row_id(value: &Value, label: &str) -> Result<String> {
    text(required(value, "id", label)?, &format!("{label} ID"))
}

fn number(value: &Value, label: &str) -> Result<f64> {
    value
        .as_f64()
        .filter(|item| item.is_finite())
        .ok_or_else(|| invalid(format!("{label} must be finite numeric")))
}

fn canonical_number(value: f64, label: &str) -> Result<Value> {
    serde_json::Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| invalid(format!("{label} is not finite")))
}

/// Exact Python `_choice` equivalent.  It is rejection-uniform and has no
/// ambient PRNG state, so thread scheduling cannot change a proposal.
pub fn deterministic_choice<T: Clone>(seed: &str, axis: &str, values: &[T]) -> Result<T> {
    if values.is_empty() {
        return Err(invalid(format!("v5 selection axis is empty: {axis}")));
    }
    let modulus = values.len() as u128;
    // `2^256 % n` cannot be represented in a u128.  Build it with a fixed
    // four-limb comparison instead: rejection is astronomically rare for the
    // small bounded v5 domains, but retaining it makes the contract exact.
    let remainder = pow2_256_mod(values.len() as u64);
    let mut nonce = 0_u64;
    loop {
        let digest = canonical_sha256(&object([
            ("schemaVersion", Value::String(V5_FACTORY_SCHEMA.to_owned())),
            ("seed", Value::String(seed.to_owned())),
            ("axis", Value::String(axis.to_owned())),
            ("nonce", Value::from(nonce)),
        ]))?;
        let hex = digest
            .strip_prefix("sha256:")
            .ok_or_else(|| invalid("canonical SHA-256 prefix drifted"))?;
        let bytes = hex_to_32(hex)?;
        // Python compares the complete 256-bit integer with
        // 2^256 - (2^256 % n).  This is equivalent to rejecting the top
        // `remainder` values, which is a fixed small tail.
        if !in_top_remainder(&bytes, remainder) {
            let index = mod_256(&bytes, values.len() as u64) as usize;
            return Ok(values[index].clone());
        }
        nonce = nonce
            .checked_add(1)
            .ok_or_else(|| invalid("v5 selection nonce overflowed"))?;
        let _ = modulus; // documents the bounded domain used above.
    }
}

fn hex_to_32(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64 {
        return Err(invalid("canonical SHA-256 has invalid hexadecimal length"));
    }
    let mut output = [0_u8; 32];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| invalid("canonical SHA-256 has invalid hexadecimal"))?;
    }
    Ok(output)
}

fn pow2_256_mod(modulus: u64) -> u64 {
    let mut value = 1_u64 % modulus;
    for _ in 0..256 {
        value = ((value as u128 * 2_u128) % modulus as u128) as u64;
    }
    value
}

fn in_top_remainder(bytes: &[u8; 32], remainder: u64) -> bool {
    if remainder == 0 {
        return false;
    }
    // The tail contains values `2^256 - remainder .. 2^256 - 1`.  All bytes
    // except the final eight are 0xff; the final word is at least
    // `u64::MAX - remainder + 1`.
    bytes[..24].iter().all(|item| *item == 0xff)
        && u64::from_be_bytes(bytes[24..].try_into().expect("eight bytes")) > u64::MAX - remainder
}

fn mod_256(bytes: &[u8; 32], modulus: u64) -> u64 {
    let mut value = 0_u128;
    for byte in bytes {
        value = ((value << 8) + *byte as u128) % modulus as u128;
    }
    value as u64
}

pub fn side_seed(proposal_seed: &str, side: &str) -> Result<String> {
    let side = exact_side(side)?;
    Ok(canonical_sha256(&object([
        ("schemaVersion", Value::String(V5_FACTORY_SCHEMA.to_owned())),
        ("proposalSeed", Value::String(proposal_seed.to_owned())),
        ("side", Value::String(side.to_owned())),
    ]))?)
}

/// Exact live v5 proposal-seed identity.  Its config snapshot already binds
/// generation/evidence authority, so the proposal formula deliberately uses
/// only the config identity and ordinal.  Do not add generation here: that
/// would change the established deterministic candidate stream.
pub fn v5_proposal_seed(config_sha256: &str, proposal_ordinal: u64) -> Result<String> {
    Ok(canonical_sha256(&object([
        (
            "schemaVersion",
            Value::String("temporal_qd_pair_generation_v2".to_owned()),
        ),
        (
            "configSha256",
            Value::String(text(
                &Value::String(config_sha256.to_owned()),
                "v5 config SHA-256",
            )?),
        ),
        ("proposalOrdinal", Value::from(proposal_ordinal)),
    ]))?)
}

fn fuzzy_evidence_contract(meta: &Value) -> Result<Option<Value>> {
    if field(meta, "signalPersistence").and_then(Value::as_str) != Some("state")
        || field(meta, "usesRangeConfiguration") != Some(&Value::Bool(true))
    {
        return Ok(None);
    }
    let range = match field(meta, "valueRange").and_then(Value::as_object) {
        Some(value) => value,
        None => return Ok(None),
    };
    let minimum = range
        .get("min")
        .map(|item| number(item, "value range min"))
        .transpose()?;
    let maximum = range
        .get("max")
        .map(|item| number(item, "value range max"))
        .transpose()?;
    let step = range
        .get("step")
        .map(|item| number(item, "value range step"))
        .transpose()?;
    let width = range
        .get("minRange")
        .map(|item| number(item, "value range minRange"))
        .transpose()?;
    let Some((minimum, maximum, step, width)) = minimum
        .zip(maximum)
        .zip(step)
        .zip(width)
        .map(|(((a, b), c), d)| (a, b, c, d))
    else {
        return Ok(None);
    };
    if step <= 0.0 || width <= 0.0 || maximum - minimum < width {
        return Ok(None);
    }
    let scalar_outputs = match field(meta, "managementScalarOutputs") {
        Some(Value::Array(rows)) => {
            let mut output = Vec::new();
            for row in rows {
                let row = match row.as_object() {
                    Some(row) => row,
                    None => return Ok(None),
                };
                let key = row
                    .get("outputKey")
                    .and_then(Value::as_str)
                    .filter(|x| !x.is_empty());
                let kind = row.get("valueKind").and_then(Value::as_str);
                let unit = row.get("unit").and_then(Value::as_str);
                let Some((key, kind, unit)) = key.zip(kind).zip(unit).map(|((a, b), c)| (a, b, c))
                else {
                    return Ok(None);
                };
                let expected = match kind {
                    "price_level" => "price",
                    "price_distance" => "price_distance",
                    _ => return Ok(None),
                };
                if unit != expected {
                    return Ok(None);
                }
                output.push(object([
                    ("outputKey", Value::String(key.to_owned())),
                    ("valueKind", Value::String(kind.to_owned())),
                    ("unit", Value::String(unit.to_owned())),
                ]));
            }
            output.sort_by_key(|row| {
                let fields = row.as_object().expect("constructed object");
                (
                    fields
                        .get("outputKey")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    fields
                        .get("valueKind")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    fields
                        .get("unit")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                )
            });
            let mut seen = BTreeSet::new();
            if output.iter().any(|row| {
                !seen.insert(
                    temporal_qd_contract::canonical_json(row).expect("constructed canonical row"),
                )
            }) {
                return Ok(None);
            }
            output
        }
        _ => Vec::new(),
    };
    let scalar = array(scalar_outputs);
    match field(meta, "familySubstitution") {
        None | Some(Value::Null) => Ok(Some(object([
            ("kind", Value::String("fuzzy_evidence".to_owned())),
            (
                "schema",
                Value::String("derived_ranged_state_score_v1".to_owned()),
            ),
            ("scalarOutputs", scalar),
        ]))),
        Some(explicit) => {
            let explicit = match explicit.as_object() {
                Some(value) => value,
                None => return Ok(None),
            };
            let keys = [
                "substitutionClass",
                "polarity",
                "scoreUnit",
                "rawUnit",
                "eventOutputSchema",
                "persistenceCompatibility",
            ];
            if keys.iter().any(|key| !explicit.contains_key(*key))
                || [
                    "substitutionClass",
                    "polarity",
                    "scoreUnit",
                    "rawUnit",
                    "persistenceCompatibility",
                ]
                .iter()
                .any(|key| {
                    explicit
                        .get(*key)
                        .and_then(Value::as_str)
                        .filter(|x| !x.is_empty())
                        .is_none()
                })
                || explicit
                    .get("persistenceCompatibility")
                    .and_then(Value::as_str)
                    != field(meta, "signalPersistence").and_then(Value::as_str)
                || !explicit
                    .get("eventOutputSchema")
                    .is_some_and(Value::is_object)
            {
                return Ok(None);
            }
            Ok(Some(object([
                ("kind", Value::String("fuzzy_evidence".to_owned())),
                (
                    "schema",
                    Value::String("explicit_family_substitution_v1".to_owned()),
                ),
                ("contract", clone_value(&Value::Object(explicit.clone()))?),
                ("scalarOutputs", scalar),
            ])))
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ImmigrantModule {
    pub program: Value,
    pub selector: Value,
    pub program_sha256: String,
}

fn budget_object(value: &Value) -> Result<Value> {
    let source = object_ref(value, "v5 budget")?;
    let keys = [
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
    if source.len() != keys.len() || keys.iter().any(|key| !source.contains_key(*key)) {
        return Err(invalid("v5 budget fields are not exact"));
    }
    for key in keys {
        if source
            .get(key)
            .and_then(Value::as_u64)
            .filter(|number| *number >= 1)
            .is_none()
        {
            return Err(invalid("v5 budget values must be positive integers"));
        }
    }
    clone_value(value)
}

fn owned(row: &Value, side: &str) -> Result<Value> {
    let mut row = clone_value(row)?;
    row.as_object_mut()
        .ok_or_else(|| invalid("frozen module resource must be an object"))?
        .insert("ownerSide".to_owned(), Value::String(side.to_owned()));
    Ok(row)
}

fn resource_id(seed: &str, side: &str, resource: &str) -> Result<String> {
    let digest = canonical_sha256(&object([
        ("seed", Value::String(seed.to_owned())),
        ("side", Value::String(side.to_owned())),
        ("resource", Value::String(resource.to_owned())),
    ]))?;
    Ok(digest[7..19].to_owned())
}

/// Construct one deterministic G0 v5 module without a Python process.  The
/// inputs are the frozen side `context` and exact v5 authority budget.
pub fn build_immigrant_module(
    side: &str,
    proposal_seed: &str,
    context: &Value,
    budget: &Value,
) -> Result<ImmigrantModule> {
    let side = exact_side(side)?;
    let context_fields = object_ref(context, "frozen v5 side context")?;
    let seed = side_seed(proposal_seed, side)?;
    let indicators = ordered_rows(context_fields.get("indicators"), "frozen v5 indicators")?;
    let events = ordered_rows(context_fields.get("events"), "frozen v5 events")?;
    let plans = context_fields
        .get("executionConfig")
        .and_then(|item| field(item, "managementLibrary"))
        .and_then(|item| field(item, "plans"))
        .map(|item| ordered_rows(Some(item), "frozen v5 management plans"))
        .transpose()?
        .unwrap_or_default();
    if indicators.is_empty() || plans.is_empty() {
        return Err(invalid(
            "v5 factory requires frozen indicators and management plans",
        ));
    }
    let event_indicators = events
        .iter()
        .filter_map(|row| {
            field(row, "indicatorInstanceId")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .collect::<BTreeSet<_>>();
    let mut states = Vec::new();
    for indicator in &indicators {
        let id = indicator_id(indicator)?;
        let meta = required(indicator, "meta", "frozen v5 indicator")?;
        if !event_indicators.contains(&id) && fuzzy_evidence_contract(meta)?.is_some() {
            states.push(indicator.clone());
        }
    }
    if states.is_empty() {
        return Err(invalid("frozen v5 context has no fuzzy evidence indicator"));
    }
    let mut ordered_events = events.clone();
    ordered_events.sort_by_key(|row| row_id(row, "frozen v5 event").unwrap_or_default());
    let mut event_choices = vec![None];
    event_choices.extend(ordered_events.into_iter().map(Some));
    let selected_event = deterministic_choice(&seed, "event", &event_choices)?;
    let event_count = usize::from(selected_event.is_some());
    let mut contracts = BTreeMap::<String, Vec<Value>>::new();
    for indicator in &states {
        let contract =
            fuzzy_evidence_contract(required(indicator, "meta", "frozen v5 indicator")?)?
                .expect("states were filtered by fuzzy contract");
        contracts
            .entry(canonical_sha256(&contract)?)
            .or_default()
            .push(indicator.clone());
    }
    let contract_keys = contracts.keys().cloned().collect::<Vec<_>>();
    let contract_key = deterministic_choice(&seed, "fuzzy_contract", &contract_keys)?;
    let mut compatible = contracts
        .remove(&contract_key)
        .expect("selected contract exists");
    compatible.sort_by_key(|row| indicator_id(row).unwrap_or_default());
    let state_cap = (3_usize.saturating_sub(event_count)).min(compatible.len());
    if state_cap == 0 {
        return Err(invalid("v5 factory state indicator cap is empty"));
    }
    let state_counts = (1..=state_cap).collect::<Vec<_>>();
    let state_count = deterministic_choice(&seed, "state_count", &state_counts)?;
    let mut selected_by_id = BTreeMap::<String, Value>::new();
    for ordinal in 0..state_count {
        let selected = deterministic_choice(&seed, &format!("state_{ordinal}"), &compatible)?;
        selected_by_id.insert(indicator_id(&selected)?, selected);
    }
    let selected_states = selected_by_id.into_values().collect::<Vec<_>>();
    let event_indicator = match &selected_event {
        Some(event) => {
            let target = text(
                required(event, "indicatorInstanceId", "frozen v5 event")?,
                "event indicator ID",
            )?;
            Some(
                indicators
                    .iter()
                    .find(|row| indicator_id(row).ok().as_deref() == Some(&target))
                    .cloned()
                    .ok_or_else(|| invalid("frozen v5 event has no frozen indicator instance"))?,
            )
        }
        None => None,
    };
    let mut all_indicators = BTreeMap::<String, Value>::new();
    for row in &selected_states {
        all_indicators.insert(indicator_id(row)?, owned(row, side)?);
    }
    if let Some(row) = &event_indicator {
        all_indicators.insert(indicator_id(row)?, owned(row, side)?);
    }
    let group_id = format!("g_{}", resource_id(&seed, side, "group")?);
    let group = object([
        ("id", Value::String(group_id.clone())),
        (
            "indicatorInstanceIds",
            array(
                selected_states
                    .iter()
                    .map(indicator_id)
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .map(Value::String),
            ),
        ),
        ("ownerSide", Value::String(side.to_owned())),
    ]);
    let event_row = match selected_event {
        Some(event) => {
            let mut row = owned(&event, side)?;
            row.as_object_mut().expect("owned object").insert(
                "id".to_owned(),
                Value::String(format!("e_{}", resource_id(&seed, side, "event")?)),
            );
            Some(row)
        }
        None => None,
    };
    let mut ordered_plans = plans;
    ordered_plans.sort_by_key(|row| row_id(row, "frozen v5 management plan").unwrap_or_default());
    let mut management = clone_value(&deterministic_choice(
        &seed,
        "management_plan",
        &ordered_plans,
    )?)?;
    management
        .as_object_mut()
        .ok_or_else(|| invalid("frozen v5 management plan must be an object"))?
        .insert(
            "id".to_owned(),
            Value::String(format!("m_{}", resource_id(&seed, side, "management")?)),
        );
    management
        .as_object_mut()
        .expect("management object")
        .insert("ownerSide".to_owned(), Value::String(side.to_owned()));
    let threshold = deterministic_choice(
        &seed,
        "threshold",
        &[45.0_f64, 50.0, 55.0, 60.0, 65.0, 70.0],
    )?;
    let mut guard_clauses = vec![object([
        ("kind", Value::String("evidence_at_least".to_owned())),
        ("groupId", Value::String(group_id.clone())),
        (
            "thresholdPercent",
            canonical_number(threshold, "threshold")?,
        ),
    ])];
    let mut setup_resources = vec![object([
        ("kind", Value::String("evidence_group".to_owned())),
        ("id", Value::String(group_id.clone())),
    ])];
    if let Some(row) = &event_row {
        let event_id = row_id(row, "v5 event resource")?;
        guard_clauses.push(object([
            ("kind", Value::String("fresh_event".to_owned())),
            ("eventId", Value::String(event_id.clone())),
        ]));
        setup_resources.push(object([
            ("kind", Value::String("event".to_owned())),
            ("id", Value::String(event_id)),
        ]));
    }
    // Python's `GenomeNodeV1` canonicalizes resource uses by `(kind, id)`;
    // keep the persisted program byte/identity-compatible rather than the
    // incidental order in which factory clauses were assembled.
    setup_resources.sort_by_key(|row| {
        (
            field(row, "kind")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            field(row, "id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        )
    });
    let has_session = deterministic_choice(&seed, "seed_session_filter", &[false, true])?;
    if has_session {
        guard_clauses.push(object([
            ("kind", Value::String("utc_time_window".to_owned())),
            ("startMinute", Value::from(0_u64)),
            ("endMinute", Value::from(1439_u64)),
            ("weekdays", array((0_u64..=6).map(Value::from))),
        ]));
    }
    let setup_guard = if guard_clauses.len() == 1 {
        guard_clauses.remove(0)
    } else {
        object([
            ("kind", Value::String("all".to_owned())),
            ("guards", array(guard_clauses)),
        ])
    };
    let management_id = row_id(&management, "v5 management resource")?;
    let mut nodes = vec![
        object([
            ("id", Value::String("start".to_owned())),
            ("zone", Value::String("entry".to_owned())),
            ("kind", Value::String("start".to_owned())),
            (
                "guard",
                object([
                    ("kind", Value::String("position_exists".to_owned())),
                    ("expected", Value::Bool(false)),
                ]),
            ),
            ("resources", array(Vec::new())),
            ("timeoutBars", Value::Null),
        ]),
        object([
            ("id", Value::String("setup".to_owned())),
            ("zone", Value::String("setup".to_owned())),
            ("kind", Value::String("setup".to_owned())),
            ("guard", setup_guard),
            ("resources", array(setup_resources)),
            ("timeoutBars", Value::Null),
        ]),
        object([
            ("id", Value::String("entry".to_owned())),
            ("zone", Value::String("entry".to_owned())),
            ("kind", Value::String("entry".to_owned())),
            ("guard", object(Vec::new())),
            (
                "resources",
                array(vec![object([
                    ("kind", Value::String("management_ref".to_owned())),
                    ("id", Value::String(management_id)),
                ])]),
            ),
            ("timeoutBars", Value::Null),
        ]),
        object([
            ("id", Value::String("hub".to_owned())),
            ("zone", Value::String("position".to_owned())),
            ("kind", Value::String("position_hub".to_owned())),
            ("guard", object(Vec::new())),
            ("resources", array(Vec::new())),
            ("timeoutBars", Value::Null),
        ]),
    ];
    let mut edges = vec![
        edge(
            "start_setup",
            "start",
            "setup",
            10,
            object(Vec::new()),
            None,
        ),
        edge(
            "setup_entry",
            "setup",
            "entry",
            10,
            object(Vec::new()),
            Some("enter_next_open"),
        ),
        edge("entry_hub", "entry", "hub", 10, object(Vec::new()), None),
    ];
    let management_effect = deterministic_choice(
        &seed,
        "management_effect",
        &[
            None,
            Some("move_stop_to_break_even_next_open"),
            Some("tighten_stop_next_open"),
            Some("activate_trailing_stop_next_open"),
        ],
    )?;
    let selector_management_effect = management_effect.map(ToOwned::to_owned);
    if let Some(effect) = management_effect {
        let age = deterministic_choice(&seed, "management_age", &[3_u64, 6, 12, 24])?;
        nodes.push(object([
            ("id", Value::String("manage".to_owned())),
            ("zone", Value::String("management".to_owned())),
            ("kind", Value::String(effect.to_owned())),
            (
                "guard",
                object([
                    ("kind", Value::String("state_age_at_least".to_owned())),
                    ("events", Value::from(age)),
                ]),
            ),
            ("resources", array(Vec::new())),
            ("timeoutBars", Value::Null),
        ]));
        edges.push(edge(
            "hub_manage",
            "hub",
            "manage",
            20,
            object([("kind", Value::String("always".to_owned()))]),
            Some(effect),
        ));
    }
    let has_exit = deterministic_choice(&seed, "include_exit", &[false, true])?;
    if has_exit {
        let age = deterministic_choice(&seed, "exit_age", &[12_u64, 24, 48, 96])?;
        nodes.push(object([
            ("id", Value::String("exit".to_owned())),
            ("zone", Value::String("exit".to_owned())),
            ("kind", Value::String("timed_exit".to_owned())),
            (
                "guard",
                object([
                    ("kind", Value::String("state_age_at_least".to_owned())),
                    ("events", Value::from(age)),
                ]),
            ),
            ("resources", array(Vec::new())),
            ("timeoutBars", Value::Null),
        ]));
        edges.push(edge(
            "hub_exit",
            "hub",
            "exit",
            30,
            object([("kind", Value::String("always".to_owned()))]),
            Some("exit_next_open"),
        ));
    }
    nodes.sort_by_key(|row| row_id(row, "v5 node").unwrap_or_default());
    edges.sort_by_key(|row| row_id(row, "v5 edge").unwrap_or_default());
    let mut indicator_rows = all_indicators.into_values().collect::<Vec<_>>();
    indicator_rows.sort_by_key(|row| indicator_id(row).unwrap_or_default());
    let program = object([
        ("schemaVersion", Value::String(V5_GENOME_SCHEMA.to_owned())),
        ("programKind", Value::String(V5_PROGRAM_KIND.to_owned())),
        ("codec", Value::String(V5_CODEC.to_owned())),
        ("direction", Value::String(side.to_owned())),
        (
            "instrument",
            Value::String(
                text(
                    required(context, "instrument", "frozen v5 side context")?,
                    "v5 instrument",
                )?
                .to_ascii_uppercase(),
            ),
        ),
        (
            "resources",
            object([
                ("indicators", array(indicator_rows)),
                ("evidenceGroups", array(vec![group])),
                ("events", array(event_row)),
                ("managementRefs", array(vec![management])),
            ]),
        ),
        ("nodes", array(nodes)),
        ("edges", array(edges)),
        ("budget", budget_object(budget)?),
    ]);
    validate_immigrant_program(&program, side)?;
    let program_sha256 = canonical_sha256(&program)?;
    let selector = object([
        ("side", Value::String(side.to_owned())),
        ("stateCount", Value::from(selected_states.len() as u64)),
        (
            "stateIndicatorIds",
            array(
                selected_states
                    .iter()
                    .map(|row| {
                        required(row, "meta", "v5 state indicator")
                            .and_then(|meta| {
                                text(
                                    required(meta, "id", "v5 state indicator meta")?,
                                    "v5 state indicator ID",
                                )
                            })
                            .map(Value::String)
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
        ),
        (
            "eventBindingId",
            field(&program, "resources")
                .and_then(|resources| field(resources, "events"))
                .and_then(Value::as_array)
                .and_then(|events| events.first())
                .and_then(|event| field(event, "id"))
                .cloned()
                .unwrap_or(Value::Null),
        ),
        (
            "thresholdPercent",
            canonical_number(threshold, "threshold")?,
        ),
        ("hasSessionFilter", Value::Bool(has_session)),
        (
            "managementEffect",
            selector_management_effect
                .map(Value::String)
                .unwrap_or(Value::Null),
        ),
        ("hasExit", Value::Bool(has_exit)),
        ("genomeSha256", Value::String(program_sha256.clone())),
    ]);
    Ok(ImmigrantModule {
        program,
        selector,
        program_sha256,
    })
}

fn edge(
    id: &str,
    source: &str,
    target: &str,
    priority: u64,
    guard: Value,
    effect: Option<&str>,
) -> Value {
    object([
        ("id", Value::String(id.to_owned())),
        ("source", Value::String(source.to_owned())),
        ("target", Value::String(target.to_owned())),
        ("eventClass", Value::String("decision".to_owned())),
        ("priority", Value::from(priority)),
        ("guard", guard),
        (
            "effect",
            effect
                .map(|item| Value::String(item.to_owned()))
                .unwrap_or(Value::Null),
        ),
    ])
}

/// Closed structural admission for the exact immigrant grammar.  This is not
/// a permissive replacement for Dashboard's general validator: unfamiliar
/// program shapes fail closed and are only broadened alongside an oracle test.
pub fn validate_immigrant_program(program: &Value, side: &str) -> Result<()> {
    let side = exact_side(side)?;
    let fields = object_ref(program, "v5 immigrant program")?;
    let keys = [
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
    if fields.len() != keys.len()
        || keys.iter().any(|key| !fields.contains_key(*key))
        || field(program, "schemaVersion").and_then(Value::as_str) != Some(V5_GENOME_SCHEMA)
        || field(program, "programKind").and_then(Value::as_str) != Some(V5_PROGRAM_KIND)
        || field(program, "codec").and_then(Value::as_str) != Some(V5_CODEC)
        || field(program, "direction").and_then(Value::as_str) != Some(side)
        || field(program, "instrument")
            .and_then(Value::as_str)
            .filter(|item| !item.is_empty())
            .is_none()
    {
        return Err(invalid("v5 immigrant program envelope is invalid"));
    }
    let _ = budget_object(required(program, "budget", "v5 immigrant program")?)?;
    let resources = required(program, "resources", "v5 immigrant program")?;
    let resource_keys = ["indicators", "evidenceGroups", "events", "managementRefs"];
    let resources_map = object_ref(resources, "v5 immigrant resources")?;
    if resources_map.len() != resource_keys.len()
        || resource_keys
            .iter()
            .any(|key| !resources_map.contains_key(*key))
    {
        return Err(invalid("v5 immigrant resource fields are not exact"));
    }
    let indicators = array_ref(
        required(resources, "indicators", "v5 immigrant resources")?,
        "v5 immigrant indicators",
    )?;
    let groups = array_ref(
        required(resources, "evidenceGroups", "v5 immigrant resources")?,
        "v5 immigrant groups",
    )?;
    let events = array_ref(
        required(resources, "events", "v5 immigrant resources")?,
        "v5 immigrant events",
    )?;
    let management = array_ref(
        required(resources, "managementRefs", "v5 immigrant resources")?,
        "v5 immigrant management",
    )?;
    if indicators.is_empty() || groups.len() != 1 || events.len() > 1 || management.len() != 1 {
        return Err(invalid("v5 immigrant resource cardinality is invalid"));
    }
    let indicator_ids = indicators
        .iter()
        .map(indicator_id)
        .collect::<Result<BTreeSet<_>>>()?;
    if indicator_ids.len() != indicators.len()
        || indicators
            .iter()
            .any(|item| field(item, "ownerSide").and_then(Value::as_str) != Some(side))
    {
        return Err(invalid("v5 immigrant indicator ownership is invalid"));
    }
    let group = &groups[0];
    if field(group, "ownerSide").and_then(Value::as_str) != Some(side)
        || array_ref(
            required(group, "indicatorInstanceIds", "v5 immigrant group")?,
            "v5 immigrant group members",
        )?
        .is_empty()
        || array_ref(
            required(group, "indicatorInstanceIds", "v5 immigrant group")?,
            "v5 immigrant group members",
        )?
        .iter()
        .any(|item| item.as_str().is_none_or(|id| !indicator_ids.contains(id)))
    {
        return Err(invalid("v5 immigrant group closure is invalid"));
    }
    for event in events {
        if field(event, "ownerSide").and_then(Value::as_str) != Some(side)
            || field(event, "indicatorInstanceId")
                .and_then(Value::as_str)
                .is_none_or(|id| !indicator_ids.contains(id))
        {
            return Err(invalid("v5 immigrant event closure is invalid"));
        }
    }
    if field(&management[0], "ownerSide").and_then(Value::as_str) != Some(side) {
        return Err(invalid("v5 immigrant management ownership is invalid"));
    }
    let nodes = array_ref(
        required(program, "nodes", "v5 immigrant program")?,
        "v5 immigrant nodes",
    )?;
    let edges = array_ref(
        required(program, "edges", "v5 immigrant program")?,
        "v5 immigrant edges",
    )?;
    if nodes.len() < 4 || nodes.len() > 6 || edges.len() < 3 || edges.len() > 5 {
        return Err(invalid("v5 immigrant graph cardinality is invalid"));
    }
    let node_ids = nodes
        .iter()
        .map(|node| row_id(node, "v5 immigrant node"))
        .collect::<Result<BTreeSet<_>>>()?;
    if node_ids.len() != nodes.len()
        || !["start", "setup", "entry", "hub"]
            .iter()
            .all(|id| node_ids.contains(*id))
    {
        return Err(invalid("v5 immigrant graph lacks required nodes"));
    }
    let edge_ids = edges
        .iter()
        .map(|edge| row_id(edge, "v5 immigrant edge"))
        .collect::<Result<BTreeSet<_>>>()?;
    if edge_ids.len() != edges.len()
        || !["start_setup", "setup_entry", "entry_hub"]
            .iter()
            .all(|id| edge_ids.contains(*id))
    {
        return Err(invalid("v5 immigrant graph lacks required edges"));
    }
    for edge in edges {
        if field(edge, "eventClass").and_then(Value::as_str) != Some("decision")
            || field(edge, "source")
                .and_then(Value::as_str)
                .is_none_or(|id| !node_ids.contains(id))
            || field(edge, "target")
                .and_then(Value::as_str)
                .is_none_or(|id| !node_ids.contains(id))
        {
            return Err(invalid("v5 immigrant edge is invalid"));
        }
    }
    Ok(())
}

const DASHBOARD_MAX_GUARD_DEPTH: u64 = 4;
const DASHBOARD_MAX_GUARDS_PER_COMPOSITE: usize = 8;

fn compiled_guard_depth(guard: &Value) -> Result<u64> {
    let fields = guard
        .as_object()
        .ok_or_else(|| invalid("v5 compiled guard must be an object"))?;
    let kind = fields
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("v5 compiled guard lacks kind"))?;
    let nested = match kind {
        "all" | "any" => {
            let children = fields
                .get("guards")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid("v5 compiled composite guard lacks ordered children"))?;
            if children.is_empty() || children.len() > DASHBOARD_MAX_GUARDS_PER_COMPOSITE {
                return Err(invalid(
                    "v5 compiled composite guard exceeds Dashboard child bounds",
                ));
            }
            children
                .iter()
                .map(compiled_guard_depth)
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .max()
                .unwrap_or(0)
        }
        "not" => compiled_guard_depth(
            fields
                .get("guard")
                .ok_or_else(|| invalid("v5 compiled negated guard lacks child"))?,
        )?,
        "predicate_edge" | "consecutive_true" => compiled_guard_depth(
            fields
                .get("predicate")
                .ok_or_else(|| invalid("v5 compiled predicate wrapper lacks child"))?,
        )?,
        _ => 0,
    };
    Ok(1 + nested)
}

/// Flatten associative conjunctions without reordering their operands.  The
/// authored genome depth budget does not include conjunctions introduced by
/// lowering (source + edge + target, and the position-existence gate).  Only
/// normalize when those wrappers would exceed Dashboard's native depth cap so
/// existing shallow compiled identities remain stable.
fn flatten_compiled_conjunctions(value: Value) -> Result<Value> {
    match value {
        Value::Object(mut fields) => {
            let kind = fields
                .get("kind")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("v5 compiled guard lacks kind"))?
                .to_owned();
            if matches!(kind.as_str(), "all" | "any") {
                let children = fields
                    .remove("guards")
                    .and_then(|value| value.as_array().cloned())
                    .ok_or_else(|| invalid("v5 compiled composite guard lacks ordered children"))?;
                let mut normalized = children
                    .into_iter()
                    .map(flatten_compiled_conjunctions)
                    .collect::<Result<Vec<_>>>()?;
                if kind == "all" {
                    let mut flattened = Vec::new();
                    for child in normalized {
                        if child.get("kind").and_then(Value::as_str) == Some("all") {
                            flattened.extend(
                                child
                                    .get("guards")
                                    .and_then(Value::as_array)
                                    .into_iter()
                                    .flatten()
                                    .cloned(),
                            );
                        } else {
                            flattened.push(child);
                        }
                    }
                    normalized = flattened;
                }
                if normalized.is_empty() || normalized.len() > DASHBOARD_MAX_GUARDS_PER_COMPOSITE {
                    return Err(invalid(
                        "v5 compiled composite guard exceeds Dashboard child bounds",
                    ));
                }
                fields.insert("guards".to_owned(), array(normalized));
            }
            let child_key = match kind.as_str() {
                "not" => Some("guard"),
                "predicate_edge" | "consecutive_true" => Some("predicate"),
                _ => None,
            };
            if let Some(key) = child_key {
                let child = fields.remove(key).ok_or_else(|| {
                    invalid(format!("v5 compiled {kind} guard lacks nested child"))
                })?;
                fields.insert(key.to_owned(), flatten_compiled_conjunctions(child)?);
            }
            Ok(Value::Object(fields))
        }
        _ => Err(invalid("v5 compiled guard must be an object")),
    }
}

fn guard_all(values: impl IntoIterator<Item = Value>) -> Result<Value> {
    let mut guards = values
        .into_iter()
        .filter(|value| value.as_object().is_none_or(|map| !map.is_empty()))
        .collect::<Vec<_>>();
    let guard = match guards.len() {
        0 => object([("kind", Value::String("always".to_owned()))]),
        1 => guards.remove(0),
        _ => object([
            ("kind", Value::String("all".to_owned())),
            ("guards", array(guards)),
        ]),
    };
    if compiled_guard_depth(&guard)? <= DASHBOARD_MAX_GUARD_DEPTH {
        return Ok(guard);
    }
    let guard = flatten_compiled_conjunctions(guard)?;
    if compiled_guard_depth(&guard)? > DASHBOARD_MAX_GUARD_DEPTH {
        return Err(invalid(
            "v5 compiled guard depth exceeds Dashboard maximum of 4",
        ));
    }
    Ok(guard)
}

/// Rewrite only cooldown references owned by the authored management edge
/// currently being expanded.  One-shot lowering can replace `e_edge` with
/// phase-specific executable transitions; arbitrary strings and cooldowns for
/// other authored actions must remain untouched.
fn rewrite_containing_action_cooldown(
    value: &Value,
    authored_transition_id: &str,
    compiled_transition_id: &str,
) -> Value {
    match value {
        Value::Object(fields) => {
            let is_owned_cooldown = fields.get("kind").and_then(Value::as_str)
                == Some("action_cooldown_elapsed")
                && fields.get("transitionId").and_then(Value::as_str)
                    == Some(authored_transition_id);
            Value::Object(
                fields
                    .iter()
                    .map(|(key, item)| {
                        if is_owned_cooldown && key == "transitionId" {
                            (
                                key.clone(),
                                Value::String(compiled_transition_id.to_owned()),
                            )
                        } else {
                            (
                                key.clone(),
                                rewrite_containing_action_cooldown(
                                    item,
                                    authored_transition_id,
                                    compiled_transition_id,
                                ),
                            )
                        }
                    })
                    .collect(),
            )
        }
        Value::Array(rows) => Value::Array(
            rows.iter()
                .map(|item| {
                    rewrite_containing_action_cooldown(
                        item,
                        authored_transition_id,
                        compiled_transition_id,
                    )
                })
                .collect(),
        ),
        _ => value.clone(),
    }
}

fn published_resource(value: &Value) -> Result<Value> {
    let mut value = clone_value(value)?;
    let fields = value
        .as_object_mut()
        .ok_or_else(|| invalid("v5 resource must be an object"))?;
    fields.remove("ownerSide");
    fields.remove("side");
    Ok(value)
}

fn collect_guard_resource_ids(
    value: &Value,
    groups: &mut BTreeSet<String>,
    events: &mut BTreeSet<String>,
) {
    let Some(fields) = value.as_object() else {
        return;
    };
    if let Some(id) = fields.get("groupId").and_then(Value::as_str) {
        groups.insert(id.to_owned());
    }
    if let Some(id) = fields.get("eventId").and_then(Value::as_str) {
        events.insert(id.to_owned());
    }
    if let Some(child) = fields.get("guard") {
        collect_guard_resource_ids(child, groups, events);
    }
    if let Some(children) = fields.get("guards").and_then(Value::as_array) {
        for child in children {
            collect_guard_resource_ids(child, groups, events);
        }
    }
    if let Some(child) = fields.get("predicate") {
        collect_guard_resource_ids(child, groups, events);
    }
}

fn compiled_transition(
    id: String,
    source: String,
    destination: String,
    priority: u64,
    guard: Value,
    actions: Vec<Value>,
    reason: &str,
    event_class: &str,
) -> Value {
    object([
        ("id", Value::String(id)),
        ("sourceStateId", Value::String(source)),
        ("destinationStateId", Value::String(destination)),
        ("eventClass", Value::String(event_class.to_owned())),
        ("priority", Value::from(priority)),
        ("guard", guard),
        ("actions", array(actions)),
        ("reasonCode", Value::String(reason.to_owned())),
    ])
}

fn management_action(effect: &str) -> Result<Value> {
    match effect {
        // These shapes mirror `EvolvableModuleCompilerV1._management_action`.
        // The broad evolved compiler owns all six effects; the narrower G0
        // factory merely enumerates a conservative subset of them.
        "set_target_next_open" => Ok(object([
            ("kind", Value::String(effect.to_owned())),
            (
                "targetLocator",
                object([
                    ("kind", Value::String("reward_multiple".to_owned())),
                    ("multiple", canonical_number(1.5, "target reward multiple")?),
                ]),
            ),
        ])),
        "tighten_stop_next_open" => Ok(object([
            ("kind", Value::String(effect.to_owned())),
            (
                "stopLocator",
                object([
                    ("kind", Value::String("initial_r_multiple".to_owned())),
                    ("multiple", canonical_number(0.0, "initial R multiple")?),
                ]),
            ),
        ])),
        "move_stop_to_break_even_next_open"
        | "cancel_target_next_open"
        | "activate_trailing_stop_next_open"
        | "deactivate_trailing_stop_next_open" => {
            Ok(object([("kind", Value::String(effect.to_owned()))]))
        }
        _ => Err(invalid("v5 compiler has unsupported management effect")),
    }
}

fn is_one_shot_break_even_effect(effect: &str) -> bool {
    matches!(
        effect,
        "move_stop_to_break_even_next_open" | "tighten_stop_next_open"
    )
}

/// Compile the narrow G0 immigrant grammar to a Dashboard-v2-shaped module
/// profile.  The general v5 compiler below reuses the exact same lowering;
/// G0 keeps its smaller admission surface as an additional proof, not as a
/// separate compiler fork.
pub fn compile_immigrant_profile(program: &Value) -> Result<Value> {
    let side = text(
        required(program, "direction", "v5 immigrant program")?,
        "v5 direction",
    )?;
    validate_immigrant_program(program, &side)?;
    compile_v5_profile_body(program)
}

/// Pure native lowering from an already authority-validated evolvable genome
/// to a Dashboard-v2-shaped module profile.  This deliberately carries no
/// validator shortcut: callers must first prove the program against either
/// the narrow G0 grammar or the full typed v5 operator authority.
fn compile_v5_profile_body(program: &Value) -> Result<Value> {
    let side = text(
        required(program, "direction", "v5 evolvable program")?,
        "v5 direction",
    )?;
    let resources = required(program, "resources", "v5 immigrant program")?;
    let indicators = array_ref(
        required(resources, "indicators", "v5 resources")?,
        "v5 indicators",
    )?;
    let groups = array_ref(
        required(resources, "evidenceGroups", "v5 resources")?,
        "v5 groups",
    )?;
    let events = array_ref(required(resources, "events", "v5 resources")?, "v5 events")?;
    let management = array_ref(
        required(resources, "managementRefs", "v5 resources")?,
        "v5 management",
    )?;
    let nodes = array_ref(
        required(program, "nodes", "v5 immigrant program")?,
        "v5 nodes",
    )?;
    let edges = array_ref(
        required(program, "edges", "v5 immigrant program")?,
        "v5 edges",
    )?;

    let mut nodes_by_id = BTreeMap::<String, &Value>::new();
    for node in nodes {
        nodes_by_id.insert(row_id(node, "v5 node")?, node);
    }
    let hub = nodes
        .iter()
        .find(|node| {
            field(node, "zone").and_then(Value::as_str) == Some("position")
                && field(node, "kind").and_then(Value::as_str) == Some("position_hub")
        })
        .ok_or_else(|| invalid("v5 immigrant profile has no position hub"))?;
    let hub_id = row_id(hub, "v5 position hub")?;
    let start = nodes
        .iter()
        .find(|node| {
            field(node, "zone").and_then(Value::as_str) == Some("entry")
                && field(node, "kind").and_then(Value::as_str) == Some("start")
        })
        .ok_or_else(|| invalid("v5 immigrant profile has no entry start"))?;
    let start_id = row_id(start, "v5 entry start")?;
    let has_exit = nodes
        .iter()
        .any(|node| field(node, "zone").and_then(Value::as_str) == Some("exit"));
    let has_one_shot_break_even = edges.iter().any(|edge| {
        field(edge, "effect")
            .and_then(Value::as_str)
            .is_some_and(is_one_shot_break_even_effect)
            && field(edge, "target")
                .and_then(Value::as_str)
                .and_then(|id| nodes_by_id.get(id))
                .is_some_and(|node| {
                    field(node, "zone").and_then(Value::as_str) == Some("management")
                })
    });
    let hub_states: Vec<&str> = if has_one_shot_break_even {
        vec!["position_hub_be0", "position_hub_be1"]
    } else {
        vec!["position_hub"]
    };
    let mut states = vec![object([("id", Value::String("entry_pending".to_owned()))])];
    states.extend(
        hub_states
            .iter()
            .map(|id| object([("id", Value::String((*id).to_owned()))])),
    );
    if has_exit {
        for phase in 0..hub_states.len() {
            let id = if has_one_shot_break_even {
                format!("exit_pending_be{phase}")
            } else {
                "exit_pending".to_owned()
            };
            states.push(object([("id", Value::String(id))]));
        }
    }
    let mut node_state = BTreeMap::<String, String>::new();
    let mut ordered_nodes = nodes_by_id.iter().collect::<Vec<_>>();
    ordered_nodes.sort_by_key(|(id, _)| (*id).clone());
    for (id, node) in &ordered_nodes {
        let zone = field(node, "zone")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let kind = field(node, "kind")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if *id == &hub_id || (zone == "entry" && kind == "entry") || zone == "exit" {
            continue;
        }
        let state = format!("n_{id}");
        node_state.insert((*id).clone(), state.clone());
        if has_one_shot_break_even && zone == "management" {
            let needs_consumed_phase = edges.iter().any(|edge| {
                field(edge, "target").and_then(Value::as_str) == Some(id.as_str())
                    && field(edge, "effect")
                        .and_then(Value::as_str)
                        .is_some_and(|effect| !is_one_shot_break_even_effect(effect))
            });
            let phase_count = if needs_consumed_phase { 2 } else { 1 };
            for phase in 0..phase_count {
                states.push(object([(
                    "id",
                    Value::String(format!("{state}_be{phase}")),
                )]));
            }
        } else {
            states.push(object([("id", Value::String(state))]));
        }
    }
    let start_state = node_state
        .get(&start_id)
        .cloned()
        .ok_or_else(|| invalid("v5 compiler cannot map entry start"))?;
    let mut recovery = ordered_nodes
        .iter()
        .filter_map(|(id, node)| {
            (field(node, "zone").and_then(Value::as_str) == Some("recovery"))
                .then_some((*id).clone())
        })
        .collect::<Vec<_>>();
    recovery.sort();
    let recovery_state = recovery
        .first()
        .and_then(|id| node_state.get(id))
        .cloned()
        .unwrap_or_else(|| start_state.clone());
    let mut transitions = Vec::<Value>::new();
    let mut ordered_edges = edges.iter().collect::<Vec<_>>();
    ordered_edges.sort_by_key(|edge| row_id(edge, "v5 edge").unwrap_or_default());
    for edge in ordered_edges {
        let id = row_id(edge, "v5 edge")?;
        let source_id = text(required(edge, "source", "v5 edge")?, "v5 edge source")?;
        let target_id = text(required(edge, "target", "v5 edge")?, "v5 edge target")?;
        let source = nodes_by_id
            .get(&source_id)
            .ok_or_else(|| invalid("v5 compiler source drifted"))?;
        let target = nodes_by_id
            .get(&target_id)
            .ok_or_else(|| invalid("v5 compiler target drifted"))?;
        if target_id == hub_id {
            continue;
        }
        let source_state = if source_id == hub_id {
            hub_states[0].to_owned()
        } else {
            node_state
                .get(&source_id)
                .cloned()
                .ok_or_else(|| invalid("v5 compiler source state is absent"))?
        };
        let target_zone = field(target, "zone")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let target_kind = field(target, "kind")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let priority = required(edge, "priority", "v5 edge")?
            .as_u64()
            .ok_or_else(|| invalid("v5 edge priority is invalid"))?;
        let guard = guard_all([
            clone_value(required(source, "guard", "v5 source node")?)?,
            clone_value(required(edge, "guard", "v5 edge")?)?,
            clone_value(required(target, "guard", "v5 target node")?)?,
        ])?;
        match (target_zone, target_kind) {
            ("entry", "entry") => {
                let plan_id = array_ref(
                    required(target, "resources", "v5 entry node")?,
                    "v5 entry resources",
                )?
                .iter()
                .find(|use_| field(use_, "kind").and_then(Value::as_str) == Some("management_ref"))
                .and_then(|use_| field(use_, "id"))
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("v5 entry node lacks management reference"))?;
                transitions.push(compiled_transition(
                    format!("e_{id}"),
                    source_state,
                    "entry_pending".to_owned(),
                    priority,
                    guard_all([
                        object([
                            ("kind", Value::String("position_exists".to_owned())),
                            ("expected", Value::Bool(false)),
                        ]),
                        guard,
                    ])?,
                    vec![object([
                        ("kind", Value::String("enter_next_open".to_owned())),
                        ("managementPlanId", Value::String(plan_id.to_owned())),
                    ])],
                    "evolvable.entry.request",
                    "decision",
                ));
            }
            ("setup", _) => {
                transitions.push(compiled_transition(
                    format!("e_{id}"),
                    source_state,
                    node_state
                        .get(&target_id)
                        .cloned()
                        .ok_or_else(|| invalid("v5 setup target state is absent"))?,
                    priority,
                    guard,
                    Vec::new(),
                    "evolvable.setup.advance",
                    "decision",
                ));
            }
            ("management", _) => {
                let pending_base = node_state
                    .get(&target_id)
                    .cloned()
                    .ok_or_else(|| invalid("v5 management target state is absent"))?;
                let effect = field(edge, "effect")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid("v5 management edge lacks effect"))?;
                let phase_count =
                    if has_one_shot_break_even && is_one_shot_break_even_effect(effect) {
                        1
                    } else {
                        hub_states.len()
                    };
                for phase in 0..phase_count {
                    let suffix = has_one_shot_break_even
                        .then(|| format!("_be{phase}"))
                        .unwrap_or_default();
                    let authored_transition_id = format!("e_{id}");
                    let compiled_transition_id = format!("{authored_transition_id}{suffix}");
                    let pending = if has_one_shot_break_even {
                        format!("{pending_base}_be{phase}")
                    } else {
                        pending_base.clone()
                    };
                    transitions.push(compiled_transition(
                        compiled_transition_id.clone(),
                        hub_states[phase].to_owned(),
                        pending.clone(),
                        priority,
                        rewrite_containing_action_cooldown(
                            &guard_all([
                                object([
                                    ("kind", Value::String("position_exists".to_owned())),
                                    ("expected", Value::Bool(true)),
                                ]),
                                guard.clone(),
                            ])?,
                            &authored_transition_id,
                            &compiled_transition_id,
                        ),
                        vec![management_action(effect)?],
                        "evolvable.management.request",
                        "decision",
                    ));
                    for (status, status_priority) in
                        [("applied", 10_u64), ("rejected", 20), ("canceled", 30)]
                    {
                        let destination_phase = usize::from(
                            has_one_shot_break_even
                                && is_one_shot_break_even_effect(effect)
                                && status == "applied",
                        );
                        transitions.push(compiled_transition(
                            format!("e_{id}_{status}{suffix}"),
                            pending.clone(),
                            hub_states[destination_phase.max(phase)].to_owned(),
                            status_priority,
                            object([
                                ("kind", Value::String("execution_status_is".to_owned())),
                                ("status", Value::String(status.to_owned())),
                            ]),
                            Vec::new(),
                            &format!("evolvable.management.{status}"),
                            "execution",
                        ));
                    }
                    transitions.push(compiled_transition(
                        format!("e_{id}_closed{suffix}"),
                        pending,
                        recovery_state.clone(),
                        40,
                        object([
                            ("kind", Value::String("execution_status_is".to_owned())),
                            ("status", Value::String("closed".to_owned())),
                        ]),
                        Vec::new(),
                        "evolvable.management.closed",
                        "execution",
                    ));
                }
            }
            ("exit", _) => {
                for phase in 0..hub_states.len() {
                    let suffix = has_one_shot_break_even
                        .then(|| format!("_be{phase}"))
                        .unwrap_or_default();
                    let pending = if has_one_shot_break_even {
                        format!("exit_pending_be{phase}")
                    } else {
                        "exit_pending".to_owned()
                    };
                    transitions.push(compiled_transition(
                        format!("e_{id}{suffix}"),
                        hub_states[phase].to_owned(),
                        pending,
                        priority,
                        guard_all([
                            object([
                                ("kind", Value::String("position_exists".to_owned())),
                                ("expected", Value::Bool(true)),
                            ]),
                            guard.clone(),
                        ])?,
                        vec![object([(
                            "kind",
                            Value::String("exit_next_open".to_owned()),
                        )])],
                        "evolvable.exit.request",
                        "decision",
                    ));
                }
            }
            ("recovery", _) => {
                transitions.push(compiled_transition(
                    format!("e_{id}"),
                    source_state,
                    node_state
                        .get(&target_id)
                        .cloned()
                        .ok_or_else(|| invalid("v5 recovery target state is absent"))?,
                    priority,
                    guard,
                    Vec::new(),
                    "evolvable.pre_position_recovery",
                    "decision",
                ));
            }
            _ => {
                return Err(invalid(
                    "v5 compiler encountered unsupported target topology",
                ));
            }
        }
    }
    transitions.extend([
        compiled_transition(
            "entry_filled".to_owned(),
            "entry_pending".to_owned(),
            hub_states[0].to_owned(),
            10,
            object([
                ("kind", Value::String("execution_status_is".to_owned())),
                ("status", Value::String("filled".to_owned())),
            ]),
            Vec::new(),
            "evolvable.entry.filled",
            "execution",
        ),
        compiled_transition(
            "entry_rejected".to_owned(),
            "entry_pending".to_owned(),
            start_state.clone(),
            20,
            object([
                ("kind", Value::String("execution_status_is".to_owned())),
                ("status", Value::String("rejected".to_owned())),
            ]),
            Vec::new(),
            "evolvable.entry.rejected",
            "execution",
        ),
        compiled_transition(
            "entry_canceled".to_owned(),
            "entry_pending".to_owned(),
            start_state.clone(),
            30,
            object([
                ("kind", Value::String("execution_status_is".to_owned())),
                ("status", Value::String("canceled".to_owned())),
            ]),
            Vec::new(),
            "evolvable.entry.canceled",
            "execution",
        ),
    ]);
    for (phase, hub_state) in hub_states.iter().enumerate() {
        let suffix = has_one_shot_break_even
            .then(|| format!("_be{phase}"))
            .unwrap_or_default();
        transitions.push(compiled_transition(
            format!("position_protective_closed{suffix}"),
            (*hub_state).to_owned(),
            recovery_state.clone(),
            10,
            object([
                ("kind", Value::String("execution_status_is".to_owned())),
                ("status", Value::String("closed".to_owned())),
            ]),
            Vec::new(),
            "position.protective_closed",
            "execution",
        ));
    }
    if has_exit {
        for (phase, hub_state) in hub_states.iter().enumerate() {
            let suffix = has_one_shot_break_even
                .then(|| format!("_be{phase}"))
                .unwrap_or_default();
            let pending = if has_one_shot_break_even {
                format!("exit_pending_be{phase}")
            } else {
                "exit_pending".to_owned()
            };
            for (status, status_priority, destination) in [
                ("closed", 10_u64, recovery_state.clone()),
                ("rejected", 20, (*hub_state).to_owned()),
                ("canceled", 30, (*hub_state).to_owned()),
            ] {
                transitions.push(compiled_transition(
                    format!("exit_{status}{suffix}"),
                    pending.clone(),
                    destination,
                    status_priority,
                    object([
                        ("kind", Value::String("execution_status_is".to_owned())),
                        ("status", Value::String(status.to_owned())),
                    ]),
                    Vec::new(),
                    &format!("evolvable.exit.{status}"),
                    "execution",
                ));
            }
        }
    }
    for (index, recovery_id) in recovery.iter().enumerate() {
        let node = nodes_by_id.get(recovery_id).expect("recovery node exists");
        let timeout = required(node, "timeoutBars", "v5 recovery node")?
            .as_u64()
            .ok_or_else(|| invalid("v5 recovery node needs timeoutBars"))?;
        let destination = recovery
            .get(index + 1)
            .and_then(|id| node_state.get(id))
            .cloned()
            .unwrap_or_else(|| start_state.clone());
        transitions.push(compiled_transition(
            format!("recovery_{index}"),
            node_state
                .get(recovery_id)
                .cloned()
                .expect("recovery state exists"),
            destination,
            10,
            guard_all([
                clone_value(required(node, "guard", "v5 recovery node")?)?,
                object([
                    ("kind", Value::String("state_age_at_least".to_owned())),
                    ("events", Value::from(timeout)),
                ]),
            ])?,
            Vec::new(),
            "evolvable.recovery.timeout",
            "decision",
        ));
    }
    let mut used_groups = BTreeSet::<String>::new();
    let mut used_events = BTreeSet::<String>::new();
    let mut used_indicators = BTreeSet::<String>::new();
    for node in nodes {
        for use_ in array_ref(required(node, "resources", "v5 node")?, "v5 node resources")? {
            let kind = field(use_, "kind")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let id = field(use_, "id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            match kind {
                "evidence_group" => {
                    used_groups.insert(id);
                }
                "event" => {
                    used_events.insert(id);
                }
                "indicator" => {
                    used_indicators.insert(id);
                }
                _ => {}
            }
        }
        collect_guard_resource_ids(
            required(node, "guard", "v5 node")?,
            &mut used_groups,
            &mut used_events,
        );
    }
    for edge in edges {
        collect_guard_resource_ids(
            required(edge, "guard", "v5 edge")?,
            &mut used_groups,
            &mut used_events,
        );
    }
    let group_by_id = groups
        .iter()
        .map(|row| row_id(row, "v5 group").map(|id| (id, row)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    let event_by_id = events
        .iter()
        .map(|row| row_id(row, "v5 event").map(|id| (id, row)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    let indicator_by_id = indicators
        .iter()
        .map(|row| indicator_id(row).map(|id| (id, row)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    for group_id in &used_groups {
        let group = group_by_id
            .get(group_id)
            .ok_or_else(|| invalid("v5 compiler group closure drifted"))?;
        for id in array_ref(
            required(group, "indicatorInstanceIds", "v5 group")?,
            "v5 group members",
        )? {
            used_indicators.insert(text(id, "v5 group indicator ID")?);
        }
    }
    for event_id in &used_events {
        let event = event_by_id
            .get(event_id)
            .ok_or_else(|| invalid("v5 compiler event closure drifted"))?;
        used_indicators.insert(text(
            required(event, "indicatorInstanceId", "v5 event")?,
            "v5 event indicator ID",
        )?);
    }
    let mut plans_by_id = BTreeMap::<String, Value>::new();
    let mut scalar_bindings = BTreeMap::<String, Value>::new();
    for node in nodes {
        for use_ in array_ref(required(node, "resources", "v5 node")?, "v5 node resources")? {
            if field(use_, "kind").and_then(Value::as_str) != Some("management_ref") {
                continue;
            }
            let id = text(
                required(use_, "id", "v5 management use")?,
                "v5 management use ID",
            )?;
            let source = management
                .iter()
                .find(|row| row_id(row, "v5 management").ok().as_deref() == Some(&id))
                .ok_or_else(|| invalid("v5 compiler management closure drifted"))?;
            let mut plan = published_resource(source)?;
            let bindings = plan
                .as_object_mut()
                .expect("published plan object")
                .remove("scalarBindings")
                .unwrap_or_else(|| array(Vec::new()));
            for binding in array_ref(&bindings, "v5 management scalarBindings")? {
                let binding = published_resource(binding)?;
                let binding_id = row_id(&binding, "v5 scalar binding")?;
                let indicator_id = text(
                    required(&binding, "indicatorInstanceId", "v5 scalar binding")?,
                    "v5 scalar binding indicator",
                )?;
                used_indicators.insert(indicator_id);
                scalar_bindings.insert(binding_id, binding);
            }
            plans_by_id.insert(id, plan);
        }
    }
    if plans_by_id.is_empty() {
        return Err(invalid("v5 compiler has no management plan"));
    }
    let default_plan_id = plans_by_id
        .keys()
        .next()
        .cloned()
        .expect("nonempty plan map");
    let mut library = Map::new();
    library.insert(
        "version".to_owned(),
        Value::String("temporal_management_v1".to_owned()),
    );
    library.insert("defaultPlanId".to_owned(), Value::String(default_plan_id));
    library.insert("plans".to_owned(), array(plans_by_id.into_values()));
    if !scalar_bindings.is_empty() {
        library.insert(
            "scalarBindings".to_owned(),
            array(scalar_bindings.into_values()),
        );
    }
    let instrument = text(
        required(program, "instrument", "v5 immigrant program")?,
        "v5 instrument",
    )?;
    Ok(object([
        ("version", Value::String("v2".to_owned())),
        ("name", Value::String(format!("evolvable v1 {side} module"))),
        (
            "description",
            Value::String(
                "AutoResearch evolvable module genotype; not an economic candidate".to_owned(),
            ),
        ),
        ("instruments", array(vec![Value::String(instrument)])),
        ("directionMode", Value::String(side)),
        ("isActive", Value::Bool(false)),
        (
            "indicators",
            array(
                used_indicators
                    .iter()
                    .map(|id| {
                        indicator_by_id
                            .get(id)
                            .ok_or_else(|| invalid("v5 compiler indicator closure drifted"))
                            .and_then(|row| published_resource(row))
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
        ),
        (
            "executionConfig",
            object([("managementLibrary", Value::Object(library))]),
        ),
        (
            "graph",
            object([
                ("kind", Value::String("temporal_graph_v1".to_owned())),
                (
                    "semanticPolicy",
                    Value::String("temporal_graph_semantics_v1".to_owned()),
                ),
                ("eventSchema", Value::String("temporal_event_v1".to_owned())),
                (
                    "factLibrary",
                    Value::String("temporal_market_facts_v1".to_owned()),
                ),
                (
                    "guardLibrary",
                    Value::String("temporal_guards_v1".to_owned()),
                ),
                (
                    "actionLibrary",
                    Value::String("temporal_market_actions_v1".to_owned()),
                ),
                (
                    "clockRequirement",
                    Value::String("clock.completed_bar".to_owned()),
                ),
                (
                    "fidelityRequirements",
                    array(vec![Value::String("data.completed_ohlc".to_owned())]),
                ),
                ("initialStateId", Value::String(start_state)),
                ("states", array(states)),
                (
                    "evidenceGroups",
                    array(
                        used_groups
                            .iter()
                            .map(|id| {
                                group_by_id
                                    .get(id)
                                    .ok_or_else(|| invalid("v5 compiler group output drifted"))
                                    .and_then(|row| published_resource(row))
                            })
                            .collect::<Result<Vec<_>>>()?,
                    ),
                ),
                (
                    "eventBindings",
                    array(
                        used_events
                            .iter()
                            .map(|id| {
                                event_by_id
                                    .get(id)
                                    .ok_or_else(|| invalid("v5 compiler event output drifted"))
                                    .and_then(|row| published_resource(row))
                            })
                            .collect::<Result<Vec<_>>>()?,
                    ),
                ),
                ("transitions", array(transitions)),
            ]),
        ),
    ]))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModuleSourceIdentities {
    pub profile_snapshot_sha256: String,
    pub program_sha256: String,
}

impl ModuleSourceIdentities {
    pub fn from_native_report(value: &Value) -> Result<Self> {
        Ok(Self {
            profile_snapshot_sha256: text(
                required(value, "profileSnapshotSha256", "v5 native module report")?,
                "v5 native module profile snapshot SHA-256",
            )?,
            program_sha256: text(
                required(value, "programSha256", "v5 native module report")?,
                "v5 native module program SHA-256",
            )?,
        })
    }
}

fn namespace_id(side: &str, value: &str) -> Result<String> {
    let value = format!("{side}_{value}");
    if value.len() > 64 {
        return Err(invalid(
            "v5 namespaced identifier exceeds the 64-character bound",
        ));
    }
    Ok(value)
}

fn rewrite_binding_refs(value: &Value, bindings: &BTreeMap<String, String>) -> Value {
    match value {
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(key, item)| {
                    (
                        key.clone(),
                        if key == "bindingId" {
                            item.as_str()
                                .and_then(|id| bindings.get(id))
                                .cloned()
                                .map(Value::String)
                                .unwrap_or_else(|| rewrite_binding_refs(item, bindings))
                        } else {
                            rewrite_binding_refs(item, bindings)
                        },
                    )
                })
                .collect(),
        ),
        Value::Array(rows) => Value::Array(
            rows.iter()
                .map(|item| rewrite_binding_refs(item, bindings))
                .collect(),
        ),
        _ => value.clone(),
    }
}

fn rewrite_guard_refs(
    value: &Value,
    groups: &BTreeMap<String, String>,
    events: &BTreeMap<String, String>,
    transitions: &BTreeMap<String, String>,
) -> Value {
    match value {
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(key, item)| {
                    let replacement = match key.as_str() {
                        "groupId" => groups,
                        "eventId" => events,
                        "transitionId" => transitions,
                        _ => {
                            return (
                                key.clone(),
                                rewrite_guard_refs(item, groups, events, transitions),
                            );
                        }
                    };
                    (
                        key.clone(),
                        item.as_str()
                            .and_then(|id| replacement.get(id))
                            .cloned()
                            .map(Value::String)
                            .unwrap_or_else(|| {
                                rewrite_guard_refs(item, groups, events, transitions)
                            }),
                    )
                })
                .collect(),
        ),
        Value::Array(rows) => Value::Array(
            rows.iter()
                .map(|item| rewrite_guard_refs(item, groups, events, transitions))
                .collect(),
        ),
        _ => value.clone(),
    }
}

#[derive(Clone, Debug)]
struct CompiledSide {
    indicators: Vec<Value>,
    states: Vec<Value>,
    evidence: Vec<Value>,
    events: Vec<Value>,
    transitions: Vec<Value>,
    management: Value,
    manifest: Value,
}

fn compile_side_v3(
    profile: &Value,
    side: &str,
    supervisor: &str,
    identities: &ModuleSourceIdentities,
) -> Result<CompiledSide> {
    let fields = object_ref(profile, "v5 source v2 profile")?;
    if fields.get("version").and_then(Value::as_str) != Some("v2")
        || fields.get("directionMode").and_then(Value::as_str) != Some(side)
    {
        return Err(invalid("v5 compiler requires matching v2 side profile"));
    }
    let graph = required(profile, "graph", "v5 source v2 profile")?;
    let _graph_fields = object_ref(graph, "v5 source graph")?;
    let initial = text(
        required(graph, "initialStateId", "v5 source graph")?,
        "v5 source initial state",
    )?;
    let states = array_ref(
        required(graph, "states", "v5 source graph")?,
        "v5 source states",
    )?;
    let indicators = array_ref(
        required(profile, "indicators", "v5 source v2 profile")?,
        "v5 source indicators",
    )?;
    let evidence = array_ref(
        required(graph, "evidenceGroups", "v5 source graph")?,
        "v5 source evidence groups",
    )?;
    let events = array_ref(
        required(graph, "eventBindings", "v5 source graph")?,
        "v5 source events",
    )?;
    let transitions = array_ref(
        required(graph, "transitions", "v5 source graph")?,
        "v5 source transitions",
    )?;
    let mut state_map = BTreeMap::<String, String>::new();
    for row in states {
        let id = row_id(row, "v5 source state")?;
        state_map.insert(id.clone(), namespace_id(side, &id)?);
    }
    state_map.insert(initial.clone(), supervisor.to_owned());
    let mut indicator_map = BTreeMap::<String, String>::new();
    for row in indicators {
        let id = indicator_id(row)?;
        indicator_map.insert(id.clone(), namespace_id(side, &id)?);
    }
    let mut group_map = BTreeMap::<String, String>::new();
    for row in evidence {
        let id = row_id(row, "v5 source evidence group")?;
        group_map.insert(id.clone(), namespace_id(side, &id)?);
    }
    let mut event_map = BTreeMap::<String, String>::new();
    for row in events {
        let id = row_id(row, "v5 source event")?;
        event_map.insert(id.clone(), namespace_id(side, &id)?);
    }
    let mut transition_map = BTreeMap::<String, String>::new();
    for row in transitions {
        let id = row_id(row, "v5 source transition")?;
        transition_map.insert(id.clone(), namespace_id(side, &id)?);
    }
    let library = required(profile, "executionConfig", "v5 source v2 profile")
        .and_then(|value| required(value, "managementLibrary", "v5 source execution config"))?;
    let library_fields = object_ref(library, "v5 source management library")?;
    let plans = array_ref(
        required(library, "plans", "v5 source management library")?,
        "v5 source plans",
    )?;
    let source_bindings = library_fields
        .get("scalarBindings")
        .map(|value| array_ref(value, "v5 source scalar bindings"))
        .transpose()?
        .unwrap_or(&[]);
    let mut plan_map = BTreeMap::<String, String>::new();
    for plan in plans {
        let id = row_id(plan, "v5 source management plan")?;
        plan_map.insert(id.clone(), namespace_id(side, &id)?);
    }
    let mut binding_map = BTreeMap::<String, String>::new();
    for binding in source_bindings {
        let id = row_id(binding, "v5 source scalar binding")?;
        binding_map.insert(id.clone(), namespace_id(side, &id)?);
    }
    let profile_hold = required(profile, "executionConfig", "v5 source v2 profile")?
        .get("holdPolicy")
        .cloned();
    let mut compiled_plans = Vec::new();
    for plan in plans {
        let source_id = row_id(plan, "v5 source management plan")?;
        let mut plan = rewrite_binding_refs(plan, &binding_map);
        plan.as_object_mut().expect("rewritten plan object").insert(
            "id".to_owned(),
            Value::String(
                plan_map
                    .get(&source_id)
                    .expect("plan map contains source ID")
                    .clone(),
            ),
        );
        if let Some(hold) = &profile_hold {
            if plan
                .get("holdPolicy")
                .is_some_and(|existing| existing != hold)
            {
                return Err(invalid(
                    "v5 profile and management plan hold policies conflict",
                ));
            }
            plan.as_object_mut()
                .expect("rewritten plan object")
                .insert("holdPolicy".to_owned(), hold.clone());
        }
        compiled_plans.push(plan);
    }
    let mut compiled_bindings = Vec::new();
    for binding in source_bindings {
        let source_id = row_id(binding, "v5 source scalar binding")?;
        let source_indicator = text(
            required(binding, "indicatorInstanceId", "v5 source scalar binding")?,
            "v5 source scalar indicator",
        )?;
        let mut binding = clone_value(binding)?;
        let fields = binding.as_object_mut().expect("cloned binding object");
        fields.insert(
            "id".to_owned(),
            Value::String(
                binding_map
                    .get(&source_id)
                    .expect("binding map contains source ID")
                    .clone(),
            ),
        );
        fields.insert(
            "indicatorInstanceId".to_owned(),
            Value::String(
                indicator_map
                    .get(&source_indicator)
                    .ok_or_else(|| invalid("v5 scalar binding indicator is absent"))?
                    .clone(),
            ),
        );
        compiled_bindings.push(binding);
    }
    let mut compiled_management = Map::new();
    compiled_management.insert(
        "version".to_owned(),
        clone_value(required(
            library,
            "version",
            "v5 source management library",
        )?)?,
    );
    let default_plan = text(
        required(library, "defaultPlanId", "v5 source management library")?,
        "v5 source default management plan",
    )?;
    compiled_management.insert(
        "defaultPlanId".to_owned(),
        Value::String(
            plan_map
                .get(&default_plan)
                .ok_or_else(|| invalid("v5 source default management plan is absent"))?
                .clone(),
        ),
    );
    compiled_management.insert("plans".to_owned(), array(compiled_plans));
    compiled_management.insert("scalarBindings".to_owned(), array(compiled_bindings));
    let mut compiled_indicators = Vec::new();
    for indicator in indicators {
        let source_id = indicator_id(indicator)?;
        let mut indicator = clone_value(indicator)?;
        let fields = indicator.as_object_mut().expect("cloned indicator object");
        let meta = fields
            .get_mut("meta")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| invalid("v5 source indicator lacks meta"))?;
        meta.insert(
            "instanceId".to_owned(),
            Value::String(
                indicator_map
                    .get(&source_id)
                    .expect("indicator map contains source ID")
                    .clone(),
            ),
        );
        compiled_indicators.push(indicator);
    }
    let mut compiled_states = Vec::new();
    for state in states {
        let source_id = row_id(state, "v5 source state")?;
        if source_id == initial {
            continue;
        }
        let mut state = clone_value(state)?;
        let fields = state.as_object_mut().expect("cloned state object");
        fields.insert(
            "id".to_owned(),
            Value::String(
                state_map
                    .get(&source_id)
                    .expect("state map contains source ID")
                    .clone(),
            ),
        );
        // `TemporalState.model_dump(exclude_none=False)` is what the frozen
        // Dashboard composite compiler returns after its final Pydantic
        // validation.  The generated v2 modules omit these optional fields,
        // but the v3 canonical snapshot carries their explicit null values.
        fields.entry("label".to_owned()).or_insert(Value::Null);
        fields
            .entry("description".to_owned())
            .or_insert(Value::Null);
        compiled_states.push(state);
    }
    let mut compiled_evidence = Vec::new();
    for group in evidence {
        let source_id = row_id(group, "v5 source evidence group")?;
        let members = array_ref(
            required(group, "indicatorInstanceIds", "v5 source evidence group")?,
            "v5 evidence members",
        )?;
        let mut group = clone_value(group)?;
        let fields = group.as_object_mut().expect("cloned group object");
        fields.insert(
            "id".to_owned(),
            Value::String(
                group_map
                    .get(&source_id)
                    .expect("group map contains source ID")
                    .clone(),
            ),
        );
        fields.insert(
            "indicatorInstanceIds".to_owned(),
            array(
                members
                    .iter()
                    .map(|member| {
                        text(member, "v5 evidence member")
                            .and_then(|id| {
                                indicator_map
                                    .get(&id)
                                    .cloned()
                                    .ok_or_else(|| invalid("v5 evidence indicator is absent"))
                            })
                            .map(Value::String)
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
        );
        compiled_evidence.push(group);
    }
    let mut compiled_events = Vec::new();
    for event in events {
        let source_id = row_id(event, "v5 source event")?;
        let source_indicator = text(
            required(event, "indicatorInstanceId", "v5 source event")?,
            "v5 source event indicator",
        )?;
        let mut event = clone_value(event)?;
        let fields = event.as_object_mut().expect("cloned event object");
        fields.insert(
            "id".to_owned(),
            Value::String(
                event_map
                    .get(&source_id)
                    .expect("event map contains source ID")
                    .clone(),
            ),
        );
        fields.insert(
            "indicatorInstanceId".to_owned(),
            Value::String(
                indicator_map
                    .get(&source_indicator)
                    .ok_or_else(|| invalid("v5 event indicator is absent"))?
                    .clone(),
            ),
        );
        compiled_events.push(event);
    }
    let mut compiled_transitions = Vec::new();
    let mut supervisor_transitions = Vec::new();
    for transition in transitions {
        let source_id = row_id(transition, "v5 source transition")?;
        let source_state = text(
            required(transition, "sourceStateId", "v5 source transition")?,
            "v5 source transition source",
        )?;
        let destination_state = text(
            required(transition, "destinationStateId", "v5 source transition")?,
            "v5 source transition destination",
        )?;
        let mut transition = clone_value(transition)?;
        let fields = transition
            .as_object_mut()
            .expect("cloned transition object");
        let compiled_id = transition_map
            .get(&source_id)
            .expect("transition map contains source ID")
            .clone();
        fields.insert("id".to_owned(), Value::String(compiled_id.clone()));
        fields.insert(
            "sourceStateId".to_owned(),
            Value::String(
                state_map
                    .get(&source_state)
                    .ok_or_else(|| invalid("v5 transition source state is absent"))?
                    .clone(),
            ),
        );
        fields.insert(
            "destinationStateId".to_owned(),
            Value::String(
                state_map
                    .get(&destination_state)
                    .ok_or_else(|| invalid("v5 transition destination state is absent"))?
                    .clone(),
            ),
        );
        let guard = fields
            .get("guard")
            .cloned()
            .ok_or_else(|| invalid("v5 transition guard is absent"))?;
        fields.insert(
            "guard".to_owned(),
            rewrite_guard_refs(&guard, &group_map, &event_map, &transition_map),
        );
        let actions = fields
            .get("actions")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| invalid("v5 transition actions are absent"))?;
        let mut compiled_actions = Vec::new();
        for action in actions {
            let mut action = rewrite_binding_refs(&action, &binding_map);
            let action_fields = action
                .as_object_mut()
                .ok_or_else(|| invalid("v5 transition action is invalid"))?;
            if let Some(plan_id) = action_fields
                .get("managementPlanId")
                .and_then(Value::as_str)
            {
                if let Some(replacement) = plan_map.get(plan_id) {
                    action_fields.insert(
                        "managementPlanId".to_owned(),
                        Value::String(replacement.clone()),
                    );
                }
            }
            if action_fields.get("kind").and_then(Value::as_str) == Some("enter_next_open") {
                action_fields.insert("direction".to_owned(), Value::String(side.to_owned()));
                if action_fields.get("managementPlanId").is_none() {
                    action_fields.insert(
                        "managementPlanId".to_owned(),
                        compiled_management
                            .get("defaultPlanId")
                            .expect("compiled default plan")
                            .clone(),
                    );
                }
            }
            compiled_actions.push(action);
        }
        fields.insert("actions".to_owned(), array(compiled_actions));
        if source_state == initial {
            supervisor_transitions.push(compiled_id);
        }
        compiled_transitions.push(transition);
    }
    let mut state_ids = state_map
        .iter()
        .filter_map(|(id, value)| (id != &initial).then_some(value.clone()))
        .collect::<Vec<_>>();
    state_ids.sort();
    let mut transition_ids = transition_map.values().cloned().collect::<Vec<_>>();
    transition_ids.sort();
    supervisor_transitions.sort();
    let mut group_ids = group_map.values().cloned().collect::<Vec<_>>();
    group_ids.sort();
    let mut event_ids = event_map.values().cloned().collect::<Vec<_>>();
    event_ids.sort();
    let mut indicator_ids = indicator_map.values().cloned().collect::<Vec<_>>();
    indicator_ids.sort();
    Ok(CompiledSide {
        indicators: compiled_indicators,
        states: compiled_states,
        evidence: compiled_evidence,
        events: compiled_events,
        transitions: compiled_transitions,
        management: Value::Object(compiled_management),
        manifest: object([
            ("id", Value::String(side.to_owned())),
            ("direction", Value::String(side.to_owned())),
            ("stateIds", array(state_ids.into_iter().map(Value::String))),
            (
                "transitionIds",
                array(transition_ids.into_iter().map(Value::String)),
            ),
            (
                "supervisorTransitionIds",
                array(supervisor_transitions.into_iter().map(Value::String)),
            ),
            (
                "evidenceGroupIds",
                array(group_ids.into_iter().map(Value::String)),
            ),
            (
                "eventBindingIds",
                array(event_ids.into_iter().map(Value::String)),
            ),
            (
                "indicatorIds",
                array(indicator_ids.into_iter().map(Value::String)),
            ),
            (
                "sourceProfileSnapshotSha256",
                Value::String(identities.profile_snapshot_sha256.clone()),
            ),
            (
                "sourceProgramSha256",
                Value::String(identities.program_sha256.clone()),
            ),
        ]),
    })
}

/// Prove that a freshly compiled side profile is inside the closed v2->v3
/// namespace surface without materializing the v3 side. All deeper source
/// references were already checked by `validate_program` before the v2
/// profile was compiled. Prefixing is the only bidirectional transformation
/// that can introduce a new side-local failure for that admitted profile.
fn validate_bidirectional_side_namespace_closure(profile: &Value, side: &str) -> Result<()> {
    if field(profile, "version").and_then(Value::as_str) != Some("v2")
        || field(profile, "directionMode").and_then(Value::as_str) != Some(side)
    {
        return Err(invalid(
            "v5 bidirectional probe requires a matching v2 side profile",
        ));
    }
    let graph = required(profile, "graph", "v5 bidirectional probe profile")?;
    for (field_name, label) in [
        ("states", "state"),
        ("evidenceGroups", "evidence group"),
        ("eventBindings", "event binding"),
        ("transitions", "transition"),
    ] {
        for row in array_ref(
            required(graph, field_name, "v5 bidirectional probe graph")?,
            "v5 bidirectional probe graph rows",
        )? {
            let _ = namespace_id(side, &row_id(row, label)?)?;
        }
    }
    for indicator in array_ref(
        required(profile, "indicators", "v5 bidirectional probe profile")?,
        "v5 bidirectional probe indicators",
    )? {
        let _ = namespace_id(side, &indicator_id(indicator)?)?;
    }
    let library = required(profile, "executionConfig", "v5 bidirectional probe profile").and_then(
        |value| {
            required(
                value,
                "managementLibrary",
                "v5 bidirectional probe execution config",
            )
        },
    )?;
    for plan in array_ref(
        required(
            library,
            "plans",
            "v5 bidirectional probe management library",
        )?,
        "v5 bidirectional probe management plans",
    )? {
        let _ = namespace_id(side, &row_id(plan, "management plan")?)?;
    }
    for binding in field(library, "scalarBindings")
        .map(|value| array_ref(value, "v5 bidirectional probe scalar bindings"))
        .transpose()?
        .unwrap_or(&[])
    {
        let _ = namespace_id(side, &row_id(binding, "management scalar binding")?)?;
    }
    Ok(())
}

/// Closed native v2→v3 compiler for the generated v5 profile subset.  Its
/// output shape intentionally mirrors the frozen Dashboard compiler; native
/// validation remains a separate explicit step.
pub fn compile_bidirectional_profile(
    long_profile: &Value,
    short_profile: &Value,
    candidate_id: &str,
    long_identities: &ModuleSourceIdentities,
    short_identities: &ModuleSourceIdentities,
) -> Result<Value> {
    let long_instruments = required(long_profile, "instruments", "v5 long profile")?;
    if long_instruments != required(short_profile, "instruments", "v5 short profile")? {
        return Err(invalid(
            "v5 bidirectional modules must use matching instruments",
        ));
    }
    let long_graph = required(long_profile, "graph", "v5 long profile")?;
    let short_graph = required(short_profile, "graph", "v5 short profile")?;
    if required(long_graph, "clockRequirement", "v5 long graph")?
        != required(short_graph, "clockRequirement", "v5 short graph")?
    {
        return Err(invalid("v5 bidirectional modules must use matching clocks"));
    }
    let supervisor = "flat_supervisor";
    let long = compile_side_v3(long_profile, "long", supervisor, long_identities)?;
    let short = compile_side_v3(short_profile, "short", supervisor, short_identities)?;
    let mut plans = array_ref(
        required(&long.management, "plans", "compiled long management")?,
        "compiled long plans",
    )?
    .to_vec();
    plans.extend(
        array_ref(
            required(&short.management, "plans", "compiled short management")?,
            "compiled short plans",
        )?
        .iter()
        .cloned(),
    );
    let mut bindings = long
        .management
        .get("scalarBindings")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    bindings.extend(
        short
            .management
            .get("scalarBindings")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .cloned(),
    );
    let mut library = Map::new();
    library.insert(
        "version".to_owned(),
        Value::String("temporal_management_v1".to_owned()),
    );
    library.insert(
        "defaultPlanId".to_owned(),
        long.management
            .get("defaultPlanId")
            .expect("compiled long default plan")
            .clone(),
    );
    library.insert("plans".to_owned(), array(plans));
    if !bindings.is_empty() {
        library.insert("scalarBindings".to_owned(), array(bindings));
    }
    let mut indicators = long.indicators;
    indicators.extend(short.indicators);
    let mut states = vec![object([
        ("id", Value::String(supervisor.to_owned())),
        ("label", Value::String("Flat supervisor".to_owned())),
        ("description", Value::Null),
    ])];
    states.extend(long.states);
    states.extend(short.states);
    let mut evidence = long.evidence;
    evidence.extend(short.evidence);
    let mut events = long.events;
    events.extend(short.events);
    let mut transitions = long.transitions;
    transitions.extend(short.transitions);
    transitions.push(compiled_transition(
        "entry_direction_conflict".to_owned(),
        supervisor.to_owned(),
        supervisor.to_owned(),
        1_000_000,
        object([("kind", Value::String("always".to_owned()))]),
        Vec::new(),
        "entry_direction_conflict_abstained",
        "decision",
    ));
    let mut fidelity = BTreeSet::<String>::new();
    for profile in [long_profile, short_profile] {
        for requirement in array_ref(
            required(
                required(profile, "graph", "v5 source profile")?,
                "fidelityRequirements",
                "v5 source graph",
            )?,
            "v5 fidelity requirements",
        )? {
            fidelity.insert(text(requirement, "v5 fidelity requirement")?);
        }
    }
    Ok(object([
        ("version", Value::String("v3".to_owned())),
        (
            "name",
            Value::String(format!("QD bidirectional {candidate_id}")),
        ),
        (
            "description",
            Value::String("Compiled one-token bidirectional temporal program.".to_owned()),
        ),
        ("instruments", clone_value(long_instruments)?),
        ("directionMode", Value::String("both".to_owned())),
        ("isActive", Value::Bool(false)),
        ("indicators", array(indicators)),
        (
            "executionConfig",
            object([("managementLibrary", Value::Object(library))]),
        ),
        (
            "graph",
            object([
                ("kind", Value::String("temporal_graph_v2".to_owned())),
                (
                    "semanticPolicy",
                    Value::String("temporal_graph_semantics_v2".to_owned()),
                ),
                (
                    "eventSchema",
                    clone_value(required(long_graph, "eventSchema", "v5 long graph")?)?,
                ),
                (
                    "factLibrary",
                    clone_value(required(long_graph, "factLibrary", "v5 long graph")?)?,
                ),
                (
                    "guardLibrary",
                    clone_value(required(long_graph, "guardLibrary", "v5 long graph")?)?,
                ),
                (
                    "actionLibrary",
                    clone_value(required(long_graph, "actionLibrary", "v5 long graph")?)?,
                ),
                (
                    "clockRequirement",
                    clone_value(required(long_graph, "clockRequirement", "v5 long graph")?)?,
                ),
                (
                    "fidelityRequirements",
                    array(fidelity.into_iter().map(Value::String)),
                ),
                ("initialStateId", Value::String(supervisor.to_owned())),
                ("states", array(states)),
                ("evidenceGroups", array(evidence)),
                ("eventBindings", array(events)),
                ("transitions", array(transitions)),
                (
                    "entryArbitration",
                    object([
                        (
                            "schemaVersion",
                            Value::String("temporal_entry_arbitration_v1".to_owned()),
                        ),
                        ("supervisorStateId", Value::String(supervisor.to_owned())),
                        (
                            "conflictTransitionId",
                            Value::String("entry_direction_conflict".to_owned()),
                        ),
                        ("modules", array(vec![long.manifest, short.manifest])),
                    ]),
                ),
            ]),
        ),
    ]))
}

// -------------------------------------------------------------------------
// Closed Dashboard identity subset
// -------------------------------------------------------------------------
//
// The production v5 constructor cannot ask the Dashboard Python validator to
// bless each candidate.  These helpers port the identity-relevant subset used
// by the generated v2/v3 profiles.  They intentionally fail closed when a
// program introduces a guard/action/resource shape that is not represented in
// this compiler; adding a shape requires an oracle golden rather than turning
// a malformed profile into an apparently-valid compact record.

fn sort_rows_by_id(rows: &[Value], label: &str) -> Result<Vec<Value>> {
    let mut rows = rows.iter().map(clone_value).collect::<Result<Vec<_>>>()?;
    rows.sort_by_key(|row| row_id(row, label).unwrap_or_default());
    let mut seen = BTreeSet::new();
    for row in &rows {
        let id = row_id(row, label)?;
        if !seen.insert(id) {
            return Err(invalid(format!("{label} IDs must be unique")));
        }
    }
    Ok(rows)
}

fn canonical_guard_payload(value: &Value) -> Result<Value> {
    let mut output = clone_value(value)?;
    let fields = output
        .as_object_mut()
        .ok_or_else(|| invalid("v5 guard must be an object"))?;
    let kind = fields
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("v5 guard lacks kind"))?
        .to_owned();
    match kind.as_str() {
        "all" | "any" => {
            let guards = fields
                .get("guards")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid("v5 boolean guard lacks ordered guards"))?;
            let mut normalized = guards
                .iter()
                .map(canonical_guard_payload)
                .collect::<Result<Vec<_>>>()?;
            normalized.sort_by_key(|guard| canonical_json(guard).unwrap_or_default());
            fields.insert("guards".to_owned(), array(normalized));
        }
        "not" => {
            let guard = fields
                .get("guard")
                .ok_or_else(|| invalid("v5 negated guard lacks child"))?;
            fields.insert("guard".to_owned(), canonical_guard_payload(guard)?);
        }
        _ => {}
    }
    Ok(output)
}

fn normalized_state_payload(value: &Value, behavior_only: bool) -> Result<Value> {
    let id = row_id(value, "v5 state")?;
    if behavior_only {
        return Ok(object([("id", Value::String(id))]));
    }
    let mut output = clone_value(value)?;
    let fields = output
        .as_object_mut()
        .ok_or_else(|| invalid("v5 state must be an object"))?;
    // Pydantic's TemporalState model_dump(exclude_none=False) writes both
    // optional fields even though our compact compiler omits them in raw
    // source profiles.
    fields.entry("label".to_owned()).or_insert(Value::Null);
    fields
        .entry("description".to_owned())
        .or_insert(Value::Null);
    Ok(output)
}

fn normalized_evidence_group_payload(value: &Value) -> Result<Value> {
    let mut output = clone_value(value)?;
    let fields = output
        .as_object_mut()
        .ok_or_else(|| invalid("v5 evidence group must be an object"))?;
    let members = fields
        .get("indicatorInstanceIds")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("v5 evidence group members are invalid"))?;
    let mut ids = BTreeSet::new();
    for member in members {
        ids.insert(text(member, "v5 evidence group indicator")?);
    }
    if ids.is_empty() {
        return Err(invalid("v5 evidence group must bind an indicator"));
    }
    fields.insert(
        "indicatorInstanceIds".to_owned(),
        array(ids.into_iter().map(Value::String)),
    );
    Ok(output)
}

fn normalized_event_binding_payload(value: &Value) -> Result<Value> {
    let mut output = clone_value(value)?;
    let fields = output
        .as_object_mut()
        .ok_or_else(|| invalid("v5 event binding must be an object"))?;
    let _ = text(
        fields
            .get("id")
            .ok_or_else(|| invalid("v5 event binding lacks ID"))?,
        "v5 event binding ID",
    )?;
    let _ = text(
        fields
            .get("indicatorInstanceId")
            .ok_or_else(|| invalid("v5 event binding lacks indicator"))?,
        "v5 event binding indicator",
    )?;
    // These are Pydantic defaults in the frozen Dashboard schema.
    fields
        .entry("longOutput".to_owned())
        .or_insert_with(|| Value::String("bullish".to_owned()));
    fields
        .entry("shortOutput".to_owned())
        .or_insert_with(|| Value::String("bearish".to_owned()));
    Ok(output)
}

fn normalized_transition_payload(value: &Value) -> Result<Value> {
    let mut output = clone_value(value)?;
    let fields = output
        .as_object_mut()
        .ok_or_else(|| invalid("v5 transition must be an object"))?;
    let guard = fields
        .get("guard")
        .ok_or_else(|| invalid("v5 transition lacks guard"))?;
    fields.insert("guard".to_owned(), canonical_guard_payload(guard)?);
    if fields.get("actions").and_then(Value::as_array).is_none() {
        return Err(invalid("v5 transition actions must be an ordered array"));
    }
    Ok(output)
}

fn normalized_entry_arbitration_payload(value: &Value, behavior_only: bool) -> Result<Value> {
    let mut output = clone_value(value)?;
    let fields = output
        .as_object_mut()
        .ok_or_else(|| invalid("v5 entry arbitration must be an object"))?;
    let modules = fields
        .get("modules")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("v5 entry arbitration lacks module manifests"))?;
    if modules.len() != 2 {
        return Err(invalid("v5 entry arbitration must contain two manifests"));
    }
    let mut normalized = Vec::with_capacity(modules.len());
    let mut sides = BTreeSet::new();
    for module in modules {
        let mut module = clone_value(module)?;
        let map = module
            .as_object_mut()
            .ok_or_else(|| invalid("v5 module manifest must be an object"))?;
        let side = text(
            map.get("direction")
                .ok_or_else(|| invalid("v5 module manifest lacks direction"))?,
            "v5 module manifest direction",
        )?;
        if map.get("id").and_then(Value::as_str) != Some(side.as_str())
            || !matches!(side.as_str(), "long" | "short")
            || !sides.insert(side)
        {
            return Err(invalid("v5 entry arbitration manifests are invalid"));
        }
        if behavior_only {
            map.remove("sourceProfileSnapshotSha256");
            map.remove("sourceProgramSha256");
        }
        normalized.push(module);
    }
    fields.insert("modules".to_owned(), array(normalized));
    Ok(output)
}

fn normalized_graph_payload(profile: &Value, behavior_only: bool) -> Result<Value> {
    let graph = required(profile, "graph", "v5 temporal profile")?;
    let graph_map = object_ref(graph, "v5 temporal graph")?;
    let states = array_ref(
        required(graph, "states", "v5 temporal graph")?,
        "v5 temporal graph states",
    )?;
    let evidence = array_ref(
        required(graph, "evidenceGroups", "v5 temporal graph")?,
        "v5 temporal graph evidence groups",
    )?;
    let events = array_ref(
        required(graph, "eventBindings", "v5 temporal graph")?,
        "v5 temporal graph event bindings",
    )?;
    let transitions = array_ref(
        required(graph, "transitions", "v5 temporal graph")?,
        "v5 temporal graph transitions",
    )?;
    let mut normalized_states = states
        .iter()
        .map(|state| normalized_state_payload(state, behavior_only))
        .collect::<Result<Vec<_>>>()?;
    normalized_states.sort_by_key(|state| row_id(state, "v5 normalized state").unwrap_or_default());
    let mut normalized_evidence = evidence
        .iter()
        .map(normalized_evidence_group_payload)
        .collect::<Result<Vec<_>>>()?;
    normalized_evidence
        .sort_by_key(|row| row_id(row, "v5 normalized evidence").unwrap_or_default());
    let mut normalized_events = events
        .iter()
        .map(normalized_event_binding_payload)
        .collect::<Result<Vec<_>>>()?;
    normalized_events.sort_by_key(|row| row_id(row, "v5 normalized event").unwrap_or_default());
    let mut normalized_transitions = transitions
        .iter()
        .map(normalized_transition_payload)
        .collect::<Result<Vec<_>>>()?;
    normalized_transitions
        .sort_by_key(|row| row_id(row, "v5 normalized transition").unwrap_or_default());
    let mut result = Map::new();
    for key in [
        "kind",
        "semanticPolicy",
        "eventSchema",
        "factLibrary",
        "guardLibrary",
        "actionLibrary",
        "clockRequirement",
        "initialStateId",
    ] {
        result.insert(
            key.to_owned(),
            clone_value(
                graph_map
                    .get(key)
                    .ok_or_else(|| invalid(format!("v5 temporal graph lacks {key}")))?,
            )?,
        );
    }
    let fidelity = array_ref(
        required(graph, "fidelityRequirements", "v5 temporal graph")?,
        "v5 temporal graph fidelity requirements",
    )?;
    let mut fidelity = fidelity
        .iter()
        .map(|value| text(value, "v5 fidelity requirement"))
        .collect::<Result<BTreeSet<_>>>()?
        .into_iter()
        .map(Value::String)
        .collect::<Vec<_>>();
    // BTreeSet is already sorted; keeping this local makes that invariant
    // obvious if representation changes later.
    fidelity.sort_by_key(|value| value.as_str().unwrap_or_default().to_owned());
    result.insert("fidelityRequirements".to_owned(), array(fidelity));
    result.insert("states".to_owned(), array(normalized_states));
    result.insert("evidenceGroups".to_owned(), array(normalized_evidence));
    result.insert("eventBindings".to_owned(), array(normalized_events));
    result.insert("transitions".to_owned(), array(normalized_transitions));
    if let Some(arbitration) = graph_map.get("entryArbitration") {
        result.insert(
            "entryArbitration".to_owned(),
            normalized_entry_arbitration_payload(arbitration, behavior_only)?,
        );
    }
    Ok(Value::Object(result))
}

fn normalized_management_library_payload(value: &Value) -> Result<Value> {
    let fields = object_ref(value, "v5 management library")?;
    let mut output = Map::new();
    for key in ["version", "defaultPlanId"] {
        output.insert(
            key.to_owned(),
            clone_value(
                fields
                    .get(key)
                    .ok_or_else(|| invalid(format!("v5 management library lacks {key}")))?,
            )?,
        );
    }
    let plans = array_ref(
        fields
            .get("plans")
            .ok_or_else(|| invalid("v5 management library lacks plans"))?,
        "v5 management plans",
    )?;
    let plans = sort_rows_by_id(plans, "v5 management plan")?;
    output.insert("plans".to_owned(), array(plans));
    if let Some(bindings) = fields.get("scalarBindings") {
        let bindings = array_ref(bindings, "v5 management scalar bindings")?;
        if !bindings.is_empty() {
            output.insert(
                "scalarBindings".to_owned(),
                array(sort_rows_by_id(bindings, "v5 management scalar binding")?),
            );
        }
    }
    Ok(Value::Object(output))
}

fn normalized_execution_behavior_payload(profile: &Value) -> Result<Value> {
    let config = required(profile, "executionConfig", "v5 temporal profile")?;
    let fields = object_ref(config, "v5 execution config")?;
    let mut output = Map::new();
    if let Some(library) = fields.get("managementLibrary") {
        output.insert(
            "managementLibrary".to_owned(),
            normalized_management_library_payload(library)?,
        );
    } else if let Some(exit_policy) = fields.get("exitPolicy") {
        let exit_fields = object_ref(exit_policy, "v5 exit policy")?;
        if let Some(selected) = exit_fields.get("selectedCell") {
            output.insert(
                "exitPolicy".to_owned(),
                object([("selectedCell", clone_value(selected)?)]),
            );
        }
    }
    for key in ["sizingPolicy", "holdPolicy"] {
        if let Some(value) = fields.get(key) {
            output.insert(key.to_owned(), clone_value(value)?);
        }
    }
    if output.is_empty() {
        Ok(Value::Null)
    } else {
        Ok(Value::Object(output))
    }
}

fn normalized_profile_snapshot_payload(profile: &Value) -> Result<Value> {
    let indicators = array_ref(
        required(profile, "indicators", "v5 temporal profile")?,
        "v5 profile indicators",
    )?;
    let mut indicators = indicators
        .iter()
        .map(clone_value)
        .collect::<Result<Vec<_>>>()?;
    indicators.sort_by_key(|indicator| indicator_id(indicator).unwrap_or_default());
    Ok(object([
        (
            "version",
            clone_value(required(profile, "version", "v5 temporal profile")?)?,
        ),
        (
            "name",
            clone_value(required(profile, "name", "v5 temporal profile")?)?,
        ),
        (
            "description",
            clone_value(required(profile, "description", "v5 temporal profile")?)?,
        ),
        (
            "instruments",
            clone_value(required(profile, "instruments", "v5 temporal profile")?)?,
        ),
        (
            "directionMode",
            clone_value(required(profile, "directionMode", "v5 temporal profile")?)?,
        ),
        (
            "isActive",
            clone_value(required(profile, "isActive", "v5 temporal profile")?)?,
        ),
        ("indicators", array(indicators)),
        (
            "executionConfig",
            clone_value(required(profile, "executionConfig", "v5 temporal profile")?)?,
        ),
        ("graph", normalized_graph_payload(profile, false)?),
    ]))
}

fn normalized_program_payload(profile: &Value) -> Result<Value> {
    let graph = required(profile, "graph", "v5 temporal profile")?;
    let mut bound_ids = BTreeSet::new();
    for group in array_ref(
        required(graph, "evidenceGroups", "v5 temporal graph")?,
        "v5 profile evidence groups",
    )? {
        for identifier in array_ref(
            required(group, "indicatorInstanceIds", "v5 evidence group")?,
            "v5 evidence group members",
        )? {
            bound_ids.insert(text(identifier, "v5 evidence indicator")?);
        }
    }
    for event in array_ref(
        required(graph, "eventBindings", "v5 temporal graph")?,
        "v5 profile event bindings",
    )? {
        bound_ids.insert(text(
            required(event, "indicatorInstanceId", "v5 event binding")?,
            "v5 event indicator",
        )?);
    }
    if let Some(bindings) = field(
        required(profile, "executionConfig", "v5 temporal profile")?,
        "managementLibrary",
    )
    .and_then(|library| field(library, "scalarBindings"))
    .and_then(Value::as_array)
    {
        for binding in bindings {
            bound_ids.insert(text(
                required(binding, "indicatorInstanceId", "v5 scalar binding")?,
                "v5 scalar indicator",
            )?);
        }
    }
    let indicators = array_ref(
        required(profile, "indicators", "v5 temporal profile")?,
        "v5 profile indicators",
    )?;
    let mut bound = indicators
        .iter()
        .filter_map(|indicator| {
            indicator_id(indicator)
                .ok()
                .filter(|id| bound_ids.contains(id))
                .map(|_| clone_value(indicator))
        })
        .collect::<Result<Vec<_>>>()?;
    bound.sort_by_key(|indicator| indicator_id(indicator).unwrap_or_default());
    Ok(object([
        (
            "version",
            clone_value(required(profile, "version", "v5 temporal profile")?)?,
        ),
        (
            "instruments",
            clone_value(required(profile, "instruments", "v5 temporal profile")?)?,
        ),
        (
            "directionMode",
            clone_value(required(profile, "directionMode", "v5 temporal profile")?)?,
        ),
        ("indicators", array(bound)),
        (
            "executionConfig",
            normalized_execution_behavior_payload(profile)?,
        ),
        ("graph", normalized_graph_payload(profile, true)?),
    ]))
}

#[derive(Clone, Debug, PartialEq)]
pub struct V5NativeValidation {
    pub report: Value,
    pub raw_profile_sha256: String,
    pub profile_snapshot_sha256: String,
    pub program_sha256: String,
    pub validation_report_sha256: String,
}

fn collect_native_capabilities(
    guard: &Value,
    required_capabilities: &mut BTreeSet<String>,
) -> Result<()> {
    let fields = object_ref(guard, "v5 guard")?;
    let kind = text(
        fields
            .get("kind")
            .ok_or_else(|| invalid("v5 guard lacks kind"))?,
        "v5 guard kind",
    )?;
    match kind.as_str() {
        "evidence_at_least" | "evidence_below" => {
            required_capabilities.insert("fact.evidence_score".to_owned());
        }
        "position_exists"
        | "unrealized_r_at_least"
        | "unrealized_r_at_most"
        | "position_age_at_least" => {
            required_capabilities.insert("fact.position_exists".to_owned());
        }
        "state_age_at_least" | "state_age_at_most" => {
            required_capabilities.insert("fact.state_age".to_owned());
        }
        "execution_status_is" | "execution_reason_is" => {
            required_capabilities.insert("fact.execution_outcome".to_owned());
        }
        "fresh_event" | "event_age_at_most" | "event_age_window" => {
            required_capabilities.insert("fact.fresh_event".to_owned());
        }
        "all"
        | "any"
        | "not"
        | "always"
        | "utc_time_window"
        | "condition_streak_at_least"
        | "consecutive_true"
        | "predicate_edge"
        | "action_cooldown_elapsed" => {}
        _ => {
            return Err(invalid(format!(
                "v5 native validator does not admit guard kind {kind}"
            )));
        }
    }
    if let Some(children) = fields.get("guards") {
        for child in array_ref(children, "v5 boolean guard children")? {
            collect_native_capabilities(child, required_capabilities)?;
        }
    }
    if let Some(child) = fields.get("guard") {
        collect_native_capabilities(child, required_capabilities)?;
    }
    Ok(())
}

fn collect_action_capability(
    action: &Value,
    required_capabilities: &mut BTreeSet<String>,
) -> Result<()> {
    let kind = text(required(action, "kind", "v5 action")?, "v5 action kind")?;
    let capability = match kind.as_str() {
        "enter_next_open" => "trade.market_entry.next_open",
        "exit_next_open" => "trade.market_exit.next_open",
        "move_stop_to_break_even_next_open" => "stop.move_break_even.next_open",
        "tighten_stop_next_open" => "stop.tighten.next_open",
        // The broad evolved compiler has explicit goldens for these before it
        // opens the later-generation path.  Listing their native contracts
        // here keeps the validation boundary closed, rather than treating
        // unfamiliar actions as no-op metadata.
        "activate_trailing_stop_next_open" => "stop.trailing.activate.next_open",
        "deactivate_trailing_stop_next_open" => "stop.trailing.deactivate.next_open",
        "cancel_target_next_open" => "target.cancel.next_open",
        "set_target_next_open" => "target.set.next_open",
        _ => {
            return Err(invalid(format!(
                "v5 native validator does not admit action kind {kind}"
            )));
        }
    };
    required_capabilities.insert(capability.to_owned());
    Ok(())
}

/// Produce the exact search-validation envelope for the closed native profile
/// subset.  This is both an admission gate and the source of every frozen
/// module/pair identity; callers must not supply their own report hashes.
pub fn validate_native_profile(profile: &Value, candidate_id: &str) -> Result<V5NativeValidation> {
    let candidate_id = text(
        &Value::String(candidate_id.to_owned()),
        "v5 native candidate ID",
    )?;
    if !candidate_id
        .bytes()
        .all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_'))
        || !candidate_id.starts_with('q')
    {
        return Err(invalid(
            "v5 native candidate ID is not a lowercase identifier",
        ));
    }
    let raw_profile_sha256 = canonical_sha256(profile)?;
    let profile_snapshot_sha256 = canonical_sha256(&normalized_profile_snapshot_payload(profile)?)?;
    let program_sha256 = canonical_sha256(&normalized_program_payload(profile)?)?;
    let graph = required(profile, "graph", "v5 temporal profile")?;
    let mut required_capabilities = BTreeSet::new();
    required_capabilities.insert(text(
        required(graph, "clockRequirement", "v5 temporal graph")?,
        "v5 clock requirement",
    )?);
    let transitions = array_ref(
        required(graph, "transitions", "v5 temporal graph")?,
        "v5 temporal graph transitions",
    )?;
    let mut has_entry_action = false;
    for transition in transitions {
        collect_native_capabilities(
            required(transition, "guard", "v5 temporal transition")?,
            &mut required_capabilities,
        )?;
        for action in array_ref(
            required(transition, "actions", "v5 temporal transition")?,
            "v5 temporal transition actions",
        )? {
            if field(action, "kind").and_then(Value::as_str) == Some("enter_next_open") {
                has_entry_action = true;
            }
            collect_action_capability(action, &mut required_capabilities)?;
        }
    }
    let execution = object_ref(
        required(profile, "executionConfig", "v5 temporal profile")?,
        "v5 execution config",
    )?;
    let has_explicit_management = execution
        .get("managementLibrary")
        .is_some_and(Value::is_object);
    if has_explicit_management {
        required_capabilities.insert("management.plan.fixed".to_owned());
    }
    if !has_entry_action || !has_explicit_management {
        return Err(invalid(
            "v5 native profile does not satisfy search admission",
        ));
    }
    let fidelity_requirements = array_ref(
        required(graph, "fidelityRequirements", "v5 temporal graph")?,
        "v5 temporal graph fidelity requirements",
    )?
    .iter()
    .map(|value| text(value, "v5 fidelity requirement"))
    .collect::<Result<BTreeSet<_>>>()?;
    let evaluator_id = if field(profile, "version").and_then(Value::as_str) == Some("v3")
        && field(profile, "directionMode").and_then(Value::as_str) == Some("both")
    {
        "bar_bidirectional_single_position_execution_v2"
    } else {
        "bar_single_position_execution_v1"
    };
    let core_validation_report = object([
        (
            "schemaVersion",
            Value::String("temporal_validation_report_v1".to_owned()),
        ),
        ("status", Value::String("valid_evaluable".to_owned())),
        (
            "profileSnapshotSha256",
            Value::String(profile_snapshot_sha256.clone()),
        ),
        ("programSha256", Value::String(program_sha256.clone())),
        ("evaluatorId", Value::String(evaluator_id.to_owned())),
        (
            "requiredCapabilities",
            array(required_capabilities.iter().cloned().map(Value::String)),
        ),
        ("missingCapabilities", array(Vec::new())),
        (
            "fidelityRequirements",
            array(fidelity_requirements.iter().cloned().map(Value::String)),
        ),
        ("missingFidelityCapabilities", array(Vec::new())),
        ("issues", array(Vec::new())),
        ("scoreable", Value::Bool(false)),
    ]);
    let validation_report_sha256 = canonical_sha256(&object([
        ("coreValidationReport", core_validation_report),
        ("searchIssues", array(Vec::new())),
    ]))?;
    let report = object([
        (
            "schemaVersion",
            Value::String("temporal_search_candidate_validation_v1".to_owned()),
        ),
        ("candidateId", Value::String(candidate_id)),
        (
            "rawSourceProfileSha256",
            Value::String(raw_profile_sha256.clone()),
        ),
        (
            "profileSnapshotSha256",
            Value::String(profile_snapshot_sha256.clone()),
        ),
        ("programSha256", Value::String(program_sha256.clone())),
        ("evaluatorId", Value::String(evaluator_id.to_owned())),
        ("status", Value::String("valid_evaluable".to_owned())),
        ("scoreable", Value::Bool(false)),
        ("candidateAcceptable", Value::Bool(true)),
        ("hasEntryAction", Value::Bool(true)),
        ("hasExplicitManagementLibrary", Value::Bool(true)),
        (
            "requiredCapabilities",
            array(required_capabilities.into_iter().map(Value::String)),
        ),
        ("missingCapabilities", array(Vec::new())),
        (
            "fidelityRequirements",
            array(fidelity_requirements.into_iter().map(Value::String)),
        ),
        ("missingFidelityCapabilities", array(Vec::new())),
        ("issues", array(Vec::new())),
        (
            "validationReportSha256",
            Value::String(validation_report_sha256.clone()),
        ),
    ]);
    Ok(V5NativeValidation {
        report,
        raw_profile_sha256,
        profile_snapshot_sha256,
        program_sha256,
        validation_report_sha256,
    })
}

/// Version two is intentionally a schema break: it carries both the exact
/// historical/source operator identity and the separate native executable
/// operator closure.  Treating a newly derived native authority as if it were
/// the historical source implementation would silently change public v5
/// identities on restart.
const V5_SHARED_CONSTRUCTION_AUTHORITY_SCHEMA: &str =
    "temporal_qd_v5_shared_construction_authority_v2";
const V5_NATIVE_OPERATOR_AUTHORITY_SCHEMA: &str = "temporal_qd_v5_native_operator_authority_v1";

fn reject_mutable_authority_aliases(value: &Value, label: &str) -> Result<()> {
    match value {
        Value::Object(fields) => {
            for (key, child) in fields {
                if [
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
                ]
                .contains(&key.as_str())
                {
                    return Err(invalid(format!(
                        "{label} must embed frozen authority, not mutable alias {key}"
                    )));
                }
                reject_mutable_authority_aliases(child, label)?;
            }
        }
        Value::Array(values) => {
            for child in values {
                reject_mutable_authority_aliases(child, label)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
struct V5IdentitySnapshot {
    value: Value,
    sha256: String,
}

impl V5IdentitySnapshot {
    fn from_value(value: &Value, expected_kind: &str) -> Result<Self> {
        let fields = object_ref(value, "v5 identity snapshot")?;
        exact_value_keys(
            fields,
            &["kind", "schemaVersion", "payload", "sha256"],
            "v5 identity snapshot",
        )?;
        if fields.get("kind").and_then(Value::as_str) != Some(expected_kind) {
            return Err(invalid("v5 identity snapshot kind is incompatible"));
        }
        let _ = text(
            fields
                .get("schemaVersion")
                .ok_or_else(|| invalid("v5 identity snapshot lacks schema"))?,
            "v5 identity snapshot schema",
        )?;
        let payload = fields
            .get("payload")
            .ok_or_else(|| invalid("v5 identity snapshot lacks payload"))?;
        if !payload.is_object() {
            return Err(invalid("v5 identity snapshot payload must be an object"));
        }
        reject_mutable_authority_aliases(payload, "v5 identity snapshot")?;
        let supplied = sha256_text(
            fields
                .get("sha256")
                .ok_or_else(|| invalid("v5 identity snapshot lacks SHA-256"))?,
            "v5 identity snapshot SHA-256",
        )?;
        if supplied != canonical_sha256(payload)? {
            return Err(invalid("v5 identity snapshot payload identity drifted"));
        }
        Ok(Self {
            value: clone_value(value)?,
            sha256: supplied,
        })
    }

    fn payload(&self) -> &Value {
        self.value
            .get("payload")
            .expect("validated v5 identity snapshot has payload")
    }
}

#[derive(Clone, Debug, PartialEq)]
struct V5SideConstructionAuthority {
    grammar_context: V5IdentitySnapshot,
    catalog: V5IdentitySnapshot,
    /// Exact historical snapshot used by FrozenModule/Pairs and public
    /// candidate identities.  It intentionally remains the lean
    /// `evolvable_module_policy_v1` source representation.
    policy: V5IdentitySnapshot,
    native_authority: V5IdentitySnapshot,
    budget: Value,
    /// Full execution-only material, sealed in shared authority but excluded
    /// from historical/public identity material.
    module_policy: Value,
    indicator_policy: Value,
    seed_names: Vec<String>,
    resource_operator_spec_sha256: String,
}

/// The finite temporal guard domains admitted by the current v5 source
/// authority.  Rust never substitutes these values: they must be present,
/// self-authenticated, and exact in the sealed native authority before an
/// evolved-program operator can use them.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct V5TemporalDomains {
    pub(crate) temporal_domains_sha256: String,
    pub(crate) event_ages: Vec<u64>,
    pub(crate) position_ages: Vec<u64>,
    pub(crate) utc_session_windows: Vec<(u64, u64)>,
    pub(crate) event_age_windows: Vec<(u64, u64)>,
    pub(crate) consecutive_counts: Vec<u64>,
    pub(crate) cooldown_counts: Vec<u64>,
}

impl V5TemporalDomains {
    /// Re-emit the exact sealed object for an operator API which deliberately
    /// accepts only a typed/static-domain value, never an unbounded authority
    /// tree.  Recomputing the self hash here makes accidental in-memory drift
    /// fail closed before it reaches an operator plan.
    pub(crate) fn canonical_value(&self) -> Result<Value> {
        let windows = |values: &[(u64, u64)]| {
            array(
                values
                    .iter()
                    .map(|(start, end)| array([Value::from(*start), Value::from(*end)])),
            )
        };
        let material = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_v5_temporal_domains_v1".to_owned()),
            ),
            (
                "eventAges",
                array(self.event_ages.iter().copied().map(Value::from)),
            ),
            (
                "positionAges",
                array(self.position_ages.iter().copied().map(Value::from)),
            ),
            ("utcSessionWindows", windows(&self.utc_session_windows)),
            ("eventAgeWindows", windows(&self.event_age_windows)),
            (
                "consecutiveCounts",
                array(self.consecutive_counts.iter().copied().map(Value::from)),
            ),
            (
                "cooldownCounts",
                array(self.cooldown_counts.iter().copied().map(Value::from)),
            ),
        ]);
        if canonical_sha256(&material)? != self.temporal_domains_sha256 {
            return Err(invalid("validated v5 temporal domains mutated in memory"));
        }
        let mut output = material;
        output
            .as_object_mut()
            .expect("temporal domains object")
            .insert(
                "temporalDomainsSha256".to_owned(),
                Value::String(self.temporal_domains_sha256.clone()),
            );
        Ok(output)
    }
}

fn temporal_domain_integers(value: &Value, label: &str) -> Result<Vec<u64>> {
    array_ref(value, label)?
        .iter()
        .map(|item| {
            item.as_u64()
                .ok_or_else(|| invalid(format!("{label} must contain canonical unsigned integers")))
        })
        .collect()
}

fn strictly_increasing(values: &[u64], minimum: u64, maximum: u64, label: &str) -> Result<()> {
    if values.is_empty()
        || values
            .iter()
            .any(|value| *value < minimum || *value > maximum)
        || values.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(invalid(format!(
            "{label} must be nonempty sorted unique bounded integers"
        )));
    }
    Ok(())
}

fn unique_windows(values: &[(u64, u64)], label: &str) -> Result<()> {
    if values.is_empty() || values.iter().collect::<BTreeSet<_>>().len() != values.len() {
        return Err(invalid(format!(
            "{label} must be nonempty unique ordered intervals"
        )));
    }
    Ok(())
}

fn temporal_domain_windows(
    value: &Value,
    label: &str,
    maximum: u64,
    allow_utc_wrap: bool,
) -> Result<Vec<(u64, u64)>> {
    array_ref(value, label)?
        .iter()
        .map(|item| {
            let row = array_ref(item, label)?;
            if row.len() != 2 {
                return Err(invalid(format!(
                    "{label} rows must contain exactly two integers"
                )));
            }
            let start = row[0]
                .as_u64()
                .ok_or_else(|| invalid(format!("{label} start must be unsigned integer")))?;
            let end = row[1]
                .as_u64()
                .ok_or_else(|| invalid(format!("{label} end must be unsigned integer")))?;
            if start > maximum
                || end > maximum
                || (!allow_utc_wrap && start > end)
                || (allow_utc_wrap && start == end)
            {
                return Err(invalid(format!(
                    "{label} interval is outside the canonical domain"
                )));
            }
            Ok((start, end))
        })
        .collect()
}

fn validate_v5_temporal_domains(value: &Value) -> Result<V5TemporalDomains> {
    let fields = object_ref(value, "v5 temporal domains")?;
    exact_value_keys(
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
        "v5 temporal domains",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some("temporal_qd_v5_temporal_domains_v1")
    {
        return Err(invalid("v5 temporal domains schema is incompatible"));
    }
    let temporal_domains_sha256 = sha256_text(
        fields
            .get("temporalDomainsSha256")
            .ok_or_else(|| invalid("v5 temporal domains lacks identity"))?,
        "v5 temporal domains SHA-256",
    )?;
    let mut material = fields.clone();
    material.remove("temporalDomainsSha256");
    if temporal_domains_sha256 != canonical_sha256(&Value::Object(material))? {
        return Err(invalid("v5 temporal domains self identity drifted"));
    }
    let event_ages = temporal_domain_integers(
        fields
            .get("eventAges")
            .ok_or_else(|| invalid("v5 temporal domains lacks eventAges"))?,
        "v5 event ages",
    )?;
    let position_ages = temporal_domain_integers(
        fields
            .get("positionAges")
            .ok_or_else(|| invalid("v5 temporal domains lacks positionAges"))?,
        "v5 position ages",
    )?;
    let utc_session_windows = temporal_domain_windows(
        fields
            .get("utcSessionWindows")
            .ok_or_else(|| invalid("v5 temporal domains lacks utcSessionWindows"))?,
        "v5 UTC session windows",
        1439,
        true,
    )?;
    let event_age_windows = temporal_domain_windows(
        fields
            .get("eventAgeWindows")
            .ok_or_else(|| invalid("v5 temporal domains lacks eventAgeWindows"))?,
        "v5 event age windows",
        1_000_000,
        false,
    )?;
    let consecutive_counts = temporal_domain_integers(
        fields
            .get("consecutiveCounts")
            .ok_or_else(|| invalid("v5 temporal domains lacks consecutiveCounts"))?,
        "v5 consecutive counts",
    )?;
    let cooldown_counts = temporal_domain_integers(
        fields
            .get("cooldownCounts")
            .ok_or_else(|| invalid("v5 temporal domains lacks cooldownCounts"))?,
        "v5 cooldown counts",
    )?;
    // The grid comes from the sealed Python temporal-operator specification,
    // never a Rust fallback/default.  We validate only the finite value domain
    // here; the caller cross-binds this exact object to specification.domains.
    strictly_increasing(&event_ages, 0, 1_000_000, "v5 event ages")?;
    strictly_increasing(&position_ages, 1, 10_000_000, "v5 position ages")?;
    strictly_increasing(&consecutive_counts, 2, 1_000_000, "v5 consecutive counts")?;
    strictly_increasing(&cooldown_counts, 1, 1_000_000, "v5 cooldown counts")?;
    unique_windows(&utc_session_windows, "v5 UTC session windows")?;
    unique_windows(&event_age_windows, "v5 event age windows")?;
    Ok(V5TemporalDomains {
        temporal_domains_sha256,
        event_ages,
        position_ages,
        utc_session_windows,
        event_age_windows,
        consecutive_counts,
        cooldown_counts,
    })
}

fn temporal_domains_projection_from_spec(domains: &Value) -> Result<Value> {
    let fields = object_ref(domains, "v5 temporal operator specification domains")?;
    exact_value_keys(
        fields,
        &[
            "eventAges",
            "positionAges",
            "utcSessionWindows",
            "eventAgeWindows",
            "consecutiveCounts",
            "cooldownCounts",
        ],
        "v5 temporal operator specification domains",
    )?;
    let semantic = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_v5_temporal_domains_v1".to_owned()),
        ),
        (
            "eventAges",
            clone_value(required(
                domains,
                "eventAges",
                "v5 temporal operator specification domains",
            )?)?,
        ),
        (
            "positionAges",
            clone_value(required(
                domains,
                "positionAges",
                "v5 temporal operator specification domains",
            )?)?,
        ),
        (
            "utcSessionWindows",
            clone_value(required(
                domains,
                "utcSessionWindows",
                "v5 temporal operator specification domains",
            )?)?,
        ),
        (
            "eventAgeWindows",
            clone_value(required(
                domains,
                "eventAgeWindows",
                "v5 temporal operator specification domains",
            )?)?,
        ),
        (
            "consecutiveCounts",
            clone_value(required(
                domains,
                "consecutiveCounts",
                "v5 temporal operator specification domains",
            )?)?,
        ),
        (
            "cooldownCounts",
            clone_value(required(
                domains,
                "cooldownCounts",
                "v5 temporal operator specification domains",
            )?)?,
        ),
    ]);
    let mut output = semantic
        .as_object()
        .expect("constructed temporal domains projection")
        .clone();
    output.insert(
        "temporalDomainsSha256".to_owned(),
        Value::String(canonical_sha256(&semantic)?),
    );
    Ok(Value::Object(output))
}

fn validate_v5_temporal_operator_specification(
    value: &Value,
    expected_compiler_policy_sha256: &str,
    expected_temporal_domains: &Value,
) -> Result<String> {
    let fields = object_ref(value, "v5 temporal operator specification")?;
    exact_value_keys(
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
        "v5 temporal operator specification",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some("evolvable_module_temporal_operator_plan_v1")
        || fields.get("operatorVersion").and_then(Value::as_str)
            != Some("evolvable_module_temporal_operators_v1")
        || fields.get("nativeValidation") != Some(&Value::Bool(false))
        || exact_sha256_value(
            required(
                value,
                "compilerPolicySha256",
                "v5 temporal operator specification",
            )?,
            "v5 temporal operator specification compiler policy SHA-256",
        )? != expected_compiler_policy_sha256
    {
        return Err(invalid(
            "v5 temporal operator specification authority drifted",
        ));
    }
    let guard_families = array_ref(
        required(value, "guardFamilies", "v5 temporal operator specification")?,
        "v5 temporal operator specification guard families",
    )?;
    if guard_families.is_empty() {
        return Err(invalid(
            "v5 temporal operator specification has no guard families",
        ));
    }
    let mut seen = BTreeSet::<String>::new();
    for family in guard_families {
        let family = exact_text_value(family, "v5 temporal operator guard family")?;
        if family
            .bytes()
            .any(|byte| !matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_'))
            || !seen.insert(family)
        {
            return Err(invalid(
                "v5 temporal operator guard families must be unique machine identifiers",
            ));
        }
    }
    let projected = temporal_domains_projection_from_spec(required(
        value,
        "domains",
        "v5 temporal operator specification",
    )?)?;
    if &projected != expected_temporal_domains {
        return Err(invalid(
            "v5 temporal operator specification domains drifted from sealed temporal domains",
        ));
    }
    let mut semantic = fields.clone();
    let supplied = exact_sha256_value(
        semantic
            .remove("operatorSpecSha256")
            .as_ref()
            .ok_or_else(|| invalid("v5 temporal operator specification lacks identity"))?,
        "v5 temporal operator specification SHA-256",
    )?;
    if supplied != canonical_sha256(&Value::Object(semantic))? {
        return Err(invalid(
            "v5 temporal operator specification identity drifted",
        ));
    }
    Ok(supplied)
}

/// Validate the immutable indicator-learning policy exactly enough to make the
/// catalog identity executable rather than an opaque assertion.  The Python
/// authority writes this object by adding `policySha256` *after* hashing the
/// remaining fields, so the self hash deliberately excludes that one field.
///
/// This is intentionally kept beside the side-authority parser: the catalog
/// SHA is semantically `sha256({payload: catalog, timeframePolicy: ...})`,
/// not a hash of the catalog JSON alone.
fn validate_v5_indicator_policy(
    policy: &Value,
    catalog: &Value,
    expected_catalog_sha256: &str,
    label: &str,
) -> Result<Vec<String>> {
    let fields = object_ref(policy, label)?;
    exact_value_keys(
        fields,
        &[
            "schemaVersion",
            "learningVersion",
            "catalogSha256",
            "timeframePolicy",
            "evidenceLookbackChoices",
            "maxBoundFuzzyInstancesPerDirection",
            "maxEvidenceGroupMembers",
            "operatorIds",
            "policySha256",
        ],
        label,
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some("temporal_indicator_learning_policy_v1")
    {
        return Err(invalid(format!("{label} schema is incompatible")));
    }
    let _learning_version = text(
        fields
            .get("learningVersion")
            .ok_or_else(|| invalid(format!("{label} lacks learningVersion")))?,
        &format!("{label} learning version"),
    )?;
    let catalog_sha = sha256_text(
        fields
            .get("catalogSha256")
            .ok_or_else(|| invalid(format!("{label} lacks catalogSha256")))?,
        &format!("{label} catalog SHA-256"),
    )?;
    if catalog_sha != expected_catalog_sha256 {
        return Err(invalid(format!("{label} catalog binding drifted")));
    }
    let timeframes = array_ref(
        fields
            .get("timeframePolicy")
            .ok_or_else(|| invalid(format!("{label} lacks timeframePolicy")))?,
        &format!("{label} timeframe policy"),
    )?
    .iter()
    .map(|item| {
        let value = text(item, &format!("{label} timeframe"))?;
        if value != value.to_ascii_uppercase() {
            return Err(invalid(format!(
                "{label} timeframe is not canonical uppercase"
            )));
        }
        Ok(value)
    })
    .collect::<Result<Vec<_>>>()?;
    if timeframes.is_empty() || timeframes.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid(format!(
            "{label} timeframe policy must be nonempty sorted unique"
        )));
    }
    let catalog_fields = object_ref(catalog, &format!("{label} catalog"))?;
    let available_timeframes = object_ref(
        catalog_fields
            .get("timeframes")
            .ok_or_else(|| invalid(format!("{label} catalog lacks timeframes")))?,
        &format!("{label} catalog timeframes"),
    )?;
    if timeframes
        .iter()
        .any(|timeframe| !available_timeframes.contains_key(timeframe))
    {
        return Err(invalid(format!(
            "{label} timeframe policy is not catalog-backed"
        )));
    }
    let lookbacks = array_ref(
        fields
            .get("evidenceLookbackChoices")
            .ok_or_else(|| invalid(format!("{label} lacks evidenceLookbackChoices")))?,
        &format!("{label} evidence lookback choices"),
    )?;
    if lookbacks.is_empty()
        || lookbacks
            .iter()
            .any(|value| value.as_u64().is_none_or(|value| value == 0))
    {
        return Err(invalid(format!(
            "{label} evidence lookback choices are invalid"
        )));
    }
    for field_name in [
        "maxBoundFuzzyInstancesPerDirection",
        "maxEvidenceGroupMembers",
    ] {
        if fields
            .get(field_name)
            .and_then(Value::as_u64)
            .is_none_or(|value| value == 0)
        {
            return Err(invalid(format!(
                "{label} {field_name} must be a positive integer"
            )));
        }
    }
    let operator_ids = array_ref(
        fields
            .get("operatorIds")
            .ok_or_else(|| invalid(format!("{label} lacks operatorIds")))?,
        &format!("{label} operator IDs"),
    )?
    .iter()
    .map(|item| text(item, &format!("{label} operator ID")))
    .collect::<Result<Vec<_>>>()?;
    if operator_ids.is_empty()
        || operator_ids.iter().collect::<BTreeSet<_>>().len() != operator_ids.len()
    {
        return Err(invalid(format!(
            "{label} operator IDs must be nonempty unique"
        )));
    }
    let supplied_policy_sha = sha256_text(
        fields
            .get("policySha256")
            .ok_or_else(|| invalid(format!("{label} lacks policySha256")))?,
        &format!("{label} policy SHA-256"),
    )?;
    let mut self_hashed = fields.clone();
    self_hashed.remove("policySha256");
    if supplied_policy_sha != canonical_sha256(&Value::Object(self_hashed))? {
        return Err(invalid(format!("{label} self identity drifted")));
    }
    let semantic_catalog_sha = canonical_sha256(&object([
        ("payload", clone_value(catalog)?),
        (
            "timeframePolicy",
            array(timeframes.iter().cloned().map(Value::String)),
        ),
    ]))?;
    if semantic_catalog_sha != expected_catalog_sha256 {
        return Err(invalid(format!(
            "{label} semantic catalog identity drifted"
        )));
    }
    Ok(timeframes)
}

impl V5SideConstructionAuthority {
    fn from_value(value: &Value, expected_side: &str) -> Result<Self> {
        let fields = object_ref(value, "v5 side construction authority")?;
        exact_value_keys(
            fields,
            &[
                "grammarContext",
                "catalog",
                "policy",
                "nativeAuthority",
                "budget",
                "modulePolicy",
                "indicatorPolicy",
                "seedNames",
                "resourceOperatorSpecSha256",
            ],
            "v5 side construction authority",
        )?;
        let grammar_context = V5IdentitySnapshot::from_value(
            required(value, "grammarContext", "v5 side construction authority")?,
            "grammarContext",
        )?;
        let catalog = V5IdentitySnapshot::from_value(
            required(value, "catalog", "v5 side construction authority")?,
            "catalog",
        )?;
        let policy = V5IdentitySnapshot::from_value(
            required(value, "policy", "v5 side construction authority")?,
            "policy",
        )?;
        let native_authority = V5IdentitySnapshot::from_value(
            required(value, "nativeAuthority", "v5 side construction authority")?,
            "nativeAuthority",
        )?;
        let budget = budget_object(required(value, "budget", "v5 side construction authority")?)?;
        let context = required(
            grammar_context.payload(),
            "context",
            "v5 grammar context snapshot",
        )?;
        let grammar_payload = object_ref(
            grammar_context.payload(),
            "v5 grammar context snapshot payload",
        )?;
        exact_value_keys(
            grammar_payload,
            &["authoritySha256", "side", "context"],
            "v5 grammar context snapshot payload",
        )?;
        if grammar_context
            .value
            .get("schemaVersion")
            .and_then(Value::as_str)
            != Some("evolvable_module_context_v1")
        {
            return Err(invalid(
                "v5 grammar context snapshot schema is incompatible",
            ));
        }
        if grammar_payload.get("side").and_then(Value::as_str) != Some(expected_side)
            || !context.is_object()
        {
            return Err(invalid("v5 grammar context side drifted"));
        }
        let catalog_payload = object_ref(catalog.payload(), "v5 catalog snapshot payload")?;
        exact_value_keys(
            catalog_payload,
            &["catalog", "catalogSha256", "side"],
            "v5 catalog snapshot payload",
        )?;
        if catalog.value.get("schemaVersion").and_then(Value::as_str)
            != Some("evolvable_module_catalog_v1")
            || catalog_payload.get("side").and_then(Value::as_str) != Some(expected_side)
        {
            return Err(invalid(
                "v5 catalog snapshot schema or side is incompatible",
            ));
        }
        let catalog_sha = sha256_text(
            catalog_payload
                .get("catalogSha256")
                .ok_or_else(|| invalid("v5 catalog snapshot lacks catalog SHA-256"))?,
            "v5 catalog snapshot catalog SHA-256",
        )?;
        let policy_payload = object_ref(policy.payload(), "v5 module policy snapshot payload")?;
        exact_value_keys(
            policy_payload,
            &[
                "authoritySha256",
                "side",
                "budget",
                "compilerPolicySha256",
                "resourceOperatorSpecSha256",
            ],
            "v5 module policy snapshot payload",
        )?;
        if policy.value.get("schemaVersion").and_then(Value::as_str)
            != Some("evolvable_module_policy_v1")
            || policy_payload.get("side").and_then(Value::as_str) != Some(expected_side)
        {
            return Err(invalid("v5 module policy snapshot is incompatible"));
        }
        let module_policy = clone_value(required(
            value,
            "modulePolicy",
            "v5 side construction authority",
        )?)?;
        let indicator_policy = clone_value(required(
            value,
            "indicatorPolicy",
            "v5 side construction authority",
        )?)?;
        if !module_policy.is_object() || !indicator_policy.is_object() {
            return Err(invalid("v5 side execution policy must be an object"));
        }
        reject_mutable_authority_aliases(&module_policy, "v5 side module policy")?;
        reject_mutable_authority_aliases(&indicator_policy, "v5 side indicator policy")?;
        let _timeframe_policy = validate_v5_indicator_policy(
            &indicator_policy,
            catalog_payload
                .get("catalog")
                .ok_or_else(|| invalid("v5 catalog snapshot lacks catalog"))?,
            &catalog_sha,
            "v5 side indicator policy",
        )?;
        let seed_names = array_ref(
            required(value, "seedNames", "v5 side construction authority")?,
            "v5 side seed names",
        )?
        .iter()
        .map(|item| text(item, "v5 side seed name"))
        .collect::<Result<Vec<_>>>()?;
        if seed_names.is_empty() || seed_names.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(invalid("v5 side seed names must be nonempty sorted unique"));
        }
        let resource_operator_spec_sha256 = sha256_text(
            required(
                value,
                "resourceOperatorSpecSha256",
                "v5 side construction authority",
            )?,
            "v5 side resource operator spec SHA-256",
        )?;
        if resource_operator_spec_sha256
            != sha256_text(
                policy_payload
                    .get("resourceOperatorSpecSha256")
                    .ok_or_else(|| {
                        invalid("v5 module policy snapshot lacks resourceOperatorSpecSha256")
                    })?,
                "v5 module policy resource operator spec SHA-256",
            )?
        {
            return Err(invalid("v5 side resource operator spec drifted"));
        }
        Ok(Self {
            grammar_context,
            catalog,
            policy,
            native_authority,
            budget,
            module_policy,
            indicator_policy,
            seed_names,
            resource_operator_spec_sha256,
        })
    }

    fn context(&self) -> Result<&Value> {
        required(
            self.grammar_context.payload(),
            "context",
            "v5 grammar context snapshot",
        )
    }
}

/// Complete sealed authority needed to regenerate and verify a compact record
/// after the source config files are gone.  This deliberately retains content
/// rather than accepting a collection of opaque hashes.
#[derive(Clone, Debug, PartialEq)]
pub struct V5SharedConstructionAuthority {
    pub shared_authority_sha256: String,
    pub qd_engine_version: String,
    /// The immutable pair-source configuration, not the per-generation
    /// pair-generation config.  The latter is a transaction input and is
    /// deliberately kept out of reusable static authority.
    pub pair_run_config_sha256: String,
    pub factory_authority_sha256: String,
    pub pair_policy_sha256: String,
    pub compiler_policy_sha256: String,
    /// Canonical identity of the exact historical operator implementation.
    /// It participates in public/source lineage and must never be replaced by
    /// the native execution authority below.
    pub source_operator_implementation_sha256: String,
    /// Self identity of the v5 native operator closure.  It is allowed in
    /// compact runtime/receipt authority only, never in historical public
    /// candidate/module/pair identity material.
    pub native_operator_authority_sha256: String,
    evolvable_module_authority: Value,
    source_operator_implementation: Value,
    native_operator_authority: Value,
    temporal_domains: V5TemporalDomains,
    temporal_operator_specification: Value,
    temporal_operator_spec_sha256: String,
    grammar_registry: Value,
    hold_operator_policy: Value,
    initial_protection_operator_policy: Value,
    immigrant_construction_policy: Value,
    long: V5SideConstructionAuthority,
    short: V5SideConstructionAuthority,
    pair_compiler: V5IdentitySnapshot,
    pair_policy: Value,
}

/// A deliberately typed, already-validated view for the later-generation
/// operator engine.  It is the only route from the sealed construction
/// authority into `v5_operators`: that module must not recursively search a
/// generic authority JSON tree and accidentally select a shadowed policy.
///
/// All values are execution closure only.  In particular, native authority
/// material carried here never participates in preserved public FrozenModule,
/// FrozenPair, or candidate identities.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct V5OperatorSideAuthority {
    pub(crate) direction: String,
    pub(crate) grammar_context: Value,
    pub(crate) catalog: Value,
    /// Semantic IndicatorLearningCatalog identity: payload plus sealed
    /// timeframe policy, not merely a hash of `catalog` bytes.
    pub(crate) catalog_sha256: String,
    pub(crate) module_policy: Value,
    pub(crate) indicator_policy: Value,
    pub(crate) seed_names: Vec<String>,
    pub(crate) resource_operator_spec_sha256: String,
    pub(crate) budget: Value,
    // Exact historical snapshots remain available when the core later needs
    // to compile an accepted result into public identity material.  The
    // operator engine should treat these as opaque binding witnesses.
    pub(crate) grammar_context_snapshot: Value,
    pub(crate) catalog_snapshot: Value,
    pub(crate) policy_snapshot: Value,
    pub(crate) native_authority_snapshot: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct V5OperatorAuthorityProjection {
    /// Full sealed shared-authority identity.  Operator plans bind this
    /// transaction-level closure, not only the native sub-authority hash.
    pub(crate) shared_authority_sha256: String,
    pub(crate) pair_run_config_sha256: String,
    pub(crate) factory_authority_sha256: String,
    pub(crate) compiler_policy_sha256: String,
    pub(crate) source_operator_implementation_sha256: String,
    pub(crate) native_operator_authority_sha256: String,
    pub(crate) operator_registry: Value,
    pub(crate) budget: Value,
    pub(crate) temporal_domains: V5TemporalDomains,
    pub(crate) temporal_operator_specification: Value,
    pub(crate) temporal_operator_spec_sha256: String,
    pub(crate) grammar_registry: Value,
    pub(crate) hold_operator_policy: Value,
    pub(crate) initial_protection_operator_policy: Value,
    pub(crate) immigrant_construction_policy: Value,
    pub(crate) long: V5OperatorSideAuthority,
    pub(crate) short: V5OperatorSideAuthority,
}

impl V5SharedConstructionAuthority {
    pub fn from_shared_object(value: &Value) -> Result<Self> {
        let envelope = object_ref(value, "v5 shared authority object")?;
        exact_value_keys(
            envelope,
            &["schemaVersion", "authority", "authoritySha256"],
            "v5 shared authority object",
        )?;
        if envelope.get("schemaVersion").and_then(Value::as_str) != Some(V5_SHARED_AUTHORITY_SCHEMA)
        {
            return Err(invalid("v5 shared authority envelope schema is invalid"));
        }
        let authority = required(value, "authority", "v5 shared authority object")?;
        let shared_authority_sha256 = sha256_text(
            required(value, "authoritySha256", "v5 shared authority object")?,
            "v5 shared authority SHA-256",
        )?;
        if shared_authority_sha256 != canonical_sha256(authority)? {
            return Err(invalid("v5 shared authority envelope identity drifted"));
        }
        let fields = object_ref(authority, "v5 shared construction authority")?;
        exact_value_keys(
            fields,
            &[
                "schemaVersion",
                "qdEngineVersion",
                "pairRunConfigSha256",
                "evolvableModuleAuthority",
                "factoryAuthoritySha256",
                "bidirectionalPairPolicy",
                "pairPolicySha256",
                "compilerPolicySha256",
                "sourceOperatorImplementation",
                "sourceOperatorImplementationSha256",
                "nativeOperatorAuthority",
                "nativeOperatorAuthoritySha256",
                "temporalDomainsSha256",
                "temporalOperatorSpecSha256",
                "grammarRegistry",
                "holdOperatorPolicy",
                "initialProtectionOperatorPolicy",
                "immigrantConstructionPolicy",
                "long",
                "short",
                "pairCompilerAuthority",
            ],
            "v5 shared construction authority",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_SHARED_CONSTRUCTION_AUTHORITY_SCHEMA)
        {
            return Err(invalid(
                "v5 shared construction authority schema is invalid",
            ));
        }
        let qd_engine_version = text(
            required(
                authority,
                "qdEngineVersion",
                "v5 shared construction authority",
            )?,
            "v5 QD engine version",
        )?;
        let pair_run_config_sha256 = sha256_text(
            required(
                authority,
                "pairRunConfigSha256",
                "v5 shared construction authority",
            )?,
            "v5 pair run config SHA-256",
        )?;
        let factory_authority_sha256 = sha256_text(
            required(
                authority,
                "factoryAuthoritySha256",
                "v5 shared construction authority",
            )?,
            "v5 factory authority SHA-256",
        )?;
        let evolvable_module_authority = clone_value(required(
            authority,
            "evolvableModuleAuthority",
            "v5 shared construction authority",
        )?)?;
        let evolvable_fields =
            object_ref(&evolvable_module_authority, "v5 evolvable module authority")?;
        exact_value_keys_with_optional(
            evolvable_fields,
            &[
                "schemaVersion",
                "programKind",
                "codec",
                "pairRunConfigSha256",
                "catalogSha256",
                "compilerPolicy",
                "compilerPolicySha256",
                "budget",
                "capacityContract",
                "archivePolicyAuthority",
                "behaviorAttributionRequirement",
                "operatorRegistry",
                "authoritySha256",
            ],
            &["capacityReceipt"],
            "v5 evolvable module authority",
        )?;
        let mut authority_identity_material = evolvable_fields.clone();
        let supplied_factory_authority = sha256_text(
            authority_identity_material
                .remove("authoritySha256")
                .as_ref()
                .ok_or_else(|| invalid("v5 evolvable module authority lacks identity"))?,
            "v5 evolvable module authority SHA-256",
        )?;
        // Capacity is an admission witness rather than construction input;
        // this mirrors Python `_authority_config_sha256` exactly.
        authority_identity_material.remove("capacityReceipt");
        if supplied_factory_authority != factory_authority_sha256
            || factory_authority_sha256
                != canonical_sha256(&Value::Object(authority_identity_material))?
            || evolvable_fields
                .get("schemaVersion")
                .and_then(Value::as_str)
                != Some("temporal_qd_evolvable_module_authority_v1")
            || evolvable_fields.get("programKind").and_then(Value::as_str) != Some(V5_PROGRAM_KIND)
            || evolvable_fields.get("codec").and_then(Value::as_str) != Some(V5_CODEC)
            || evolvable_fields
                .get("pairRunConfigSha256")
                .and_then(Value::as_str)
                != Some(pair_run_config_sha256.as_str())
        {
            return Err(invalid(
                "v5 evolvable module authority identity or schema drifted",
            ));
        }
        let compiler_policy_sha256 = sha256_text(
            required(
                authority,
                "compilerPolicySha256",
                "v5 shared construction authority",
            )?,
            "v5 compiler policy SHA-256",
        )?;
        if compiler_policy_sha256
            != sha256_text(
                evolvable_fields
                    .get("compilerPolicySha256")
                    .ok_or_else(|| {
                        invalid("v5 evolvable authority lacks compiler policy identity")
                    })?,
                "v5 evolvable compiler policy SHA-256",
            )?
            || compiler_policy_sha256
                != canonical_sha256(
                    evolvable_fields
                        .get("compilerPolicy")
                        .ok_or_else(|| invalid("v5 evolvable authority lacks compiler policy"))?,
                )?
        {
            return Err(invalid("v5 compiler policy authority drifted"));
        }
        // Keep the historical/source operator implementation exact.  Stopped
        // v5 corpora use a legacy non-self-hashed v4 source implementation;
        // its canonical hash is a public lineage input.  Do not normalize it
        // into the new native execution object below.
        let source_operator_implementation = clone_value(required(
            authority,
            "sourceOperatorImplementation",
            "v5 shared construction authority",
        )?)?;
        reject_mutable_authority_aliases(
            &source_operator_implementation,
            "v5 source operator implementation",
        )?;
        let source_operator_fields = object_ref(
            &source_operator_implementation,
            "v5 source operator implementation",
        )?;
        let source_operator_implementation_sha256 = sha256_text(
            required(
                authority,
                "sourceOperatorImplementationSha256",
                "v5 shared construction authority",
            )?,
            "v5 source operator implementation SHA-256",
        )?;
        if source_operator_fields
            .get("schemaVersion")
            .and_then(Value::as_str)
            .filter(|schema| !schema.trim().is_empty())
            .is_none()
            || source_operator_implementation_sha256
                != canonical_sha256(&source_operator_implementation)?
        {
            return Err(invalid(
                "v5 source operator implementation identity drifted",
            ));
        }
        let grammar_registry = clone_value(required(
            authority,
            "grammarRegistry",
            "v5 shared construction authority",
        )?)?;
        let hold_operator_policy = clone_value(required(
            authority,
            "holdOperatorPolicy",
            "v5 shared construction authority",
        )?)?;
        let initial_protection_operator_policy = clone_value(required(
            authority,
            "initialProtectionOperatorPolicy",
            "v5 shared construction authority",
        )?)?;
        let immigrant_construction_policy = clone_value(required(
            authority,
            "immigrantConstructionPolicy",
            "v5 shared construction authority",
        )?)?;
        for (label, static_policy) in [
            ("v5 grammar registry", &grammar_registry),
            ("v5 hold operator policy", &hold_operator_policy),
            (
                "v5 initial protection operator policy",
                &initial_protection_operator_policy,
            ),
            (
                "v5 immigrant construction policy",
                &immigrant_construction_policy,
            ),
        ] {
            if !static_policy.is_object() {
                return Err(invalid(format!("{label} must be a complete object")));
            }
            reject_mutable_authority_aliases(static_policy, label)?;
        }
        let native_operator_authority = clone_value(required(
            authority,
            "nativeOperatorAuthority",
            "v5 shared construction authority",
        )?)?;
        let native_operator_authority_sha256 = sha256_text(
            required(
                authority,
                "nativeOperatorAuthoritySha256",
                "v5 shared construction authority",
            )?,
            "v5 native operator authority SHA-256",
        )?;
        let native_operator_fields =
            object_ref(&native_operator_authority, "v5 native operator authority")?;
        exact_value_keys(
            native_operator_fields,
            &[
                "schemaVersion",
                "sourceOperatorImplementationSha256",
                "factoryAuthoritySha256",
                "compilerPolicySha256",
                "programKind",
                "codec",
                "operatorRegistry",
                "budget",
                "grammarRegistry",
                "holdOperatorPolicy",
                "initialProtectionOperatorPolicy",
                "immigrantConstructionPolicy",
                "temporalDomains",
                "temporalOperatorSpecification",
                "nativeOperatorAuthoritySha256",
            ],
            "v5 native operator authority",
        )?;
        let mut native_operator_identity_material = native_operator_fields.clone();
        let supplied_native_operator_authority = sha256_text(
            native_operator_identity_material
                .remove("nativeOperatorAuthoritySha256")
                .as_ref()
                .ok_or_else(|| invalid("v5 native operator authority lacks identity"))?,
            "v5 native operator authority SHA-256",
        )?;
        let temporal_domains = validate_v5_temporal_domains(
            native_operator_fields
                .get("temporalDomains")
                .ok_or_else(|| invalid("v5 native operator authority lacks temporal domains"))?,
        )?;
        let temporal_operator_specification = clone_value(
            native_operator_fields
                .get("temporalOperatorSpecification")
                .ok_or_else(|| {
                    invalid("v5 native operator authority lacks temporal operator specification")
                })?,
        )?;
        let temporal_operator_spec_sha256 = validate_v5_temporal_operator_specification(
            &temporal_operator_specification,
            &compiler_policy_sha256,
            native_operator_fields
                .get("temporalDomains")
                .ok_or_else(|| invalid("v5 native operator authority lacks temporal domains"))?,
        )?;
        let outer_temporal_operator_spec_sha256 = exact_sha256_value(
            required(
                authority,
                "temporalOperatorSpecSha256",
                "v5 shared construction authority",
            )?,
            "v5 outer temporal operator specification SHA-256",
        )?;
        let outer_temporal_domains_sha256 = sha256_text(
            required(
                authority,
                "temporalDomainsSha256",
                "v5 shared construction authority",
            )?,
            "v5 outer temporal domains SHA-256",
        )?;
        if outer_temporal_domains_sha256 != temporal_domains.temporal_domains_sha256
            || outer_temporal_operator_spec_sha256 != temporal_operator_spec_sha256
        {
            return Err(invalid("v5 outer temporal domains binding drifted"));
        }
        if native_operator_fields
            .get("schemaVersion")
            .and_then(Value::as_str)
            != Some(V5_NATIVE_OPERATOR_AUTHORITY_SCHEMA)
            || supplied_native_operator_authority != native_operator_authority_sha256
            || native_operator_authority_sha256
                != canonical_sha256(&Value::Object(native_operator_identity_material))?
            || native_operator_fields
                .get("sourceOperatorImplementationSha256")
                .and_then(Value::as_str)
                != Some(source_operator_implementation_sha256.as_str())
            || native_operator_fields
                .get("factoryAuthoritySha256")
                .and_then(Value::as_str)
                != Some(factory_authority_sha256.as_str())
            || native_operator_fields
                .get("compilerPolicySha256")
                .and_then(Value::as_str)
                != Some(compiler_policy_sha256.as_str())
            || native_operator_fields
                .get("programKind")
                .and_then(Value::as_str)
                != Some(V5_PROGRAM_KIND)
            || native_operator_fields.get("codec").and_then(Value::as_str) != Some(V5_CODEC)
            || native_operator_fields.get("operatorRegistry")
                != evolvable_fields.get("operatorRegistry")
            || native_operator_fields.get("budget") != evolvable_fields.get("budget")
            || native_operator_fields.get("grammarRegistry") != Some(&grammar_registry)
            || native_operator_fields.get("holdOperatorPolicy") != Some(&hold_operator_policy)
            || native_operator_fields.get("initialProtectionOperatorPolicy")
                != Some(&initial_protection_operator_policy)
            || native_operator_fields.get("immigrantConstructionPolicy")
                != Some(&immigrant_construction_policy)
        {
            return Err(invalid("v5 native operator authority drifted"));
        }
        let pair_policy = clone_value(required(
            authority,
            "bidirectionalPairPolicy",
            "v5 shared construction authority",
        )?)?;
        let pair_policy_sha256 = sha256_text(
            required(
                authority,
                "pairPolicySha256",
                "v5 shared construction authority",
            )?,
            "v5 pair policy SHA-256",
        )?;
        let pair_policy_fields = object_ref(&pair_policy, "v5 pair policy")?;
        exact_value_keys(
            pair_policy_fields,
            &["schemaVersion", "enabled", "compilerAuthority"],
            "v5 pair policy",
        )?;
        if pair_policy_sha256 != canonical_sha256(&pair_policy)?
            || pair_policy_fields
                .get("schemaVersion")
                .and_then(Value::as_str)
                != Some("temporal_qd_bidirectional_pair_policy_v1")
            || pair_policy_fields.get("enabled") != Some(&Value::Bool(true))
        {
            return Err(invalid("v5 pair policy identity drifted"));
        }
        let long = V5SideConstructionAuthority::from_value(
            required(authority, "long", "v5 shared construction authority")?,
            "long",
        )?;
        let short = V5SideConstructionAuthority::from_value(
            required(authority, "short", "v5 shared construction authority")?,
            "short",
        )?;
        if long.budget
            != *evolvable_fields
                .get("budget")
                .ok_or_else(|| invalid("v5 evolvable authority lacks budget"))?
            || short.budget
                != *evolvable_fields
                    .get("budget")
                    .ok_or_else(|| invalid("v5 evolvable authority lacks budget"))?
        {
            return Err(invalid("v5 sealed budget authority drifted"));
        }
        for (side_name, side_authority) in [("long", &long), ("short", &short)] {
            let catalog_payload = side_authority.catalog.payload();
            let catalog_sha = sha256_text(
                required(catalog_payload, "catalogSha256", "v5 catalog snapshot")?,
                "v5 catalog snapshot SHA-256",
            )?;
            let policy_payload = side_authority.policy.payload();
            if catalog_sha
                != sha256_text(
                    evolvable_fields
                        .get("catalogSha256")
                        .ok_or_else(|| invalid("v5 evolvable authority lacks catalog SHA-256"))?,
                    "v5 evolvable catalog SHA-256",
                )?
                || sha256_text(
                    required(
                        policy_payload,
                        "authoritySha256",
                        "v5 module policy snapshot",
                    )?,
                    "v5 policy factory authority SHA-256",
                )? != factory_authority_sha256
                || required(policy_payload, "budget", "v5 module policy snapshot")?
                    != evolvable_fields
                        .get("budget")
                        .ok_or_else(|| invalid("v5 evolvable authority lacks budget"))?
                || sha256_text(
                    required(
                        policy_payload,
                        "compilerPolicySha256",
                        "v5 module policy snapshot",
                    )?,
                    "v5 policy compiler SHA-256",
                )? != compiler_policy_sha256
                || side_authority
                    .grammar_context
                    .payload()
                    .get("side")
                    .and_then(Value::as_str)
                    != Some(side_name)
            {
                return Err(invalid("v5 per-side sealed authority drifted"));
            }
        }
        let pair_compiler = V5IdentitySnapshot::from_value(
            required(
                authority,
                "pairCompilerAuthority",
                "v5 shared construction authority",
            )?,
            "pairCompiler",
        )?;
        let policy_compiler = required(&pair_policy, "compilerAuthority", "v5 pair policy")?;
        if policy_compiler != &pair_compiler.value
            || pair_compiler.sha256
                != pair_policy
                    .get("compilerAuthority")
                    .and_then(|value| value.get("sha256"))
                    .and_then(Value::as_str)
                    .unwrap_or_default()
        {
            return Err(invalid("v5 pair policy compiler authority drifted"));
        }
        for side in [&long, &short] {
            let embedded = required(
                side.grammar_context.payload(),
                "authoritySha256",
                "v5 grammar context snapshot",
            )?;
            if sha256_text(embedded, "v5 embedded factory authority SHA-256")?
                != factory_authority_sha256
            {
                return Err(invalid("v5 grammar context factory authority drifted"));
            }
        }
        Ok(Self {
            shared_authority_sha256,
            qd_engine_version,
            pair_run_config_sha256,
            factory_authority_sha256,
            pair_policy_sha256,
            compiler_policy_sha256,
            source_operator_implementation_sha256,
            native_operator_authority_sha256,
            evolvable_module_authority,
            source_operator_implementation,
            native_operator_authority,
            temporal_domains,
            temporal_operator_specification,
            temporal_operator_spec_sha256,
            grammar_registry,
            hold_operator_policy,
            initial_protection_operator_policy,
            immigrant_construction_policy,
            long,
            short,
            pair_compiler,
            pair_policy,
        })
    }

    fn side(&self, side: &str) -> Result<&V5SideConstructionAuthority> {
        match exact_side(side)? {
            "long" => Ok(&self.long),
            "short" => Ok(&self.short),
            _ => unreachable!("exact_side only returns two sides"),
        }
    }

    /// Return a non-heuristic closed authority for Rust operator planning and
    /// application.  The parser above has already cross-bound every field;
    /// cloning here deliberately prevents an operator from mutating the
    /// authority retained by a restart/admission transaction.
    pub(crate) fn operator_authority_projection(&self) -> Result<V5OperatorAuthorityProjection> {
        let side = |direction: &str,
                    authority: &V5SideConstructionAuthority|
         -> Result<V5OperatorSideAuthority> {
            let catalog_payload = authority.catalog.payload();
            Ok(V5OperatorSideAuthority {
                direction: direction.to_owned(),
                grammar_context: clone_value(authority.context()?)?,
                catalog: clone_value(required(catalog_payload, "catalog", "v5 catalog snapshot")?)?,
                catalog_sha256: sha256_text(
                    required(catalog_payload, "catalogSha256", "v5 catalog snapshot")?,
                    "v5 catalog snapshot SHA-256",
                )?,
                module_policy: clone_value(&authority.module_policy)?,
                indicator_policy: clone_value(&authority.indicator_policy)?,
                seed_names: authority.seed_names.clone(),
                resource_operator_spec_sha256: authority.resource_operator_spec_sha256.clone(),
                budget: clone_value(&authority.budget)?,
                grammar_context_snapshot: clone_value(&authority.grammar_context.value)?,
                catalog_snapshot: clone_value(&authority.catalog.value)?,
                policy_snapshot: clone_value(&authority.policy.value)?,
                native_authority_snapshot: clone_value(&authority.native_authority.value)?,
            })
        };
        let operator_registry = clone_value(
            self.native_operator_authority
                .get("operatorRegistry")
                .ok_or_else(|| invalid("validated native operator authority lacks registry"))?,
        )?;
        let budget = clone_value(
            self.native_operator_authority
                .get("budget")
                .ok_or_else(|| invalid("validated native operator authority lacks budget"))?,
        )?;
        Ok(V5OperatorAuthorityProjection {
            shared_authority_sha256: self.shared_authority_sha256.clone(),
            pair_run_config_sha256: self.pair_run_config_sha256.clone(),
            factory_authority_sha256: self.factory_authority_sha256.clone(),
            compiler_policy_sha256: self.compiler_policy_sha256.clone(),
            source_operator_implementation_sha256: self
                .source_operator_implementation_sha256
                .clone(),
            native_operator_authority_sha256: self.native_operator_authority_sha256.clone(),
            operator_registry,
            budget,
            temporal_domains: self.temporal_domains.clone(),
            temporal_operator_specification: clone_value(&self.temporal_operator_specification)?,
            temporal_operator_spec_sha256: self.temporal_operator_spec_sha256.clone(),
            grammar_registry: clone_value(&self.grammar_registry)?,
            hold_operator_policy: clone_value(&self.hold_operator_policy)?,
            initial_protection_operator_policy: clone_value(
                &self.initial_protection_operator_policy,
            )?,
            immigrant_construction_policy: clone_value(&self.immigrant_construction_policy)?,
            long: side("long", &self.long)?,
            short: side("short", &self.short)?,
        })
    }
}

impl V5OperatorAuthorityProjection {
    fn side(&self, direction: &str) -> Result<&V5OperatorSideAuthority> {
        match exact_side(direction)? {
            "long" => Ok(&self.long),
            "short" => Ok(&self.short),
            _ => unreachable!("exact_side only returns two sides"),
        }
    }

    /// Build one typed operator authority from the already-sealed core
    /// projection.  Transactions create/cache this once per side and share it
    /// across proposals; this helper is deliberately not a per-candidate JSON
    /// parser.
    pub(crate) fn operator_authority(
        &self,
        direction: &str,
    ) -> Result<crate::v5_operators::V5OperatorAuthority> {
        let side = self.side(direction)?;
        // `temporal-qd-batch` admits exactly one manifest and then exits, so
        // this process-global cache is generation scoped in production. The
        // full shared-authority hash prevents cross-authority reuse in test
        // processes that intentionally execute several transactions.
        type OperatorAuthorityCache =
            Mutex<BTreeMap<(String, String), crate::v5_operators::V5OperatorAuthority>>;
        static CACHE: OnceLock<OperatorAuthorityCache> = OnceLock::new();
        let cache_key = (self.shared_authority_sha256.clone(), side.direction.clone());
        let mut cache = CACHE
            .get_or_init(|| Mutex::new(BTreeMap::new()))
            .lock()
            .map_err(|_| invalid("v5 operator authority cache lock was poisoned"))?;
        if let Some(authority) = cache.get(&cache_key) {
            return Ok(authority.clone());
        }
        let instrument = text(
            required(
                &side.grammar_context,
                "instrument",
                "v5 operator side grammar context",
            )?,
            "v5 operator side instrument",
        )?;
        let temporal_domains = self.temporal_domains.canonical_value()?;
        let authority = crate::v5_operators::V5OperatorAuthority::from_sealed_static_parts(
            &self.shared_authority_sha256,
            &side.direction,
            &instrument,
            &side.budget,
            &side.catalog,
            &side.indicator_policy,
            &self.hold_operator_policy,
            &self.initial_protection_operator_policy,
            &temporal_domains,
        )
        .map_err(|error| {
            invalid(format!(
                "v5 operator authority construction failed: {error}"
            ))
        })?;
        let authority = authority
            .with_legacy_selection_static(
                &side.catalog_sha256,
                &side.resource_operator_spec_sha256,
                &self.compiler_policy_sha256,
                &self.temporal_operator_specification,
                &self.temporal_operator_spec_sha256,
            )
            .map_err(|error| {
                invalid(format!(
                    "v5 operator legacy selection authority failed: {error}"
                ))
            })?;
        cache.insert(cache_key, authority.clone());
        Ok(authority)
    }
}

/// Native compiled profile facts for a validated evolvable program.  The
/// genome SHA is intentionally explicit and distinct from
/// `native_program_sha256`, which identifies the Dashboard-normalized
/// executable form used by the validation report.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct V5CompiledEvolvableProfile {
    pub(crate) genome_program_sha256: String,
    pub(crate) profile: Value,
    pub(crate) raw_profile_sha256: String,
    pub(crate) profile_snapshot_sha256: String,
    pub(crate) native_program_sha256: String,
    pub(crate) native_validation_report_sha256: String,
    pub(crate) native_validation_report: Value,
}

/// Compile one authority-validated v5 program entirely in Rust.  This is the
/// later-generation compiler seam: it validates the exact sealed side first,
/// then uses the same lowering as G0, and finally derives the normalized
/// native report that binds the supplied candidate ID.  No Python validator,
/// JSONL bridge, or ambient catalog is involved.
pub(crate) fn compile_v5_module_profile(
    program: &Value,
    projection: &V5OperatorAuthorityProjection,
    direction: &str,
    candidate_id: &str,
) -> Result<V5CompiledEvolvableProfile> {
    let direction = exact_side(direction)?;
    let sealed_side = projection.side(direction)?;
    if text(
        required(program, "direction", "v5 evolvable program")?,
        "v5 evolvable program direction",
    )? != direction
    {
        return Err(invalid(
            "v5 evolved program direction does not match selected authority side",
        ));
    }
    let instrument = text(
        required(program, "instrument", "v5 evolvable program")?,
        "v5 evolvable program instrument",
    )?;
    let sealed_instrument = text(
        required(
            &sealed_side.grammar_context,
            "instrument",
            "v5 operator side grammar context",
        )?,
        "v5 operator side instrument",
    )?;
    if instrument != sealed_instrument {
        return Err(invalid(
            "v5 evolved program instrument does not match sealed side authority",
        ));
    }
    let operator_authority = projection.operator_authority(direction)?;
    crate::v5_operators::validate_program(program, &operator_authority)
        .map_err(|error| invalid(format!("v5 evolved program admission failed: {error}")))?;
    let profile = compile_v5_profile_body(program)?;
    let validation = validate_native_profile(&profile, candidate_id)?;
    Ok(V5CompiledEvolvableProfile {
        genome_program_sha256: canonical_sha256(program)?,
        profile,
        raw_profile_sha256: validation.raw_profile_sha256.clone(),
        profile_snapshot_sha256: validation.profile_snapshot_sha256.clone(),
        native_program_sha256: validation.program_sha256.clone(),
        native_validation_report_sha256: validation.validation_report_sha256.clone(),
        native_validation_report: validation.report,
    })
}

fn v5_proposal_side(proposal_seed: &str) -> Result<&'static str> {
    let digest = canonical_sha256(&object([
        ("schemaVersion", Value::String(V5_GENOME_SCHEMA.to_owned())),
        ("proposalSeed", Value::String(proposal_seed.to_owned())),
    ]))?;
    let terminal = digest
        .as_bytes()
        .last()
        .copied()
        .ok_or_else(|| invalid("v5 proposal-side identity is empty"))?;
    let nibble = match terminal {
        b'0'..=b'9' => terminal - b'0',
        b'a'..=b'f' => 10 + terminal - b'a',
        _ => return Err(invalid("v5 proposal-side identity is malformed")),
    };
    Ok(if nibble % 2 == 0 { "long" } else { "short" })
}

/// Render the closed topology-signature value vocabulary exactly as CPython's
/// `repr` renders the tuples used by `EvolvableModuleGenomeV1`.
///
/// Python deliberately uses `key=repr` for guard children and one-step edge
/// neighborhoods.  JSON canonical ordering is not equivalent once an
/// admitted guard contains nested tuples, `None`, or strings that need
/// escaping, so keep this small compatibility renderer separate from the
/// canonical JSON serializer used for the eventual SHA-256 material.
fn python_topology_string_repr(value: &str) -> String {
    // CPython prefers a double quoted literal only when that avoids escaping a
    // single quote.  All current closed identifiers are ASCII, but preserve
    // the actual `repr` behavior for control characters too so a malformed
    // future authority cannot silently reorder a topology identity.
    let quote = if value.contains('\'') && !value.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut rendered = String::new();
    rendered.push(quote);
    for character in value.chars() {
        match character {
            '\\' => rendered.push_str("\\\\"),
            '\n' => rendered.push_str("\\n"),
            '\r' => rendered.push_str("\\r"),
            '\t' => rendered.push_str("\\t"),
            '\x08' => rendered.push_str("\\x08"),
            '\x0c' => rendered.push_str("\\x0c"),
            '\'' if quote == '\'' => rendered.push_str("\\'"),
            '"' if quote == '"' => rendered.push_str("\\\""),
            control if control.is_control() => {
                let scalar = control as u32;
                if scalar <= 0xff {
                    rendered.push_str(&format!("\\x{scalar:02x}"));
                } else if scalar <= 0xffff {
                    rendered.push_str(&format!("\\u{scalar:04x}"));
                } else {
                    rendered.push_str(&format!("\\U{scalar:08x}"));
                }
            }
            ordinary => rendered.push(ordinary),
        }
    }
    rendered.push(quote);
    rendered
}

fn python_topology_repr(value: &Value) -> Result<String> {
    match value {
        Value::Null => Ok("None".to_owned()),
        Value::Bool(value) => Ok(if *value { "True" } else { "False" }.to_owned()),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(value.to_string())
            } else if let Some(value) = value.as_u64() {
                Ok(value.to_string())
            } else {
                // Closed topology priorities are integral.  Retain an exact
                // textual spelling only for a future explicitly admitted
                // floating grammar surface rather than normalizing it here.
                Err(invalid(
                    "v5 topology repr does not admit non-integral numbers",
                ))
            }
        }
        Value::String(value) => Ok(python_topology_string_repr(value)),
        Value::Array(values) => {
            let rendered = values
                .iter()
                .map(python_topology_repr)
                .collect::<Result<Vec<_>>>()?;
            Ok(match rendered.as_slice() {
                [] => "()".to_owned(),
                [only] => format!("({only},)"),
                _ => format!("({})", rendered.join(", ")),
            })
        }
        Value::Object(_) => Err(invalid("v5 topology repr does not admit object values")),
    }
}

fn sort_python_repr_values(values: &mut Vec<Value>) -> Result<()> {
    let mut keyed = values
        .drain(..)
        .map(|value| Ok((python_topology_repr(&value)?, value)))
        .collect::<Result<Vec<_>>>()?;
    keyed.sort_by(|left, right| left.0.cmp(&right.0));
    values.extend(keyed.into_iter().map(|(_, value)| value));
    Ok(())
}

/// Python's final topology edge list uses ordinary tuple ordering, unlike the
/// neighborhood list above which uses `key=repr`.  The closed vocabulary is
/// intentionally narrow; cross-type comparison is rejected because CPython
/// would raise instead of inventing an ordering.
fn python_topology_value_cmp(left: &Value, right: &Value) -> Result<std::cmp::Ordering> {
    use std::cmp::Ordering;
    match (left, right) {
        (Value::Null, Value::Null) => Ok(Ordering::Equal),
        (Value::Bool(left), Value::Bool(right)) => Ok(left.cmp(right)),
        (Value::Number(left), Value::Number(right)) => {
            let left = left
                .as_i64()
                .ok_or_else(|| invalid("v5 topology sort requires integral left number"))?;
            let right = right
                .as_i64()
                .ok_or_else(|| invalid("v5 topology sort requires integral right number"))?;
            Ok(left.cmp(&right))
        }
        (Value::String(left), Value::String(right)) => Ok(left.cmp(right)),
        (Value::Array(left), Value::Array(right)) => {
            for (left, right) in left.iter().zip(right) {
                let ordering = python_topology_value_cmp(left, right)?;
                if ordering != Ordering::Equal {
                    return Ok(ordering);
                }
            }
            Ok(left.len().cmp(&right.len()))
        }
        _ => Err(invalid(
            "v5 topology sort encountered Python-incomparable values",
        )),
    }
}

fn sort_python_tuple_values(values: &mut [Value]) -> Result<()> {
    // `slice::sort_by` cannot surface a fallible comparator.  The compact
    // candidate graphs are bounded, so a stable insertion sort is clearer and
    // lets us fail closed on a Python-incomparable row.
    for index in 1..values.len() {
        let pending = values[index].clone();
        let mut cursor = index;
        while cursor > 0
            && python_topology_value_cmp(&values[cursor - 1], &pending)?
                == std::cmp::Ordering::Greater
        {
            values[cursor] = values[cursor - 1].clone();
            cursor -= 1;
        }
        values[cursor] = pending;
    }
    Ok(())
}

fn v5_guard_shape(value: &Value) -> Result<Value> {
    let fields = object_ref(value, "v5 topology guard")?;
    let kind = fields
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    match kind.as_str() {
        "all" | "any" => {
            let mut children = array_ref(
                fields
                    .get("guards")
                    .ok_or_else(|| invalid("v5 topology boolean guard lacks children"))?,
                "v5 topology boolean guard children",
            )?
            .iter()
            .map(v5_guard_shape)
            .collect::<Result<Vec<_>>>()?;
            sort_python_repr_values(&mut children)?;
            Ok(array([Value::String(kind), array(children)]))
        }
        "predicate_edge" | "consecutive_true" => Ok(array([
            Value::String(kind),
            v5_guard_shape(
                fields
                    .get("predicate")
                    .ok_or_else(|| invalid("v5 topology predicate guard lacks predicate"))?,
            )?,
        ])),
        _ => Ok(Value::String(kind)),
    }
}

fn v5_semantic_topology_sha256(program: &Value) -> Result<String> {
    let nodes = array_ref(
        required(program, "nodes", "v5 topology program")?,
        "v5 topology nodes",
    )?;
    let edges = array_ref(
        required(program, "edges", "v5 topology program")?,
        "v5 topology edges",
    )?;
    let mut node_rows = BTreeMap::new();
    for node in nodes {
        let id = row_id(node, "v5 topology node")?;
        if node_rows.insert(id, node).is_some() {
            return Err(invalid("v5 topology nodes duplicate IDs"));
        }
    }
    if node_rows.is_empty() {
        return Err(invalid("v5 topology has no nodes"));
    }
    let mut labels = BTreeMap::new();
    for (id, node) in &node_rows {
        let resources = array_ref(
            required(node, "resources", "v5 topology node")?,
            "v5 topology node resources",
        )?;
        // Resource-use ordering is transport/authored representation, not a
        // topology property.  Preserve the multiplicity of each resource
        // kind, but sort the kind list so an equivalent program with e.g.
        // EVENT/EVIDENCE_GROUP reversed cannot split the semantic topology
        // identity (the historical d166 drift came from exactly this leak).
        let mut kinds = resources
            .iter()
            .map(|resource| {
                text(
                    required(resource, "kind", "v5 topology resource")?,
                    "v5 topology resource kind",
                )
            })
            .collect::<Result<Vec<_>>>()?;
        kinds.sort();
        let timeout = !matches!(
            required(node, "timeoutBars", "v5 topology node")?,
            Value::Null
        );
        labels.insert(
            id.clone(),
            canonical_sha256(&object([
                (
                    "zone",
                    clone_value(required(node, "zone", "v5 topology node")?)?,
                ),
                (
                    "kind",
                    clone_value(required(node, "kind", "v5 topology node")?)?,
                ),
                (
                    "guard",
                    v5_guard_shape(required(node, "guard", "v5 topology node")?)?,
                ),
                ("resources", array(kinds.into_iter().map(Value::String))),
                ("timeoutBars", Value::Bool(timeout)),
            ]))?,
        );
    }
    for _ in 0..nodes.len().max(1) {
        let mut updated = BTreeMap::new();
        for (id, label) in &labels {
            let mut outgoing = Vec::new();
            let mut incoming = Vec::new();
            for edge in edges {
                let source = text(
                    required(edge, "source", "v5 topology edge")?,
                    "v5 topology edge source",
                )?;
                let target = text(
                    required(edge, "target", "v5 topology edge")?,
                    "v5 topology edge target",
                )?;
                if !labels.contains_key(&source) || !labels.contains_key(&target) {
                    return Err(invalid("v5 topology edge is dangling"));
                }
                let row = array([
                    clone_value(required(edge, "eventClass", "v5 topology edge")?)?,
                    clone_value(required(edge, "priority", "v5 topology edge")?)?,
                    clone_value(required(edge, "effect", "v5 topology edge")?)?,
                    v5_guard_shape(required(edge, "guard", "v5 topology edge")?)?,
                    Value::String(
                        labels
                            .get(if source == *id { &target } else { &source })
                            .expect("checked topology endpoint")
                            .clone(),
                    ),
                ]);
                if source == *id {
                    outgoing.push(row.clone());
                }
                if target == *id {
                    incoming.push(row);
                }
            }
            sort_python_repr_values(&mut outgoing)?;
            sort_python_repr_values(&mut incoming)?;
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
        let source = text(
            required(edge, "source", "v5 topology edge")?,
            "v5 topology edge source",
        )?;
        let target = text(
            required(edge, "target", "v5 topology edge")?,
            "v5 topology edge target",
        )?;
        final_edges.push(array([
            Value::String(
                labels
                    .get(&source)
                    .ok_or_else(|| invalid("v5 topology source disappeared"))?
                    .clone(),
            ),
            Value::String(
                labels
                    .get(&target)
                    .ok_or_else(|| invalid("v5 topology target disappeared"))?
                    .clone(),
            ),
            clone_value(required(edge, "eventClass", "v5 topology edge")?)?,
            clone_value(required(edge, "priority", "v5 topology edge")?)?,
            clone_value(required(edge, "effect", "v5 topology edge")?)?,
            v5_guard_shape(required(edge, "guard", "v5 topology edge")?)?,
        ]));
    }
    sort_python_tuple_values(&mut final_edges)?;
    let mut final_nodes = labels.into_values().map(Value::String).collect::<Vec<_>>();
    // Python's `sorted(labels.values())` is a lexical SHA ordering, not node
    // identifier order.  The latter leaks generated IDs into an ostensibly
    // ID-independent topology identity.
    final_nodes.sort_by(|left, right| {
        left.as_str()
            .unwrap_or_default()
            .cmp(right.as_str().unwrap_or_default())
    });
    Ok(canonical_sha256(&object([
        (
            "schemaVersion",
            Value::String("evolvable_module_semantic_topology_v1".to_owned()),
        ),
        ("nodes", array(final_nodes)),
        ("edges", array(final_edges)),
    ]))?)
}

fn v5_resource_fingerprint_sha256(program: &Value) -> Result<String> {
    let resources = required(program, "resources", "v5 resource fingerprint program")?;
    let indicators = array_ref(
        required(resources, "indicators", "v5 resource fingerprint")?,
        "v5 resource fingerprint indicators",
    )?;
    let groups = array_ref(
        required(resources, "evidenceGroups", "v5 resource fingerprint")?,
        "v5 resource fingerprint groups",
    )?;
    let events = array_ref(
        required(resources, "events", "v5 resource fingerprint")?,
        "v5 resource fingerprint events",
    )?;
    let edges = array_ref(
        required(program, "edges", "v5 resource fingerprint program")?,
        "v5 resource fingerprint edges",
    )?;
    let indicator_values = indicators
        .iter()
        .map(|indicator| {
            Ok(array([
                clone_value(required(
                    required(indicator, "meta", "v5 indicator")?,
                    "id",
                    "v5 indicator meta",
                )?)?,
                clone_value(required(
                    required(indicator, "config", "v5 indicator")?,
                    "timeframe",
                    "v5 indicator config",
                )?)?,
            ]))
        })
        .collect::<Result<Vec<_>>>()?;
    let group_values = groups
        .iter()
        .map(|group| {
            clone_value(required(
                group,
                "indicatorInstanceIds",
                "v5 evidence group",
            )?)
        })
        .collect::<Result<Vec<_>>>()?;
    let event_values = events
        .iter()
        .map(|event| clone_value(required(event, "indicatorInstanceId", "v5 event")?))
        .collect::<Result<Vec<_>>>()?;
    let management_effects = [
        "move_stop_to_break_even_next_open",
        "tighten_stop_next_open",
        "activate_trailing_stop_next_open",
        "deactivate_trailing_stop_next_open",
        "set_target_next_open",
        "cancel_target_next_open",
    ];
    let management = edges
        .iter()
        .filter_map(|edge| field(edge, "effect").and_then(Value::as_str))
        .filter(|effect| management_effects.contains(effect))
        .map(|effect| Value::String(effect.to_owned()))
        .collect::<Vec<_>>();
    let exits = edges
        .iter()
        .filter(|edge| field(edge, "effect").and_then(Value::as_str) == Some("exit_next_open"))
        .count() as u64;
    Ok(canonical_sha256(&object([
        ("indicators", array(indicator_values)),
        ("groups", array(group_values)),
        ("events", array(event_values)),
        ("management", array(management)),
        ("exits", Value::from(exits)),
    ]))?)
}

fn rebuild_immigrant_from_sealed_authority(
    authority: &V5SharedConstructionAuthority,
    proposal_seed: &str,
    side: &str,
    persisted_program: &Value,
    persisted_selector: &Value,
) -> Result<ImmigrantModule> {
    let sealed_side = authority.side(side)?;
    let rebuilt = build_immigrant_module(
        side,
        proposal_seed,
        sealed_side.context()?,
        &sealed_side.budget,
    )?;
    if &rebuilt.program != persisted_program || &rebuilt.selector != persisted_selector {
        return Err(invalid(
            "v5 immigrant delta does not derive from sealed authority",
        ));
    }
    Ok(rebuilt)
}

const V5_FROZEN_MODULE_SCHEMA: &str = "temporal_bidirectional_module_snapshot_v1";
const V5_FROZEN_PAIR_SCHEMA: &str = "temporal_bidirectional_pair_snapshot_v1";
const V5_PAIR_PROPOSAL_SCHEMA: &str = "temporal_qd_pair_proposal_v2";
const V5_CANDIDATE_IDENTITY_SCHEMA: &str = "temporal_qd_bidirectional_candidate_identity_v1";
const V5_EXECUTABLE_SEMANTIC_SCHEMA: &str = "temporal_qd_pair_genome_semantics_v1";
const V5_FACTORY_AUDIT_SCHEMA: &str = "temporal_qd_evolvable_module_factory_audit_v1";

fn v5_id_from_sha(prefix: &str, identity: &str, label: &str) -> Result<String> {
    let identity = sha256_text(&Value::String(identity.to_owned()), label)?;
    let suffix = identity
        .get(7..35)
        .ok_or_else(|| invalid(format!("{label} lacks an identifier suffix")))?;
    Ok(format!("{prefix}{suffix}"))
}

fn v5_module_candidate_id(proposal_seed: &str, side: &str, program_sha256: &str) -> Result<String> {
    v5_id_from_sha(
        "qd_evolvable_module_",
        &canonical_sha256(&object([
            ("seed", Value::String(proposal_seed.to_owned())),
            ("side", Value::String(exact_side(side)?.to_owned())),
            ("genome", Value::String(program_sha256.to_owned())),
        ]))?,
        "v5 module candidate identity",
    )
}

fn v5_pair_candidate_id(proposal_seed: &str) -> Result<String> {
    v5_id_from_sha(
        "qd_evolvable_pair_",
        &canonical_sha256(&object([("seed", Value::String(proposal_seed.to_owned()))]))?,
        "v5 pair candidate identity",
    )
}

fn v5_seed_module_lineage(
    authority: &V5SharedConstructionAuthority,
    side: &str,
    proposal_seed: &str,
    program_sha256: &str,
    topology_sha256: &str,
) -> Result<Value> {
    Ok(object([
        (
            "operation",
            Value::String("evolvable_module_seed".to_owned()),
        ),
        ("side", Value::String(exact_side(side)?.to_owned())),
        ("proposalSeed", Value::String(proposal_seed.to_owned())),
        ("programKind", Value::String(V5_PROGRAM_KIND.to_owned())),
        ("codec", Value::String(V5_CODEC.to_owned())),
        (
            "compilerPolicySha256",
            Value::String(authority.compiler_policy_sha256.clone()),
        ),
        ("genomeSha256", Value::String(program_sha256.to_owned())),
        (
            "semanticTopologySha256",
            Value::String(topology_sha256.to_owned()),
        ),
    ]))
}

fn v5_seed_pair_lineage(
    authority: &V5SharedConstructionAuthority,
    side: &str,
    proposal_seed: &str,
    program_sha256: &str,
) -> Result<Value> {
    Ok(object([
        (
            "operation",
            Value::String("evolvable_module_pair_seed".to_owned()),
        ),
        ("side", Value::String(exact_side(side)?.to_owned())),
        ("proposalSeed", Value::String(proposal_seed.to_owned())),
        ("programKind", Value::String(V5_PROGRAM_KIND.to_owned())),
        ("codec", Value::String(V5_CODEC.to_owned())),
        (
            "authoritySha256",
            Value::String(authority.factory_authority_sha256.clone()),
        ),
        (
            "compilerPolicySha256",
            Value::String(authority.compiler_policy_sha256.clone()),
        ),
        ("genomeSha256", Value::String(program_sha256.to_owned())),
    ]))
}

#[derive(Clone, Debug)]
struct V5ReconstructedModule {
    /// Identity of the evolvable genome itself.  This is deliberately
    /// distinct from the Dashboard-normalized executable program emitted by
    /// native validation below; the preserved Python corpus uses both.
    genome_program_sha256: String,
    program: Value,
    selector: Value,
    profile: Value,
    validation: V5NativeValidation,
    lineage: Vec<Value>,
    identity_sha256: String,
    semantic_topology_sha256: String,
    resource_fingerprint_sha256: String,
}

impl V5ReconstructedModule {
    /// Recompute the public FrozenModule identity from the current module
    /// facts.  Keeping this in one place is important because the native
    /// executable program SHA and the genome SHA intentionally occupy
    /// different identity slots.
    fn recompute_identity_sha256(&self, side: &V5SideConstructionAuthority) -> Result<String> {
        Ok(canonical_sha256(&self.identity_envelope(side)?)?)
    }

    /// Project a historical, already-persisted seed lineage into an otherwise
    /// freshly reconstructed module.
    ///
    /// This is deliberately an import/projection adapter, never a native
    /// construction path.  A stopped historical journal may retain a literal
    /// `semanticTopologySha256` that predates the current Python/Rust
    /// topology-signature implementation.  That literal must remain in its
    /// old FrozenModule identity, while every fresh native proposal uses the
    /// recomputed `semantic_topology_sha256` on this module.  We therefore
    /// validate every other lineage binding against the sealed authority and
    /// current deterministic genome, but do not attempt to synthesize an old
    /// topology identity from current code.
    fn with_preserved_seed_lineage(
        &self,
        authority: &V5SharedConstructionAuthority,
        side: &V5SideConstructionAuthority,
        proposal_seed: &str,
        preserved_lineage: &Value,
    ) -> Result<Self> {
        let rows = array_ref(preserved_lineage, "preserved v5 module lineage")?;
        if rows.len() != 1 {
            return Err(invalid(
                "preserved v5 seed lineage must contain exactly one entry",
            ));
        }
        let entry = &rows[0];
        let fields = object_ref(entry, "preserved v5 seed lineage entry")?;
        let keys = [
            "operation",
            "side",
            "proposalSeed",
            "programKind",
            "codec",
            "compilerPolicySha256",
            "genomeSha256",
            "semanticTopologySha256",
        ];
        exact_value_keys(fields, &keys, "preserved v5 seed lineage entry")?;
        let expected_direction = text(
            required(&self.program, "direction", "reconstructed module")?,
            "reconstructed module direction",
        )?;
        if fields.get("operation").and_then(Value::as_str) != Some("evolvable_module_seed")
            || fields.get("side").and_then(Value::as_str) != Some(expected_direction.as_str())
            || fields.get("proposalSeed").and_then(Value::as_str) != Some(proposal_seed)
            || fields.get("programKind").and_then(Value::as_str) != Some(V5_PROGRAM_KIND)
            || fields.get("codec").and_then(Value::as_str) != Some(V5_CODEC)
            || fields.get("compilerPolicySha256").and_then(Value::as_str)
                != Some(authority.compiler_policy_sha256.as_str())
            || fields.get("genomeSha256").and_then(Value::as_str)
                != Some(self.genome_program_sha256.as_str())
        {
            return Err(invalid(
                "preserved v5 seed lineage is incompatible with reconstructed module",
            ));
        }
        let _ = sha256_text(
            required(entry, "semanticTopologySha256", "preserved v5 seed lineage")?,
            "preserved v5 seed lineage topology SHA-256",
        )?;

        let mut projected = self.clone();
        projected.lineage = rows.to_vec();
        projected.identity_sha256 = projected.recompute_identity_sha256(side)?;
        Ok(projected)
    }

    fn identity_material(&self, side: &V5SideConstructionAuthority) -> Result<Value> {
        Ok(object([
            (
                "direction",
                Value::String(text(
                    required(&self.program, "direction", "v5 reconstructed module")?,
                    "v5 reconstructed module direction",
                )?),
            ),
            (
                "programSha256",
                Value::String(self.genome_program_sha256.clone()),
            ),
            (
                "profileSha256",
                Value::String(self.validation.raw_profile_sha256.clone()),
            ),
            ("grammarContext", clone_value(&side.grammar_context.value)?),
            ("catalog", clone_value(&side.catalog.value)?),
            ("policy", clone_value(&side.policy.value)?),
            (
                "nativeAuthority",
                clone_value(&side.native_authority.value)?,
            ),
            (
                "nativeSnapshotSha256",
                Value::String(self.validation.profile_snapshot_sha256.clone()),
            ),
            (
                "nativeProgramSha256",
                Value::String(self.validation.program_sha256.clone()),
            ),
            (
                "nativeValidationReportSha256",
                Value::String(self.validation.validation_report_sha256.clone()),
            ),
            ("lineage", array(self.lineage.iter().cloned())),
        ]))
    }

    fn identity_envelope(&self, side: &V5SideConstructionAuthority) -> Result<Value> {
        let material = self.identity_material(side)?;
        let mut fields = material
            .as_object()
            .expect("reconstructed v5 module identity material")
            .clone();
        fields.insert(
            "schemaVersion".to_owned(),
            Value::String(V5_FROZEN_MODULE_SCHEMA.to_owned()),
        );
        Ok(Value::Object(fields))
    }

    fn canonical_payload(&self, side: &V5SideConstructionAuthority) -> Result<Value> {
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_FROZEN_MODULE_SCHEMA.to_owned()),
            ),
            (
                "direction",
                clone_value(required(
                    &self.program,
                    "direction",
                    "v5 reconstructed module",
                )?)?,
            ),
            ("program", clone_value(&self.program)?),
            ("profile", clone_value(&self.profile)?),
            ("grammarContext", clone_value(&side.grammar_context.value)?),
            ("catalog", clone_value(&side.catalog.value)?),
            ("policy", clone_value(&side.policy.value)?),
            (
                "nativeAuthority",
                clone_value(&side.native_authority.value)?,
            ),
            ("nativeReport", clone_value(&self.validation.report)?),
            ("lineage", array(self.lineage.iter().cloned())),
            (
                "identities",
                object([
                    (
                        "programSha256",
                        Value::String(self.genome_program_sha256.clone()),
                    ),
                    (
                        "profileSha256",
                        Value::String(self.validation.raw_profile_sha256.clone()),
                    ),
                    (
                        "nativeSnapshotSha256",
                        Value::String(self.validation.profile_snapshot_sha256.clone()),
                    ),
                    (
                        "nativeProgramSha256",
                        Value::String(self.validation.program_sha256.clone()),
                    ),
                    (
                        "nativeValidationReportSha256",
                        Value::String(self.validation.validation_report_sha256.clone()),
                    ),
                    (
                        "moduleIdentitySha256",
                        Value::String(self.identity_sha256.clone()),
                    ),
                ]),
            ),
        ]))
    }

    fn compact_facts(&self, side: &V5SideConstructionAuthority) -> V5ModuleCompactFacts {
        V5ModuleCompactFacts {
            direction: self
                .program
                .get("direction")
                .and_then(Value::as_str)
                .expect("internally rebuilt module has direction")
                .to_owned(),
            genome_program_sha256: self.genome_program_sha256.clone(),
            raw_profile_sha256: self.validation.raw_profile_sha256.clone(),
            profile_snapshot_sha256: self.validation.profile_snapshot_sha256.clone(),
            native_program_sha256: self.validation.program_sha256.clone(),
            validation_report_sha256: self.validation.validation_report_sha256.clone(),
            module_identity_sha256: self.identity_sha256.clone(),
            grammar_context_sha256: side.grammar_context.sha256.clone(),
            catalog_sha256: side.catalog.sha256.clone(),
            policy_sha256: side.policy.sha256.clone(),
            native_authority_sha256: side.native_authority.sha256.clone(),
            semantic_topology_sha256: self.semantic_topology_sha256.clone(),
            resource_fingerprint_sha256: self.resource_fingerprint_sha256.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct V5ReconstructedPair {
    long: V5ReconstructedModule,
    short: V5ReconstructedModule,
    profile: Value,
    validation: V5NativeValidation,
    side_targeted_lineage: Vec<Value>,
    pair_identity_sha256: String,
    factory_construction_audit: Value,
    proposal_sha256: String,
    candidate_identity_material: Value,
    candidate_identity_sha256: String,
    candidate_id: String,
    executable_semantic_sha256: String,
}

impl V5ReconstructedPair {
    fn identity_envelope(&self, authority: &V5SharedConstructionAuthority) -> Result<Value> {
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_FROZEN_PAIR_SCHEMA.to_owned()),
            ),
            ("longModule", self.long.identity_material(&authority.long)?),
            (
                "shortModule",
                self.short.identity_material(&authority.short)?,
            ),
            ("pairCompiler", clone_value(&authority.pair_compiler.value)?),
            (
                "compiledV3",
                object([
                    (
                        "rawPairSha256",
                        Value::String(self.validation.raw_profile_sha256.clone()),
                    ),
                    (
                        "profileSha256",
                        Value::String(self.validation.profile_snapshot_sha256.clone()),
                    ),
                    (
                        "programSha256",
                        Value::String(self.validation.program_sha256.clone()),
                    ),
                    (
                        "validationReportSha256",
                        Value::String(self.validation.validation_report_sha256.clone()),
                    ),
                ]),
            ),
            (
                "sideTargetedLineage",
                array(self.side_targeted_lineage.iter().cloned()),
            ),
        ]))
    }

    fn compact_g0_descriptor_input(
        &self,
        authority: &V5SharedConstructionAuthority,
        proposal_seed: &str,
    ) -> Result<crate::g0::CompactV5DescriptorInput> {
        let module = |direction: &str,
                      reconstructed: &V5ReconstructedModule,
                      sealed: &V5SideConstructionAuthority|
         -> Result<crate::g0::CompactV5DescriptorModuleInput> {
            Ok(crate::g0::CompactV5DescriptorModuleInput {
                direction: direction.to_owned(),
                profile: clone_value(&reconstructed.profile)?,
                catalog_payload: clone_value(sealed.catalog.payload())?,
                catalog_snapshot_sha256: sealed.catalog.sha256.clone(),
                grammar_context_snapshot_sha256: sealed.grammar_context.sha256.clone(),
                policy_snapshot_sha256: sealed.policy.sha256.clone(),
                native_authority_snapshot_sha256: sealed.native_authority.sha256.clone(),
                genome_program_sha256: reconstructed.genome_program_sha256.clone(),
                native_program_sha256: reconstructed.validation.program_sha256.clone(),
                raw_profile_sha256: reconstructed.validation.raw_profile_sha256.clone(),
                profile_snapshot_sha256: reconstructed.validation.profile_snapshot_sha256.clone(),
                validation_report_sha256: reconstructed.validation.validation_report_sha256.clone(),
                module_identity_sha256: reconstructed.identity_sha256.clone(),
                module_identity_envelope: reconstructed.identity_envelope(sealed)?,
            })
        };
        Ok(crate::g0::CompactV5DescriptorInput {
            candidate_id: self.candidate_id.clone(),
            candidate_identity_sha256: self.candidate_identity_sha256.clone(),
            candidate_identity_material: clone_value(&self.candidate_identity_material)?,
            proposal_seed: proposal_seed.to_owned(),
            pair_identity_sha256: self.pair_identity_sha256.clone(),
            pair_identity_envelope: self.identity_envelope(authority)?,
            pair_profile: clone_value(&self.profile)?,
            pair_raw_profile_sha256: self.validation.raw_profile_sha256.clone(),
            pair_profile_snapshot_sha256: self.validation.profile_snapshot_sha256.clone(),
            pair_program_sha256: self.validation.program_sha256.clone(),
            pair_validation_report_sha256: self.validation.validation_report_sha256.clone(),
            pair_compiler_authority_sha256: authority.pair_compiler.sha256.clone(),
            pair_policy_sha256: authority.pair_policy_sha256.clone(),
            qd_engine_version: authority.qd_engine_version.clone(),
            proposal_sha256: self.proposal_sha256.clone(),
            side_targeted_lineage: array(self.side_targeted_lineage.iter().cloned()),
            executable_semantic_sha256: self.executable_semantic_sha256.clone(),
            long: module("long", &self.long, &authority.long)?,
            short: module("short", &self.short, &authority.short)?,
        })
    }

    fn compact_g0_descriptor_projection(
        &self,
        authority: &V5SharedConstructionAuthority,
        proposal_seed: &str,
    ) -> Result<Value> {
        let input = self.compact_g0_descriptor_input(authority, proposal_seed)?;
        crate::g0::derive_descriptor_projection_from_compact_v5(&input).map_err(|error| {
            invalid(format!(
                "v5 compact-to-G0 descriptor admission failed: {error}"
            ))
        })
    }

    /// Derive a descriptor for a compiler-reconstructed structural offspring.
    /// This deliberately uses the evolved origin wrapper rather than the G0
    /// immigrant wrapper: the descriptor is identical in shape, but its
    /// candidate identity must remain bound to `structural_offspring`.
    fn compact_evolved_descriptor_projection(
        &self,
        authority: &V5SharedConstructionAuthority,
        proposal_seed: &str,
    ) -> Result<Value> {
        let input = self.compact_g0_descriptor_input(authority, proposal_seed)?;
        crate::g0::derive_descriptor_projection_from_evolved_compact_v5(&input).map_err(|error| {
            invalid(format!(
                "v5 compact evolved descriptor admission failed: {error}"
            ))
        })
    }

    fn canonical_payload(&self, authority: &V5SharedConstructionAuthority) -> Result<Value> {
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_FROZEN_PAIR_SCHEMA.to_owned()),
            ),
            ("long", self.long.canonical_payload(&authority.long)?),
            ("short", self.short.canonical_payload(&authority.short)?),
            ("pairCompiler", clone_value(&authority.pair_compiler.value)?),
            ("profile", clone_value(&self.profile)?),
            ("validation", clone_value(&self.validation.report)?),
            (
                "sideTargetedLineage",
                array(self.side_targeted_lineage.iter().cloned()),
            ),
            (
                "identities",
                object([
                    (
                        "rawPairSha256",
                        Value::String(self.validation.raw_profile_sha256.clone()),
                    ),
                    (
                        "profileSha256",
                        Value::String(self.validation.profile_snapshot_sha256.clone()),
                    ),
                    (
                        "programSha256",
                        Value::String(self.validation.program_sha256.clone()),
                    ),
                    (
                        "validationReportSha256",
                        Value::String(self.validation.validation_report_sha256.clone()),
                    ),
                    (
                        "pairIdentitySha256",
                        Value::String(self.pair_identity_sha256.clone()),
                    ),
                ]),
            ),
        ]))
    }

    fn compiled_facts(&self, authority: &V5SharedConstructionAuthority) -> V5CompiledPairFacts {
        V5CompiledPairFacts {
            raw_pair_sha256: self.validation.raw_profile_sha256.clone(),
            profile_snapshot_sha256: self.validation.profile_snapshot_sha256.clone(),
            program_sha256: self.validation.program_sha256.clone(),
            validation_report_sha256: self.validation.validation_report_sha256.clone(),
            pair_compiler_authority_sha256: authority.pair_compiler.sha256.clone(),
        }
    }
}

fn v5_pair_identity_sha256(
    authority: &V5SharedConstructionAuthority,
    long: &V5ReconstructedModule,
    short: &V5ReconstructedModule,
    validation: &V5NativeValidation,
    side_targeted_lineage: &[Value],
) -> Result<String> {
    Ok(canonical_sha256(&object([
        (
            "schemaVersion",
            Value::String(V5_FROZEN_PAIR_SCHEMA.to_owned()),
        ),
        ("longModule", long.identity_material(&authority.long)?),
        ("shortModule", short.identity_material(&authority.short)?),
        ("pairCompiler", clone_value(&authority.pair_compiler.value)?),
        (
            "compiledV3",
            object([
                (
                    "rawPairSha256",
                    Value::String(validation.raw_profile_sha256.clone()),
                ),
                (
                    "profileSha256",
                    Value::String(validation.profile_snapshot_sha256.clone()),
                ),
                (
                    "programSha256",
                    Value::String(validation.program_sha256.clone()),
                ),
                (
                    "validationReportSha256",
                    Value::String(validation.validation_report_sha256.clone()),
                ),
            ]),
        ),
        (
            "sideTargetedLineage",
            array(side_targeted_lineage.iter().cloned()),
        ),
    ]))?)
}

/// Construct the immutable factory audit from freshly recomputed native
/// facts.  This intentionally never reads the historical FrozenModule
/// lineage projection: the audit describes the current factory semantics,
/// while legacy lineages are a separate public-identity compatibility layer.
fn v5_factory_construction_audit(
    authority: &V5SharedConstructionAuthority,
    pair_identity_sha256: &str,
    long: &V5ReconstructedModule,
    short: &V5ReconstructedModule,
) -> Result<Value> {
    let semantic = object([
        (
            "schemaVersion",
            Value::String(V5_FACTORY_AUDIT_SCHEMA.to_owned()),
        ),
        (
            "authoritySha256",
            Value::String(authority.factory_authority_sha256.clone()),
        ),
        (
            "pairIdentitySha256",
            Value::String(pair_identity_sha256.to_owned()),
        ),
        (
            "sides",
            object([
                (
                    "long",
                    object([
                        ("programKind", Value::String(V5_PROGRAM_KIND.to_owned())),
                        ("codec", Value::String(V5_CODEC.to_owned())),
                        (
                            "genomeSha256",
                            Value::String(long.genome_program_sha256.clone()),
                        ),
                        (
                            "semanticTopologySha256",
                            Value::String(long.semantic_topology_sha256.clone()),
                        ),
                        (
                            "resourceFingerprintSha256",
                            Value::String(long.resource_fingerprint_sha256.clone()),
                        ),
                    ]),
                ),
                (
                    "short",
                    object([
                        ("programKind", Value::String(V5_PROGRAM_KIND.to_owned())),
                        ("codec", Value::String(V5_CODEC.to_owned())),
                        (
                            "genomeSha256",
                            Value::String(short.genome_program_sha256.clone()),
                        ),
                        (
                            "semanticTopologySha256",
                            Value::String(short.semantic_topology_sha256.clone()),
                        ),
                        (
                            "resourceFingerprintSha256",
                            Value::String(short.resource_fingerprint_sha256.clone()),
                        ),
                    ]),
                ),
            ]),
        ),
    ]);
    let mut fields = semantic
        .as_object()
        .expect("constructed v5 factory audit")
        .clone();
    fields.insert(
        "auditSha256".to_owned(),
        Value::String(canonical_sha256(&semantic)?),
    );
    Ok(Value::Object(fields))
}

fn finalize_g0_reconstructed_pair(
    authority: &V5SharedConstructionAuthority,
    proposal_seed: &str,
    long: V5ReconstructedModule,
    short: V5ReconstructedModule,
    profile: Value,
    validation: V5NativeValidation,
    side_targeted_lineage: Vec<Value>,
) -> Result<V5ReconstructedPair> {
    let pair_identity_sha256 = v5_pair_identity_sha256(
        authority,
        &long,
        &short,
        &validation,
        &side_targeted_lineage,
    )?;
    let factory_construction_audit =
        v5_factory_construction_audit(authority, &pair_identity_sha256, &long, &short)?;
    let provisional = V5ReconstructedPair {
        long,
        short,
        profile,
        validation,
        side_targeted_lineage,
        pair_identity_sha256,
        factory_construction_audit,
        proposal_sha256: String::new(),
        candidate_identity_material: Value::Null,
        candidate_identity_sha256: String::new(),
        candidate_id: String::new(),
        executable_semantic_sha256: String::new(),
    };
    let factory_pair = provisional.canonical_payload(authority)?;
    let proposal_semantic = object([
        (
            "schemaVersion",
            Value::String(V5_PAIR_PROPOSAL_SCHEMA.to_owned()),
        ),
        ("proposalSeed", Value::String(proposal_seed.to_owned())),
        ("originKind", Value::String("random_immigrant".to_owned())),
        (
            "side",
            Value::String(v5_proposal_side(proposal_seed)?.to_owned()),
        ),
        ("factoryPair", factory_pair),
        (
            "pairIdentitySha256",
            Value::String(provisional.pair_identity_sha256.clone()),
        ),
        ("disposition", Value::String("materialized".to_owned())),
        (
            "factoryConstructionAudit",
            clone_value(&provisional.factory_construction_audit)?,
        ),
    ]);
    let proposal_sha256 = canonical_sha256(&proposal_semantic)?;
    let candidate_identity_material = object([
        (
            "schemaVersion",
            Value::String(V5_CANDIDATE_IDENTITY_SCHEMA.to_owned()),
        ),
        (
            "qdEngineVersion",
            Value::String(authority.qd_engine_version.clone()),
        ),
        ("originKind", Value::String("random_immigrant".to_owned())),
        (
            "bidirectionalGenomeIdentitySha256",
            Value::String(provisional.pair_identity_sha256.clone()),
        ),
        (
            "pairPolicySha256",
            Value::String(authority.pair_policy_sha256.clone()),
        ),
        (
            "longModuleIdentitySha256",
            Value::String(provisional.long.identity_sha256.clone()),
        ),
        (
            "shortModuleIdentitySha256",
            Value::String(provisional.short.identity_sha256.clone()),
        ),
        (
            "longGrammarContextSha256",
            Value::String(authority.long.grammar_context.sha256.clone()),
        ),
        (
            "shortGrammarContextSha256",
            Value::String(authority.short.grammar_context.sha256.clone()),
        ),
        (
            "longCatalogSha256",
            Value::String(authority.long.catalog.sha256.clone()),
        ),
        (
            "shortCatalogSha256",
            Value::String(authority.short.catalog.sha256.clone()),
        ),
        (
            "longPolicySha256",
            Value::String(authority.long.policy.sha256.clone()),
        ),
        (
            "shortPolicySha256",
            Value::String(authority.short.policy.sha256.clone()),
        ),
        (
            "longNativeAuthoritySha256",
            Value::String(authority.long.native_authority.sha256.clone()),
        ),
        (
            "shortNativeAuthoritySha256",
            Value::String(authority.short.native_authority.sha256.clone()),
        ),
        (
            "pairCompilerAuthoritySha256",
            Value::String(authority.pair_compiler.sha256.clone()),
        ),
        (
            "compiledRawPairSha256",
            Value::String(provisional.validation.raw_profile_sha256.clone()),
        ),
        (
            "compiledProfileSha256",
            Value::String(provisional.validation.profile_snapshot_sha256.clone()),
        ),
        (
            "compiledProgramSha256",
            Value::String(provisional.validation.program_sha256.clone()),
        ),
        (
            "compiledValidationReportSha256",
            Value::String(provisional.validation.validation_report_sha256.clone()),
        ),
        (
            "orderedSideLineage",
            array(provisional.side_targeted_lineage.iter().cloned()),
        ),
        (
            "materializedPairProposalSha256",
            Value::String(proposal_sha256.clone()),
        ),
    ]);
    let candidate_identity_sha256 = canonical_sha256(&candidate_identity_material)?;
    let candidate_id = v5_id_from_sha("qd_", &candidate_identity_sha256, "v5 candidate identity")?;
    let executable_semantic_sha256 = canonical_sha256(&object([
        (
            "schemaVersion",
            Value::String(V5_EXECUTABLE_SEMANTIC_SCHEMA.to_owned()),
        ),
        (
            "longProfileSha256",
            Value::String(provisional.long.validation.raw_profile_sha256.clone()),
        ),
        (
            "shortProfileSha256",
            Value::String(provisional.short.validation.raw_profile_sha256.clone()),
        ),
    ]))?;
    Ok(V5ReconstructedPair {
        proposal_sha256,
        candidate_identity_material,
        candidate_identity_sha256,
        candidate_id,
        executable_semantic_sha256,
        ..provisional
    })
}

fn reconstruct_g0_module(
    authority: &V5SharedConstructionAuthority,
    side: &str,
    proposal_seed: &str,
    persisted_program: Option<&Value>,
    persisted_selector: Option<&Value>,
) -> Result<V5ReconstructedModule> {
    let sealed_side = authority.side(side)?;
    let rebuilt = build_immigrant_module(
        side,
        proposal_seed,
        sealed_side.context()?,
        &sealed_side.budget,
    )?;
    if let (Some(program), Some(selector)) = (persisted_program, persisted_selector) {
        // This is the compact G0 corruption gate.  Delta content is never an
        // authority: it must exactly equal a fresh deterministic build from
        // sealed inputs before any profile/compiler fact is used.
        if &rebuilt.program != program || &rebuilt.selector != selector {
            return Err(invalid("v5 compact G0 module delta is not reproducible"));
        }
    } else if persisted_program.is_some() || persisted_selector.is_some() {
        return Err(invalid(
            "v5 compact G0 module program/selector presence drifted",
        ));
    }
    let genome_program_sha256 = rebuilt.program_sha256.clone();
    let candidate_id = v5_module_candidate_id(proposal_seed, side, &genome_program_sha256)?;
    let profile = compile_immigrant_profile(&rebuilt.program)?;
    let validation = validate_native_profile(&profile, &candidate_id)?;
    let semantic_topology_sha256 = v5_semantic_topology_sha256(&rebuilt.program)?;
    let resource_fingerprint_sha256 = v5_resource_fingerprint_sha256(&rebuilt.program)?;
    let lineage = vec![v5_seed_module_lineage(
        authority,
        side,
        proposal_seed,
        &genome_program_sha256,
        &semantic_topology_sha256,
    )?];
    let provisional = V5ReconstructedModule {
        genome_program_sha256,
        program: rebuilt.program,
        selector: rebuilt.selector,
        profile,
        validation,
        lineage,
        identity_sha256: String::new(),
        semantic_topology_sha256,
        resource_fingerprint_sha256,
    };
    let identity_sha256 = canonical_sha256(&object([
        (
            "schemaVersion",
            Value::String(V5_FROZEN_MODULE_SCHEMA.to_owned()),
        ),
        (
            "direction",
            clone_value(required(
                &provisional.program,
                "direction",
                "v5 reconstructed module",
            )?)?,
        ),
        (
            "programSha256",
            Value::String(provisional.genome_program_sha256.clone()),
        ),
        (
            "profileSha256",
            Value::String(provisional.validation.raw_profile_sha256.clone()),
        ),
        (
            "grammarContext",
            clone_value(&sealed_side.grammar_context.value)?,
        ),
        ("catalog", clone_value(&sealed_side.catalog.value)?),
        ("policy", clone_value(&sealed_side.policy.value)?),
        (
            "nativeAuthority",
            clone_value(&sealed_side.native_authority.value)?,
        ),
        (
            "nativeSnapshotSha256",
            Value::String(provisional.validation.profile_snapshot_sha256.clone()),
        ),
        (
            "nativeProgramSha256",
            Value::String(provisional.validation.program_sha256.clone()),
        ),
        (
            "nativeValidationReportSha256",
            Value::String(provisional.validation.validation_report_sha256.clone()),
        ),
        ("lineage", array(provisional.lineage.iter().cloned())),
    ]))?;
    Ok(V5ReconstructedModule {
        identity_sha256,
        ..provisional
    })
}

fn reconstruct_g0_pair(
    authority: &V5SharedConstructionAuthority,
    proposal_seed: &str,
    persisted_delta: Option<&Value>,
) -> Result<V5ReconstructedPair> {
    let (long_program, long_selector, short_program, short_selector) =
        if let Some(delta) = persisted_delta {
            validate_proposal_delta(delta)?;
            if required(delta, "proposalSeed", "v5 compact proposal delta")?.as_str()
                != Some(proposal_seed)
            {
                return Err(invalid(
                    "v5 compact G0 proposal delta belongs to a different proposal seed",
                ));
            }
            (
                Some(required(delta, "longProgram", "v5 compact proposal delta")?),
                Some(required(
                    delta,
                    "longSelector",
                    "v5 compact proposal delta",
                )?),
                Some(required(
                    delta,
                    "shortProgram",
                    "v5 compact proposal delta",
                )?),
                Some(required(
                    delta,
                    "shortSelector",
                    "v5 compact proposal delta",
                )?),
            )
        } else {
            (None, None, None, None)
        };
    let long = reconstruct_g0_module(
        authority,
        "long",
        proposal_seed,
        long_program,
        long_selector,
    )?;
    let short = reconstruct_g0_module(
        authority,
        "short",
        proposal_seed,
        short_program,
        short_selector,
    )?;
    let pair_candidate_id = v5_pair_candidate_id(proposal_seed)?;
    let profile = compile_bidirectional_profile(
        &long.profile,
        &short.profile,
        &pair_candidate_id,
        &ModuleSourceIdentities::from_native_report(&long.validation.report)?,
        &ModuleSourceIdentities::from_native_report(&short.validation.report)?,
    )?;
    let validation = validate_native_profile(&profile, &pair_candidate_id)?;
    let side_targeted_lineage = vec![
        v5_seed_pair_lineage(
            authority,
            "long",
            proposal_seed,
            &long.genome_program_sha256,
        )?,
        v5_seed_pair_lineage(
            authority,
            "short",
            proposal_seed,
            &short.genome_program_sha256,
        )?,
    ];
    let pair_identity_sha256 = canonical_sha256(&object([
        (
            "schemaVersion",
            Value::String(V5_FROZEN_PAIR_SCHEMA.to_owned()),
        ),
        ("longModule", long.identity_material(&authority.long)?),
        ("shortModule", short.identity_material(&authority.short)?),
        ("pairCompiler", clone_value(&authority.pair_compiler.value)?),
        (
            "compiledV3",
            object([
                (
                    "rawPairSha256",
                    Value::String(validation.raw_profile_sha256.clone()),
                ),
                (
                    "profileSha256",
                    Value::String(validation.profile_snapshot_sha256.clone()),
                ),
                (
                    "programSha256",
                    Value::String(validation.program_sha256.clone()),
                ),
                (
                    "validationReportSha256",
                    Value::String(validation.validation_report_sha256.clone()),
                ),
            ]),
        ),
        (
            "sideTargetedLineage",
            array(side_targeted_lineage.iter().cloned()),
        ),
    ]))?;
    let factory_construction_audit = {
        let semantic = object([
            (
                "schemaVersion",
                Value::String(V5_FACTORY_AUDIT_SCHEMA.to_owned()),
            ),
            (
                "authoritySha256",
                Value::String(authority.factory_authority_sha256.clone()),
            ),
            (
                "pairIdentitySha256",
                Value::String(pair_identity_sha256.clone()),
            ),
            (
                "sides",
                object([
                    (
                        "long",
                        object([
                            ("programKind", Value::String(V5_PROGRAM_KIND.to_owned())),
                            ("codec", Value::String(V5_CODEC.to_owned())),
                            (
                                "genomeSha256",
                                Value::String(long.genome_program_sha256.clone()),
                            ),
                            (
                                "semanticTopologySha256",
                                Value::String(long.semantic_topology_sha256.clone()),
                            ),
                            (
                                "resourceFingerprintSha256",
                                Value::String(long.resource_fingerprint_sha256.clone()),
                            ),
                        ]),
                    ),
                    (
                        "short",
                        object([
                            ("programKind", Value::String(V5_PROGRAM_KIND.to_owned())),
                            ("codec", Value::String(V5_CODEC.to_owned())),
                            (
                                "genomeSha256",
                                Value::String(short.genome_program_sha256.clone()),
                            ),
                            (
                                "semanticTopologySha256",
                                Value::String(short.semantic_topology_sha256.clone()),
                            ),
                            (
                                "resourceFingerprintSha256",
                                Value::String(short.resource_fingerprint_sha256.clone()),
                            ),
                        ]),
                    ),
                ]),
            ),
        ]);
        let mut fields = semantic
            .as_object()
            .expect("constructed factory audit")
            .clone();
        fields.insert(
            "auditSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Value::Object(fields)
    };
    let provisional = V5ReconstructedPair {
        long,
        short,
        profile,
        validation,
        side_targeted_lineage,
        pair_identity_sha256,
        factory_construction_audit,
        proposal_sha256: String::new(),
        candidate_identity_material: Value::Null,
        candidate_identity_sha256: String::new(),
        candidate_id: String::new(),
        executable_semantic_sha256: String::new(),
    };
    let factory_pair = provisional.canonical_payload(authority)?;
    let proposal_semantic = object([
        (
            "schemaVersion",
            Value::String(V5_PAIR_PROPOSAL_SCHEMA.to_owned()),
        ),
        ("proposalSeed", Value::String(proposal_seed.to_owned())),
        ("originKind", Value::String("random_immigrant".to_owned())),
        (
            "side",
            Value::String(v5_proposal_side(proposal_seed)?.to_owned()),
        ),
        ("factoryPair", factory_pair),
        (
            "pairIdentitySha256",
            Value::String(provisional.pair_identity_sha256.clone()),
        ),
        ("disposition", Value::String("materialized".to_owned())),
        (
            "factoryConstructionAudit",
            clone_value(&provisional.factory_construction_audit)?,
        ),
    ]);
    let proposal_sha256 = canonical_sha256(&proposal_semantic)?;
    let candidate_identity_material = object([
        (
            "schemaVersion",
            Value::String(V5_CANDIDATE_IDENTITY_SCHEMA.to_owned()),
        ),
        (
            "qdEngineVersion",
            Value::String(authority.qd_engine_version.clone()),
        ),
        ("originKind", Value::String("random_immigrant".to_owned())),
        (
            "bidirectionalGenomeIdentitySha256",
            Value::String(provisional.pair_identity_sha256.clone()),
        ),
        (
            "pairPolicySha256",
            Value::String(authority.pair_policy_sha256.clone()),
        ),
        (
            "longModuleIdentitySha256",
            Value::String(provisional.long.identity_sha256.clone()),
        ),
        (
            "shortModuleIdentitySha256",
            Value::String(provisional.short.identity_sha256.clone()),
        ),
        (
            "longGrammarContextSha256",
            Value::String(authority.long.grammar_context.sha256.clone()),
        ),
        (
            "shortGrammarContextSha256",
            Value::String(authority.short.grammar_context.sha256.clone()),
        ),
        (
            "longCatalogSha256",
            Value::String(authority.long.catalog.sha256.clone()),
        ),
        (
            "shortCatalogSha256",
            Value::String(authority.short.catalog.sha256.clone()),
        ),
        (
            "longPolicySha256",
            Value::String(authority.long.policy.sha256.clone()),
        ),
        (
            "shortPolicySha256",
            Value::String(authority.short.policy.sha256.clone()),
        ),
        (
            "longNativeAuthoritySha256",
            Value::String(authority.long.native_authority.sha256.clone()),
        ),
        (
            "shortNativeAuthoritySha256",
            Value::String(authority.short.native_authority.sha256.clone()),
        ),
        (
            "pairCompilerAuthoritySha256",
            Value::String(authority.pair_compiler.sha256.clone()),
        ),
        (
            "compiledRawPairSha256",
            Value::String(provisional.validation.raw_profile_sha256.clone()),
        ),
        (
            "compiledProfileSha256",
            Value::String(provisional.validation.profile_snapshot_sha256.clone()),
        ),
        (
            "compiledProgramSha256",
            Value::String(provisional.validation.program_sha256.clone()),
        ),
        (
            "compiledValidationReportSha256",
            Value::String(provisional.validation.validation_report_sha256.clone()),
        ),
        (
            "orderedSideLineage",
            array(provisional.side_targeted_lineage.iter().cloned()),
        ),
        (
            "materializedPairProposalSha256",
            Value::String(proposal_sha256.clone()),
        ),
    ]);
    let candidate_identity_sha256 = canonical_sha256(&candidate_identity_material)?;
    let candidate_id = v5_id_from_sha("qd_", &candidate_identity_sha256, "v5 candidate identity")?;
    let executable_semantic_sha256 = canonical_sha256(&object([
        (
            "schemaVersion",
            Value::String(V5_EXECUTABLE_SEMANTIC_SCHEMA.to_owned()),
        ),
        (
            "longProfileSha256",
            Value::String(provisional.long.validation.raw_profile_sha256.clone()),
        ),
        (
            "shortProfileSha256",
            Value::String(provisional.short.validation.raw_profile_sha256.clone()),
        ),
    ]))?;
    Ok(V5ReconstructedPair {
        proposal_sha256,
        candidate_identity_material,
        candidate_identity_sha256,
        candidate_id,
        executable_semantic_sha256,
        ..provisional
    })
}

/// The complete compact material produced for one accepted native G0
/// immigrant.  This is deliberately crate-private: the transaction owns the
/// all-attempt/retry journal while this compiler boundary owns reconstruction
/// from sealed authority.  No rich `FrozenPair` is retained or published.
#[derive(Clone, Debug)]
pub(crate) struct V5G0AcceptedMaterial {
    pub(crate) proposal_delta: Value,
    pub(crate) record: V5CompactAcceptedRecord,
}

/// Construction kind accepted by the sealed later-generation materializer.
/// It is intentionally narrower than scheduler intent: the scheduler owns
/// retry/quota state while this compiler boundary owns only a final admitted
/// pair and its immutable construction facts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum V5EvolvedBuildKind {
    Immigrant,
    Mutation,
    Crossover,
}

impl V5EvolvedBuildKind {
    fn origin_kind(self) -> &'static str {
        match self {
            Self::Immigrant => "random_immigrant",
            Self::Mutation | Self::Crossover => "structural_offspring",
        }
    }

    fn scheduled_kind(self) -> &'static str {
        match self {
            Self::Immigrant => "random_immigrant",
            Self::Mutation => "structural_offspring",
            Self::Crossover => "same_side_crossover",
        }
    }
}

/// Sealed parent facts supplied by the later-generation archive boundary.
/// This type is crate-visible only: a transaction can retain it between
/// deterministic operator steps, but an external caller cannot substitute a
/// self-hashed compact record for the compiler-reconstructed parent state.
#[derive(Clone, Debug)]
pub(crate) struct V5EvolvedParentMaterial {
    pub attempt_reference: V5AttemptParentReference,
    pub candidate_id: String,
    pub candidate_identity_sha256: String,
    pub pair_identity_sha256: String,
    pub accepted_record: V5CompactAcceptedRecord,
    /// Exact compact delta named by `accepted_record`.  It is retained only
    /// as an immutable archive witness: `load_v5_evolved_parent` reparses and
    /// recompiles it before any operator state is exposed.
    pub proposal_delta: Value,
    pub long_program: Value,
    pub short_program: Value,
    pub long_module_lineage: Vec<Value>,
    pub short_module_lineage: Vec<Value>,
    pub side_targeted_lineage: Vec<Value>,
    pub long_state: crate::v5_operators::V5EvolvedSideState,
    pub short_state: crate::v5_operators::V5EvolvedSideState,
    /// Compiler-owned pair produced by the same sealed reconstruction that
    /// populated the public compact record. Publication may project this
    /// pair only while replay owns the opaque material; it is never serialized
    /// or accepted as construction authority on a later invocation.
    sealed_pair: Arc<V5ReconstructedPair>,
}

/// Offline, content-addressed witness for one selected evolved parent.
///
/// `parent_reference` is retained exactly as selected, while the compact
/// record, delta, and lineages are duplicated as individually typed fields so
/// replay can rebuild the opaque payload and reject any mismatch before it
/// invokes the sealed compiler.  The source archive input-binding and
/// semantic identities document where selection occurred without making an
/// old archive file a runtime dependency after this snapshot has been sealed.
#[derive(Clone, Debug)]
pub(crate) struct V5EvolvedParentSnapshot {
    pub source_parent_archive_input_binding_sha256: String,
    pub source_parent_archive_semantic_sha256: String,
    pub parent_reference: ParentReference,
    pub attempt_reference: V5AttemptParentReference,
    pub accepted_record: V5CompactAcceptedRecord,
    pub proposal_delta: Value,
    pub long_module_lineage: Vec<Value>,
    pub short_module_lineage: Vec<Value>,
    pub side_targeted_lineage: Vec<Value>,
}

/// Canonical immutable object binding for an evolved parent snapshot.  The
/// transaction owns inventory ordering; this type owns only the sealed
/// identity/path relation so it cannot be reinterpreted by a batch writer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct V5EvolvedParentSnapshotObjectBinding {
    pub parent_snapshot_sha256: String,
    pub relative_path: String,
}

/// Exact input to the sealed later-generation accepted-material compiler.
/// The transaction passes an already-journalable compact delta, but this
/// function treats it as a corruption witness: it independently recompiles
/// both programs and derives all identity/audit/record facts from authority.
#[derive(Clone, Debug)]
pub(crate) struct V5EvolvedAcceptedBuildInput {
    pub generation_config_sha256: String,
    pub generation_index: u64,
    pub birth_ordinal: u64,
    pub proposal_ordinal: u64,
    pub proposal_seed: String,
    pub kind: V5EvolvedBuildKind,
    pub parent: Option<V5EvolvedParentMaterial>,
    pub mate: Option<V5EvolvedParentMaterial>,
    pub parent_selection_receipt: Option<Value>,
    pub long_program: Option<Value>,
    pub short_program: Option<Value>,
    pub operator_trace: Option<Value>,
    pub terminal_operator_plan: Option<Value>,
    pub terminal_operator_application: Option<Value>,
    /// Exact `temporal_qd_v5_evolved_proposal_delta_v1` object including its
    /// self hash.  Its own schema remains transaction-owned, while this
    /// compiler verifies the authority/seed/program bindings it consumes.
    pub proposal_delta: Value,
}

/// Sealed output consumed by the later-generation transaction before its
/// identity-ledger decision.  It contains no rich legacy pair expansion.
#[derive(Clone, Debug)]
pub(crate) struct V5EvolvedAcceptedMaterial {
    pub proposal_delta: Value,
    pub record: V5CompactAcceptedRecord,
    pub ledger_candidate: Value,
    pub long_state: crate::v5_operators::V5EvolvedSideState,
    pub short_state: crate::v5_operators::V5EvolvedSideState,
    pub parent_material: V5EvolvedParentMaterial,
}

/// Build the two deterministic immigrant programs for a later-generation
/// `RichImmigrant` proposal.  This is deliberately narrower than the G0
/// accepted-material constructor: the caller still owns its all-attempt
/// delta, admission, and receipt, while only sealed authority constructs the
/// programs that the delta is allowed to name.
pub(crate) fn build_v5_evolved_immigrant_programs(
    authority: &V5SharedConstructionAuthority,
    proposal_seed: &str,
) -> Result<(Value, Value)> {
    let _ = sha256_text(
        &Value::String(proposal_seed.to_owned()),
        "v5 evolved immigrant proposal seed",
    )?;
    let long = build_immigrant_module(
        "long",
        proposal_seed,
        authority.long.context()?,
        &authority.long.budget,
    )?;
    let short = build_immigrant_module(
        "short",
        proposal_seed,
        authority.short.context()?,
        &authority.short.budget,
    )?;
    Ok((clone_value(&long.program)?, clone_value(&short.program)?))
}

fn evolved_delta_sha256(value: &Value) -> Result<String> {
    let fields = object_ref(value, "v5 evolved proposal delta")?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some("temporal_qd_v5_evolved_proposal_delta_v1")
    {
        return Err(invalid("v5 evolved proposal delta schema is invalid"));
    }
    let supplied = sha256_text(
        required(value, "deltaSha256", "v5 evolved proposal delta")?,
        "v5 evolved proposal delta SHA-256",
    )?;
    let mut semantic = fields.clone();
    semantic.remove("deltaSha256");
    if canonical_sha256(&Value::Object(semantic))? != supplied {
        return Err(invalid("v5 evolved proposal delta identity drifted"));
    }
    Ok(supplied)
}

fn evolved_delta_program(value: &Value, side: &str) -> Result<Value> {
    clone_value(required(
        value,
        match exact_side(side)? {
            "long" => "longProgram",
            "short" => "shortProgram",
            _ => unreachable!("exact_side returns a closed side"),
        },
        "v5 evolved proposal delta",
    )?)
}

fn evolved_delta_text(value: &Value, field_name: &str, label: &str) -> Result<String> {
    text(
        required(value, field_name, "v5 evolved proposal delta")?,
        label,
    )
}

fn reconstructed_module_from_evolved_program(
    authority: &V5SharedConstructionAuthority,
    projection: &V5OperatorAuthorityProjection,
    proposal_seed: &str,
    side: &str,
    program: &Value,
    lineage: Vec<Value>,
) -> Result<V5ReconstructedModule> {
    let side = exact_side(side)?;
    let genome_program_sha256 = canonical_sha256(program)?;
    let candidate_id = v5_module_candidate_id(proposal_seed, side, &genome_program_sha256)?;
    let compiled = compile_v5_module_profile(program, projection, side, &candidate_id)?;
    let validation = V5NativeValidation {
        report: clone_value(&compiled.native_validation_report)?,
        raw_profile_sha256: compiled.raw_profile_sha256,
        profile_snapshot_sha256: compiled.profile_snapshot_sha256,
        program_sha256: compiled.native_program_sha256,
        validation_report_sha256: compiled.native_validation_report_sha256,
    };
    if compiled.genome_program_sha256 != genome_program_sha256 {
        return Err(invalid("v5 evolved compiler program identity drifted"));
    }
    let provisional = V5ReconstructedModule {
        genome_program_sha256,
        program: clone_value(program)?,
        // Evolved programs are not factory-selector products.  The selector
        // is intentionally absent from every evolved identity envelope.
        selector: Value::Null,
        profile: compiled.profile,
        validation,
        lineage,
        identity_sha256: String::new(),
        semantic_topology_sha256: v5_semantic_topology_sha256(program)?,
        resource_fingerprint_sha256: v5_resource_fingerprint_sha256(program)?,
    };
    let sealed_side = authority.side(side)?;
    let identity_sha256 = provisional.recompute_identity_sha256(sealed_side)?;
    Ok(V5ReconstructedModule {
        identity_sha256,
        ..provisional
    })
}

fn finalize_evolved_reconstructed_pair(
    authority: &V5SharedConstructionAuthority,
    proposal_seed: &str,
    origin_kind: &str,
    proposal_delta_sha256: &str,
    long: V5ReconstructedModule,
    short: V5ReconstructedModule,
    side_targeted_lineage: Vec<Value>,
) -> Result<V5ReconstructedPair> {
    let pair_candidate_id = v5_pair_candidate_id(proposal_seed)?;
    let profile = compile_bidirectional_profile(
        &long.profile,
        &short.profile,
        &pair_candidate_id,
        &ModuleSourceIdentities::from_native_report(&long.validation.report)?,
        &ModuleSourceIdentities::from_native_report(&short.validation.report)?,
    )?;
    let validation = validate_native_profile(&profile, &pair_candidate_id)?;
    let pair_identity_sha256 = v5_pair_identity_sha256(
        authority,
        &long,
        &short,
        &validation,
        &side_targeted_lineage,
    )?;
    let factory_construction_audit =
        v5_factory_construction_audit(authority, &pair_identity_sha256, &long, &short)?;
    let provisional = V5ReconstructedPair {
        long,
        short,
        profile,
        validation,
        side_targeted_lineage,
        pair_identity_sha256,
        factory_construction_audit,
        proposal_sha256: String::new(),
        candidate_identity_material: Value::Null,
        candidate_identity_sha256: String::new(),
        candidate_id: String::new(),
        executable_semantic_sha256: String::new(),
    };
    let proposal_semantic = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_v5_evolved_pair_proposal_v1".to_owned()),
        ),
        ("proposalSeed", Value::String(proposal_seed.to_owned())),
        ("originKind", Value::String(origin_kind.to_owned())),
        (
            "proposalDeltaSha256",
            Value::String(proposal_delta_sha256.to_owned()),
        ),
        ("pair", provisional.canonical_payload(authority)?),
        (
            "pairIdentitySha256",
            Value::String(provisional.pair_identity_sha256.clone()),
        ),
        ("disposition", Value::String("materialized".to_owned())),
    ]);
    let proposal_sha256 = canonical_sha256(&proposal_semantic)?;
    let candidate_identity_material = object([
        (
            "schemaVersion",
            Value::String(V5_CANDIDATE_IDENTITY_SCHEMA.to_owned()),
        ),
        (
            "qdEngineVersion",
            Value::String(authority.qd_engine_version.clone()),
        ),
        ("originKind", Value::String(origin_kind.to_owned())),
        (
            "bidirectionalGenomeIdentitySha256",
            Value::String(provisional.pair_identity_sha256.clone()),
        ),
        (
            "pairPolicySha256",
            Value::String(authority.pair_policy_sha256.clone()),
        ),
        (
            "longModuleIdentitySha256",
            Value::String(provisional.long.identity_sha256.clone()),
        ),
        (
            "shortModuleIdentitySha256",
            Value::String(provisional.short.identity_sha256.clone()),
        ),
        (
            "longGrammarContextSha256",
            Value::String(authority.long.grammar_context.sha256.clone()),
        ),
        (
            "shortGrammarContextSha256",
            Value::String(authority.short.grammar_context.sha256.clone()),
        ),
        (
            "longCatalogSha256",
            Value::String(authority.long.catalog.sha256.clone()),
        ),
        (
            "shortCatalogSha256",
            Value::String(authority.short.catalog.sha256.clone()),
        ),
        (
            "longPolicySha256",
            Value::String(authority.long.policy.sha256.clone()),
        ),
        (
            "shortPolicySha256",
            Value::String(authority.short.policy.sha256.clone()),
        ),
        (
            "longNativeAuthoritySha256",
            Value::String(authority.long.native_authority.sha256.clone()),
        ),
        (
            "shortNativeAuthoritySha256",
            Value::String(authority.short.native_authority.sha256.clone()),
        ),
        (
            "pairCompilerAuthoritySha256",
            Value::String(authority.pair_compiler.sha256.clone()),
        ),
        (
            "compiledRawPairSha256",
            Value::String(provisional.validation.raw_profile_sha256.clone()),
        ),
        (
            "compiledProfileSha256",
            Value::String(provisional.validation.profile_snapshot_sha256.clone()),
        ),
        (
            "compiledProgramSha256",
            Value::String(provisional.validation.program_sha256.clone()),
        ),
        (
            "compiledValidationReportSha256",
            Value::String(provisional.validation.validation_report_sha256.clone()),
        ),
        (
            "orderedSideLineage",
            array(provisional.side_targeted_lineage.iter().cloned()),
        ),
        (
            "materializedPairProposalSha256",
            Value::String(proposal_sha256.clone()),
        ),
    ]);
    let candidate_identity_sha256 = canonical_sha256(&candidate_identity_material)?;
    let candidate_id = v5_id_from_sha(
        "qd_",
        &candidate_identity_sha256,
        "v5 evolved candidate identity",
    )?;
    let executable_semantic_sha256 = canonical_sha256(&object([
        (
            "schemaVersion",
            Value::String(V5_EXECUTABLE_SEMANTIC_SCHEMA.to_owned()),
        ),
        (
            "longProfileSha256",
            Value::String(provisional.long.validation.raw_profile_sha256.clone()),
        ),
        (
            "shortProfileSha256",
            Value::String(provisional.short.validation.raw_profile_sha256.clone()),
        ),
    ]))?;
    Ok(V5ReconstructedPair {
        proposal_sha256,
        candidate_identity_material,
        candidate_identity_sha256,
        candidate_id,
        executable_semantic_sha256,
        ..provisional
    })
}

fn evolved_side_state(
    pair: &V5ReconstructedPair,
    projection: &V5OperatorAuthorityProjection,
    side: &str,
) -> Result<crate::v5_operators::V5EvolvedSideState> {
    let side = exact_side(side)?;
    let (module, sealed_authority) = match side {
        "long" => (&pair.long, projection.operator_authority("long")?),
        "short" => (&pair.short, projection.operator_authority("short")?),
        _ => unreachable!("exact_side returns a closed side"),
    };
    let compiled = crate::v5_operators::V5CompiledProfileView::from_core_compilation(
        &module.program,
        &sealed_authority,
        module.genome_program_sha256.clone(),
        clone_value(&module.profile)?,
        module.validation.raw_profile_sha256.clone(),
        module.validation.profile_snapshot_sha256.clone(),
        module.validation.program_sha256.clone(),
        module.validation.validation_report_sha256.clone(),
        clone_value(&module.validation.report)?,
    )
    .map_err(|error| invalid(format!("v5 evolved side-state compilation failed: {error}")))?;
    crate::v5_operators::V5EvolvedSideState::from_recompiled_pair(
        pair.pair_identity_sha256.clone(),
        module.identity_sha256.clone(),
        &sealed_authority,
        clone_value(&module.program)?,
        compiled,
    )
    .map_err(|error| invalid(format!("v5 evolved side-state binding failed: {error}")))
}

fn evolved_lineage_row(
    kind: V5EvolvedBuildKind,
    side: &str,
    proposal_seed: &str,
    parent: &V5EvolvedParentMaterial,
    mate: Option<&V5EvolvedParentMaterial>,
    operator_trace: &Value,
    terminal_operator_plan: &Value,
    terminal_operator_application: &Value,
    program_sha256: &str,
    topology_sha256: &str,
) -> Result<Value> {
    Ok(object([
        (
            "operation",
            Value::String(match kind {
                V5EvolvedBuildKind::Mutation => "evolvable_module_mutation".to_owned(),
                V5EvolvedBuildKind::Crossover => "evolvable_module_same_side_crossover".to_owned(),
                V5EvolvedBuildKind::Immigrant => {
                    return Err(invalid("immigrant has no evolved lineage row"));
                }
            }),
        ),
        ("side", Value::String(exact_side(side)?.to_owned())),
        ("proposalSeed", Value::String(proposal_seed.to_owned())),
        (
            "parentCandidateIdentitySha256",
            Value::String(parent.candidate_identity_sha256.clone()),
        ),
        (
            "mateCandidateIdentitySha256",
            mate.map(|value| Value::String(value.candidate_identity_sha256.clone()))
                .unwrap_or(Value::Null),
        ),
        (
            "operatorTraceSha256",
            Value::String(canonical_sha256(operator_trace)?),
        ),
        (
            "terminalOperatorPlanSha256",
            Value::String(canonical_sha256(terminal_operator_plan)?),
        ),
        (
            "terminalOperatorApplicationSha256",
            Value::String(canonical_sha256(terminal_operator_application)?),
        ),
        ("genomeSha256", Value::String(program_sha256.to_owned())),
        (
            "semanticTopologySha256",
            Value::String(topology_sha256.to_owned()),
        ),
    ]))
}

fn self_hashed_value_sha(value: &Value, field_name: &str, label: &str) -> Result<String> {
    let fields = object_ref(value, label)?;
    let supplied = sha256_text(
        required(value, field_name, label)?,
        &format!("{label} SHA-256"),
    )?;
    let mut semantic = fields.clone();
    semantic.remove(field_name);
    if canonical_sha256(&Value::Object(semantic))? != supplied {
        return Err(invalid(format!("{label} identity drifted")));
    }
    Ok(supplied)
}

fn require_structural_input<'a>(value: &'a Option<Value>, label: &str) -> Result<&'a Value> {
    value.as_ref().ok_or_else(|| {
        invalid(format!(
            "v5 evolved {label} is required for structural offspring"
        ))
    })
}

/// Independently compile and identify one final later-generation pair from
/// sealed authority.  The incoming compact delta is retained verbatim only
/// after it proves it names the same deterministic proposal/programs; none
/// of its stored profile, pair, or candidate facts are trusted.
pub(crate) fn build_v5_evolved_accepted_material(
    authority: &V5SharedConstructionAuthority,
    input: V5EvolvedAcceptedBuildInput,
) -> Result<V5EvolvedAcceptedMaterial> {
    if input.generation_index < 2 || input.birth_ordinal > input.proposal_ordinal {
        return Err(invalid(
            "v5 evolved accepted material ordinal facts are invalid",
        ));
    }
    let generation_config_sha256 = sha256_text(
        &Value::String(input.generation_config_sha256.clone()),
        "v5 evolved generation config SHA-256",
    )?;
    if input.proposal_seed != v5_proposal_seed(&generation_config_sha256, input.proposal_ordinal)? {
        return Err(invalid(
            "v5 evolved proposal seed does not bind config and ordinal",
        ));
    }
    let proposal_delta_sha256 = evolved_delta_sha256(&input.proposal_delta)?;
    if evolved_delta_text(
        &input.proposal_delta,
        "generationConfigSha256",
        "v5 evolved delta generation config SHA-256",
    )? != generation_config_sha256
        || evolved_delta_text(
            &input.proposal_delta,
            "sharedAuthoritySha256",
            "v5 evolved delta shared authority SHA-256",
        )? != authority.shared_authority_sha256
        || input
            .proposal_delta
            .get("generationIndex")
            .and_then(Value::as_u64)
            != Some(input.generation_index)
        || input
            .proposal_delta
            .get("proposalOrdinal")
            .and_then(Value::as_u64)
            != Some(input.proposal_ordinal)
        || evolved_delta_text(
            &input.proposal_delta,
            "proposalSeed",
            "v5 evolved delta seed",
        )? != input.proposal_seed
        || evolved_delta_text(
            &input.proposal_delta,
            "originKind",
            "v5 evolved delta origin",
        )? != input.kind.origin_kind()
        || evolved_delta_text(
            &input.proposal_delta,
            "scheduledKind",
            "v5 evolved delta scheduled kind",
        )? != input.kind.scheduled_kind()
    {
        return Err(invalid(
            "v5 evolved proposal delta does not bind accepted build input",
        ));
    }
    let delta_long = evolved_delta_program(&input.proposal_delta, "long")?;
    let delta_short = evolved_delta_program(&input.proposal_delta, "short")?;
    let projection = authority.operator_authority_projection()?;

    let (
        long_program,
        short_program,
        long_module_lineage,
        short_module_lineage,
        side_targeted_lineage,
    ) = match input.kind {
        V5EvolvedBuildKind::Immigrant => {
            if input.parent.is_some()
                || input.mate.is_some()
                || input.parent_selection_receipt.is_some()
                || input.operator_trace.is_some()
                || input.terminal_operator_plan.is_some()
                || input.terminal_operator_application.is_some()
            {
                return Err(invalid(
                    "v5 evolved immigrant must not carry structural parent/trace facts",
                ));
            }
            let (rebuilt_long, rebuilt_short) =
                build_v5_evolved_immigrant_programs(authority, &input.proposal_seed)?;
            if rebuilt_long != delta_long || rebuilt_short != delta_short {
                return Err(invalid(
                    "v5 evolved immigrant delta programs do not reproduce from sealed authority",
                ));
            }
            if input
                .long_program
                .as_ref()
                .is_some_and(|value| value != &delta_long)
                || input
                    .short_program
                    .as_ref()
                    .is_some_and(|value| value != &delta_short)
            {
                return Err(invalid(
                    "v5 evolved immigrant explicit program drifted from compact delta",
                ));
            }
            let long_sha = canonical_sha256(&delta_long)?;
            let short_sha = canonical_sha256(&delta_short)?;
            let long_topology = v5_semantic_topology_sha256(&delta_long)?;
            let short_topology = v5_semantic_topology_sha256(&delta_short)?;
            (
                delta_long,
                delta_short,
                vec![v5_seed_module_lineage(
                    authority,
                    "long",
                    &input.proposal_seed,
                    &long_sha,
                    &long_topology,
                )?],
                vec![v5_seed_module_lineage(
                    authority,
                    "short",
                    &input.proposal_seed,
                    &short_sha,
                    &short_topology,
                )?],
                vec![
                    v5_seed_pair_lineage(authority, "long", &input.proposal_seed, &long_sha)?,
                    v5_seed_pair_lineage(authority, "short", &input.proposal_seed, &short_sha)?,
                ],
            )
        }
        V5EvolvedBuildKind::Mutation | V5EvolvedBuildKind::Crossover => {
            let parent = input
                .parent
                .as_ref()
                .ok_or_else(|| invalid("v5 evolved structural child lacks parent material"))?;
            let mate = input.mate.as_ref();
            if matches!(input.kind, V5EvolvedBuildKind::Mutation) && mate.is_some() {
                return Err(invalid("v5 evolved mutation must not name a mate"));
            }
            if matches!(input.kind, V5EvolvedBuildKind::Crossover) && mate.is_none() {
                return Err(invalid("v5 evolved crossover requires a mate"));
            }
            let selection = require_structural_input(
                &input.parent_selection_receipt,
                "parent selection receipt",
            )?;
            if selection.get("schemaVersion").and_then(Value::as_str)
                != Some("temporal_qd_v5_evolved_parent_selection_receipt_v1")
            {
                return Err(invalid(
                    "v5 evolved parent selection receipt schema is invalid",
                ));
            }
            let _ = self_hashed_value_sha(
                selection,
                "selectionSha256",
                "v5 evolved parent selection receipt",
            )?;
            let trace = require_structural_input(&input.operator_trace, "operator trace")?;
            let terminal_plan =
                require_structural_input(&input.terminal_operator_plan, "terminal operator plan")?;
            let terminal_application = require_structural_input(
                &input.terminal_operator_application,
                "terminal operator application",
            )?;
            let long_program = input
                .long_program
                .as_ref()
                .ok_or_else(|| invalid("v5 evolved structural child lacks long program"))?;
            let short_program = input
                .short_program
                .as_ref()
                .ok_or_else(|| invalid("v5 evolved structural child lacks short program"))?;
            if long_program != &delta_long || short_program != &delta_short {
                return Err(invalid(
                    "v5 evolved structural programs drifted from compact delta",
                ));
            }
            let long_changed = long_program != &parent.long_program;
            let short_changed = short_program != &parent.short_program;
            if long_changed == short_changed {
                return Err(invalid(
                    "v5 evolved structural child must change exactly one side",
                ));
            }
            let changed_side = if long_changed { "long" } else { "short" };
            let changed_program = if long_changed {
                long_program
            } else {
                short_program
            };
            let changed_sha = canonical_sha256(changed_program)?;
            let changed_topology = v5_semantic_topology_sha256(changed_program)?;
            let line = evolved_lineage_row(
                input.kind,
                changed_side,
                &input.proposal_seed,
                parent,
                mate,
                trace,
                terminal_plan,
                terminal_application,
                &changed_sha,
                &changed_topology,
            )?;
            let mut long_lineage = parent.long_module_lineage.clone();
            let mut short_lineage = parent.short_module_lineage.clone();
            if long_changed {
                long_lineage.push(line.clone());
            } else {
                short_lineage.push(line.clone());
            }
            let mut side_lineage = parent.side_targeted_lineage.clone();
            if side_lineage.len() != 2
                || side_lineage[0].get("side").and_then(Value::as_str) != Some("long")
                || side_lineage[1].get("side").and_then(Value::as_str) != Some("short")
            {
                return Err(invalid("v5 evolved parent side lineage is invalid"));
            }
            side_lineage[if long_changed { 0 } else { 1 }] = line;
            (
                clone_value(long_program)?,
                clone_value(short_program)?,
                long_lineage,
                short_lineage,
                side_lineage,
            )
        }
    };

    let long = reconstructed_module_from_evolved_program(
        authority,
        &projection,
        &input.proposal_seed,
        "long",
        &long_program,
        long_module_lineage.clone(),
    )?;
    let short = reconstructed_module_from_evolved_program(
        authority,
        &projection,
        &input.proposal_seed,
        "short",
        &short_program,
        short_module_lineage.clone(),
    )?;
    let pair = finalize_evolved_reconstructed_pair(
        authority,
        &input.proposal_seed,
        input.kind.origin_kind(),
        &proposal_delta_sha256,
        long,
        short,
        side_targeted_lineage.clone(),
    )?;
    let construction_audit = match input.kind {
        V5EvolvedBuildKind::Immigrant => V5ConstructionAudit::ImmigrantFactory {
            factory_construction_audit: clone_value(&pair.factory_construction_audit)?,
        },
        V5EvolvedBuildKind::Mutation | V5EvolvedBuildKind::Crossover => {
            let parent = input.parent.as_ref().expect("checked structural parent");
            let audit = V5EvolvedConstructionAudit {
                shared_authority_sha256: authority.shared_authority_sha256.clone(),
                proposal_delta_sha256: proposal_delta_sha256.clone(),
                parent_selection_receipt_sha256: canonical_sha256(
                    input
                        .parent_selection_receipt
                        .as_ref()
                        .expect("checked selection"),
                )?,
                operator_trace_sha256: canonical_sha256(
                    input.operator_trace.as_ref().expect("checked trace"),
                )?,
                terminal_operator_plan_sha256: canonical_sha256(
                    input.terminal_operator_plan.as_ref().expect("checked plan"),
                )?,
                terminal_operator_application_sha256: canonical_sha256(
                    input
                        .terminal_operator_application
                        .as_ref()
                        .expect("checked application"),
                )?,
                parent_candidate_identity_sha256: parent.candidate_identity_sha256.clone(),
                mate_candidate_identity_sha256: input
                    .mate
                    .as_ref()
                    .map(|value| value.candidate_identity_sha256.clone()),
                candidate_identity_sha256: pair.candidate_identity_sha256.clone(),
                pair_identity_sha256: pair.pair_identity_sha256.clone(),
                long_program_sha256: pair.long.genome_program_sha256.clone(),
                short_program_sha256: pair.short.genome_program_sha256.clone(),
                compiled_program_sha256: pair.validation.program_sha256.clone(),
            };
            if matches!(input.kind, V5EvolvedBuildKind::Mutation) {
                V5ConstructionAudit::MutationTrace(audit)
            } else {
                V5ConstructionAudit::CrossoverTrace(audit)
            }
        }
    };
    let descriptor_projection = match input.kind {
        V5EvolvedBuildKind::Immigrant => {
            pair.compact_g0_descriptor_projection(authority, &input.proposal_seed)?
        }
        V5EvolvedBuildKind::Mutation | V5EvolvedBuildKind::Crossover => {
            pair.compact_evolved_descriptor_projection(authority, &input.proposal_seed)?
        }
    };
    let record = V5CompactAcceptedRecord {
        generation_index: input.generation_index,
        birth_ordinal: input.birth_ordinal,
        proposal_ordinal: input.proposal_ordinal,
        origin_kind: input.kind.origin_kind().to_owned(),
        proposal_seed: input.proposal_seed.clone(),
        proposal_delta_sha256: proposal_delta_sha256.clone(),
        shared_authority_sha256: authority.shared_authority_sha256.clone(),
        candidate_id: pair.candidate_id.clone(),
        candidate_identity_sha256: pair.candidate_identity_sha256.clone(),
        pair_identity_sha256: pair.pair_identity_sha256.clone(),
        executable_semantic_sha256: pair.executable_semantic_sha256.clone(),
        long: pair.long.compact_facts(&authority.long),
        short: pair.short.compact_facts(&authority.short),
        compiled: pair.compiled_facts(authority),
        construction_audit,
        lineage: v5_compact_candidate_lineage(&pair),
        construction_evidence_scope: v5_compact_evidence_scope()?,
        funnel_summary: v5_compact_funnel_summary(&pair)?,
        descriptor_projection,
    };
    let _ = record.to_value()?;
    let long_state = evolved_side_state(&pair, &projection, "long")?;
    let short_state = evolved_side_state(&pair, &projection, "short")?;
    let attempt_reference = attempt_reference_from_compact_record(&record)?;
    let parent_material = V5EvolvedParentMaterial {
        attempt_reference,
        candidate_id: record.candidate_id.clone(),
        candidate_identity_sha256: record.candidate_identity_sha256.clone(),
        pair_identity_sha256: record.pair_identity_sha256.clone(),
        accepted_record: record.clone(),
        proposal_delta: clone_value(&input.proposal_delta)?,
        long_program,
        short_program,
        long_module_lineage,
        short_module_lineage,
        side_targeted_lineage,
        long_state: long_state.clone(),
        short_state: short_state.clone(),
        sealed_pair: Arc::new(pair.clone()),
    };
    let ledger_candidate = object([
        ("candidateId", Value::String(record.candidate_id.clone())),
        (
            "candidateIdentitySha256",
            Value::String(record.candidate_identity_sha256.clone()),
        ),
        (
            "pairIdentitySha256",
            Value::String(record.pair_identity_sha256.clone()),
        ),
        (
            "programSha256",
            Value::String(record.compiled.program_sha256.clone()),
        ),
        (
            "sourceProfileSha256",
            Value::String(record.compiled.raw_pair_sha256.clone()),
        ),
        (
            "profileSnapshotSha256",
            Value::String(record.compiled.profile_snapshot_sha256.clone()),
        ),
        (
            "canonicalEvidenceIdentitySha256",
            Value::String(record.executable_semantic_sha256.clone()),
        ),
    ]);
    Ok(V5EvolvedAcceptedMaterial {
        proposal_delta: input.proposal_delta,
        record,
        ledger_candidate,
        long_state,
        short_state,
        parent_material,
    })
}

fn parent_material_payload_value_from_parts(
    accepted_record: &V5CompactAcceptedRecord,
    proposal_delta: &Value,
    long_module_lineage: &[Value],
    short_module_lineage: &[Value],
    side_targeted_lineage: &[Value],
) -> Result<Value> {
    let semantic = object([
        (
            "schemaVersion",
            Value::String(V5_EVOLVED_PARENT_MATERIAL_SCHEMA.to_owned()),
        ),
        ("acceptedRecord", accepted_record.to_value()?),
        ("proposalDelta", clone_value(proposal_delta)?),
        (
            "longModuleLineage",
            array(long_module_lineage.iter().cloned()),
        ),
        (
            "shortModuleLineage",
            array(short_module_lineage.iter().cloned()),
        ),
        (
            "sideTargetedLineage",
            array(side_targeted_lineage.iter().cloned()),
        ),
    ]);
    let mut fields = semantic
        .as_object()
        .expect("constructed v5 evolved parent material")
        .clone();
    fields.insert(
        "parentMaterialSha256".to_owned(),
        Value::String(canonical_sha256(&semantic)?),
    );
    Ok(Value::Object(fields))
}

fn parent_material_payload_value(material: &V5EvolvedParentMaterial) -> Result<Value> {
    parent_material_payload_value_from_parts(
        &material.accepted_record,
        &material.proposal_delta,
        &material.long_module_lineage,
        &material.short_module_lineage,
        &material.side_targeted_lineage,
    )
}

/// Build the opaque archive reference for a compiler-validated compact
/// parent.  The selector may retain this value, but it cannot interpret it:
/// `load_v5_evolved_parent` reparses the record/delta and recompiles both
/// sides before exposing selection state.
pub(crate) fn parent_reference_from_v5_evolved_material(
    material: &V5EvolvedParentMaterial,
) -> Result<ParentReference> {
    if material.attempt_reference.candidate_id != material.candidate_id
        || material.attempt_reference.candidate_identity_sha256
            != material.candidate_identity_sha256
        || material.attempt_reference.accepted_record_sha256
            != material.accepted_record.record_sha256()?
        || material.attempt_reference.long_program_sha256
            != canonical_sha256(&material.long_program)?
        || material.attempt_reference.short_program_sha256
            != canonical_sha256(&material.short_program)?
        || material.pair_identity_sha256 != material.accepted_record.pair_identity_sha256
    {
        return Err(invalid("v5 evolved parent material binding drifted"));
    }
    Ok(ParentReference {
        pair_identity_sha256: material.pair_identity_sha256.clone(),
        candidate_id: material.candidate_id.clone(),
        pair_payload: parent_material_payload_value(material)?,
        selection_audit: None,
    })
}

#[derive(Clone, Debug)]
struct V5EvolvedParentPayloadParts {
    accepted_record: V5CompactAcceptedRecord,
    proposal_delta: Value,
    long_module_lineage: Vec<Value>,
    short_module_lineage: Vec<Value>,
    side_targeted_lineage: Vec<Value>,
}

fn attempt_reference_from_compact_record(
    record: &V5CompactAcceptedRecord,
) -> Result<V5AttemptParentReference> {
    Ok(V5AttemptParentReference {
        candidate_id: record.candidate_id.clone(),
        candidate_identity_sha256: record.candidate_identity_sha256.clone(),
        accepted_record_sha256: record.record_sha256()?,
        long_program_sha256: record.long.genome_program_sha256.clone(),
        short_program_sha256: record.short.genome_program_sha256.clone(),
    })
}

fn parent_reference_snapshot_value(parent: &ParentReference) -> Result<Value> {
    parent
        .validate()
        .map_err(|error| invalid(format!("v5 evolved snapshot parent reference: {error}")))?;
    let pair_identity_sha256 = exact_sha256_string(
        &parent.pair_identity_sha256,
        "v5 evolved snapshot parent pair identity SHA-256",
    )
    .map(|_| parent.pair_identity_sha256.clone())?;
    exact_text_string(
        &parent.candidate_id,
        "v5 evolved snapshot parent candidate ID",
    )?;
    let selection_audit = parent
        .selection_audit
        .as_ref()
        .map(clone_value)
        .transpose()?
        .unwrap_or(Value::Null);
    Ok(object([
        ("pairIdentitySha256", Value::String(pair_identity_sha256)),
        ("candidateId", Value::String(parent.candidate_id.clone())),
        ("pairPayload", clone_value(&parent.pair_payload)?),
        ("selectionAudit", selection_audit),
    ]))
}

fn parent_reference_from_snapshot_value(value: &Value) -> Result<ParentReference> {
    let fields = object_ref(value, "v5 evolved snapshot parent reference")?;
    exact_value_keys(
        fields,
        &[
            "pairIdentitySha256",
            "candidateId",
            "pairPayload",
            "selectionAudit",
        ],
        "v5 evolved snapshot parent reference",
    )?;
    let selection_audit = required(
        value,
        "selectionAudit",
        "v5 evolved snapshot parent reference",
    )?;
    let parent = ParentReference {
        pair_identity_sha256: exact_sha256_value(
            required(
                value,
                "pairIdentitySha256",
                "v5 evolved snapshot parent reference",
            )?,
            "v5 evolved snapshot parent pair identity SHA-256",
        )?,
        candidate_id: exact_text_value(
            required(value, "candidateId", "v5 evolved snapshot parent reference")?,
            "v5 evolved snapshot parent candidate ID",
        )?,
        pair_payload: clone_value(required(
            value,
            "pairPayload",
            "v5 evolved snapshot parent reference",
        )?)?,
        selection_audit: if selection_audit.is_null() {
            None
        } else {
            Some(clone_value(selection_audit)?)
        },
    };
    parent
        .validate()
        .map_err(|error| invalid(format!("v5 evolved snapshot parent reference: {error}")))?;
    if parent_reference_snapshot_value(&parent)? != *value {
        return Err(invalid(
            "v5 evolved snapshot parent reference is not canonical",
        ));
    }
    Ok(parent)
}

fn parse_v5_evolved_parent_payload(value: &Value) -> Result<V5EvolvedParentPayloadParts> {
    let fields = object_ref(value, "v5 evolved parent payload")?;
    exact_value_keys(
        fields,
        &[
            "schemaVersion",
            "acceptedRecord",
            "proposalDelta",
            "longModuleLineage",
            "shortModuleLineage",
            "sideTargetedLineage",
            "parentMaterialSha256",
        ],
        "v5 evolved parent payload",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some(V5_EVOLVED_PARENT_MATERIAL_SCHEMA)
    {
        return Err(invalid("v5 evolved parent payload schema is invalid"));
    }
    let _ = self_hashed_value_sha(value, "parentMaterialSha256", "v5 evolved parent payload")?;
    Ok(V5EvolvedParentPayloadParts {
        accepted_record: V5CompactAcceptedRecord::from_value(required(
            value,
            "acceptedRecord",
            "v5 evolved parent payload",
        )?)?,
        proposal_delta: clone_value(required(
            value,
            "proposalDelta",
            "v5 evolved parent payload",
        )?)?,
        long_module_lineage: ordered_rows(
            field(value, "longModuleLineage"),
            "v5 evolved parent payload longModuleLineage",
        )?,
        short_module_lineage: ordered_rows(
            field(value, "shortModuleLineage"),
            "v5 evolved parent payload shortModuleLineage",
        )?,
        side_targeted_lineage: ordered_rows(
            field(value, "sideTargetedLineage"),
            "v5 evolved parent payload sideTargetedLineage",
        )?,
    })
}

fn validate_parent_snapshot_delta_shape(proposal_delta: &Value) -> Result<()> {
    match proposal_delta.get("schemaVersion").and_then(Value::as_str) {
        Some(V5_PROPOSAL_DELTA_SCHEMA) => validate_proposal_delta(proposal_delta),
        Some("temporal_qd_v5_evolved_proposal_delta_v1") => {
            let _ = evolved_delta_sha256(proposal_delta)?;
            Ok(())
        }
        _ => Err(invalid(
            "v5 evolved parent snapshot delta schema is unsupported",
        )),
    }
}

fn canonical_parent_reference_from_snapshot_parts(
    parent_reference: &ParentReference,
    attempt_reference: &V5AttemptParentReference,
    accepted_record: &V5CompactAcceptedRecord,
    proposal_delta: &Value,
    long_module_lineage: &[Value],
    short_module_lineage: &[Value],
    side_targeted_lineage: &[Value],
) -> Result<ParentReference> {
    parent_reference
        .validate()
        .map_err(|error| invalid(format!("v5 evolved snapshot parent reference: {error}")))?;
    let expected_attempt_reference = attempt_reference_from_compact_record(accepted_record)?;
    if attempt_reference != &expected_attempt_reference {
        return Err(invalid(
            "v5 evolved parent snapshot attempt reference does not bind compact record",
        ));
    }
    let expected_payload = parent_material_payload_value_from_parts(
        accepted_record,
        proposal_delta,
        long_module_lineage,
        short_module_lineage,
        side_targeted_lineage,
    )?;
    if parent_reference.pair_identity_sha256 != accepted_record.pair_identity_sha256
        || parent_reference.candidate_id != accepted_record.candidate_id
        || parent_reference.pair_payload != expected_payload
    {
        return Err(invalid(
            "v5 evolved parent snapshot exact parent reference drifted from compact material",
        ));
    }
    Ok(ParentReference {
        pair_identity_sha256: accepted_record.pair_identity_sha256.clone(),
        candidate_id: accepted_record.candidate_id.clone(),
        pair_payload: expected_payload,
        selection_audit: parent_reference
            .selection_audit
            .as_ref()
            .map(clone_value)
            .transpose()?,
    })
}

impl V5EvolvedParentSnapshot {
    /// Capture a selected parent once the source archive has been authenticated.
    /// The source hashes are provenance bindings only; replay uses the sealed
    /// compact values carried here and never reopens that archive.
    pub(crate) fn from_parent_reference(
        source_parent_archive_input_binding_sha256: &str,
        source_parent_archive_semantic_sha256: &str,
        parent_reference: &ParentReference,
    ) -> Result<Self> {
        let parts = parse_v5_evolved_parent_payload(&parent_reference.pair_payload)?;
        let snapshot = Self {
            source_parent_archive_input_binding_sha256: source_parent_archive_input_binding_sha256
                .to_owned(),
            source_parent_archive_semantic_sha256: source_parent_archive_semantic_sha256.to_owned(),
            parent_reference: parent_reference.clone(),
            attempt_reference: attempt_reference_from_compact_record(&parts.accepted_record)?,
            accepted_record: parts.accepted_record,
            proposal_delta: parts.proposal_delta,
            long_module_lineage: parts.long_module_lineage,
            short_module_lineage: parts.short_module_lineage,
            side_targeted_lineage: parts.side_targeted_lineage,
        };
        snapshot.validate_bindings()?;
        Ok(snapshot)
    }

    fn validate_bindings(&self) -> Result<()> {
        exact_sha256_string(
            &self.source_parent_archive_input_binding_sha256,
            "v5 evolved parent snapshot source parent archive input binding SHA-256",
        )?;
        exact_sha256_string(
            &self.source_parent_archive_semantic_sha256,
            "v5 evolved parent snapshot source parent archive semantic SHA-256",
        )?;
        validate_parent_snapshot_delta_shape(&self.proposal_delta)?;
        let _ = canonical_parent_reference_from_snapshot_parts(
            &self.parent_reference,
            &self.attempt_reference,
            &self.accepted_record,
            &self.proposal_delta,
            &self.long_module_lineage,
            &self.short_module_lineage,
            &self.side_targeted_lineage,
        )?;
        Ok(())
    }

    fn semantic_value(&self) -> Result<Value> {
        self.validate_bindings()?;
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_EVOLVED_PARENT_SNAPSHOT_SCHEMA.to_owned()),
            ),
            (
                "sourceParentArchiveInputBindingSha256",
                Value::String(self.source_parent_archive_input_binding_sha256.clone()),
            ),
            (
                "sourceParentArchiveSemanticSha256",
                Value::String(self.source_parent_archive_semantic_sha256.clone()),
            ),
            (
                "parentReference",
                parent_reference_snapshot_value(&self.parent_reference)?,
            ),
            ("attemptReference", self.attempt_reference.to_value()?),
            ("acceptedRecord", self.accepted_record.to_value()?),
            ("proposalDelta", clone_value(&self.proposal_delta)?),
            (
                "longModuleLineage",
                array(self.long_module_lineage.iter().cloned()),
            ),
            (
                "shortModuleLineage",
                array(self.short_module_lineage.iter().cloned()),
            ),
            (
                "sideTargetedLineage",
                array(self.side_targeted_lineage.iter().cloned()),
            ),
        ]))
    }

    pub(crate) fn parent_snapshot_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub(crate) fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 evolved parent snapshot")
            .clone();
        fields.insert(
            "parentSnapshotSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub(crate) fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 evolved parent snapshot")?;
        exact_value_keys(
            fields,
            &[
                "schemaVersion",
                "sourceParentArchiveInputBindingSha256",
                "sourceParentArchiveSemanticSha256",
                "parentReference",
                "attemptReference",
                "acceptedRecord",
                "proposalDelta",
                "longModuleLineage",
                "shortModuleLineage",
                "sideTargetedLineage",
                "parentSnapshotSha256",
            ],
            "v5 evolved parent snapshot",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_EVOLVED_PARENT_SNAPSHOT_SCHEMA)
        {
            return Err(invalid("v5 evolved parent snapshot schema is invalid"));
        }
        let snapshot = Self {
            source_parent_archive_input_binding_sha256: exact_sha256_value(
                required(
                    value,
                    "sourceParentArchiveInputBindingSha256",
                    "v5 evolved parent snapshot",
                )?,
                "v5 evolved parent snapshot source parent archive input binding SHA-256",
            )?,
            source_parent_archive_semantic_sha256: exact_sha256_value(
                required(
                    value,
                    "sourceParentArchiveSemanticSha256",
                    "v5 evolved parent snapshot",
                )?,
                "v5 evolved parent snapshot source parent archive semantic SHA-256",
            )?,
            parent_reference: parent_reference_from_snapshot_value(required(
                value,
                "parentReference",
                "v5 evolved parent snapshot",
            )?)?,
            attempt_reference: V5AttemptParentReference::from_value(required(
                value,
                "attemptReference",
                "v5 evolved parent snapshot",
            )?)?,
            accepted_record: V5CompactAcceptedRecord::from_value(required(
                value,
                "acceptedRecord",
                "v5 evolved parent snapshot",
            )?)?,
            proposal_delta: clone_value(required(
                value,
                "proposalDelta",
                "v5 evolved parent snapshot",
            )?)?,
            long_module_lineage: ordered_rows(
                field(value, "longModuleLineage"),
                "v5 evolved parent snapshot longModuleLineage",
            )?,
            short_module_lineage: ordered_rows(
                field(value, "shortModuleLineage"),
                "v5 evolved parent snapshot shortModuleLineage",
            )?,
            side_targeted_lineage: ordered_rows(
                field(value, "sideTargetedLineage"),
                "v5 evolved parent snapshot sideTargetedLineage",
            )?,
        };
        let supplied = exact_sha256_value(
            required(value, "parentSnapshotSha256", "v5 evolved parent snapshot")?,
            "v5 evolved parent snapshot SHA-256",
        )?;
        if supplied != snapshot.parent_snapshot_sha256()? || &snapshot.to_value()? != value {
            return Err(invalid("v5 evolved parent snapshot identity drifted"));
        }
        Ok(snapshot)
    }

    /// Return the exact archive parent only after proving the snapshot's
    /// duplicated compact fields reproduce its opaque pair payload.
    pub(crate) fn parent_reference_for_replay(&self) -> Result<ParentReference> {
        self.validate_bindings()?;
        canonical_parent_reference_from_snapshot_parts(
            &self.parent_reference,
            &self.attempt_reference,
            &self.accepted_record,
            &self.proposal_delta,
            &self.long_module_lineage,
            &self.short_module_lineage,
            &self.side_targeted_lineage,
        )
    }

    pub(crate) fn object_binding(&self) -> Result<V5EvolvedParentSnapshotObjectBinding> {
        let parent_snapshot_sha256 = self.parent_snapshot_sha256()?;
        Ok(V5EvolvedParentSnapshotObjectBinding {
            relative_path: v5_native_object_relative_path(&parent_snapshot_sha256)?,
            parent_snapshot_sha256,
        })
    }
}

impl V5EvolvedParentSnapshotObjectBinding {
    pub(crate) fn to_value(&self) -> Result<Value> {
        exact_sha256_string(
            &self.parent_snapshot_sha256,
            "v5 evolved parent snapshot object SHA-256",
        )?;
        let expected_path = v5_native_object_relative_path(&self.parent_snapshot_sha256)?;
        if self.relative_path != expected_path {
            return Err(invalid(
                "v5 evolved parent snapshot object binding path drifted",
            ));
        }
        Ok(object([
            (
                "parentSnapshotSha256",
                Value::String(self.parent_snapshot_sha256.clone()),
            ),
            ("relativePath", Value::String(expected_path)),
        ]))
    }
}

/// Replay a sealed offline parent snapshot through the exact same loader used
/// by live archive selection.  The snapshot is an authenticated local object,
/// not a substitute construction authority: the loader still recompiles both
/// programs/profiles/pair identities from `authority` before returning state.
pub(crate) fn load_v5_evolved_parent_from_snapshot(
    authority: &V5SharedConstructionAuthority,
    snapshot: &V5EvolvedParentSnapshot,
) -> Result<V5EvolvedParentMaterial> {
    let parent_reference = snapshot.parent_reference_for_replay()?;
    load_v5_evolved_parent(authority, &parent_reference)
}

fn validate_pair_matches_compact_record(
    authority: &V5SharedConstructionAuthority,
    pair: &V5ReconstructedPair,
    record: &V5CompactAcceptedRecord,
) -> Result<()> {
    let expected_lineage = v5_compact_candidate_lineage(pair);
    let expected_descriptor = match record.origin_kind.as_str() {
        "random_immigrant" => {
            pair.compact_g0_descriptor_projection(authority, &record.proposal_seed)?
        }
        "structural_offspring" => {
            pair.compact_evolved_descriptor_projection(authority, &record.proposal_seed)?
        }
        _ => return Err(invalid("v5 evolved parent record origin is invalid")),
    };
    if record.candidate_id != pair.candidate_id
        || record.candidate_identity_sha256 != pair.candidate_identity_sha256
        || record.pair_identity_sha256 != pair.pair_identity_sha256
        || record.executable_semantic_sha256 != pair.executable_semantic_sha256
        || record.long != pair.long.compact_facts(&authority.long)
        || record.short != pair.short.compact_facts(&authority.short)
        || record.compiled != pair.compiled_facts(authority)
        || record.lineage != expected_lineage
        || record.funnel_summary != v5_compact_funnel_summary(pair)?
        || record.descriptor_projection != expected_descriptor
    {
        return Err(invalid(
            "v5 evolved parent record does not bind independently reconstructed pair facts",
        ));
    }
    Ok(())
}

fn parent_material_from_reconstructed_pair(
    authority: &V5SharedConstructionAuthority,
    projection: &V5OperatorAuthorityProjection,
    pair: V5ReconstructedPair,
    record: V5CompactAcceptedRecord,
    proposal_delta: Value,
) -> Result<V5EvolvedParentMaterial> {
    validate_pair_matches_compact_record(authority, &pair, &record)?;
    let long_state = evolved_side_state(&pair, projection, "long")?;
    let short_state = evolved_side_state(&pair, projection, "short")?;
    let attempt_reference = attempt_reference_from_compact_record(&record)?;
    Ok(V5EvolvedParentMaterial {
        attempt_reference,
        candidate_id: record.candidate_id.clone(),
        candidate_identity_sha256: record.candidate_identity_sha256.clone(),
        pair_identity_sha256: record.pair_identity_sha256.clone(),
        accepted_record: record,
        proposal_delta,
        long_program: clone_value(&pair.long.program)?,
        short_program: clone_value(&pair.short.program)?,
        long_module_lineage: pair.long.lineage.clone(),
        short_module_lineage: pair.short.lineage.clone(),
        side_targeted_lineage: pair.side_targeted_lineage.clone(),
        long_state,
        short_state,
        sealed_pair: Arc::new(pair),
    })
}

/// Construct an archive parent reference from a compact G0 record.  This is
/// the only G0-to-later-generation bridge; it verifies compact reconstruction
/// before creating an opaque selector payload.
pub fn parent_reference_from_v5_compact_record(
    authority: &V5SharedConstructionAuthority,
    proposal_delta: &Value,
    record: &V5CompactAcceptedRecord,
) -> Result<ParentReference> {
    verify_reconstruct_compact_g0_record(authority, proposal_delta, record)?;
    let pair = reconstruct_g0_pair(authority, &record.proposal_seed, Some(proposal_delta))?;
    let projection = authority.operator_authority_projection()?;
    let material = parent_material_from_reconstructed_pair(
        authority,
        &projection,
        pair,
        record.clone(),
        clone_value(proposal_delta)?,
    )?;
    parent_reference_from_v5_evolved_material(&material)
}

fn validate_evolved_parent_delta_against_record(
    authority: &V5SharedConstructionAuthority,
    proposal_delta: &Value,
    record: &V5CompactAcceptedRecord,
) -> Result<()> {
    let proposal_delta_sha256 = evolved_delta_sha256(proposal_delta)?;
    let long_program = evolved_delta_program(proposal_delta, "long")?;
    let short_program = evolved_delta_program(proposal_delta, "short")?;
    if proposal_delta_sha256 != record.proposal_delta_sha256
        || evolved_delta_text(
            proposal_delta,
            "sharedAuthoritySha256",
            "v5 evolved parent delta authority SHA-256",
        )? != authority.shared_authority_sha256
        || proposal_delta
            .get("generationIndex")
            .and_then(Value::as_u64)
            != Some(record.generation_index)
        || proposal_delta
            .get("proposalOrdinal")
            .and_then(Value::as_u64)
            != Some(record.proposal_ordinal)
        || evolved_delta_text(
            proposal_delta,
            "proposalSeed",
            "v5 evolved parent delta seed",
        )? != record.proposal_seed
        || evolved_delta_text(
            proposal_delta,
            "originKind",
            "v5 evolved parent delta origin",
        )? != record.origin_kind
        || proposal_delta
            .get("terminalDisposition")
            .and_then(Value::as_str)
            != Some("accepted")
        || canonical_sha256(&long_program)? != record.long.genome_program_sha256
        || canonical_sha256(&short_program)? != record.short.genome_program_sha256
    {
        return Err(invalid(
            "v5 evolved parent delta does not bind accepted compact record",
        ));
    }
    match (
        &record.construction_audit,
        proposal_delta.get("scheduledKind").and_then(Value::as_str),
    ) {
        (V5ConstructionAudit::ImmigrantFactory { .. }, Some("random_immigrant")) => {}
        (V5ConstructionAudit::MutationTrace(audit), Some("structural_offspring"))
        | (V5ConstructionAudit::CrossoverTrace(audit), Some("same_side_crossover")) => {
            let parent = optional_parent_reference(
                required(proposal_delta, "parent", "v5 evolved parent delta")?,
                "v5 evolved parent delta parent",
            )?
            .ok_or_else(|| invalid("v5 evolved parent delta lacks structural parent"))?;
            let mate = optional_parent_reference(
                required(proposal_delta, "mate", "v5 evolved parent delta")?,
                "v5 evolved parent delta mate",
            )?;
            let receipt = required(
                proposal_delta,
                "parentSelectionReceipt",
                "v5 evolved parent delta",
            )?;
            let trace = required(
                proposal_delta,
                "terminalOperatorTrace",
                "v5 evolved parent delta",
            )?;
            let plan = required(
                proposal_delta,
                "terminalOperatorPlan",
                "v5 evolved parent delta",
            )?;
            let application = required(
                proposal_delta,
                "terminalOperatorApplication",
                "v5 evolved parent delta",
            )?;
            if receipt.is_null()
                || trace.is_null()
                || plan.is_null()
                || application.is_null()
                || audit.shared_authority_sha256 != authority.shared_authority_sha256
                || audit.proposal_delta_sha256 != proposal_delta_sha256
                || audit.parent_selection_receipt_sha256 != canonical_sha256(receipt)?
                || audit.operator_trace_sha256 != canonical_sha256(trace)?
                || audit.terminal_operator_plan_sha256 != canonical_sha256(plan)?
                || audit.terminal_operator_application_sha256 != canonical_sha256(application)?
                || audit.parent_candidate_identity_sha256 != parent.candidate_identity_sha256
                || audit.mate_candidate_identity_sha256
                    != mate
                        .as_ref()
                        .map(|value| value.candidate_identity_sha256.clone())
            {
                return Err(invalid(
                    "v5 evolved parent construction audit does not bind delta evidence",
                ));
            }
        }
        _ => {
            return Err(invalid(
                "v5 evolved parent audit kind does not match compact delta scheduling",
            ));
        }
    }
    Ok(())
}

/// Parse a selected archive parent and independently rebuild both programs,
/// profiles, and pair identities.  A content hash on the parent payload alone
/// is never sufficient: every returned state originates from the sealed v5
/// compiler and is therefore safe to hand to operator selection.
pub(crate) fn load_v5_evolved_parent(
    authority: &V5SharedConstructionAuthority,
    parent: &ParentReference,
) -> Result<V5EvolvedParentMaterial> {
    parent
        .validate()
        .map_err(|error| invalid(format!("v5 evolved parent reference: {error}")))?;
    let payload = parse_v5_evolved_parent_payload(&parent.pair_payload)?;
    let record = payload.accepted_record;
    let proposal_delta = payload.proposal_delta;
    if parent.pair_identity_sha256 != record.pair_identity_sha256
        || parent.candidate_id != record.candidate_id
    {
        return Err(invalid(
            "v5 evolved parent selector reference does not bind its compact record",
        ));
    }
    let long_lineage = payload.long_module_lineage;
    let short_lineage = payload.short_module_lineage;
    let side_lineage = payload.side_targeted_lineage;
    if side_lineage.len() != 2
        || side_lineage[0].get("side").and_then(Value::as_str) != Some("long")
        || side_lineage[1].get("side").and_then(Value::as_str) != Some("short")
    {
        return Err(invalid("v5 evolved parent payload side lineage is invalid"));
    }
    let projection = authority.operator_authority_projection()?;
    let pair = match proposal_delta.get("schemaVersion").and_then(Value::as_str) {
        Some(V5_PROPOSAL_DELTA_SCHEMA) => {
            verify_reconstruct_compact_g0_record(authority, &proposal_delta, &record)?;
            let pair =
                reconstruct_g0_pair(authority, &record.proposal_seed, Some(&proposal_delta))?;
            if long_lineage != pair.long.lineage
                || short_lineage != pair.short.lineage
                || side_lineage != pair.side_targeted_lineage
            {
                return Err(invalid("v5 G0 parent lineage payload drifted"));
            }
            pair
        }
        Some("temporal_qd_v5_evolved_proposal_delta_v1") => {
            validate_evolved_parent_delta_against_record(authority, &proposal_delta, &record)?;
            let long = reconstructed_module_from_evolved_program(
                authority,
                &projection,
                &record.proposal_seed,
                "long",
                &evolved_delta_program(&proposal_delta, "long")?,
                long_lineage,
            )?;
            let short = reconstructed_module_from_evolved_program(
                authority,
                &projection,
                &record.proposal_seed,
                "short",
                &evolved_delta_program(&proposal_delta, "short")?,
                short_lineage,
            )?;
            finalize_evolved_reconstructed_pair(
                authority,
                &record.proposal_seed,
                &record.origin_kind,
                &record.proposal_delta_sha256,
                long,
                short,
                side_lineage,
            )?
        }
        _ => {
            return Err(invalid(
                "v5 evolved parent payload delta schema is unsupported",
            ));
        }
    };
    parent_material_from_reconstructed_pair(authority, &projection, pair, record, proposal_delta)
}

/// Independently recompile and validate one opaque parent reference without
/// exposing its compiler-owned material. Fast-ephemeral readers use this at
/// the archive boundary before a reference can enter the runtime selector.
pub fn verify_v5_evolved_parent_reference(
    authority: &V5SharedConstructionAuthority,
    parent: &ParentReference,
) -> Result<()> {
    load_v5_evolved_parent(authority, parent).map(|_| ())
}

/// Compiler-owned, ephemeral pair state used only between structural steps.
/// It is intentionally not a compact accepted record: every instance is
/// rebuilt from program bytes and sealed authority before it may drive the
/// next operator choice.
#[derive(Clone, Debug)]
struct V5EvolvedPairRuntime {
    pair_identity_sha256: String,
    long_program: Value,
    short_program: Value,
    long_module_lineage: Vec<Value>,
    short_module_lineage: Vec<Value>,
    side_targeted_lineage: Vec<Value>,
    long_state: crate::v5_operators::V5EvolvedSideState,
    short_state: crate::v5_operators::V5EvolvedSideState,
}

impl V5EvolvedPairRuntime {
    fn state(&self, side: &str) -> Result<&crate::v5_operators::V5EvolvedSideState> {
        match exact_side(side)? {
            "long" => Ok(&self.long_state),
            "short" => Ok(&self.short_state),
            _ => unreachable!("exact_side returns a closed side"),
        }
    }

    fn program(&self, side: &str) -> Result<&Value> {
        match exact_side(side)? {
            "long" => Ok(&self.long_program),
            "short" => Ok(&self.short_program),
            _ => unreachable!("exact_side returns a closed side"),
        }
    }
}

/// A sealed pair compiler/admission adapter for later-generation operators.
/// The adapter is immutable; after every accepted step the transaction must
/// use `advance_evolved_operator` (or the crossover equivalent) to obtain a
/// new adapter whose two side states were freshly compiled as one pair.
#[derive(Clone, Debug)]
pub(crate) struct V5SealedEvolvedPairRecompiler {
    authority: V5SharedConstructionAuthority,
    projection: V5OperatorAuthorityProjection,
    proposal_seed: String,
    runtime: V5EvolvedPairRuntime,
}

fn operator_error(error: V5Error) -> crate::v5_operators::V5OperatorError {
    crate::v5_operators::V5OperatorError::Invalid(error.to_string())
}

fn runtime_step_lineage(
    proposal_seed: &str,
    side: &str,
    parent_pair_identity_sha256: &str,
    parent_program_sha256: &str,
    child_program_sha256: &str,
    native_plan: &Value,
    legacy_choice: &Value,
    trace: &Value,
) -> Result<Value> {
    Ok(object([
        (
            "schemaVersion",
            Value::String("temporal_qd_v5_evolved_pair_step_lineage_v1".to_owned()),
        ),
        (
            "operation",
            Value::String("evolvable_module_intermediate_step".to_owned()),
        ),
        ("side", Value::String(exact_side(side)?.to_owned())),
        ("proposalSeed", Value::String(proposal_seed.to_owned())),
        (
            "parentPairIdentitySha256",
            Value::String(parent_pair_identity_sha256.to_owned()),
        ),
        (
            "parentProgramSha256",
            Value::String(parent_program_sha256.to_owned()),
        ),
        (
            "childProgramSha256",
            Value::String(child_program_sha256.to_owned()),
        ),
        (
            "nativePlanSha256",
            Value::String(canonical_sha256(native_plan)?),
        ),
        (
            "legacyChoiceSha256",
            Value::String(canonical_sha256(legacy_choice)?),
        ),
        (
            "operatorTraceSha256",
            Value::String(canonical_sha256(trace)?),
        ),
    ]))
}

fn runtime_crossover_lineage(
    proposal_seed: &str,
    delta: &crate::v5_operators::V5EvolvedSameSideCrossoverDelta,
) -> Result<Value> {
    Ok(object([
        (
            "schemaVersion",
            Value::String("temporal_qd_v5_evolved_pair_step_lineage_v1".to_owned()),
        ),
        (
            "operation",
            Value::String("evolvable_module_intermediate_crossover".to_owned()),
        ),
        ("side", Value::String(exact_side(&delta.side)?.to_owned())),
        ("proposalSeed", Value::String(proposal_seed.to_owned())),
        (
            "parentPairIdentitySha256",
            Value::String(delta.recipient_pair_identity_sha256.clone()),
        ),
        (
            "parentProgramSha256",
            Value::String(delta.recipient_program_sha256.clone()),
        ),
        (
            "childProgramSha256",
            Value::String(delta.child_program_sha256.clone()),
        ),
        (
            "donorPairIdentitySha256",
            Value::String(delta.donor_pair_identity_sha256.clone()),
        ),
        (
            "donorProgramSha256",
            Value::String(delta.donor_program_sha256.clone()),
        ),
        (
            "nativePlanSha256",
            Value::String(canonical_sha256(&delta.native_plan)?),
        ),
        (
            "selectionSha256",
            Value::String(canonical_sha256(&delta.selection)?),
        ),
        (
            "operatorTraceSha256",
            Value::String(canonical_sha256(&delta.trace)?),
        ),
    ]))
}

fn rebuild_runtime_pair(
    authority: &V5SharedConstructionAuthority,
    projection: &V5OperatorAuthorityProjection,
    proposal_seed: &str,
    long_program: Value,
    short_program: Value,
    long_module_lineage: Vec<Value>,
    short_module_lineage: Vec<Value>,
    side_targeted_lineage: Vec<Value>,
    runtime_receipt: Value,
) -> Result<V5EvolvedPairRuntime> {
    if side_targeted_lineage.len() != 2
        || side_targeted_lineage[0].get("side").and_then(Value::as_str) != Some("long")
        || side_targeted_lineage[1].get("side").and_then(Value::as_str) != Some("short")
    {
        return Err(invalid("v5 evolved runtime pair side lineage is invalid"));
    }
    let long = reconstructed_module_from_evolved_program(
        authority,
        projection,
        proposal_seed,
        "long",
        &long_program,
        long_module_lineage.clone(),
    )?;
    let short = reconstructed_module_from_evolved_program(
        authority,
        projection,
        proposal_seed,
        "short",
        &short_program,
        short_module_lineage.clone(),
    )?;
    // This SHA is intentionally an ephemeral compiler receipt, never a
    // durable proposal delta.  Pair identity depends on fresh program/profile
    // and lineage reconstruction; the terminal accepted materializer later
    // replaces it with the all-attempt compact delta identity.
    let receipt_sha256 = canonical_sha256(&runtime_receipt)?;
    let pair = finalize_evolved_reconstructed_pair(
        authority,
        proposal_seed,
        "structural_offspring",
        &receipt_sha256,
        long,
        short,
        side_targeted_lineage.clone(),
    )?;
    let long_state = evolved_side_state(&pair, projection, "long")?;
    let short_state = evolved_side_state(&pair, projection, "short")?;
    Ok(V5EvolvedPairRuntime {
        pair_identity_sha256: pair.pair_identity_sha256,
        long_program,
        short_program,
        long_module_lineage,
        short_module_lineage,
        side_targeted_lineage,
        long_state,
        short_state,
    })
}

impl V5SealedEvolvedPairRecompiler {
    fn from_verified_parent_owned(
        authority: &V5SharedConstructionAuthority,
        parent: V5EvolvedParentMaterial,
        proposal_seed: &str,
    ) -> Result<Self> {
        let proposal_seed = sha256_text(
            &Value::String(proposal_seed.to_owned()),
            "v5 evolved recompiler proposal seed",
        )?;
        let projection = authority.operator_authority_projection()?;
        Ok(Self {
            authority: authority.clone(),
            projection,
            proposal_seed,
            runtime: V5EvolvedPairRuntime {
                pair_identity_sha256: parent.pair_identity_sha256,
                long_program: parent.long_program,
                short_program: parent.short_program,
                long_module_lineage: parent.long_module_lineage,
                short_module_lineage: parent.short_module_lineage,
                side_targeted_lineage: parent.side_targeted_lineage,
                long_state: parent.long_state,
                short_state: parent.short_state,
            },
        })
    }

    /// Start a sealed step compiler from an archive parent. Durable callers
    /// deliberately reconstruct the reference again at this boundary.
    pub(crate) fn from_parent(
        authority: &V5SharedConstructionAuthority,
        parent: &V5EvolvedParentMaterial,
        proposal_seed: &str,
    ) -> Result<Self> {
        let parent_reference = parent_reference_from_v5_evolved_material(parent)?;
        let parent = load_v5_evolved_parent(authority, &parent_reference)?;
        Self::from_verified_parent_owned(authority, parent, proposal_seed)
    }

    /// Fast-ephemeral seam for opaque material already reconstructed by
    /// `load_v5_evolved_parent` under this exact transaction authority.
    pub(crate) fn from_verified_parent(
        authority: &V5SharedConstructionAuthority,
        parent: &V5EvolvedParentMaterial,
        proposal_seed: &str,
    ) -> Result<Self> {
        Self::from_verified_parent_owned(authority, parent.clone(), proposal_seed)
    }

    /// Alias retained for transaction code that prefers constructor spelling.
    pub(crate) fn new(
        authority: &V5SharedConstructionAuthority,
        parent: &V5EvolvedParentMaterial,
        proposal_seed: &str,
    ) -> Result<Self> {
        Self::from_parent(authority, parent, proposal_seed)
    }

    pub(crate) fn pair_identity_sha256(&self) -> &str {
        &self.runtime.pair_identity_sha256
    }

    pub(crate) fn state_for_side(
        &self,
        side: &str,
    ) -> Result<crate::v5_operators::V5EvolvedSideState> {
        Ok(self.runtime.state(side)?.clone())
    }

    fn admit_evolved_child_core(&self, operator_id: &str, child_program: &Value) -> Result<()> {
        let _ = text(
            &Value::String(operator_id.to_owned()),
            "v5 evolved child operator ID",
        )?;
        let side = exact_side(&text(
            required(child_program, "direction", "v5 evolved child program")?,
            "v5 evolved child program direction",
        )?)?;
        let opposite_side = if side == "long" { "short" } else { "long" };
        let child_program_sha256 = canonical_sha256(child_program)?;
        let child_candidate_id =
            v5_module_candidate_id(&self.proposal_seed, side, &child_program_sha256)?;
        let child =
            compile_v5_module_profile(child_program, &self.projection, side, &child_candidate_id)?;
        validate_bidirectional_side_namespace_closure(&child.profile, side)?;
        let opposite = &self.runtime.state(opposite_side)?.compiled_profile;
        if required(&child.profile, "instruments", "v5 evolved child profile")?
            != required(
                opposite.profile(),
                "instruments",
                "v5 evolved opposite profile",
            )?
            || required(
                required(&child.profile, "graph", "v5 evolved child profile")?,
                "clockRequirement",
                "v5 evolved child graph",
            )? != required(
                required(opposite.profile(), "graph", "v5 evolved opposite profile")?,
                "clockRequirement",
                "v5 evolved opposite graph",
            )?
        {
            return Err(invalid(
                "v5 evolved child is incompatible with the sealed opposite module",
            ));
        }
        Ok(())
    }

    /// Reference admission retained for fallback error precedence and
    /// differential tests. Normal selection must use the changed-side probe
    /// above; only an unexpected selected rebuild failure pays this complete
    /// two-module/pair compiler sweep.
    pub(crate) fn admit_evolved_child_full_pair(
        &self,
        operator_id: &str,
        child_program: &Value,
    ) -> Result<()> {
        let _ = text(
            &Value::String(operator_id.to_owned()),
            "v5 evolved child operator ID",
        )?;
        let side = exact_side(&text(
            required(child_program, "direction", "v5 evolved child program")?,
            "v5 evolved child program direction",
        )?)?;
        let opposite_side = if side == "long" { "short" } else { "long" };
        let child_program_sha256 = canonical_sha256(child_program)?;
        let child_candidate_id =
            v5_module_candidate_id(&self.proposal_seed, side, &child_program_sha256)?;
        let child =
            compile_v5_module_profile(child_program, &self.projection, side, &child_candidate_id)?;
        let opposite_program = self.runtime.program(opposite_side)?;
        let opposite_program_sha256 = canonical_sha256(opposite_program)?;
        let opposite_candidate_id =
            v5_module_candidate_id(&self.proposal_seed, opposite_side, &opposite_program_sha256)?;
        let opposite = compile_v5_module_profile(
            opposite_program,
            &self.projection,
            opposite_side,
            &opposite_candidate_id,
        )?;
        let (long, short) = if side == "long" {
            (&child, &opposite)
        } else {
            (&opposite, &child)
        };
        let pair_candidate_id = v5_pair_candidate_id(&self.proposal_seed)?;
        let profile = compile_bidirectional_profile(
            &long.profile,
            &short.profile,
            &pair_candidate_id,
            &ModuleSourceIdentities::from_native_report(&long.native_validation_report)?,
            &ModuleSourceIdentities::from_native_report(&short.native_validation_report)?,
        )?;
        let _ = validate_native_profile(&profile, &pair_candidate_id)?;
        Ok(())
    }

    fn recompile_operator_runtime(
        &self,
        delta: &crate::v5_operators::V5EvolvedOperatorDelta,
    ) -> Result<V5EvolvedPairRuntime> {
        let side = exact_side(&delta.side)?;
        let parent_program = self.runtime.program(side)?;
        let parent_state = self.runtime.state(side)?;
        let parent_program_sha256 = canonical_sha256(parent_program)?;
        let child_program_sha256 = canonical_sha256(&delta.child_program)?;
        if delta.parent_pair_identity_sha256 != self.runtime.pair_identity_sha256
            || delta.parent_program_sha256 != parent_program_sha256
            || delta.child_program_sha256 != child_program_sha256
            || parent_state.pair_identity_sha256 != self.runtime.pair_identity_sha256
            || parent_state.program != *parent_program
            || delta.child_program == *parent_program
        {
            return Err(invalid(
                "v5 evolved operator delta does not bind the current sealed pair state",
            ));
        }
        let line = runtime_step_lineage(
            &self.proposal_seed,
            side,
            &self.runtime.pair_identity_sha256,
            &parent_program_sha256,
            &child_program_sha256,
            &delta.native_plan,
            &delta.legacy_choice,
            &delta.trace,
        )?;
        let mut long_program = clone_value(&self.runtime.long_program)?;
        let mut short_program = clone_value(&self.runtime.short_program)?;
        let mut long_lineage = self.runtime.long_module_lineage.clone();
        let mut short_lineage = self.runtime.short_module_lineage.clone();
        let mut side_lineage = self.runtime.side_targeted_lineage.clone();
        if side == "long" {
            long_program = clone_value(&delta.child_program)?;
            long_lineage.push(line.clone());
            side_lineage[0] = line;
        } else {
            short_program = clone_value(&delta.child_program)?;
            short_lineage.push(line.clone());
            side_lineage[1] = line;
        }
        let receipt = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_v5_evolved_pair_recompile_receipt_v1".to_owned()),
            ),
            ("kind", Value::String("operator_step".to_owned())),
            ("proposalSeed", Value::String(self.proposal_seed.clone())),
            (
                "parentPairIdentitySha256",
                Value::String(self.runtime.pair_identity_sha256.clone()),
            ),
            (
                "operatorTraceSha256",
                Value::String(canonical_sha256(&delta.trace)?),
            ),
        ]);
        let rebuilt = rebuild_runtime_pair(
            &self.authority,
            &self.projection,
            &self.proposal_seed,
            long_program,
            short_program,
            long_lineage,
            short_lineage,
            side_lineage,
            receipt,
        )?;
        if rebuilt.pair_identity_sha256 == self.runtime.pair_identity_sha256 {
            return Err(invalid(
                "v5 evolved pair recompiler reused the parent pair identity",
            ));
        }
        Ok(rebuilt)
    }

    fn recompiled_output(
        runtime: &V5EvolvedPairRuntime,
        side: &str,
    ) -> Result<crate::v5_operators::V5RecompiledEvolvedPair> {
        let state = runtime.state(side)?;
        Ok(crate::v5_operators::V5RecompiledEvolvedPair {
            pair_identity_sha256: runtime.pair_identity_sha256.clone(),
            module_identity_sha256: state.module_identity_sha256.clone(),
            compiled_profile: state.compiled_profile.clone(),
        })
    }

    /// Return a freshly sealed compiler after an accepted operator delta.
    /// The next operation must use `state_for_side` from this value, never the
    /// state carried by its predecessor.
    pub(crate) fn advance_evolved_operator(
        &self,
        delta: &crate::v5_operators::V5EvolvedOperatorDelta,
    ) -> Result<Self> {
        Ok(Self {
            authority: self.authority.clone(),
            projection: self.projection.clone(),
            proposal_seed: self.proposal_seed.clone(),
            runtime: self.recompile_operator_runtime(delta)?,
        })
    }

    /// Fast-ephemeral transition: compile once and retain that exact admitted
    /// runtime instead of compiling the identical delta once for evidence and
    /// again for advancement. Durable callers keep the two-pass API above.
    pub(crate) fn advance_evolved_operator_once(
        &self,
        delta: &crate::v5_operators::V5EvolvedOperatorDelta,
    ) -> Result<(Self, crate::v5_operators::V5RecompiledEvolvedPair)> {
        let runtime = self.recompile_operator_runtime(delta)?;
        let output = Self::recompiled_output(&runtime, &delta.side)?;
        Ok((
            Self {
                authority: self.authority.clone(),
                projection: self.projection.clone(),
                proposal_seed: self.proposal_seed.clone(),
                runtime,
            },
            output,
        ))
    }

    fn recompile_crossover_runtime(
        &self,
        donor: &V5EvolvedParentMaterial,
        delta: &crate::v5_operators::V5EvolvedSameSideCrossoverDelta,
    ) -> Result<V5EvolvedPairRuntime> {
        let donor_reference = parent_reference_from_v5_evolved_material(donor)?;
        let donor = load_v5_evolved_parent(&self.authority, &donor_reference)?;
        self.recompile_crossover_runtime_from_verified_donor(&donor, delta)
    }

    /// Rebuild a crossover child from donor material already reconstructed by
    /// the generation-scoped verified-parent cache. The explicit compact,
    /// program, module, and pair checks keep this fast seam fail-closed while
    /// avoiding a second full donor compilation in fast-ephemeral execution.
    fn recompile_crossover_runtime_from_verified_donor(
        &self,
        donor: &V5EvolvedParentMaterial,
        delta: &crate::v5_operators::V5EvolvedSameSideCrossoverDelta,
    ) -> Result<V5EvolvedPairRuntime> {
        let side = exact_side(&delta.side)?;
        let recipient_state = self.runtime.state(side)?;
        let recipient_program = self.runtime.program(side)?;
        if delta.recipient_pair_identity_sha256 != self.runtime.pair_identity_sha256
            || delta.recipient_module_identity_sha256 != recipient_state.module_identity_sha256
            || delta.recipient_program_sha256 != canonical_sha256(recipient_program)?
            || delta.child_program_sha256 != canonical_sha256(&delta.child_program)?
            || delta.child_program == *recipient_program
        {
            return Err(invalid(
                "v5 evolved crossover delta does not bind the current recipient state",
            ));
        }
        let donor_state = match side {
            "long" => &donor.long_state,
            "short" => &donor.short_state,
            _ => unreachable!("exact_side returns a closed side"),
        };
        let donor_program = match side {
            "long" => &donor.long_program,
            "short" => &donor.short_program,
            _ => unreachable!("exact_side returns a closed side"),
        };
        if delta.donor_pair_identity_sha256 != donor.pair_identity_sha256
            || delta.donor_module_identity_sha256 != donor_state.module_identity_sha256
            || delta.donor_program_sha256 != canonical_sha256(donor_program)?
            || donor.accepted_record.shared_authority_sha256
                != self.authority.shared_authority_sha256
            || donor.accepted_record.candidate_id != donor.candidate_id
            || donor.accepted_record.candidate_identity_sha256 != donor.candidate_identity_sha256
            || donor.accepted_record.pair_identity_sha256 != donor.pair_identity_sha256
            || donor.long_state.pair_identity_sha256 != donor.pair_identity_sha256
            || donor.short_state.pair_identity_sha256 != donor.pair_identity_sha256
            || donor.long_state.program != donor.long_program
            || donor.short_state.program != donor.short_program
            || donor.long_state.module_identity_sha256
                != donor.accepted_record.long.module_identity_sha256
            || donor.short_state.module_identity_sha256
                != donor.accepted_record.short.module_identity_sha256
            || donor.sealed_pair.pair_identity_sha256 != donor.pair_identity_sha256
            || donor.sealed_pair.long.program != donor.long_program
            || donor.sealed_pair.short.program != donor.short_program
        {
            return Err(invalid(
                "v5 evolved crossover delta does not bind the sealed donor state",
            ));
        }
        let line = runtime_crossover_lineage(&self.proposal_seed, delta)?;
        let mut long_program = clone_value(&self.runtime.long_program)?;
        let mut short_program = clone_value(&self.runtime.short_program)?;
        let mut long_lineage = self.runtime.long_module_lineage.clone();
        let mut short_lineage = self.runtime.short_module_lineage.clone();
        let mut side_lineage = self.runtime.side_targeted_lineage.clone();
        if side == "long" {
            long_program = clone_value(&delta.child_program)?;
            long_lineage.push(line.clone());
            side_lineage[0] = line;
        } else {
            short_program = clone_value(&delta.child_program)?;
            short_lineage.push(line.clone());
            side_lineage[1] = line;
        }
        let receipt = object([
            (
                "schemaVersion",
                Value::String("temporal_qd_v5_evolved_pair_recompile_receipt_v1".to_owned()),
            ),
            ("kind", Value::String("same_side_crossover".to_owned())),
            ("proposalSeed", Value::String(self.proposal_seed.clone())),
            (
                "recipientPairIdentitySha256",
                Value::String(self.runtime.pair_identity_sha256.clone()),
            ),
            (
                "donorPairIdentitySha256",
                Value::String(donor.pair_identity_sha256.clone()),
            ),
            (
                "selectionSha256",
                Value::String(canonical_sha256(&delta.selection)?),
            ),
            (
                "operatorTraceSha256",
                Value::String(canonical_sha256(&delta.trace)?),
            ),
        ]);
        let rebuilt = rebuild_runtime_pair(
            &self.authority,
            &self.projection,
            &self.proposal_seed,
            long_program,
            short_program,
            long_lineage,
            short_lineage,
            side_lineage,
            receipt,
        )?;
        if rebuilt.pair_identity_sha256 == self.runtime.pair_identity_sha256 {
            return Err(invalid(
                "v5 evolved crossover recompiler reused the recipient pair identity",
            ));
        }
        Ok(rebuilt)
    }

    /// Direct crossover recompile path.  The caller supplies the selected
    /// donor material so this boundary can verify the donor reference before
    /// it accepts the two-parent child program.
    pub(crate) fn recompile_same_side_crossover_pair(
        &self,
        donor: &V5EvolvedParentMaterial,
        delta: &crate::v5_operators::V5EvolvedSameSideCrossoverDelta,
    ) -> Result<crate::v5_operators::V5RecompiledEvolvedPair> {
        let rebuilt = self.recompile_crossover_runtime(donor, delta)?;
        Self::recompiled_output(&rebuilt, &delta.side)
    }

    /// Advance to the fresh pair state produced by a same-side crossover.
    pub(crate) fn advance_same_side_crossover(
        &self,
        donor: &V5EvolvedParentMaterial,
        delta: &crate::v5_operators::V5EvolvedSameSideCrossoverDelta,
    ) -> Result<Self> {
        Ok(Self {
            authority: self.authority.clone(),
            projection: self.projection.clone(),
            proposal_seed: self.proposal_seed.clone(),
            runtime: self.recompile_crossover_runtime(donor, delta)?,
        })
    }

    /// Fast-ephemeral crossover transition: retain the exact runtime produced
    /// for direct evidence instead of compiling the identical child again to
    /// advance the recipient state. Durable callers keep the two-pass API.
    pub(crate) fn advance_same_side_crossover_once(
        &self,
        donor: &V5EvolvedParentMaterial,
        delta: &crate::v5_operators::V5EvolvedSameSideCrossoverDelta,
    ) -> Result<(Self, crate::v5_operators::V5RecompiledEvolvedPair)> {
        let runtime = self.recompile_crossover_runtime_from_verified_donor(donor, delta)?;
        let output = Self::recompiled_output(&runtime, &delta.side)?;
        Ok((
            Self {
                authority: self.authority.clone(),
                projection: self.projection.clone(),
                proposal_seed: self.proposal_seed.clone(),
                runtime,
            },
            output,
        ))
    }
}

impl crate::v5_operators::V5EvolvedChildAdmission for V5SealedEvolvedPairRecompiler {
    fn admit_evolved_child(
        &self,
        operator_id: &str,
        child_program: &Value,
    ) -> crate::v5_operators::Result<()> {
        self.admit_evolved_child_core(operator_id, child_program)
            .map_err(operator_error)
    }
}

impl crate::v5_operators::V5EvolvedPairRecompiler for V5SealedEvolvedPairRecompiler {
    fn recompile_evolved_pair(
        &self,
        delta: &crate::v5_operators::V5EvolvedOperatorDelta,
    ) -> crate::v5_operators::Result<crate::v5_operators::V5RecompiledEvolvedPair> {
        self.recompile_operator_runtime(delta)
            .and_then(|runtime| Self::recompiled_output(&runtime, &delta.side))
            .map_err(operator_error)
    }
}

fn v5_compact_evidence_scope() -> Result<Value> {
    let semantic = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_construction_evidence_scope_v1".to_owned()),
        ),
        ("evidencePlanRotationRequired", Value::Bool(false)),
        ("lakeScopeRegenerationRequired", Value::Bool(false)),
        ("reasons", Value::Array(Vec::new())),
        ("timeframeMutationTraceSha256s", Value::Array(Vec::new())),
    ]);
    let mut fields = semantic
        .as_object()
        .expect("constructed compact evidence scope")
        .clone();
    fields.insert(
        "evidenceScopeSha256".to_owned(),
        Value::String(canonical_sha256(&semantic)?),
    );
    Ok(Value::Object(fields))
}

fn v5_compact_funnel_summary(pair: &V5ReconstructedPair) -> Result<Value> {
    let semantic = object([
        (
            "schemaVersion",
            Value::String("temporal_qd_v5_compact_funnel_summary_v1".to_owned()),
        ),
        ("candidateId", Value::String(pair.candidate_id.clone())),
        (
            "candidateIdentitySha256",
            Value::String(pair.candidate_identity_sha256.clone()),
        ),
        (
            "pairIdentitySha256",
            Value::String(pair.pair_identity_sha256.clone()),
        ),
        (
            "executableSemanticSha256",
            Value::String(pair.executable_semantic_sha256.clone()),
        ),
        (
            "longGenomeProgramSha256",
            Value::String(pair.long.genome_program_sha256.clone()),
        ),
        (
            "shortGenomeProgramSha256",
            Value::String(pair.short.genome_program_sha256.clone()),
        ),
        (
            "compiledProgramSha256",
            Value::String(pair.validation.program_sha256.clone()),
        ),
    ]);
    let mut fields = semantic
        .as_object()
        .expect("constructed compact funnel summary")
        .clone();
    fields.insert(
        "funnelSummarySha256".to_owned(),
        Value::String(canonical_sha256(&semantic)?),
    );
    Ok(Value::Object(fields))
}

/// Versioned envelope used for each ordered public proposal-funnel row.
///
/// The nested `funnelCandidate` remains the legacy stage projection consumed
/// by the reducer.  Keeping the envelope separate lets a rejected/no-op
/// attempt remain candidate-free while still occupying its immutable proposal
/// ordinal in the funnel stream.
pub(crate) const V5_PROPOSAL_FUNNEL_ENTRY_SCHEMA: &str = "temporal_qd_v5_proposal_funnel_entry_v1";

/// The admission fact represented by a public proposal-funnel stage row.
///
/// `V5CompactAcceptedRecord::funnel_summary` is a compact compiler witness;
/// it is deliberately *not* the historical `funnelCandidate` surface
/// consumed by the Python/native funnel reducer.  Keep the two projections
/// separate so a compact record cannot be mistaken for a complete stage
/// transcript.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum V5FunnelAdmission {
    /// G0's selected-only bootstrap uses its separately sealed proof instead
    /// of a canonical evidence identity at construction time.
    G0BootstrapAccepted,
    /// A normal later-generation candidate was admitted to the identity
    /// ledger.  Its executable semantic identity is the canonical evidence
    /// binding used by the native funnel.
    Accepted,
    /// Compilation reached a complete candidate, but ledger admission
    /// rejected it as a duplicate.  The candidate stage must remain visible
    /// rather than being rewritten as a candidate-free failed attempt.
    Duplicate { reason_code: String },
}

/// Project independently validated compact record facts into the exact
/// legacy-compatible candidate-stage value consumed by
/// `build_qd_generation_funnel`.
///
/// The caller still owns the outer attempt row (ordinal, entry identity, and
/// disposition).  This helper owns only facts that are established at native
/// construction time and never consults stored rich candidate bytes.
pub(crate) fn v5_funnel_candidate_projection(
    record: &V5CompactAcceptedRecord,
    admission: V5FunnelAdmission,
) -> Result<Value> {
    // Revalidate the compact record before projecting it.  In particular,
    // this proves the compiler/profile identities below were not copied from
    // an untrusted cached report.
    let _ = record.to_value()?;
    let (outcome, reasons, canonical_evidence) = match admission {
        V5FunnelAdmission::G0BootstrapAccepted => {
            ("admitted", Value::Array(Vec::new()), Value::Null)
        }
        V5FunnelAdmission::Accepted => (
            "admitted",
            Value::Array(Vec::new()),
            Value::String(record.executable_semantic_sha256.clone()),
        ),
        V5FunnelAdmission::Duplicate { reason_code } => {
            let reason = stable_attempt_code(
                &Value::String(reason_code),
                "v5 duplicate funnel admission reason",
            )?;
            (
                "rejected_duplicate",
                Value::Array(vec![Value::String(reason)]),
                Value::String(record.executable_semantic_sha256.clone()),
            )
        }
    };
    Ok(object([
        (
            "schemaVersion",
            Value::String("temporal_qd_proposal_funnel_stage_v1".to_owned()),
        ),
        ("candidateId", Value::String(record.candidate_id.clone())),
        (
            "rawSourceProfileSha256",
            Value::String(record.compiled.raw_pair_sha256.clone()),
        ),
        (
            "staticReachability",
            object([
                ("outcome", Value::String("reachable".to_owned())),
                ("reasons", Value::Array(Vec::new())),
            ]),
        ),
        (
            "nativeValidation",
            object([
                ("outcome", Value::String("valid".to_owned())),
                ("reasons", Value::Array(Vec::new())),
                (
                    "resolvedProfileSha256",
                    Value::String(record.compiled.profile_snapshot_sha256.clone()),
                ),
                (
                    "programSha256",
                    Value::String(record.compiled.program_sha256.clone()),
                ),
                (
                    "validationReportSha256",
                    Value::String(record.compiled.validation_report_sha256.clone()),
                ),
            ]),
        ),
        (
            "admission",
            object([
                ("outcome", Value::String(outcome.to_owned())),
                ("reasons", reasons),
                ("canonicalEvidenceIdentitySha256", canonical_evidence),
            ]),
        ),
    ]))
}

fn v5_compact_candidate_lineage(pair: &V5ReconstructedPair) -> Value {
    object([
        (
            "schemaVersion",
            Value::String("temporal_qd_bidirectional_candidate_lineage_v1".to_owned()),
        ),
        ("candidateId", Value::String(pair.candidate_id.clone())),
        (
            "candidateIdentitySha256",
            Value::String(pair.candidate_identity_sha256.clone()),
        ),
        (
            "pairIdentitySha256",
            Value::String(pair.pair_identity_sha256.clone()),
        ),
        (
            "orderedSideLineage",
            array(pair.side_targeted_lineage.iter().cloned()),
        ),
    ])
}

/// Reconstruct one G0 immigrant from sealed authority and emit the compact
/// record that G0 consumes directly.  The proposal delta is regenerated and
/// immediately replayed through the compiler so it is a corruption witness,
/// never an alternate construction authority.
pub(crate) fn build_v5_g0_accepted_material(
    authority: &V5SharedConstructionAuthority,
    generation_index: u64,
    birth_ordinal: u64,
    proposal_ordinal: u64,
    proposal_seed: &str,
) -> Result<V5G0AcceptedMaterial> {
    if generation_index != 1 {
        return Err(invalid(
            "native v5 G0 construction requires generation index one",
        ));
    }
    let long = build_immigrant_module(
        "long",
        proposal_seed,
        authority.long.context()?,
        &authority.long.budget,
    )?;
    let short = build_immigrant_module(
        "short",
        proposal_seed,
        authority.short.context()?,
        &authority.short.budget,
    )?;
    let proposal_delta = proposal_delta(
        proposal_ordinal,
        proposal_seed,
        "random_immigrant",
        &long,
        &short,
    )?;
    validate_proposal_delta(&proposal_delta)?;
    let pair = reconstruct_g0_pair(authority, proposal_seed, Some(&proposal_delta))?;
    let proposal_delta_sha256 = exact_sha256_value(
        required(&proposal_delta, "deltaSha256", "v5 compact proposal delta")?,
        "v5 compact proposal delta SHA-256",
    )?;
    let record = V5CompactAcceptedRecord {
        generation_index,
        birth_ordinal,
        proposal_ordinal,
        origin_kind: "random_immigrant".to_owned(),
        proposal_seed: proposal_seed.to_owned(),
        proposal_delta_sha256,
        shared_authority_sha256: authority.shared_authority_sha256.clone(),
        candidate_id: pair.candidate_id.clone(),
        candidate_identity_sha256: pair.candidate_identity_sha256.clone(),
        pair_identity_sha256: pair.pair_identity_sha256.clone(),
        executable_semantic_sha256: pair.executable_semantic_sha256.clone(),
        long: pair.long.compact_facts(&authority.long),
        short: pair.short.compact_facts(&authority.short),
        compiled: pair.compiled_facts(authority),
        construction_audit: V5ConstructionAudit::ImmigrantFactory {
            factory_construction_audit: clone_value(&pair.factory_construction_audit)?,
        },
        lineage: v5_compact_candidate_lineage(&pair),
        construction_evidence_scope: v5_compact_evidence_scope()?,
        funnel_summary: v5_compact_funnel_summary(&pair)?,
        descriptor_projection: pair.compact_g0_descriptor_projection(authority, proposal_seed)?,
    };
    // Run the exact writer-side contract before returning material to the
    // transaction.  This catches a native constructor drift just as a parsed
    // compact JSONL row would, without materializing a rich profile entry.
    let _ = record.to_value()?;
    Ok(V5G0AcceptedMaterial {
        proposal_delta,
        record,
    })
}

/// Rebuild one compact G0 accepted record from the sealed authority and its
/// exact compact delta.  A self-hash on a compact record is deliberately not
/// an admission authority: restart/adoption must prove the program, native
/// profile, pair identity, factory audit, and direct G0 descriptor can all be
/// regenerated from the immutable authority closure.
///
/// This is intentionally compact-only.  It does not materialize a legacy
/// rich `FrozenPair`/proposal entry and is therefore safe for the 4K G0
/// funnel.  Callers which also own the generation configuration should use
/// the transaction replay gate to bind the delta's ordinal to its proposal
/// seed.
pub fn verify_reconstruct_compact_g0_record(
    authority: &V5SharedConstructionAuthority,
    proposal_delta: &Value,
    record: &V5CompactAcceptedRecord,
) -> Result<()> {
    validate_proposal_delta(proposal_delta)?;
    let proposal_ordinal = required(
        proposal_delta,
        "proposalOrdinal",
        "v5 compact proposal delta",
    )?
    .as_u64()
    .ok_or_else(|| invalid("v5 compact proposal delta ordinal is invalid"))?;
    let proposal_seed = exact_sha256_value(
        required(proposal_delta, "proposalSeed", "v5 compact proposal delta")?,
        "v5 compact proposal delta seed",
    )?;
    let proposal_delta_sha256 = exact_sha256_value(
        required(proposal_delta, "deltaSha256", "v5 compact proposal delta")?,
        "v5 compact proposal delta SHA-256",
    )?;
    if record.generation_index != 1
        || record.proposal_ordinal != proposal_ordinal
        || record.proposal_seed != proposal_seed
        || record.proposal_delta_sha256 != proposal_delta_sha256
        || record.origin_kind != "random_immigrant"
        || record.shared_authority_sha256 != authority.shared_authority_sha256
    {
        return Err(invalid(
            "v5 compact record does not bind its authority/delta surface",
        ));
    }
    let rebuilt = build_v5_g0_accepted_material(
        authority,
        record.generation_index,
        record.birth_ordinal,
        record.proposal_ordinal,
        &record.proposal_seed,
    )?;
    if rebuilt.proposal_delta != *proposal_delta || rebuilt.record != *record {
        return Err(invalid(
            "v5 compact G0 record does not reproduce from sealed authority",
        ));
    }
    Ok(())
}

/// Reconstruct a preserved historical G0 pair for exact rich-entry import
/// tests/projection.  New compact transactions must call `reconstruct_g0_pair`
/// instead: this adapter is intentionally the only code allowed to retain a
/// literal pre-native topology signature in a public FrozenModule lineage.
fn project_preserved_g0_pair(
    authority: &V5SharedConstructionAuthority,
    proposal_seed: &str,
    persisted_delta: Option<&Value>,
    preserved_long_lineage: &Value,
    preserved_short_lineage: &Value,
) -> Result<V5ReconstructedPair> {
    let fresh = reconstruct_g0_pair(authority, proposal_seed, persisted_delta)?;
    let long = fresh.long.with_preserved_seed_lineage(
        authority,
        &authority.long,
        proposal_seed,
        preserved_long_lineage,
    )?;
    let short = fresh.short.with_preserved_seed_lineage(
        authority,
        &authority.short,
        proposal_seed,
        preserved_short_lineage,
    )?;
    finalize_g0_reconstructed_pair(
        authority,
        proposal_seed,
        long,
        short,
        fresh.profile,
        fresh.validation,
        fresh.side_targeted_lineage,
    )
}

/// Shared frozen material is stored exactly once in the compact journal.  The
/// object is content-addressed, so a missing or altered authority object is a
/// hard restart failure rather than an opportunity to regenerate candidates.
pub fn shared_authority_object(authority: &Value) -> Result<Value> {
    let payload = clone_value(authority)?;
    let sha256 = canonical_sha256(&payload)?;
    Ok(object([
        (
            "schemaVersion",
            Value::String(V5_SHARED_AUTHORITY_SCHEMA.to_owned()),
        ),
        ("authority", payload),
        ("authoritySha256", Value::String(sha256)),
    ]))
}

pub fn proposal_delta(
    proposal_ordinal: u64,
    proposal_seed: &str,
    origin_kind: &str,
    long: &ImmigrantModule,
    short: &ImmigrantModule,
) -> Result<Value> {
    if origin_kind != "random_immigrant" {
        return Err(invalid("initial v5 delta only admits random_immigrant"));
    }
    let semantic = object([
        (
            "schemaVersion",
            Value::String(V5_PROPOSAL_DELTA_SCHEMA.to_owned()),
        ),
        ("proposalOrdinal", Value::from(proposal_ordinal)),
        ("proposalSeed", Value::String(proposal_seed.to_owned())),
        ("originKind", Value::String(origin_kind.to_owned())),
        ("longProgram", long.program.clone()),
        ("shortProgram", short.program.clone()),
        ("longSelector", long.selector.clone()),
        ("shortSelector", short.selector.clone()),
    ]);
    let delta_sha256 = canonical_sha256(&semantic)?;
    let mut value = semantic.as_object().expect("constructed object").clone();
    value.insert("deltaSha256".to_owned(), Value::String(delta_sha256));
    Ok(Value::Object(value))
}

pub fn validate_proposal_delta(value: &Value) -> Result<()> {
    let fields = object_ref(value, "v5 compact proposal delta")?;
    let keys = [
        "schemaVersion",
        "proposalOrdinal",
        "proposalSeed",
        "originKind",
        "longProgram",
        "shortProgram",
        "longSelector",
        "shortSelector",
        "deltaSha256",
    ];
    if fields.len() != keys.len()
        || keys.iter().any(|key| !fields.contains_key(*key))
        || fields.get("schemaVersion").and_then(Value::as_str) != Some(V5_PROPOSAL_DELTA_SCHEMA)
        || fields.get("originKind").and_then(Value::as_str) != Some("random_immigrant")
    {
        return Err(invalid("v5 compact proposal delta envelope is invalid"));
    }
    validate_immigrant_program(
        required(value, "longProgram", "v5 compact proposal delta")?,
        "long",
    )?;
    validate_immigrant_program(
        required(value, "shortProgram", "v5 compact proposal delta")?,
        "short",
    )?;
    let mut semantic = fields.clone();
    let supplied = semantic
        .remove("deltaSha256")
        .and_then(|item| item.as_str().map(ToOwned::to_owned))
        .ok_or_else(|| invalid("v5 delta identity is invalid"))?;
    if supplied != canonical_sha256(&Value::Object(semantic))? {
        return Err(invalid("v5 compact proposal delta identity drifted"));
    }
    Ok(())
}

fn sha256_text(value: &Value, label: &str) -> Result<String> {
    let value = text(value, label)?;
    let hex = value
        .strip_prefix("sha256:")
        .ok_or_else(|| invalid(format!("{label} must be a SHA-256 identity")))?;
    // `str::is_ascii_lowercase` is deliberately not sufficient here: it
    // accepts g-z as well.  Python's persisted `_SHA` contract is the exact
    // lowercase hexadecimal alphabet, and accepting a broader spelling would
    // make a forged compact reference look syntactically canonical.
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(invalid(format!("{label} must be a SHA-256 identity")));
    }
    Ok(value)
}

fn exact_value_keys(fields: &Map<String, Value>, expected: &[&str], label: &str) -> Result<()> {
    if fields.len() != expected.len() || expected.iter().any(|key| !fields.contains_key(*key)) {
        return Err(invalid(format!("{label} fields are not exact")));
    }
    Ok(())
}

fn exact_value_keys_with_optional(
    fields: &Map<String, Value>,
    required_keys: &[&str],
    optional_keys: &[&str],
    label: &str,
) -> Result<()> {
    if required_keys.iter().any(|key| !fields.contains_key(*key))
        || fields.keys().any(|key| {
            !required_keys.contains(&key.as_str()) && !optional_keys.contains(&key.as_str())
        })
    {
        return Err(invalid(format!("{label} fields are not exact")));
    }
    Ok(())
}

/// Compact module facts retained for G0 selection, later duplicate detection,
/// and selected-only legacy projection.  The actual catalog/context/policy
/// payloads remain content-addressed shared authority objects, never repeated
/// in a per-candidate record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5ModuleCompactFacts {
    pub direction: String,
    /// Evolvable genome identity (`FrozenModule.identities.programSha256`).
    pub genome_program_sha256: String,
    pub raw_profile_sha256: String,
    pub profile_snapshot_sha256: String,
    /// Dashboard/native normalized executable identity
    /// (`FrozenModule.identities.nativeProgramSha256`).
    pub native_program_sha256: String,
    pub validation_report_sha256: String,
    pub module_identity_sha256: String,
    pub grammar_context_sha256: String,
    pub catalog_sha256: String,
    pub policy_sha256: String,
    pub native_authority_sha256: String,
    pub semantic_topology_sha256: String,
    pub resource_fingerprint_sha256: String,
}

impl V5ModuleCompactFacts {
    fn validate(&self, expected_direction: &str) -> Result<()> {
        if self.direction != expected_direction
            || exact_side(&self.direction)? != expected_direction
        {
            return Err(invalid(
                "v5 compact module facts are in the wrong directional slot",
            ));
        }
        for (label, value) in [
            ("genome program", &self.genome_program_sha256),
            ("raw profile", &self.raw_profile_sha256),
            ("profile snapshot", &self.profile_snapshot_sha256),
            ("native program", &self.native_program_sha256),
            ("validation report", &self.validation_report_sha256),
            ("module identity", &self.module_identity_sha256),
            ("grammar context", &self.grammar_context_sha256),
            ("catalog", &self.catalog_sha256),
            ("policy", &self.policy_sha256),
            ("native authority", &self.native_authority_sha256),
            ("semantic topology", &self.semantic_topology_sha256),
            ("resource fingerprint", &self.resource_fingerprint_sha256),
        ] {
            if sha256_text(
                &Value::String(value.clone()),
                &format!("v5 compact module {label} SHA-256"),
            )? != *value
            {
                return Err(invalid("v5 compact module SHA spelling is not canonical"));
            }
        }
        Ok(())
    }

    fn to_value(&self) -> Result<Value> {
        self.validate(&self.direction)?;
        Ok(object([
            ("direction", Value::String(self.direction.clone())),
            (
                "genomeProgramSha256",
                Value::String(self.genome_program_sha256.clone()),
            ),
            (
                "rawProfileSha256",
                Value::String(self.raw_profile_sha256.clone()),
            ),
            (
                "profileSnapshotSha256",
                Value::String(self.profile_snapshot_sha256.clone()),
            ),
            (
                "nativeProgramSha256",
                Value::String(self.native_program_sha256.clone()),
            ),
            (
                "validationReportSha256",
                Value::String(self.validation_report_sha256.clone()),
            ),
            (
                "moduleIdentitySha256",
                Value::String(self.module_identity_sha256.clone()),
            ),
            (
                "grammarContextSha256",
                Value::String(self.grammar_context_sha256.clone()),
            ),
            ("catalogSha256", Value::String(self.catalog_sha256.clone())),
            ("policySha256", Value::String(self.policy_sha256.clone())),
            (
                "nativeAuthoritySha256",
                Value::String(self.native_authority_sha256.clone()),
            ),
            (
                "semanticTopologySha256",
                Value::String(self.semantic_topology_sha256.clone()),
            ),
            (
                "resourceFingerprintSha256",
                Value::String(self.resource_fingerprint_sha256.clone()),
            ),
        ]))
    }

    fn from_value(value: &Value, expected_direction: &str) -> Result<Self> {
        let fields = object_ref(value, "v5 compact module facts")?;
        let keys = [
            "direction",
            "genomeProgramSha256",
            "rawProfileSha256",
            "profileSnapshotSha256",
            "nativeProgramSha256",
            "validationReportSha256",
            "moduleIdentitySha256",
            "grammarContextSha256",
            "catalogSha256",
            "policySha256",
            "nativeAuthoritySha256",
            "semanticTopologySha256",
            "resourceFingerprintSha256",
        ];
        exact_value_keys(fields, &keys, "v5 compact module facts")?;
        let direction = text(
            required(value, "direction", "v5 compact module facts")?,
            "v5 compact module direction",
        )?;
        if direction != expected_direction {
            return Err(invalid(
                "v5 compact module direction is incompatible with its slot",
            ));
        }
        let facts = Self {
            direction,
            genome_program_sha256: sha256_text(
                required(value, "genomeProgramSha256", "v5 compact module facts")?,
                "v5 compact module genome program SHA-256",
            )?,
            raw_profile_sha256: sha256_text(
                required(value, "rawProfileSha256", "v5 compact module facts")?,
                "v5 compact module raw profile SHA-256",
            )?,
            profile_snapshot_sha256: sha256_text(
                required(value, "profileSnapshotSha256", "v5 compact module facts")?,
                "v5 compact module profile snapshot SHA-256",
            )?,
            native_program_sha256: sha256_text(
                required(value, "nativeProgramSha256", "v5 compact module facts")?,
                "v5 compact module native program SHA-256",
            )?,
            validation_report_sha256: sha256_text(
                required(value, "validationReportSha256", "v5 compact module facts")?,
                "v5 compact module validation SHA-256",
            )?,
            module_identity_sha256: sha256_text(
                required(value, "moduleIdentitySha256", "v5 compact module facts")?,
                "v5 compact module identity SHA-256",
            )?,
            grammar_context_sha256: sha256_text(
                required(value, "grammarContextSha256", "v5 compact module facts")?,
                "v5 compact module grammar context SHA-256",
            )?,
            catalog_sha256: sha256_text(
                required(value, "catalogSha256", "v5 compact module facts")?,
                "v5 compact module catalog SHA-256",
            )?,
            policy_sha256: sha256_text(
                required(value, "policySha256", "v5 compact module facts")?,
                "v5 compact module policy SHA-256",
            )?,
            native_authority_sha256: sha256_text(
                required(value, "nativeAuthoritySha256", "v5 compact module facts")?,
                "v5 compact module native authority SHA-256",
            )?,
            semantic_topology_sha256: sha256_text(
                required(value, "semanticTopologySha256", "v5 compact module facts")?,
                "v5 compact module topology SHA-256",
            )?,
            resource_fingerprint_sha256: sha256_text(
                required(
                    value,
                    "resourceFingerprintSha256",
                    "v5 compact module facts",
                )?,
                "v5 compact module resource fingerprint SHA-256",
            )?,
        };
        facts.validate(expected_direction)?;
        if &facts.to_value()? != value {
            return Err(invalid("v5 compact module facts are not canonical"));
        }
        Ok(facts)
    }
}

/// Compiled v3 facts needed by the selector and global semantic ledger.  The
/// selected projection can resolve the actual v3 source profile through the
/// compact delta plus shared authority; the 4K construction journal carries
/// only these identities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5CompiledPairFacts {
    pub raw_pair_sha256: String,
    pub profile_snapshot_sha256: String,
    pub program_sha256: String,
    pub validation_report_sha256: String,
    pub pair_compiler_authority_sha256: String,
}

impl V5CompiledPairFacts {
    fn validate(&self) -> Result<()> {
        for (label, value) in [
            ("raw pair", &self.raw_pair_sha256),
            ("profile snapshot", &self.profile_snapshot_sha256),
            ("program", &self.program_sha256),
            ("validation report", &self.validation_report_sha256),
            (
                "pair compiler authority",
                &self.pair_compiler_authority_sha256,
            ),
        ] {
            if sha256_text(
                &Value::String(value.clone()),
                &format!("v5 compact compiled {label} SHA-256"),
            )? != *value
            {
                return Err(invalid(
                    "v5 compact compiled pair SHA spelling is not canonical",
                ));
            }
        }
        Ok(())
    }

    fn to_value(&self) -> Result<Value> {
        self.validate()?;
        Ok(object([
            ("rawPairSha256", Value::String(self.raw_pair_sha256.clone())),
            (
                "profileSnapshotSha256",
                Value::String(self.profile_snapshot_sha256.clone()),
            ),
            ("programSha256", Value::String(self.program_sha256.clone())),
            (
                "validationReportSha256",
                Value::String(self.validation_report_sha256.clone()),
            ),
            (
                "pairCompilerAuthoritySha256",
                Value::String(self.pair_compiler_authority_sha256.clone()),
            ),
        ]))
    }

    fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 compact compiled pair facts")?;
        let keys = [
            "rawPairSha256",
            "profileSnapshotSha256",
            "programSha256",
            "validationReportSha256",
            "pairCompilerAuthoritySha256",
        ];
        exact_value_keys(fields, &keys, "v5 compact compiled pair facts")?;
        let facts = Self {
            raw_pair_sha256: sha256_text(
                required(value, "rawPairSha256", "v5 compact compiled pair facts")?,
                "v5 compact raw pair SHA-256",
            )?,
            profile_snapshot_sha256: sha256_text(
                required(
                    value,
                    "profileSnapshotSha256",
                    "v5 compact compiled pair facts",
                )?,
                "v5 compact pair snapshot SHA-256",
            )?,
            program_sha256: sha256_text(
                required(value, "programSha256", "v5 compact compiled pair facts")?,
                "v5 compact pair program SHA-256",
            )?,
            validation_report_sha256: sha256_text(
                required(
                    value,
                    "validationReportSha256",
                    "v5 compact compiled pair facts",
                )?,
                "v5 compact pair validation SHA-256",
            )?,
            pair_compiler_authority_sha256: sha256_text(
                required(
                    value,
                    "pairCompilerAuthoritySha256",
                    "v5 compact compiled pair facts",
                )?,
                "v5 compact pair compiler authority SHA-256",
            )?,
        };
        facts.validate()?;
        if &facts.to_value()? != value {
            return Err(invalid("v5 compact compiled pair facts are not canonical"));
        }
        Ok(facts)
    }
}

/// Immutable construction evidence for one accepted compact record.  The
/// historical factory audit remains byte-for-byte available only in the
/// immigrant branch.  Structural children instead carry the native trace
/// bindings which make their parent selection and operator application
/// replayable; pretending that they were factory immigrants would corrupt
/// lineage and make restarts unverifiable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum V5ConstructionAudit {
    ImmigrantFactory { factory_construction_audit: Value },
    MutationTrace(V5EvolvedConstructionAudit),
    CrossoverTrace(V5EvolvedConstructionAudit),
}

/// The compact, final-step binding for an evolved child.  The complete
/// ordered multi-step history remains in the content-addressed operator trace
/// named here; these duplicated terminal facts deliberately bind that trace
/// to the exact admitted child rather than merely to some self-consistent
/// mutation object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5EvolvedConstructionAudit {
    pub shared_authority_sha256: String,
    pub proposal_delta_sha256: String,
    pub parent_selection_receipt_sha256: String,
    pub operator_trace_sha256: String,
    pub terminal_operator_plan_sha256: String,
    pub terminal_operator_application_sha256: String,
    pub parent_candidate_identity_sha256: String,
    pub mate_candidate_identity_sha256: Option<String>,
    pub candidate_identity_sha256: String,
    pub pair_identity_sha256: String,
    pub long_program_sha256: String,
    pub short_program_sha256: String,
    pub compiled_program_sha256: String,
}

impl V5EvolvedConstructionAudit {
    fn validate(&self, kind: &str) -> Result<()> {
        if kind != "mutation_trace" && kind != "crossover_trace" {
            return Err(invalid("v5 evolved construction audit kind is invalid"));
        }
        for (label, value) in [
            ("shared authority", &self.shared_authority_sha256),
            ("proposal delta", &self.proposal_delta_sha256),
            (
                "parent selection receipt",
                &self.parent_selection_receipt_sha256,
            ),
            ("operator trace", &self.operator_trace_sha256),
            (
                "terminal operator plan",
                &self.terminal_operator_plan_sha256,
            ),
            (
                "terminal operator application",
                &self.terminal_operator_application_sha256,
            ),
            (
                "parent candidate identity",
                &self.parent_candidate_identity_sha256,
            ),
            ("candidate identity", &self.candidate_identity_sha256),
            ("pair identity", &self.pair_identity_sha256),
            ("long program", &self.long_program_sha256),
            ("short program", &self.short_program_sha256),
            ("compiled program", &self.compiled_program_sha256),
        ] {
            exact_sha256_string(
                value,
                &format!("v5 evolved construction audit {label} SHA-256"),
            )?;
        }
        match (kind, &self.mate_candidate_identity_sha256) {
            ("mutation_trace", None) => {}
            ("crossover_trace", Some(value)) => {
                exact_sha256_string(
                    value,
                    "v5 evolved crossover mate candidate identity SHA-256",
                )?;
            }
            ("mutation_trace", Some(_)) => {
                return Err(invalid(
                    "v5 mutation construction audit must not name a mate",
                ));
            }
            ("crossover_trace", None) => {
                return Err(invalid("v5 crossover construction audit requires a mate"));
            }
            _ => unreachable!("kind was validated above"),
        }
        Ok(())
    }

    fn semantic_value(&self, kind: &str) -> Result<Value> {
        self.validate(kind)?;
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_EVOLVED_CONSTRUCTION_AUDIT_SCHEMA.to_owned()),
            ),
            ("kind", Value::String(kind.to_owned())),
            (
                "sharedAuthoritySha256",
                Value::String(self.shared_authority_sha256.clone()),
            ),
            (
                "proposalDeltaSha256",
                Value::String(self.proposal_delta_sha256.clone()),
            ),
            (
                "parentSelectionReceiptSha256",
                Value::String(self.parent_selection_receipt_sha256.clone()),
            ),
            (
                "operatorTraceSha256",
                Value::String(self.operator_trace_sha256.clone()),
            ),
            (
                "terminalOperatorPlanSha256",
                Value::String(self.terminal_operator_plan_sha256.clone()),
            ),
            (
                "terminalOperatorApplicationSha256",
                Value::String(self.terminal_operator_application_sha256.clone()),
            ),
            (
                "parentCandidateIdentitySha256",
                Value::String(self.parent_candidate_identity_sha256.clone()),
            ),
            (
                "mateCandidateIdentitySha256",
                self.mate_candidate_identity_sha256
                    .as_ref()
                    .map_or(Value::Null, |value| Value::String(value.clone())),
            ),
            (
                "candidateIdentitySha256",
                Value::String(self.candidate_identity_sha256.clone()),
            ),
            (
                "pairIdentitySha256",
                Value::String(self.pair_identity_sha256.clone()),
            ),
            (
                "longProgramSha256",
                Value::String(self.long_program_sha256.clone()),
            ),
            (
                "shortProgramSha256",
                Value::String(self.short_program_sha256.clone()),
            ),
            (
                "compiledProgramSha256",
                Value::String(self.compiled_program_sha256.clone()),
            ),
        ]))
    }

    fn to_value(&self, kind: &str) -> Result<Value> {
        let semantic = self.semantic_value(kind)?;
        let mut fields = semantic
            .as_object()
            .expect("constructed evolved construction audit")
            .clone();
        fields.insert(
            "auditSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    fn from_value(value: &Value, expected_kind: &str) -> Result<Self> {
        let fields = object_ref(value, "v5 evolved construction audit")?;
        let keys = [
            "schemaVersion",
            "kind",
            "sharedAuthoritySha256",
            "proposalDeltaSha256",
            "parentSelectionReceiptSha256",
            "operatorTraceSha256",
            "terminalOperatorPlanSha256",
            "terminalOperatorApplicationSha256",
            "parentCandidateIdentitySha256",
            "mateCandidateIdentitySha256",
            "candidateIdentitySha256",
            "pairIdentitySha256",
            "longProgramSha256",
            "shortProgramSha256",
            "compiledProgramSha256",
            "auditSha256",
        ];
        exact_value_keys(fields, &keys, "v5 evolved construction audit")?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_EVOLVED_CONSTRUCTION_AUDIT_SCHEMA)
            || fields.get("kind").and_then(Value::as_str) != Some(expected_kind)
        {
            return Err(invalid(
                "v5 evolved construction audit schema or kind is invalid",
            ));
        }
        let audit = Self {
            shared_authority_sha256: sha256_text(
                required(
                    value,
                    "sharedAuthoritySha256",
                    "v5 evolved construction audit",
                )?,
                "v5 evolved construction audit shared authority SHA-256",
            )?,
            proposal_delta_sha256: sha256_text(
                required(
                    value,
                    "proposalDeltaSha256",
                    "v5 evolved construction audit",
                )?,
                "v5 evolved construction audit delta SHA-256",
            )?,
            parent_selection_receipt_sha256: sha256_text(
                required(
                    value,
                    "parentSelectionReceiptSha256",
                    "v5 evolved construction audit",
                )?,
                "v5 evolved construction audit parent selection SHA-256",
            )?,
            operator_trace_sha256: sha256_text(
                required(
                    value,
                    "operatorTraceSha256",
                    "v5 evolved construction audit",
                )?,
                "v5 evolved construction audit trace SHA-256",
            )?,
            terminal_operator_plan_sha256: sha256_text(
                required(
                    value,
                    "terminalOperatorPlanSha256",
                    "v5 evolved construction audit",
                )?,
                "v5 evolved construction audit plan SHA-256",
            )?,
            terminal_operator_application_sha256: sha256_text(
                required(
                    value,
                    "terminalOperatorApplicationSha256",
                    "v5 evolved construction audit",
                )?,
                "v5 evolved construction audit application SHA-256",
            )?,
            parent_candidate_identity_sha256: sha256_text(
                required(
                    value,
                    "parentCandidateIdentitySha256",
                    "v5 evolved construction audit",
                )?,
                "v5 evolved construction audit parent identity SHA-256",
            )?,
            mate_candidate_identity_sha256: optional_sha256_text(
                required(
                    value,
                    "mateCandidateIdentitySha256",
                    "v5 evolved construction audit",
                )?,
                "v5 evolved construction audit mate identity SHA-256",
            )?,
            candidate_identity_sha256: sha256_text(
                required(
                    value,
                    "candidateIdentitySha256",
                    "v5 evolved construction audit",
                )?,
                "v5 evolved construction audit candidate identity SHA-256",
            )?,
            pair_identity_sha256: sha256_text(
                required(value, "pairIdentitySha256", "v5 evolved construction audit")?,
                "v5 evolved construction audit pair identity SHA-256",
            )?,
            long_program_sha256: sha256_text(
                required(value, "longProgramSha256", "v5 evolved construction audit")?,
                "v5 evolved construction audit long program SHA-256",
            )?,
            short_program_sha256: sha256_text(
                required(value, "shortProgramSha256", "v5 evolved construction audit")?,
                "v5 evolved construction audit short program SHA-256",
            )?,
            compiled_program_sha256: sha256_text(
                required(
                    value,
                    "compiledProgramSha256",
                    "v5 evolved construction audit",
                )?,
                "v5 evolved construction audit compiled program SHA-256",
            )?,
        };
        audit.validate(expected_kind)?;
        let supplied = sha256_text(
            required(value, "auditSha256", "v5 evolved construction audit")?,
            "v5 evolved construction audit SHA-256",
        )?;
        if supplied != canonical_sha256(&audit.semantic_value(expected_kind)?)? {
            return Err(invalid("v5 evolved construction audit identity drifted"));
        }
        if &audit.to_value(expected_kind)? != value {
            return Err(invalid("v5 evolved construction audit is not canonical"));
        }
        Ok(audit)
    }
}

fn validate_factory_audit_side(
    value: &Value,
    expected_program_sha256: Option<&str>,
    expected_topology_sha256: Option<&str>,
    expected_resource_sha256: Option<&str>,
    label: &str,
) -> Result<()> {
    let fields = object_ref(value, label)?;
    let keys = [
        "programKind",
        "codec",
        "genomeSha256",
        "semanticTopologySha256",
        "resourceFingerprintSha256",
    ];
    exact_value_keys(fields, &keys, label)?;
    if fields.get("programKind").and_then(Value::as_str) != Some(V5_PROGRAM_KIND)
        || fields.get("codec").and_then(Value::as_str) != Some(V5_CODEC)
    {
        return Err(invalid(format!("{label} program codec is invalid")));
    }
    let genome = exact_sha256_value(
        required(value, "genomeSha256", label)?,
        &format!("{label} genome SHA-256"),
    )?;
    let topology = exact_sha256_value(
        required(value, "semanticTopologySha256", label)?,
        &format!("{label} topology SHA-256"),
    )?;
    let resource = exact_sha256_value(
        required(value, "resourceFingerprintSha256", label)?,
        &format!("{label} resource SHA-256"),
    )?;
    if expected_program_sha256.is_some_and(|expected| expected != genome)
        || expected_topology_sha256.is_some_and(|expected| expected != topology)
        || expected_resource_sha256.is_some_and(|expected| expected != resource)
    {
        return Err(invalid(format!(
            "{label} does not bind compact module facts"
        )));
    }
    Ok(())
}

fn validate_factory_construction_audit(
    value: &Value,
    expected_pair_identity_sha256: Option<&str>,
    long: Option<&V5ModuleCompactFacts>,
    short: Option<&V5ModuleCompactFacts>,
) -> Result<()> {
    let fields = object_ref(value, "v5 factory construction audit")?;
    let keys = [
        "schemaVersion",
        "authoritySha256",
        "pairIdentitySha256",
        "sides",
        "auditSha256",
    ];
    exact_value_keys(fields, &keys, "v5 factory construction audit")?;
    if fields.get("schemaVersion").and_then(Value::as_str) != Some(V5_FACTORY_AUDIT_SCHEMA) {
        return Err(invalid("v5 factory construction audit schema is invalid"));
    }
    exact_sha256_value(
        required(value, "authoritySha256", "v5 factory construction audit")?,
        "v5 factory construction audit authority SHA-256",
    )?;
    let pair_identity_sha256 = exact_sha256_value(
        required(value, "pairIdentitySha256", "v5 factory construction audit")?,
        "v5 factory construction audit pair identity SHA-256",
    )?;
    if expected_pair_identity_sha256.is_some_and(|expected| expected != pair_identity_sha256) {
        return Err(invalid(
            "v5 factory construction audit pair identity drifted",
        ));
    }
    let sides = object_ref(
        required(value, "sides", "v5 factory construction audit")?,
        "v5 factory construction audit sides",
    )?;
    exact_value_keys(
        sides,
        &["long", "short"],
        "v5 factory construction audit sides",
    )?;
    validate_factory_audit_side(
        required(value, "sides", "v5 factory construction audit")?
            .get("long")
            .ok_or_else(|| invalid("v5 factory construction audit lacks long side"))?,
        long.map(|facts| facts.genome_program_sha256.as_str()),
        long.map(|facts| facts.semantic_topology_sha256.as_str()),
        long.map(|facts| facts.resource_fingerprint_sha256.as_str()),
        "v5 factory construction audit long side",
    )?;
    validate_factory_audit_side(
        required(value, "sides", "v5 factory construction audit")?
            .get("short")
            .ok_or_else(|| invalid("v5 factory construction audit lacks short side"))?,
        short.map(|facts| facts.genome_program_sha256.as_str()),
        short.map(|facts| facts.semantic_topology_sha256.as_str()),
        short.map(|facts| facts.resource_fingerprint_sha256.as_str()),
        "v5 factory construction audit short side",
    )?;
    let mut semantic = fields.clone();
    let supplied = exact_sha256_value(
        semantic
            .remove("auditSha256")
            .as_ref()
            .ok_or_else(|| invalid("v5 factory construction audit lacks identity"))?,
        "v5 factory construction audit SHA-256",
    )?;
    if supplied != canonical_sha256(&Value::Object(semantic))? {
        return Err(invalid("v5 factory construction audit identity drifted"));
    }
    Ok(())
}

impl V5ConstructionAudit {
    fn kind(&self) -> &'static str {
        match self {
            Self::ImmigrantFactory { .. } => "immigrant_factory",
            Self::MutationTrace(_) => "mutation_trace",
            Self::CrossoverTrace(_) => "crossover_trace",
        }
    }

    fn validate_for_record(
        &self,
        origin_kind: &str,
        shared_authority_sha256: &str,
        proposal_delta_sha256: &str,
        candidate_identity_sha256: &str,
        pair_identity_sha256: &str,
        long: &V5ModuleCompactFacts,
        short: &V5ModuleCompactFacts,
        compiled: &V5CompiledPairFacts,
    ) -> Result<()> {
        match (origin_kind, self) {
            (
                "random_immigrant",
                Self::ImmigrantFactory {
                    factory_construction_audit,
                },
            ) => {
                validate_factory_construction_audit(
                    factory_construction_audit,
                    Some(pair_identity_sha256),
                    Some(long),
                    Some(short),
                )?;
            }
            ("structural_offspring", Self::MutationTrace(audit))
            | ("structural_offspring", Self::CrossoverTrace(audit)) => {
                let kind = self.kind();
                audit.validate(kind)?;
                if audit.shared_authority_sha256 != shared_authority_sha256
                    || audit.proposal_delta_sha256 != proposal_delta_sha256
                    || audit.candidate_identity_sha256 != candidate_identity_sha256
                    || audit.pair_identity_sha256 != pair_identity_sha256
                    || audit.long_program_sha256 != long.genome_program_sha256
                    || audit.short_program_sha256 != short.genome_program_sha256
                    || audit.compiled_program_sha256 != compiled.program_sha256
                {
                    return Err(invalid(
                        "v5 evolved construction audit does not bind compact record facts",
                    ));
                }
            }
            ("random_immigrant", _) | ("structural_offspring", _) => {
                return Err(invalid(
                    "v5 construction audit kind is incompatible with record origin",
                ));
            }
            _ => return Err(invalid("v5 compact record origin is invalid")),
        }
        Ok(())
    }

    fn semantic_value(&self) -> Result<Value> {
        let kind = self.kind();
        let payload = match self {
            Self::ImmigrantFactory {
                factory_construction_audit,
            } => {
                validate_factory_construction_audit(factory_construction_audit, None, None, None)?;
                object([(
                    "factoryConstructionAudit",
                    clone_value(factory_construction_audit)?,
                )])
            }
            Self::MutationTrace(audit) | Self::CrossoverTrace(audit) => {
                object([("evolvedAudit", audit.to_value(kind)?)])
            }
        };
        let mut fields = Map::new();
        fields.insert(
            "schemaVersion".to_owned(),
            Value::String(V5_CONSTRUCTION_AUDIT_SCHEMA.to_owned()),
        );
        fields.insert("kind".to_owned(), Value::String(kind.to_owned()));
        let payload = payload
            .as_object()
            .expect("constructed construction audit payload");
        for (key, value) in payload {
            fields.insert(key.clone(), value.clone());
        }
        Ok(Value::Object(fields))
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 construction audit")
            .clone();
        fields.insert(
            "constructionAuditSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 construction audit")?;
        let kind = exact_text_value(
            required(value, "kind", "v5 construction audit")?,
            "v5 construction audit kind",
        )?;
        let expected = match kind.as_str() {
            "immigrant_factory" => vec![
                "schemaVersion",
                "kind",
                "factoryConstructionAudit",
                "constructionAuditSha256",
            ],
            "mutation_trace" | "crossover_trace" => vec![
                "schemaVersion",
                "kind",
                "evolvedAudit",
                "constructionAuditSha256",
            ],
            _ => return Err(invalid("v5 construction audit kind is invalid")),
        };
        exact_value_keys(fields, &expected, "v5 construction audit")?;
        if fields.get("schemaVersion").and_then(Value::as_str) != Some(V5_CONSTRUCTION_AUDIT_SCHEMA)
        {
            return Err(invalid("v5 construction audit schema is invalid"));
        }
        let audit = match kind.as_str() {
            "immigrant_factory" => {
                let factory = clone_value(required(
                    value,
                    "factoryConstructionAudit",
                    "v5 construction audit",
                )?)?;
                validate_factory_construction_audit(&factory, None, None, None)?;
                Self::ImmigrantFactory {
                    factory_construction_audit: factory,
                }
            }
            "mutation_trace" => Self::MutationTrace(V5EvolvedConstructionAudit::from_value(
                required(value, "evolvedAudit", "v5 construction audit")?,
                "mutation_trace",
            )?),
            "crossover_trace" => Self::CrossoverTrace(V5EvolvedConstructionAudit::from_value(
                required(value, "evolvedAudit", "v5 construction audit")?,
                "crossover_trace",
            )?),
            _ => unreachable!("kind was checked above"),
        };
        let supplied = exact_sha256_value(
            required(value, "constructionAuditSha256", "v5 construction audit")?,
            "v5 construction audit SHA-256",
        )?;
        if supplied != canonical_sha256(&audit.semantic_value()?)? {
            return Err(invalid("v5 construction audit identity drifted"));
        }
        if &audit.to_value()? != value {
            return Err(invalid("v5 construction audit is not canonical"));
        }
        Ok(audit)
    }
}

fn validate_compact_candidate_lineage(
    value: &Value,
    candidate_id: &str,
    candidate_identity_sha256: &str,
    pair_identity_sha256: &str,
) -> Result<()> {
    let fields = object_ref(value, "v5 compact candidate lineage")?;
    exact_value_keys(
        fields,
        &[
            "schemaVersion",
            "candidateId",
            "candidateIdentitySha256",
            "pairIdentitySha256",
            "orderedSideLineage",
        ],
        "v5 compact candidate lineage",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some("temporal_qd_bidirectional_candidate_lineage_v1")
        || exact_text_value(
            required(value, "candidateId", "v5 compact candidate lineage")?,
            "v5 compact lineage candidate ID",
        )? != candidate_id
        || exact_sha256_value(
            required(
                value,
                "candidateIdentitySha256",
                "v5 compact candidate lineage",
            )?,
            "v5 compact lineage candidate identity SHA-256",
        )? != candidate_identity_sha256
        || exact_sha256_value(
            required(value, "pairIdentitySha256", "v5 compact candidate lineage")?,
            "v5 compact lineage pair identity SHA-256",
        )? != pair_identity_sha256
    {
        return Err(invalid("v5 compact candidate lineage binding drifted"));
    }
    let side_lineage = array_ref(
        required(value, "orderedSideLineage", "v5 compact candidate lineage")?,
        "v5 compact ordered side lineage",
    )?;
    if side_lineage.len() != 2 {
        return Err(invalid(
            "v5 compact candidate lineage requires two ordered side entries",
        ));
    }
    for (expected_side, row) in ["long", "short"].iter().zip(side_lineage) {
        let row_fields = object_ref(row, "v5 compact side lineage")?;
        if row_fields.is_empty()
            || exact_text_value(
                required(row, "side", "v5 compact side lineage")?,
                "v5 compact side lineage direction",
            )? != *expected_side
        {
            return Err(invalid(
                "v5 compact side lineage order or direction drifted",
            ));
        }
        // A lineage row is public Python identity material whose allowed
        // operation-specific fields vary by evolved family.  The compact
        // record does not reinterpret it; its containing record self-hash and
        // authority-bound reconstruction below bind the exact canonical row.
        clone_value(row)?;
    }
    Ok(())
}

fn validate_compact_evidence_scope(value: &Value) -> Result<()> {
    let fields = object_ref(value, "v5 compact construction evidence scope")?;
    exact_value_keys(
        fields,
        &[
            "schemaVersion",
            "evidencePlanRotationRequired",
            "lakeScopeRegenerationRequired",
            "reasons",
            "timeframeMutationTraceSha256s",
            "evidenceScopeSha256",
        ],
        "v5 compact construction evidence scope",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some("temporal_qd_construction_evidence_scope_v1")
    {
        return Err(invalid(
            "v5 compact construction evidence scope schema is invalid",
        ));
    }
    let rotate = required(
        value,
        "evidencePlanRotationRequired",
        "v5 compact construction evidence scope",
    )? == &Value::Bool(true);
    let regenerate = required(
        value,
        "lakeScopeRegenerationRequired",
        "v5 compact construction evidence scope",
    )? == &Value::Bool(true);
    if rotate != regenerate {
        return Err(invalid("v5 compact evidence/lake scope flags must agree"));
    }
    let reasons = array_ref(
        required(value, "reasons", "v5 compact construction evidence scope")?,
        "v5 compact construction evidence scope reasons",
    )?;
    let traces = array_ref(
        required(
            value,
            "timeframeMutationTraceSha256s",
            "v5 compact construction evidence scope",
        )?,
        "v5 compact construction evidence scope trace identities",
    )?;
    for value in traces {
        exact_sha256_value(value, "v5 compact evidence scope trace SHA-256")?;
    }
    if rotate {
        if reasons
            != [Value::String(
                "graph_bound_indicator_timeframe_changed".to_owned(),
            )]
            || traces.is_empty()
        {
            return Err(invalid(
                "v5 compact evidence scope rotation facts are invalid",
            ));
        }
    } else if !reasons.is_empty() || !traces.is_empty() {
        return Err(invalid(
            "v5 compact evidence scope has unneeded invalidation facts",
        ));
    }
    let mut semantic = fields.clone();
    let supplied = exact_sha256_value(
        semantic
            .remove("evidenceScopeSha256")
            .as_ref()
            .ok_or_else(|| invalid("v5 compact evidence scope lacks identity"))?,
        "v5 compact evidence scope SHA-256",
    )?;
    if supplied != canonical_sha256(&Value::Object(semantic))? {
        return Err(invalid("v5 compact evidence scope identity drifted"));
    }
    Ok(())
}

fn validate_compact_funnel_summary(
    value: &Value,
    candidate_id: &str,
    candidate_identity_sha256: &str,
    pair_identity_sha256: &str,
    executable_semantic_sha256: &str,
    long: &V5ModuleCompactFacts,
    short: &V5ModuleCompactFacts,
    compiled: &V5CompiledPairFacts,
) -> Result<()> {
    let fields = object_ref(value, "v5 compact funnel summary")?;
    exact_value_keys(
        fields,
        &[
            "schemaVersion",
            "candidateId",
            "candidateIdentitySha256",
            "pairIdentitySha256",
            "executableSemanticSha256",
            "longGenomeProgramSha256",
            "shortGenomeProgramSha256",
            "compiledProgramSha256",
            "funnelSummarySha256",
        ],
        "v5 compact funnel summary",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some("temporal_qd_v5_compact_funnel_summary_v1")
        || exact_text_value(
            required(value, "candidateId", "v5 compact funnel summary")?,
            "v5 compact funnel candidate ID",
        )? != candidate_id
        || exact_sha256_value(
            required(
                value,
                "candidateIdentitySha256",
                "v5 compact funnel summary",
            )?,
            "v5 compact funnel candidate identity SHA-256",
        )? != candidate_identity_sha256
        || exact_sha256_value(
            required(value, "pairIdentitySha256", "v5 compact funnel summary")?,
            "v5 compact funnel pair identity SHA-256",
        )? != pair_identity_sha256
        || exact_sha256_value(
            required(
                value,
                "executableSemanticSha256",
                "v5 compact funnel summary",
            )?,
            "v5 compact funnel executable semantic SHA-256",
        )? != executable_semantic_sha256
        || exact_sha256_value(
            required(
                value,
                "longGenomeProgramSha256",
                "v5 compact funnel summary",
            )?,
            "v5 compact funnel long genome SHA-256",
        )? != long.genome_program_sha256
        || exact_sha256_value(
            required(
                value,
                "shortGenomeProgramSha256",
                "v5 compact funnel summary",
            )?,
            "v5 compact funnel short genome SHA-256",
        )? != short.genome_program_sha256
        || exact_sha256_value(
            required(value, "compiledProgramSha256", "v5 compact funnel summary")?,
            "v5 compact funnel compiled program SHA-256",
        )? != compiled.program_sha256
    {
        return Err(invalid("v5 compact funnel summary binding drifted"));
    }
    let mut semantic = fields.clone();
    let supplied = exact_sha256_value(
        semantic
            .remove("funnelSummarySha256")
            .as_ref()
            .ok_or_else(|| invalid("v5 compact funnel summary lacks identity"))?,
        "v5 compact funnel summary SHA-256",
    )?;
    if supplied != canonical_sha256(&Value::Object(semantic))? {
        return Err(invalid("v5 compact funnel summary identity drifted"));
    }
    Ok(())
}

fn validate_compact_descriptor_projection(
    value: &Value,
    candidate_id: &str,
    candidate_identity_sha256: &str,
    pair_identity_sha256: &str,
    long: &V5ModuleCompactFacts,
    short: &V5ModuleCompactFacts,
    compiled: &V5CompiledPairFacts,
) -> Result<()> {
    let fields = object_ref(value, "v5 compact descriptor projection")?;
    exact_value_keys(
        fields,
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
        "v5 compact descriptor projection",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some(crate::g0::DESCRIPTOR_PROJECTION_SCHEMA)
        || exact_text_value(
            required(value, "candidateId", "v5 compact descriptor projection")?,
            "v5 compact descriptor candidate ID",
        )? != candidate_id
        || exact_sha256_value(
            required(
                value,
                "candidateIdentitySha256",
                "v5 compact descriptor projection",
            )?,
            "v5 compact descriptor candidate identity SHA-256",
        )? != candidate_identity_sha256
        || exact_sha256_value(
            required(
                value,
                "pairIdentitySha256",
                "v5 compact descriptor projection",
            )?,
            "v5 compact descriptor pair identity SHA-256",
        )? != pair_identity_sha256
        || exact_sha256_value(
            required(
                value,
                "longCatalogSha256",
                "v5 compact descriptor projection",
            )?,
            "v5 compact descriptor long catalog SHA-256",
        )? != long.catalog_sha256
        || exact_sha256_value(
            required(
                value,
                "shortCatalogSha256",
                "v5 compact descriptor projection",
            )?,
            "v5 compact descriptor short catalog SHA-256",
        )? != short.catalog_sha256
        || exact_sha256_value(
            required(
                value,
                "nativeValidationReportSha256",
                "v5 compact descriptor projection",
            )?,
            "v5 compact descriptor validation SHA-256",
        )? != compiled.validation_report_sha256
    {
        return Err(invalid("v5 compact descriptor projection binding drifted"));
    }
    exact_sha256_value(
        required(
            value,
            "staticReachabilityReportSha256",
            "v5 compact descriptor projection",
        )?,
        "v5 compact descriptor reachability SHA-256",
    )?;
    let liveness = object_ref(
        required(
            value,
            "perSideLivenessProof",
            "v5 compact descriptor projection",
        )?,
        "v5 compact descriptor liveness proof",
    )?;
    exact_value_keys(
        liveness,
        &["long", "short"],
        "v5 compact descriptor liveness proof",
    )?;
    for side in ["long", "short"] {
        let proof = object_ref(
            liveness
                .get(side)
                .ok_or_else(|| invalid("v5 compact descriptor liveness lacks side"))?,
            "v5 compact descriptor side liveness proof",
        )?;
        exact_value_keys(
            proof,
            &[
                "entryActionRouteCount",
                "reachableEntryActionRouteCount",
                "potential",
            ],
            "v5 compact descriptor side liveness proof",
        )?;
        if proof
            .get("entryActionRouteCount")
            .and_then(Value::as_u64)
            .is_none()
            || proof
                .get("reachableEntryActionRouteCount")
                .and_then(Value::as_u64)
                .is_none()
            || proof.get("potential") != Some(&Value::Bool(true))
        {
            return Err(invalid("v5 compact descriptor liveness proof is invalid"));
        }
    }
    let vector = object_ref(
        required(
            value,
            "descriptorVector",
            "v5 compact descriptor projection",
        )?,
        "v5 compact descriptor vector",
    )?;
    exact_value_keys(
        vector,
        &crate::g0::DESCRIPTOR_AXES,
        "v5 compact descriptor vector",
    )?;
    for axis in crate::g0::DESCRIPTOR_AXES {
        exact_text_value(
            vector
                .get(axis)
                .ok_or_else(|| invalid("v5 compact descriptor vector lacks axis"))?,
            "v5 compact descriptor axis",
        )?;
    }
    let mut semantic = fields.clone();
    let supplied = exact_sha256_value(
        semantic
            .remove("descriptorProjectionSha256")
            .as_ref()
            .ok_or_else(|| invalid("v5 compact descriptor projection lacks identity"))?,
        "v5 compact descriptor projection SHA-256",
    )?;
    if supplied != canonical_sha256(&Value::Object(semantic))? {
        return Err(invalid("v5 compact descriptor projection identity drifted"));
    }
    Ok(())
}

/// Self-authenticating compact accepted record.  This is the only record G0
/// needs: static source material is referenced by content hash and rich
/// Dashboard-shaped projection is deliberately deferred until selection.
#[derive(Clone, Debug, PartialEq)]
pub struct V5CompactAcceptedRecord {
    pub generation_index: u64,
    pub birth_ordinal: u64,
    pub proposal_ordinal: u64,
    pub origin_kind: String,
    pub proposal_seed: String,
    pub proposal_delta_sha256: String,
    pub shared_authority_sha256: String,
    pub candidate_id: String,
    pub candidate_identity_sha256: String,
    pub pair_identity_sha256: String,
    pub executable_semantic_sha256: String,
    pub long: V5ModuleCompactFacts,
    pub short: V5ModuleCompactFacts,
    pub compiled: V5CompiledPairFacts,
    pub construction_audit: V5ConstructionAudit,
    pub lineage: Value,
    pub construction_evidence_scope: Value,
    pub funnel_summary: Value,
    pub descriptor_projection: Value,
}

impl V5CompactAcceptedRecord {
    fn semantic_value(&self) -> Result<Value> {
        if self.generation_index == 0 {
            return Err(invalid(
                "v5 compact accepted record generation index must be positive",
            ));
        }
        if self.birth_ordinal > self.proposal_ordinal {
            return Err(invalid(
                "v5 compact accepted record birth ordinal exceeds proposal ordinal",
            ));
        }
        if self.origin_kind != "random_immigrant" && self.origin_kind != "structural_offspring" {
            return Err(invalid("v5 compact accepted record origin is invalid"));
        }
        exact_text_string(&self.origin_kind, "v5 compact record origin")?;
        for (label, value) in [
            ("proposal seed", &self.proposal_seed),
            ("proposal delta", &self.proposal_delta_sha256),
            ("shared authority", &self.shared_authority_sha256),
            ("candidate identity", &self.candidate_identity_sha256),
            ("pair identity", &self.pair_identity_sha256),
            ("executable semantic", &self.executable_semantic_sha256),
        ] {
            exact_sha256_string(value, &format!("v5 compact record {label} SHA-256"))?;
        }
        exact_text_string(&self.candidate_id, "v5 compact record candidate ID")?;
        if self.candidate_id
            != v5_id_from_sha(
                "qd_",
                &self.candidate_identity_sha256,
                "v5 compact record candidate identity",
            )?
        {
            return Err(invalid(
                "v5 compact record candidate ID does not derive from identity",
            ));
        }
        self.long.validate("long")?;
        self.short.validate("short")?;
        self.compiled.validate()?;
        self.construction_audit.validate_for_record(
            &self.origin_kind,
            &self.shared_authority_sha256,
            &self.proposal_delta_sha256,
            &self.candidate_identity_sha256,
            &self.pair_identity_sha256,
            &self.long,
            &self.short,
            &self.compiled,
        )?;
        validate_compact_candidate_lineage(
            &self.lineage,
            &self.candidate_id,
            &self.candidate_identity_sha256,
            &self.pair_identity_sha256,
        )?;
        validate_compact_evidence_scope(&self.construction_evidence_scope)?;
        validate_compact_funnel_summary(
            &self.funnel_summary,
            &self.candidate_id,
            &self.candidate_identity_sha256,
            &self.pair_identity_sha256,
            &self.executable_semantic_sha256,
            &self.long,
            &self.short,
            &self.compiled,
        )?;
        validate_compact_descriptor_projection(
            &self.descriptor_projection,
            &self.candidate_id,
            &self.candidate_identity_sha256,
            &self.pair_identity_sha256,
            &self.long,
            &self.short,
            &self.compiled,
        )?;
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_COMPACT_ACCEPTED_RECORD_SCHEMA.to_owned()),
            ),
            ("generationIndex", Value::from(self.generation_index)),
            ("birthOrdinal", Value::from(self.birth_ordinal)),
            ("proposalOrdinal", Value::from(self.proposal_ordinal)),
            ("originKind", Value::String(self.origin_kind.clone())),
            ("proposalSeed", Value::String(self.proposal_seed.clone())),
            (
                "proposalDeltaSha256",
                Value::String(self.proposal_delta_sha256.clone()),
            ),
            (
                "sharedAuthoritySha256",
                Value::String(self.shared_authority_sha256.clone()),
            ),
            ("candidateId", Value::String(self.candidate_id.clone())),
            (
                "candidateIdentitySha256",
                Value::String(self.candidate_identity_sha256.clone()),
            ),
            (
                "pairIdentitySha256",
                Value::String(self.pair_identity_sha256.clone()),
            ),
            (
                "executableSemanticSha256",
                Value::String(self.executable_semantic_sha256.clone()),
            ),
            ("long", self.long.to_value()?),
            ("short", self.short.to_value()?),
            ("compiled", self.compiled.to_value()?),
            ("constructionAudit", self.construction_audit.to_value()?),
            ("lineage", clone_value(&self.lineage)?),
            (
                "constructionEvidenceScope",
                clone_value(&self.construction_evidence_scope)?,
            ),
            ("funnelSummary", clone_value(&self.funnel_summary)?),
            (
                "descriptorProjection",
                clone_value(&self.descriptor_projection)?,
            ),
        ]))
    }

    /// Canonical compact bytes with a trailing self-hash.  Callers must write
    /// this exact value atomically; readers use `from_value` before consuming
    /// any identity or descriptor field.
    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed compact accepted record")
            .clone();
        fields.insert(
            "recordSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 compact accepted record")?;
        let keys = [
            "schemaVersion",
            "generationIndex",
            "birthOrdinal",
            "proposalOrdinal",
            "originKind",
            "proposalSeed",
            "proposalDeltaSha256",
            "sharedAuthoritySha256",
            "candidateId",
            "candidateIdentitySha256",
            "pairIdentitySha256",
            "executableSemanticSha256",
            "long",
            "short",
            "compiled",
            "constructionAudit",
            "lineage",
            "constructionEvidenceScope",
            "funnelSummary",
            "descriptorProjection",
            "recordSha256",
        ];
        exact_value_keys(fields, &keys, "v5 compact accepted record")?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_COMPACT_ACCEPTED_RECORD_SCHEMA)
        {
            return Err(invalid("v5 compact accepted record schema is invalid"));
        }
        let record = Self {
            generation_index: required(value, "generationIndex", "v5 compact accepted record")?
                .as_u64()
                .ok_or_else(|| invalid("v5 compact generation index is invalid"))?,
            birth_ordinal: required(value, "birthOrdinal", "v5 compact accepted record")?
                .as_u64()
                .ok_or_else(|| invalid("v5 compact birth ordinal is invalid"))?,
            proposal_ordinal: required(value, "proposalOrdinal", "v5 compact accepted record")?
                .as_u64()
                .ok_or_else(|| invalid("v5 compact proposal ordinal is invalid"))?,
            origin_kind: exact_text_value(
                required(value, "originKind", "v5 compact accepted record")?,
                "v5 compact origin",
            )?,
            proposal_seed: exact_sha256_value(
                required(value, "proposalSeed", "v5 compact accepted record")?,
                "v5 compact proposal seed",
            )?,
            proposal_delta_sha256: exact_sha256_value(
                required(value, "proposalDeltaSha256", "v5 compact accepted record")?,
                "v5 compact proposal delta SHA-256",
            )?,
            shared_authority_sha256: exact_sha256_value(
                required(value, "sharedAuthoritySha256", "v5 compact accepted record")?,
                "v5 compact shared authority SHA-256",
            )?,
            candidate_id: exact_text_value(
                required(value, "candidateId", "v5 compact accepted record")?,
                "v5 compact candidate ID",
            )?,
            candidate_identity_sha256: exact_sha256_value(
                required(
                    value,
                    "candidateIdentitySha256",
                    "v5 compact accepted record",
                )?,
                "v5 compact candidate identity SHA-256",
            )?,
            pair_identity_sha256: exact_sha256_value(
                required(value, "pairIdentitySha256", "v5 compact accepted record")?,
                "v5 compact pair identity SHA-256",
            )?,
            executable_semantic_sha256: exact_sha256_value(
                required(
                    value,
                    "executableSemanticSha256",
                    "v5 compact accepted record",
                )?,
                "v5 compact executable semantic SHA-256",
            )?,
            long: V5ModuleCompactFacts::from_value(
                required(value, "long", "v5 compact accepted record")?,
                "long",
            )?,
            short: V5ModuleCompactFacts::from_value(
                required(value, "short", "v5 compact accepted record")?,
                "short",
            )?,
            compiled: V5CompiledPairFacts::from_value(required(
                value,
                "compiled",
                "v5 compact accepted record",
            )?)?,
            construction_audit: V5ConstructionAudit::from_value(required(
                value,
                "constructionAudit",
                "v5 compact accepted record",
            )?)?,
            lineage: clone_value(required(value, "lineage", "v5 compact accepted record")?)?,
            construction_evidence_scope: clone_value(required(
                value,
                "constructionEvidenceScope",
                "v5 compact accepted record",
            )?)?,
            funnel_summary: clone_value(required(
                value,
                "funnelSummary",
                "v5 compact accepted record",
            )?)?,
            descriptor_projection: clone_value(required(
                value,
                "descriptorProjection",
                "v5 compact accepted record",
            )?)?,
        };
        let supplied = exact_sha256_value(
            required(value, "recordSha256", "v5 compact accepted record")?,
            "v5 compact accepted record SHA-256",
        )?;
        if supplied != canonical_sha256(&record.semantic_value()?)? {
            return Err(invalid("v5 compact accepted record identity drifted"));
        }
        if &record.to_value()? != value {
            return Err(invalid("v5 compact accepted record is not canonical"));
        }
        Ok(record)
    }

    pub fn record_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub fn selected_projection(&self) -> Result<V5SelectedProjection> {
        Ok(V5SelectedProjection {
            generation_index: self.generation_index,
            birth_ordinal: self.birth_ordinal,
            proposal_ordinal: self.proposal_ordinal,
            origin_kind: self.origin_kind.clone(),
            proposal_seed: self.proposal_seed.clone(),
            proposal_delta_sha256: self.proposal_delta_sha256.clone(),
            shared_authority_sha256: self.shared_authority_sha256.clone(),
            candidate_id: self.candidate_id.clone(),
            candidate_identity_sha256: self.candidate_identity_sha256.clone(),
            pair_identity_sha256: self.pair_identity_sha256.clone(),
            long: self.long.clone(),
            short: self.short.clone(),
            compiled: self.compiled.clone(),
            lineage: clone_value(&self.lineage)?,
            record_sha256: self.record_sha256()?,
        })
    }
}

/// The selected-only handoff.  It names everything required to materialize a
/// legacy rich candidate after G0 selection, but deliberately contains no
/// catalog/profile/program blob.  Batch code must resolve the two content
/// addressed objects and call the native materializer, never re-run factory
/// assembly or invoke Python validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5SelectedProjection {
    pub generation_index: u64,
    pub birth_ordinal: u64,
    pub proposal_ordinal: u64,
    pub origin_kind: String,
    pub proposal_seed: String,
    pub proposal_delta_sha256: String,
    pub shared_authority_sha256: String,
    pub candidate_id: String,
    pub candidate_identity_sha256: String,
    pub pair_identity_sha256: String,
    pub long: V5ModuleCompactFacts,
    pub short: V5ModuleCompactFacts,
    pub compiled: V5CompiledPairFacts,
    pub lineage: Value,
    pub record_sha256: String,
}

impl V5SelectedProjection {
    fn semantic_value(&self) -> Result<Value> {
        if self.generation_index == 0 || self.birth_ordinal > self.proposal_ordinal {
            return Err(invalid("v5 selected projection ordinal facts are invalid"));
        }
        if self.origin_kind != "random_immigrant" && self.origin_kind != "structural_offspring" {
            return Err(invalid("v5 selected projection origin is invalid"));
        }
        exact_text_string(&self.origin_kind, "v5 selected projection origin")?;
        for (label, value) in [
            ("proposal seed", &self.proposal_seed),
            ("proposal delta", &self.proposal_delta_sha256),
            ("shared authority", &self.shared_authority_sha256),
            ("candidate identity", &self.candidate_identity_sha256),
            ("pair identity", &self.pair_identity_sha256),
            ("record", &self.record_sha256),
        ] {
            exact_sha256_string(value, &format!("v5 selected projection {label} SHA-256"))?;
        }
        if self.candidate_id
            != v5_id_from_sha(
                "qd_",
                &self.candidate_identity_sha256,
                "v5 selected projection candidate identity",
            )?
        {
            return Err(invalid(
                "v5 selected projection candidate ID does not derive from identity",
            ));
        }
        self.long.validate("long")?;
        self.short.validate("short")?;
        self.compiled.validate()?;
        validate_compact_candidate_lineage(
            &self.lineage,
            &self.candidate_id,
            &self.candidate_identity_sha256,
            &self.pair_identity_sha256,
        )?;
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_SELECTED_PROJECTION_SCHEMA.to_owned()),
            ),
            ("generationIndex", Value::from(self.generation_index)),
            ("birthOrdinal", Value::from(self.birth_ordinal)),
            ("proposalOrdinal", Value::from(self.proposal_ordinal)),
            ("originKind", Value::String(self.origin_kind.clone())),
            ("proposalSeed", Value::String(self.proposal_seed.clone())),
            (
                "proposalDeltaSha256",
                Value::String(self.proposal_delta_sha256.clone()),
            ),
            (
                "sharedAuthoritySha256",
                Value::String(self.shared_authority_sha256.clone()),
            ),
            ("candidateId", Value::String(self.candidate_id.clone())),
            (
                "candidateIdentitySha256",
                Value::String(self.candidate_identity_sha256.clone()),
            ),
            (
                "pairIdentitySha256",
                Value::String(self.pair_identity_sha256.clone()),
            ),
            ("long", self.long.to_value()?),
            ("short", self.short.to_value()?),
            ("compiled", self.compiled.to_value()?),
            ("lineage", clone_value(&self.lineage)?),
            ("recordSha256", Value::String(self.record_sha256.clone())),
        ]))
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 selected projection")
            .clone();
        fields.insert(
            "projectionSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 selected projection")?;
        let keys = [
            "schemaVersion",
            "generationIndex",
            "birthOrdinal",
            "proposalOrdinal",
            "originKind",
            "proposalSeed",
            "proposalDeltaSha256",
            "sharedAuthoritySha256",
            "candidateId",
            "candidateIdentitySha256",
            "pairIdentitySha256",
            "long",
            "short",
            "compiled",
            "lineage",
            "recordSha256",
            "projectionSha256",
        ];
        exact_value_keys(fields, &keys, "v5 selected projection")?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_SELECTED_PROJECTION_SCHEMA)
        {
            return Err(invalid("v5 selected projection schema is invalid"));
        }
        let projection = Self {
            generation_index: required(value, "generationIndex", "v5 selected projection")?
                .as_u64()
                .ok_or_else(|| invalid("v5 selected projection generation index is invalid"))?,
            birth_ordinal: required(value, "birthOrdinal", "v5 selected projection")?
                .as_u64()
                .ok_or_else(|| invalid("v5 selected projection birth ordinal is invalid"))?,
            proposal_ordinal: required(value, "proposalOrdinal", "v5 selected projection")?
                .as_u64()
                .ok_or_else(|| invalid("v5 selected projection proposal ordinal is invalid"))?,
            origin_kind: exact_text_value(
                required(value, "originKind", "v5 selected projection")?,
                "v5 selected projection origin",
            )?,
            proposal_seed: exact_sha256_value(
                required(value, "proposalSeed", "v5 selected projection")?,
                "v5 selected projection proposal seed",
            )?,
            proposal_delta_sha256: exact_sha256_value(
                required(value, "proposalDeltaSha256", "v5 selected projection")?,
                "v5 selected projection proposal delta SHA-256",
            )?,
            shared_authority_sha256: exact_sha256_value(
                required(value, "sharedAuthoritySha256", "v5 selected projection")?,
                "v5 selected projection shared authority SHA-256",
            )?,
            candidate_id: exact_text_value(
                required(value, "candidateId", "v5 selected projection")?,
                "v5 selected projection candidate ID",
            )?,
            candidate_identity_sha256: exact_sha256_value(
                required(value, "candidateIdentitySha256", "v5 selected projection")?,
                "v5 selected projection candidate identity SHA-256",
            )?,
            pair_identity_sha256: exact_sha256_value(
                required(value, "pairIdentitySha256", "v5 selected projection")?,
                "v5 selected projection pair identity SHA-256",
            )?,
            long: V5ModuleCompactFacts::from_value(
                required(value, "long", "v5 selected projection")?,
                "long",
            )?,
            short: V5ModuleCompactFacts::from_value(
                required(value, "short", "v5 selected projection")?,
                "short",
            )?,
            compiled: V5CompiledPairFacts::from_value(required(
                value,
                "compiled",
                "v5 selected projection",
            )?)?,
            lineage: clone_value(required(value, "lineage", "v5 selected projection")?)?,
            record_sha256: exact_sha256_value(
                required(value, "recordSha256", "v5 selected projection")?,
                "v5 selected projection record SHA-256",
            )?,
        };
        let supplied = exact_sha256_value(
            required(value, "projectionSha256", "v5 selected projection")?,
            "v5 selected projection SHA-256",
        )?;
        if supplied != canonical_sha256(&projection.semantic_value()?)? {
            return Err(invalid("v5 selected projection identity drifted"));
        }
        if &projection.to_value()? != value {
            return Err(invalid("v5 selected projection is not canonical"));
        }
        Ok(projection)
    }

    pub fn verify_against_record(&self, record: &V5CompactAcceptedRecord) -> Result<()> {
        if self.record_sha256 != record.record_sha256()?
            || self.generation_index != record.generation_index
            || self.birth_ordinal != record.birth_ordinal
            || self.proposal_ordinal != record.proposal_ordinal
            || self.origin_kind != record.origin_kind
            || self.proposal_seed != record.proposal_seed
            || self.proposal_delta_sha256 != record.proposal_delta_sha256
            || self.shared_authority_sha256 != record.shared_authority_sha256
            || self.candidate_id != record.candidate_id
            || self.candidate_identity_sha256 != record.candidate_identity_sha256
            || self.pair_identity_sha256 != record.pair_identity_sha256
            || self.long != record.long
            || self.short != record.short
            || self.compiled != record.compiled
            || self.lineage != record.lineage
        {
            return Err(invalid(
                "v5 selected projection does not bind compact record",
            ));
        }
        Ok(())
    }
}

/// Rehydrate one *selected* native G0 compact record into the rich candidate
/// surface needed by evaluation/publication.  This is deliberately
/// crate-visible rather than a general compact-record expansion API: the
/// transaction must first prove that the projection is selected and that the
/// exact record/delta reconstruct from sealed authority.  No G0 construction
/// loop calls this helper, so an unselected 4K pool is never expanded.
///
/// The public transaction wrapper is
/// `v5_transaction::materialize_selected_v5_g0_record`; batch code must use
/// that wrapper instead of rebuilding factories or accessing these compiler
/// internals.
pub(crate) fn materialize_selected_v5_g0_rich_candidate(
    authority: &V5SharedConstructionAuthority,
    selected: &V5SelectedProjection,
    proposal_delta: &Value,
    record: &V5CompactAcceptedRecord,
) -> Result<Value> {
    if selected.origin_kind != "random_immigrant" || record.origin_kind != "random_immigrant" {
        return Err(invalid(
            "selected native v5 G0 materializer only accepts immigrants",
        ));
    }
    selected.verify_against_record(record)?;
    verify_reconstruct_compact_g0_record(authority, proposal_delta, record)?;
    let pair = reconstruct_g0_pair(authority, &record.proposal_seed, Some(proposal_delta))?;
    if pair.candidate_id != record.candidate_id
        || pair.candidate_identity_sha256 != record.candidate_identity_sha256
        || pair.pair_identity_sha256 != record.pair_identity_sha256
        || pair.executable_semantic_sha256 != record.executable_semantic_sha256
        || pair.validation.raw_profile_sha256 != record.compiled.raw_pair_sha256
        || pair.validation.profile_snapshot_sha256 != record.compiled.profile_snapshot_sha256
        || pair.validation.program_sha256 != record.compiled.program_sha256
        || pair.validation.validation_report_sha256 != record.compiled.validation_report_sha256
    {
        return Err(invalid(
            "selected native v5 G0 materializer reconstructed compact facts drifted",
        ));
    }
    let history = clone_value(required(
        &record.lineage,
        "orderedSideLineage",
        "v5 selected compact lineage",
    )?)?;
    Ok(object([
        ("activationAwareRepairs", Value::Array(Vec::new())),
        ("birthOrdinal", Value::from(record.birth_ordinal)),
        ("candidateId", Value::String(pair.candidate_id.clone())),
        (
            "candidateIdentityMaterial",
            clone_value(&pair.candidate_identity_material)?,
        ),
        (
            "candidateIdentitySha256",
            Value::String(pair.candidate_identity_sha256.clone()),
        ),
        (
            "constructionEvidenceScope",
            clone_value(&record.construction_evidence_scope)?,
        ),
        ("generationIndex", Value::from(record.generation_index)),
        ("lineage", clone_value(&record.lineage)?),
        ("mutationTrace", Value::Array(Vec::new())),
        (
            "pairProposalSha256",
            Value::String(pair.proposal_sha256.clone()),
        ),
        (
            "profileSnapshotSha256",
            Value::String(pair.validation.profile_snapshot_sha256.clone()),
        ),
        (
            "programSha256",
            Value::String(pair.validation.program_sha256.clone()),
        ),
        ("proposalOrdinal", Value::from(record.proposal_ordinal)),
        ("seedId", Value::String("bidirectional_pair".to_owned())),
        (
            "sourceMode",
            Value::String("qd_random_immigrant_bidirectional_pair".to_owned()),
        ),
        ("sourceProfile", clone_value(&pair.profile)?),
        (
            "sourceProfileSha256",
            Value::String(pair.validation.raw_profile_sha256.clone()),
        ),
        ("structuralDepth", Value::from(2_u64)),
        ("structuralOperatorHistory", history),
        (
            "validationReportSha256",
            Value::String(pair.validation.validation_report_sha256.clone()),
        ),
    ]))
}

/// Rehydrate one accepted later-generation compact record into the narrow
/// rich candidate surface required by the public population/evaluation
/// stream.  This is deliberately crate-visible: a v5 evolved transaction
/// must first replay the exact attempt from its sealed parent snapshots, and
/// the publication layer receives the resulting transient material only
/// inside that replay.  It never grants a general compact-record expansion
/// API to batch or Python.
///
/// Unlike the G0 helper above, this accepts both native immigrants and
/// structural offspring.  It reconstructs both final modules and the pair
/// from the sealed authority and exact all-attempt delta, then cross-checks
/// every compact identity before returning a rich value.  Stored compiled
/// profiles, descriptors, and candidate facts are witnesses rather than
/// construction authority.
pub(crate) fn materialize_v5_evolved_rich_candidate(
    authority: &V5SharedConstructionAuthority,
    material: &V5EvolvedAcceptedMaterial,
) -> Result<Value> {
    let record = &material.record;
    if record.generation_index < 2
        || !matches!(
            record.origin_kind.as_str(),
            "random_immigrant" | "structural_offspring"
        )
        || record.shared_authority_sha256 != authority.shared_authority_sha256
    {
        return Err(invalid(
            "v5 evolved rich materializer record authority/origin binding is invalid",
        ));
    }
    let proposal_delta_sha256 = evolved_delta_sha256(&material.proposal_delta)?;
    if proposal_delta_sha256 != record.proposal_delta_sha256
        || material.parent_material.accepted_record.to_value()? != record.to_value()?
        || material.parent_material.proposal_delta != material.proposal_delta
        || material.parent_material.candidate_id != record.candidate_id
        || material.parent_material.candidate_identity_sha256 != record.candidate_identity_sha256
        || material.parent_material.pair_identity_sha256 != record.pair_identity_sha256
    {
        return Err(invalid(
            "v5 evolved rich materializer material does not bind compact record/delta",
        ));
    }
    let parent = &material.parent_material;
    // The accepted constructor immediately preceding this callback already
    // rebuilt both modules and the pair from the authenticated delta and
    // sealed authority. Keep that compiler-owned result inside the opaque
    // replay material instead of compiling the identical accepted pair a
    // second time merely to project public rows. Every durable identity is
    // still checked below, and no cached pair crosses the replay boundary.
    let pair = parent.sealed_pair.as_ref();
    if pair.candidate_id != record.candidate_id
        || pair.candidate_identity_sha256 != record.candidate_identity_sha256
        || pair.pair_identity_sha256 != record.pair_identity_sha256
        || pair.executable_semantic_sha256 != record.executable_semantic_sha256
        || pair.validation.raw_profile_sha256 != record.compiled.raw_pair_sha256
        || pair.validation.profile_snapshot_sha256 != record.compiled.profile_snapshot_sha256
        || pair.validation.program_sha256 != record.compiled.program_sha256
        || pair.validation.validation_report_sha256 != record.compiled.validation_report_sha256
        || pair.long.compact_facts(&authority.long) != record.long
        || pair.short.compact_facts(&authority.short) != record.short
        || v5_compact_candidate_lineage(&pair) != record.lineage
        || v5_compact_funnel_summary(&pair)? != record.funnel_summary
        || pair.long.program != parent.long_program
        || pair.short.program != parent.short_program
        || pair.long.lineage != parent.long_module_lineage
        || pair.short.lineage != parent.short_module_lineage
        || pair.side_targeted_lineage != parent.side_targeted_lineage
    {
        return Err(invalid(
            "v5 evolved rich materializer reconstructed compact facts drifted",
        ));
    }
    let history = clone_value(required(
        &record.lineage,
        "orderedSideLineage",
        "v5 evolved compact lineage",
    )?)?;
    let structural_depth = history
        .as_array()
        .map(|rows| rows.len() as u64)
        .ok_or_else(|| invalid("v5 evolved compact ordered side lineage is invalid"))?;
    Ok(object([
        ("activationAwareRepairs", Value::Array(Vec::new())),
        ("birthOrdinal", Value::from(record.birth_ordinal)),
        ("candidateId", Value::String(pair.candidate_id.clone())),
        (
            "candidateIdentityMaterial",
            clone_value(&pair.candidate_identity_material)?,
        ),
        (
            "candidateIdentitySha256",
            Value::String(pair.candidate_identity_sha256.clone()),
        ),
        (
            "constructionEvidenceScope",
            clone_value(&record.construction_evidence_scope)?,
        ),
        ("generationIndex", Value::from(record.generation_index)),
        ("lineage", clone_value(&record.lineage)?),
        ("mutationTrace", Value::Array(Vec::new())),
        (
            "pairProposalSha256",
            Value::String(pair.proposal_sha256.clone()),
        ),
        (
            "profileSnapshotSha256",
            Value::String(pair.validation.profile_snapshot_sha256.clone()),
        ),
        (
            "programSha256",
            Value::String(pair.validation.program_sha256.clone()),
        ),
        ("proposalOrdinal", Value::from(record.proposal_ordinal)),
        ("seedId", Value::String("bidirectional_pair".to_owned())),
        (
            "sourceMode",
            Value::String(format!("qd_{}_bidirectional_pair", record.origin_kind)),
        ),
        ("sourceProfile", clone_value(&pair.profile)?),
        (
            "sourceProfileSha256",
            Value::String(pair.validation.raw_profile_sha256.clone()),
        ),
        ("structuralDepth", Value::from(structural_depth)),
        ("structuralOperatorHistory", history),
        (
            "validationReportSha256",
            Value::String(pair.validation.validation_report_sha256.clone()),
        ),
    ]))
}

fn stable_attempt_code(value: &Value, label: &str) -> Result<String> {
    let code = text(value, label)?;
    if !code
        .bytes()
        .all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'.' | b':'))
    {
        return Err(invalid(format!("{label} is not a stable machine code")));
    }
    Ok(code)
}

fn optional_sha256_text(value: &Value, label: &str) -> Result<Option<String>> {
    if value.is_null() {
        Ok(None)
    } else {
        Ok(Some(sha256_text(value, label)?))
    }
}

fn nullable_sha256(value: Option<&str>) -> Value {
    value
        .map(|value| Value::String(value.to_owned()))
        .unwrap_or(Value::Null)
}

fn nullable_u64(value: Option<u64>) -> Value {
    value.map(Value::from).unwrap_or(Value::Null)
}

/// A content-addressed parent or crossover-mate reference.  It names the
/// exact accepted record and both side programs observed during deterministic
/// parent selection; a later restart cannot silently substitute a newer
/// archive entry with the same candidate label.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5AttemptParentReference {
    pub candidate_id: String,
    pub candidate_identity_sha256: String,
    pub accepted_record_sha256: String,
    pub long_program_sha256: String,
    pub short_program_sha256: String,
}

impl V5AttemptParentReference {
    fn semantic_value(&self) -> Result<Value> {
        let candidate_id = text(
            &Value::String(self.candidate_id.clone()),
            "v5 attempt parent candidate ID",
        )?;
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_ATTEMPT_PARENT_REF_SCHEMA.to_owned()),
            ),
            ("candidateId", Value::String(candidate_id)),
            (
                "candidateIdentitySha256",
                Value::String(sha256_text(
                    &Value::String(self.candidate_identity_sha256.clone()),
                    "v5 attempt parent candidate identity SHA-256",
                )?),
            ),
            (
                "acceptedRecordSha256",
                Value::String(sha256_text(
                    &Value::String(self.accepted_record_sha256.clone()),
                    "v5 attempt parent accepted record SHA-256",
                )?),
            ),
            (
                "longProgramSha256",
                Value::String(sha256_text(
                    &Value::String(self.long_program_sha256.clone()),
                    "v5 attempt parent long program SHA-256",
                )?),
            ),
            (
                "shortProgramSha256",
                Value::String(sha256_text(
                    &Value::String(self.short_program_sha256.clone()),
                    "v5 attempt parent short program SHA-256",
                )?),
            ),
        ]))
    }

    pub fn to_value(&self) -> Result<Value> {
        self.semantic_value()
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 attempt parent reference")?;
        exact_value_keys(
            fields,
            &[
                "schemaVersion",
                "candidateId",
                "candidateIdentitySha256",
                "acceptedRecordSha256",
                "longProgramSha256",
                "shortProgramSha256",
            ],
            "v5 attempt parent reference",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str) != Some(V5_ATTEMPT_PARENT_REF_SCHEMA)
        {
            return Err(invalid("v5 attempt parent reference schema is invalid"));
        }
        let reference = Self {
            candidate_id: text(
                required(value, "candidateId", "v5 attempt parent reference")?,
                "v5 attempt parent candidate ID",
            )?,
            candidate_identity_sha256: sha256_text(
                required(
                    value,
                    "candidateIdentitySha256",
                    "v5 attempt parent reference",
                )?,
                "v5 attempt parent candidate identity SHA-256",
            )?,
            accepted_record_sha256: sha256_text(
                required(value, "acceptedRecordSha256", "v5 attempt parent reference")?,
                "v5 attempt parent accepted record SHA-256",
            )?,
            long_program_sha256: sha256_text(
                required(value, "longProgramSha256", "v5 attempt parent reference")?,
                "v5 attempt parent long program SHA-256",
            )?,
            short_program_sha256: sha256_text(
                required(value, "shortProgramSha256", "v5 attempt parent reference")?,
                "v5 attempt parent short program SHA-256",
            )?,
        };
        if &reference.to_value()? != value {
            return Err(invalid("v5 attempt parent reference is not canonical"));
        }
        Ok(reference)
    }
}

fn optional_parent_reference(
    value: &Value,
    label: &str,
) -> Result<Option<V5AttemptParentReference>> {
    if value.is_null() {
        Ok(None)
    } else {
        V5AttemptParentReference::from_value(value)
            .map(Some)
            .map_err(|error| invalid(format!("{label}: {error}")))
    }
}

fn nullable_parent_reference(value: Option<&V5AttemptParentReference>) -> Result<Value> {
    match value {
        Some(value) => value.to_value(),
        None => Ok(Value::Null),
    }
}

/// Exact links between an attempted proposal and the deterministic archive
/// state/operator work that produced it.  `random_immigrant` attempts carry
/// no such links; offspring attempts must name their selected parent,
/// selection receipt, and the current operator plan before construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5AttemptLineageRefs {
    pub parent: Option<V5AttemptParentReference>,
    pub mate: Option<V5AttemptParentReference>,
    pub parent_selection_receipt_sha256: Option<String>,
    pub operator_plan_sha256: Option<String>,
    pub operator_application_sha256: Option<String>,
    /// Self-hash of the complete ordered operator trace.  The scalar plan /
    /// application / step fields below are terminal-step summaries only and
    /// must never be interpreted as a complete multi-step mutation history.
    pub operator_trace_sha256: Option<String>,
    pub step_index: Option<u64>,
}

impl V5AttemptLineageRefs {
    fn semantic_value(&self) -> Result<Value> {
        if self.mate.is_some() && self.parent.is_none() {
            return Err(invalid("v5 attempt mate requires a primary parent"));
        }
        let has_plan = self.operator_plan_sha256.is_some();
        if has_plan != self.step_index.is_some() {
            return Err(invalid(
                "v5 attempt plan and step index presence must agree",
            ));
        }
        if self.operator_application_sha256.is_some() && !has_plan {
            return Err(invalid("v5 attempt application requires an operator plan"));
        }
        if has_plan != self.operator_trace_sha256.is_some() {
            return Err(invalid(
                "v5 attempt plan and operator trace presence must agree",
            ));
        }
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_ATTEMPT_LINEAGE_REFS_SCHEMA.to_owned()),
            ),
            ("parent", nullable_parent_reference(self.parent.as_ref())?),
            ("mate", nullable_parent_reference(self.mate.as_ref())?),
            (
                "parentSelectionReceiptSha256",
                nullable_sha256(self.parent_selection_receipt_sha256.as_deref()),
            ),
            (
                "operatorPlanSha256",
                nullable_sha256(self.operator_plan_sha256.as_deref()),
            ),
            (
                "operatorApplicationSha256",
                nullable_sha256(self.operator_application_sha256.as_deref()),
            ),
            (
                "operatorTraceSha256",
                nullable_sha256(self.operator_trace_sha256.as_deref()),
            ),
            ("stepIndex", nullable_u64(self.step_index)),
        ]))
    }

    pub fn to_value(&self) -> Result<Value> {
        self.semantic_value()
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 attempt lineage refs")?;
        exact_value_keys(
            fields,
            &[
                "schemaVersion",
                "parent",
                "mate",
                "parentSelectionReceiptSha256",
                "operatorPlanSha256",
                "operatorApplicationSha256",
                "operatorTraceSha256",
                "stepIndex",
            ],
            "v5 attempt lineage refs",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_ATTEMPT_LINEAGE_REFS_SCHEMA)
        {
            return Err(invalid("v5 attempt lineage refs schema is invalid"));
        }
        let refs = Self {
            parent: optional_parent_reference(
                required(value, "parent", "v5 attempt lineage refs")?,
                "v5 attempt parent",
            )?,
            mate: optional_parent_reference(
                required(value, "mate", "v5 attempt lineage refs")?,
                "v5 attempt mate",
            )?,
            parent_selection_receipt_sha256: optional_sha256_text(
                required(
                    value,
                    "parentSelectionReceiptSha256",
                    "v5 attempt lineage refs",
                )?,
                "v5 parent selection receipt SHA-256",
            )?,
            operator_plan_sha256: optional_sha256_text(
                required(value, "operatorPlanSha256", "v5 attempt lineage refs")?,
                "v5 operator plan SHA-256",
            )?,
            operator_application_sha256: optional_sha256_text(
                required(
                    value,
                    "operatorApplicationSha256",
                    "v5 attempt lineage refs",
                )?,
                "v5 operator application SHA-256",
            )?,
            operator_trace_sha256: optional_sha256_text(
                required(value, "operatorTraceSha256", "v5 attempt lineage refs")?,
                "v5 operator trace SHA-256",
            )?,
            step_index: match required(value, "stepIndex", "v5 attempt lineage refs")? {
                Value::Null => None,
                value => Some(
                    value
                        .as_u64()
                        .ok_or_else(|| invalid("v5 attempt step index is invalid"))?,
                ),
            },
        };
        if &refs.to_value()? != value {
            return Err(invalid("v5 attempt lineage refs are not canonical"));
        }
        Ok(refs)
    }

    fn require_for_origin(&self, origin_kind: &str) -> Result<()> {
        match origin_kind {
            "random_immigrant" => {
                if self.parent.is_some()
                    || self.mate.is_some()
                    || self.parent_selection_receipt_sha256.is_some()
                    || self.operator_plan_sha256.is_some()
                    || self.operator_application_sha256.is_some()
                    || self.operator_trace_sha256.is_some()
                    || self.step_index.is_some()
                {
                    return Err(invalid(
                        "random immigrant attempt has offspring lineage refs",
                    ));
                }
            }
            "structural_offspring" => {
                if self.parent.is_none() || self.parent_selection_receipt_sha256.is_none() {
                    return Err(invalid(
                        "structural offspring attempt lacks deterministic lineage refs",
                    ));
                }
                // Python can reject while it is still enumerating eligible
                // operations (`operation_rejected` / no eligible side) before
                // any plan exists.  Preserve that as a typed pre-plan attempt
                // rather than fabricating a plan or discarding the retry.
                let any_operator_ref = self.operator_plan_sha256.is_some()
                    || self.operator_application_sha256.is_some()
                    || self.operator_trace_sha256.is_some()
                    || self.step_index.is_some();
                if any_operator_ref
                    && (self.operator_plan_sha256.is_none()
                        || self.operator_trace_sha256.is_none()
                        || self.step_index.is_none())
                {
                    return Err(invalid("structural offspring operator refs are partial"));
                }
            }
            _ => return Err(invalid("v5 attempt origin is unsupported")),
        }
        Ok(())
    }
}

/// One immutable proposal attempt.  It records failures just as carefully as
/// accepted candidates, so restart preserves retries, duplicate disposition,
/// and the exact population/ledger history rather than reconstructing a
/// plausible-but-different generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5ProposalAttemptRecord {
    pub generation_index: u64,
    pub proposal_ordinal: u64,
    pub generation_config_sha256: String,
    pub shared_authority_sha256: String,
    pub proposal_seed: String,
    pub origin_kind: String,
    pub proposal_delta_sha256: Option<String>,
    pub disposition: String,
    pub reason_code: String,
    pub lineage_refs: V5AttemptLineageRefs,
    pub identity_ledger_effect: String,
    /// Content-addressed, self-authenticating outcome evidence.  The attempt
    /// stream stores only its identity; adoption resolves and cross-binds the
    /// full audit through `V5AttemptOutcomeAudit`.
    pub outcome_audit_sha256: String,
    pub accepted_record_sha256: Option<String>,
}

impl V5ProposalAttemptRecord {
    fn semantic_value(&self) -> Result<Value> {
        let origin_kind = stable_attempt_code(
            &Value::String(self.origin_kind.clone()),
            "v5 attempt origin kind",
        )?;
        if origin_kind != "random_immigrant" && origin_kind != "structural_offspring" {
            return Err(invalid("v5 attempt origin kind is unsupported"));
        }
        self.lineage_refs.require_for_origin(&origin_kind)?;
        let disposition = stable_attempt_code(
            &Value::String(self.disposition.clone()),
            "v5 attempt disposition",
        )?;
        if !matches!(disposition.as_str(), "accepted" | "no_op" | "rejected") {
            return Err(invalid("v5 attempt disposition is unsupported"));
        }
        let reason_code = stable_attempt_code(
            &Value::String(self.reason_code.clone()),
            "v5 attempt reason code",
        )?;
        let ledger_effect = stable_attempt_code(
            &Value::String(self.identity_ledger_effect.clone()),
            "v5 identity ledger effect",
        )?;
        if !matches!(
            ledger_effect.as_str(),
            "inserted"
                | "duplicate_candidate"
                | "duplicate_pair"
                | "duplicate_executable"
                | "not_checked"
                | "not_applicable"
        ) {
            return Err(invalid("v5 attempt identity ledger effect is unsupported"));
        }
        let accepted = self.accepted_record_sha256.is_some();
        if (disposition == "accepted") != accepted
            || (disposition == "accepted" && ledger_effect != "inserted")
            || (disposition != "accepted" && ledger_effect == "inserted")
            || (disposition == "accepted" && self.proposal_delta_sha256.is_none())
        {
            return Err(invalid(
                "v5 attempt disposition/record/ledger binding is invalid",
            ));
        }
        let reached_plan = self.lineage_refs.operator_plan_sha256.is_some();
        if reached_plan && self.lineage_refs.operator_trace_sha256.is_none() {
            return Err(invalid("v5 attempted operator plan lacks a trace root"));
        }
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_PROPOSAL_ATTEMPT_SCHEMA.to_owned()),
            ),
            ("generationIndex", Value::from(self.generation_index)),
            ("proposalOrdinal", Value::from(self.proposal_ordinal)),
            (
                "generationConfigSha256",
                Value::String(sha256_text(
                    &Value::String(self.generation_config_sha256.clone()),
                    "v5 attempt generation config SHA-256",
                )?),
            ),
            (
                "sharedAuthoritySha256",
                Value::String(sha256_text(
                    &Value::String(self.shared_authority_sha256.clone()),
                    "v5 attempt shared authority SHA-256",
                )?),
            ),
            (
                "proposalSeed",
                Value::String(sha256_text(
                    &Value::String(self.proposal_seed.clone()),
                    "v5 attempt proposal seed",
                )?),
            ),
            ("originKind", Value::String(origin_kind)),
            (
                "proposalDeltaSha256",
                nullable_sha256(self.proposal_delta_sha256.as_deref()),
            ),
            ("disposition", Value::String(disposition)),
            ("reasonCode", Value::String(reason_code)),
            ("lineageRefs", self.lineage_refs.to_value()?),
            ("identityLedgerEffect", Value::String(ledger_effect)),
            (
                "outcomeAuditSha256",
                Value::String(sha256_text(
                    &Value::String(self.outcome_audit_sha256.clone()),
                    "v5 attempt outcome audit SHA-256",
                )?),
            ),
            (
                "acceptedRecordSha256",
                nullable_sha256(self.accepted_record_sha256.as_deref()),
            ),
        ]))
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 proposal attempt")
            .clone();
        fields.insert(
            "attemptSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub fn attempt_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 proposal attempt")?;
        exact_value_keys(
            fields,
            &[
                "schemaVersion",
                "generationIndex",
                "proposalOrdinal",
                "generationConfigSha256",
                "sharedAuthoritySha256",
                "proposalSeed",
                "originKind",
                "proposalDeltaSha256",
                "disposition",
                "reasonCode",
                "lineageRefs",
                "identityLedgerEffect",
                "outcomeAuditSha256",
                "acceptedRecordSha256",
                "attemptSha256",
            ],
            "v5 proposal attempt",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str) != Some(V5_PROPOSAL_ATTEMPT_SCHEMA) {
            return Err(invalid("v5 proposal attempt schema is invalid"));
        }
        let record = Self {
            generation_index: required(value, "generationIndex", "v5 proposal attempt")?
                .as_u64()
                .ok_or_else(|| invalid("v5 attempt generation index is invalid"))?,
            proposal_ordinal: required(value, "proposalOrdinal", "v5 proposal attempt")?
                .as_u64()
                .ok_or_else(|| invalid("v5 attempt proposal ordinal is invalid"))?,
            generation_config_sha256: sha256_text(
                required(value, "generationConfigSha256", "v5 proposal attempt")?,
                "v5 attempt generation config SHA-256",
            )?,
            shared_authority_sha256: sha256_text(
                required(value, "sharedAuthoritySha256", "v5 proposal attempt")?,
                "v5 attempt shared authority SHA-256",
            )?,
            proposal_seed: sha256_text(
                required(value, "proposalSeed", "v5 proposal attempt")?,
                "v5 attempt proposal seed",
            )?,
            origin_kind: stable_attempt_code(
                required(value, "originKind", "v5 proposal attempt")?,
                "v5 attempt origin kind",
            )?,
            proposal_delta_sha256: optional_sha256_text(
                required(value, "proposalDeltaSha256", "v5 proposal attempt")?,
                "v5 attempt proposal delta SHA-256",
            )?,
            disposition: stable_attempt_code(
                required(value, "disposition", "v5 proposal attempt")?,
                "v5 attempt disposition",
            )?,
            reason_code: stable_attempt_code(
                required(value, "reasonCode", "v5 proposal attempt")?,
                "v5 attempt reason code",
            )?,
            lineage_refs: V5AttemptLineageRefs::from_value(required(
                value,
                "lineageRefs",
                "v5 proposal attempt",
            )?)?,
            identity_ledger_effect: stable_attempt_code(
                required(value, "identityLedgerEffect", "v5 proposal attempt")?,
                "v5 attempt identity ledger effect",
            )?,
            outcome_audit_sha256: sha256_text(
                required(value, "outcomeAuditSha256", "v5 proposal attempt")?,
                "v5 attempt outcome audit SHA-256",
            )?,
            accepted_record_sha256: optional_sha256_text(
                required(value, "acceptedRecordSha256", "v5 proposal attempt")?,
                "v5 attempt accepted record SHA-256",
            )?,
        };
        let supplied = sha256_text(
            required(value, "attemptSha256", "v5 proposal attempt")?,
            "v5 proposal attempt SHA-256",
        )?;
        if supplied != record.attempt_sha256()? {
            return Err(invalid("v5 proposal attempt identity drifted"));
        }
        if &record.to_value()? != value {
            return Err(invalid("v5 proposal attempt is not canonical"));
        }
        Ok(record)
    }
}

/// Immutable evidence for a proposal result.  It is stored content-addressed
/// beside the JSONL stream, allowing the small attempt row to remain compact
/// while restart can still prove *why* a rejection/no-op/duplicate happened.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5AttemptOutcomeAudit {
    pub generation_index: u64,
    pub proposal_ordinal: u64,
    pub generation_config_sha256: String,
    pub shared_authority_sha256: String,
    pub proposal_seed: String,
    pub origin_kind: String,
    pub disposition: String,
    pub reason_code: String,
    pub stage: String,
    pub proposal_delta_sha256: Option<String>,
    pub lineage_refs_sha256: String,
    pub identity_ledger_effect: String,
    pub accepted_record_sha256: Option<String>,
}

impl V5AttemptOutcomeAudit {
    fn semantic_value(&self) -> Result<Value> {
        let origin_kind = stable_attempt_code(
            &Value::String(self.origin_kind.clone()),
            "v5 outcome audit origin kind",
        )?;
        let disposition = stable_attempt_code(
            &Value::String(self.disposition.clone()),
            "v5 outcome audit disposition",
        )?;
        let reason_code = stable_attempt_code(
            &Value::String(self.reason_code.clone()),
            "v5 outcome audit reason code",
        )?;
        let stage =
            stable_attempt_code(&Value::String(self.stage.clone()), "v5 outcome audit stage")?;
        if !matches!(
            origin_kind.as_str(),
            "random_immigrant" | "structural_offspring"
        ) || !matches!(disposition.as_str(), "accepted" | "no_op" | "rejected")
            || !matches!(
                stage.as_str(),
                "pre_plan"
                    | "operator_plan"
                    | "operator_apply"
                    | "compile"
                    | "admission"
                    | "identity_ledger"
                    | "accepted"
            )
        {
            return Err(invalid("v5 outcome audit state is unsupported"));
        }
        let delta_present = self.proposal_delta_sha256.is_some();
        let accepted_present = self.accepted_record_sha256.is_some();
        let valid_stage = match stage.as_str() {
            "pre_plan" => {
                matches!(disposition.as_str(), "rejected" | "no_op")
                    && !delta_present
                    && !accepted_present
                    && matches!(
                        reason_code.as_str(),
                        "operation_rejected"
                            | "pre_plan_rejected"
                            | "no_eligible_side_operation"
                            | "no_eligible_operation"
                    )
            }
            "operator_plan" | "operator_apply" => {
                matches!(disposition.as_str(), "no_op" | "rejected") && !accepted_present
            }
            "compile" | "admission" => {
                disposition == "rejected" && delta_present && !accepted_present
            }
            "identity_ledger" => disposition == "rejected" && delta_present && !accepted_present,
            "accepted" => disposition == "accepted" && delta_present && accepted_present,
            _ => false,
        };
        if !valid_stage {
            return Err(invalid(
                "v5 outcome audit stage/disposition facts are incompatible",
            ));
        }
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_ATTEMPT_OUTCOME_AUDIT_SCHEMA.to_owned()),
            ),
            ("generationIndex", Value::from(self.generation_index)),
            ("proposalOrdinal", Value::from(self.proposal_ordinal)),
            (
                "generationConfigSha256",
                Value::String(sha256_text(
                    &Value::String(self.generation_config_sha256.clone()),
                    "v5 outcome audit generation config SHA-256",
                )?),
            ),
            (
                "sharedAuthoritySha256",
                Value::String(sha256_text(
                    &Value::String(self.shared_authority_sha256.clone()),
                    "v5 outcome audit shared authority SHA-256",
                )?),
            ),
            (
                "proposalSeed",
                Value::String(sha256_text(
                    &Value::String(self.proposal_seed.clone()),
                    "v5 outcome audit proposal seed",
                )?),
            ),
            ("originKind", Value::String(origin_kind)),
            ("disposition", Value::String(disposition)),
            ("reasonCode", Value::String(reason_code)),
            ("stage", Value::String(stage)),
            (
                "proposalDeltaSha256",
                nullable_sha256(self.proposal_delta_sha256.as_deref()),
            ),
            (
                "lineageRefsSha256",
                Value::String(sha256_text(
                    &Value::String(self.lineage_refs_sha256.clone()),
                    "v5 outcome audit lineage refs SHA-256",
                )?),
            ),
            (
                "identityLedgerEffect",
                Value::String(stable_attempt_code(
                    &Value::String(self.identity_ledger_effect.clone()),
                    "v5 outcome audit ledger effect",
                )?),
            ),
            (
                "acceptedRecordSha256",
                nullable_sha256(self.accepted_record_sha256.as_deref()),
            ),
        ]))
    }

    pub fn audit_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 outcome audit")
            .clone();
        fields.insert(
            "outcomeAuditSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 attempt outcome audit")?;
        exact_value_keys(
            fields,
            &[
                "schemaVersion",
                "generationIndex",
                "proposalOrdinal",
                "generationConfigSha256",
                "sharedAuthoritySha256",
                "proposalSeed",
                "originKind",
                "disposition",
                "reasonCode",
                "stage",
                "proposalDeltaSha256",
                "lineageRefsSha256",
                "identityLedgerEffect",
                "acceptedRecordSha256",
                "outcomeAuditSha256",
            ],
            "v5 attempt outcome audit",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str)
            != Some(V5_ATTEMPT_OUTCOME_AUDIT_SCHEMA)
        {
            return Err(invalid("v5 outcome audit schema is invalid"));
        }
        let audit = Self {
            generation_index: required(value, "generationIndex", "v5 attempt outcome audit")?
                .as_u64()
                .ok_or_else(|| invalid("v5 outcome audit generation index is invalid"))?,
            proposal_ordinal: required(value, "proposalOrdinal", "v5 attempt outcome audit")?
                .as_u64()
                .ok_or_else(|| invalid("v5 outcome audit proposal ordinal is invalid"))?,
            generation_config_sha256: sha256_text(
                required(value, "generationConfigSha256", "v5 attempt outcome audit")?,
                "v5 outcome audit generation config SHA-256",
            )?,
            shared_authority_sha256: sha256_text(
                required(value, "sharedAuthoritySha256", "v5 attempt outcome audit")?,
                "v5 outcome audit shared authority SHA-256",
            )?,
            proposal_seed: sha256_text(
                required(value, "proposalSeed", "v5 attempt outcome audit")?,
                "v5 outcome audit proposal seed",
            )?,
            origin_kind: stable_attempt_code(
                required(value, "originKind", "v5 attempt outcome audit")?,
                "v5 outcome audit origin kind",
            )?,
            disposition: stable_attempt_code(
                required(value, "disposition", "v5 attempt outcome audit")?,
                "v5 outcome audit disposition",
            )?,
            reason_code: stable_attempt_code(
                required(value, "reasonCode", "v5 attempt outcome audit")?,
                "v5 outcome audit reason code",
            )?,
            stage: stable_attempt_code(
                required(value, "stage", "v5 attempt outcome audit")?,
                "v5 outcome audit stage",
            )?,
            proposal_delta_sha256: optional_sha256_text(
                required(value, "proposalDeltaSha256", "v5 attempt outcome audit")?,
                "v5 outcome audit proposal delta SHA-256",
            )?,
            lineage_refs_sha256: sha256_text(
                required(value, "lineageRefsSha256", "v5 attempt outcome audit")?,
                "v5 outcome audit lineage refs SHA-256",
            )?,
            identity_ledger_effect: stable_attempt_code(
                required(value, "identityLedgerEffect", "v5 attempt outcome audit")?,
                "v5 outcome audit ledger effect",
            )?,
            accepted_record_sha256: optional_sha256_text(
                required(value, "acceptedRecordSha256", "v5 attempt outcome audit")?,
                "v5 outcome audit accepted record SHA-256",
            )?,
        };
        let supplied = sha256_text(
            required(value, "outcomeAuditSha256", "v5 attempt outcome audit")?,
            "v5 outcome audit SHA-256",
        )?;
        if supplied != audit.audit_sha256()? || &audit.to_value()? != value {
            return Err(invalid("v5 outcome audit identity/canonical bytes drifted"));
        }
        Ok(audit)
    }

    pub fn verify_binds_attempt(&self, attempt: &V5ProposalAttemptRecord) -> Result<()> {
        let lineage_refs_sha256 = canonical_sha256(&attempt.lineage_refs.to_value()?)?;
        if self.generation_index != attempt.generation_index
            || self.proposal_ordinal != attempt.proposal_ordinal
            || self.generation_config_sha256 != attempt.generation_config_sha256
            || self.shared_authority_sha256 != attempt.shared_authority_sha256
            || self.proposal_seed != attempt.proposal_seed
            || self.origin_kind != attempt.origin_kind
            || self.disposition != attempt.disposition
            || self.reason_code != attempt.reason_code
            || self.proposal_delta_sha256 != attempt.proposal_delta_sha256
            || self.lineage_refs_sha256 != lineage_refs_sha256
            || self.identity_ledger_effect != attempt.identity_ledger_effect
            || self.accepted_record_sha256 != attempt.accepted_record_sha256
            || self.audit_sha256()? != attempt.outcome_audit_sha256
        {
            return Err(invalid("v5 outcome audit does not bind its attempt"));
        }
        let has_plan = attempt.lineage_refs.operator_plan_sha256.is_some();
        let valid_trace_stage = match self.stage.as_str() {
            "pre_plan" => !has_plan && attempt.proposal_delta_sha256.is_none(),
            "operator_plan" | "operator_apply" => has_plan,
            "compile" | "admission" | "identity_ledger" | "accepted" => {
                attempt.proposal_delta_sha256.is_some()
            }
            _ => false,
        };
        if !valid_trace_stage {
            return Err(invalid(
                "v5 outcome audit stage does not match attempt trace/delta",
            ));
        }
        Ok(())
    }
}

/// Semantic root of the immutable JSONL attempt stream.  The file writer may
/// independently authenticate byte length and file SHA in its output
/// inventory; this root binds parsed, canonical rows and is what restart uses
/// to reject a reordered, truncated, or substituted retry history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct V5AttemptJournal {
    pub generation_index: u64,
    pub generation_config_sha256: String,
    pub shared_authority_sha256: String,
    pub attempts: Vec<V5ProposalAttemptRecord>,
}

impl V5AttemptJournal {
    fn semantic_value(&self) -> Result<Value> {
        let generation_config_sha256 = sha256_text(
            &Value::String(self.generation_config_sha256.clone()),
            "v5 attempt journal generation config SHA-256",
        )?;
        let shared_authority_sha256 = sha256_text(
            &Value::String(self.shared_authority_sha256.clone()),
            "v5 attempt journal shared authority SHA-256",
        )?;
        let mut accepted_records = BTreeSet::new();
        for (index, attempt) in self.attempts.iter().enumerate() {
            if attempt.generation_index != self.generation_index
                || attempt.generation_config_sha256 != generation_config_sha256
                || attempt.shared_authority_sha256 != shared_authority_sha256
                || attempt.proposal_ordinal != index as u64
                || attempt.proposal_seed
                    != v5_proposal_seed(&generation_config_sha256, attempt.proposal_ordinal)?
            {
                return Err(invalid(
                    "v5 attempt journal row order or authority binding drifted",
                ));
            }
            if let Some(record_sha) = &attempt.accepted_record_sha256 {
                if !accepted_records.insert(record_sha.clone()) {
                    return Err(invalid(
                        "v5 attempt journal repeats an accepted compact record",
                    ));
                }
            }
            // Re-run the row contract even when a caller constructed the
            // Rust struct directly rather than parsing a persisted JSONL row.
            let _ = attempt.semantic_value()?;
        }
        Ok(object([
            (
                "schemaVersion",
                Value::String(V5_ATTEMPT_JOURNAL_SCHEMA.to_owned()),
            ),
            ("generationIndex", Value::from(self.generation_index)),
            (
                "generationConfigSha256",
                Value::String(generation_config_sha256),
            ),
            (
                "sharedAuthoritySha256",
                Value::String(shared_authority_sha256),
            ),
            (
                "attempts",
                array(
                    self.attempts
                        .iter()
                        .map(V5ProposalAttemptRecord::to_value)
                        .collect::<Result<Vec<_>>>()?,
                ),
            ),
        ]))
    }

    pub fn attempt_journal_sha256(&self) -> Result<String> {
        Ok(canonical_sha256(&self.semantic_value()?)?)
    }

    pub fn to_value(&self) -> Result<Value> {
        let semantic = self.semantic_value()?;
        let mut fields = semantic
            .as_object()
            .expect("constructed v5 attempt journal")
            .clone();
        fields.insert(
            "attemptJournalSha256".to_owned(),
            Value::String(canonical_sha256(&semantic)?),
        );
        Ok(Value::Object(fields))
    }

    pub fn from_rows(
        generation_index: u64,
        generation_config_sha256: &str,
        shared_authority_sha256: &str,
        rows: &[Value],
    ) -> Result<Self> {
        let journal = Self {
            generation_index,
            generation_config_sha256: sha256_text(
                &Value::String(generation_config_sha256.to_owned()),
                "v5 attempt journal generation config SHA-256",
            )?,
            shared_authority_sha256: sha256_text(
                &Value::String(shared_authority_sha256.to_owned()),
                "v5 attempt journal shared authority SHA-256",
            )?,
            attempts: rows
                .iter()
                .map(V5ProposalAttemptRecord::from_value)
                .collect::<Result<Vec<_>>>()?,
        };
        // `semantic_value` performs the order, seed, authority, and duplicate
        // checks.  It is deliberately run before returning an adoptable value.
        let _ = journal.semantic_value()?;
        Ok(journal)
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = object_ref(value, "v5 attempt journal")?;
        exact_value_keys(
            fields,
            &[
                "schemaVersion",
                "generationIndex",
                "generationConfigSha256",
                "sharedAuthoritySha256",
                "attempts",
                "attemptJournalSha256",
            ],
            "v5 attempt journal",
        )?;
        if fields.get("schemaVersion").and_then(Value::as_str) != Some(V5_ATTEMPT_JOURNAL_SCHEMA) {
            return Err(invalid("v5 attempt journal schema is invalid"));
        }
        let journal = Self::from_rows(
            required(value, "generationIndex", "v5 attempt journal")?
                .as_u64()
                .ok_or_else(|| invalid("v5 attempt journal generation index is invalid"))?,
            &sha256_text(
                required(value, "generationConfigSha256", "v5 attempt journal")?,
                "v5 attempt journal generation config SHA-256",
            )?,
            &sha256_text(
                required(value, "sharedAuthoritySha256", "v5 attempt journal")?,
                "v5 attempt journal shared authority SHA-256",
            )?,
            array_ref(
                required(value, "attempts", "v5 attempt journal")?,
                "v5 attempt journal rows",
            )?,
        )?;
        let supplied = sha256_text(
            required(value, "attemptJournalSha256", "v5 attempt journal")?,
            "v5 attempt journal SHA-256",
        )?;
        if supplied != journal.attempt_journal_sha256()? {
            return Err(invalid("v5 attempt journal identity drifted"));
        }
        if &journal.to_value()? != value {
            return Err(invalid("v5 attempt journal is not canonical"));
        }
        Ok(journal)
    }

    /// Resolve every object-store outcome audit and prove it binds exactly to
    /// the corresponding JSONL row.  Extra/missing audits are rejected, so a
    /// restart cannot merely retain a pretty reason label while dropping the
    /// evidence that made the scheduler take that retry path.
    pub fn verify_outcome_audit_replay<I>(&self, audits: I) -> Result<()>
    where
        I: IntoIterator<Item = V5AttemptOutcomeAudit>,
    {
        let mut by_sha = BTreeMap::new();
        for audit in audits {
            let sha = audit.audit_sha256()?;
            if by_sha.insert(sha, audit).is_some() {
                return Err(invalid("v5 attempt replay repeats an outcome audit"));
            }
        }
        if by_sha.len() != self.attempts.len() {
            return Err(invalid("v5 attempt replay outcome audit count drifted"));
        }
        for attempt in &self.attempts {
            let audit = by_sha
                .remove(&attempt.outcome_audit_sha256)
                .ok_or_else(|| invalid("v5 attempt replay lacks an outcome audit"))?;
            audit.verify_binds_attempt(attempt)?;
        }
        if !by_sha.is_empty() {
            return Err(invalid("v5 attempt replay has unreferenced outcome audits"));
        }
        Ok(())
    }

    /// Verify resolved compact accepted-record objects against the durable
    /// attempt history.  This is intentionally an ordered, typed replay gate:
    /// a set of pretty record hashes cannot prove that a restart retained the
    /// correct proposal, seed, origin, or accepted-slot ordering.
    ///
    /// The caller must parse each row with `V5CompactAcceptedRecord::from_value`
    /// before calling this method.  No rich candidate expansion occurs here.
    pub fn verify_accepted_record_replay<I>(&self, records: I) -> Result<()>
    where
        I: IntoIterator<Item = V5CompactAcceptedRecord>,
    {
        let mut by_record_sha = BTreeMap::<String, V5CompactAcceptedRecord>::new();
        let mut candidate_ids = BTreeSet::new();
        let mut candidate_identities = BTreeSet::new();
        let mut pair_identities = BTreeSet::new();
        let mut executable_semantics = BTreeSet::new();
        for record in records {
            // Re-run the write-side validator for direct Rust constructors as
            // well as parsed values.  `record_sha256` is recomputed from the
            // complete semantic object, never trusted from an index key.
            let _ = record.to_value()?;
            if record.generation_index != self.generation_index
                || record.shared_authority_sha256 != self.shared_authority_sha256
            {
                return Err(invalid(
                    "v5 accepted record authority or generation drifted",
                ));
            }
            let record_sha = record.record_sha256()?;
            if by_record_sha.insert(record_sha, record.clone()).is_some() {
                return Err(invalid(
                    "v5 accepted replay repeats a compact record object",
                ));
            }
            if !candidate_ids.insert(record.candidate_id.clone())
                || !candidate_identities.insert(record.candidate_identity_sha256.clone())
                || !pair_identities.insert(record.pair_identity_sha256.clone())
                || !executable_semantics.insert(record.executable_semantic_sha256.clone())
            {
                return Err(invalid(
                    "v5 accepted replay repeats an executable candidate identity",
                ));
            }
        }

        let mut expected_birth_ordinal = 0_u64;
        for attempt in &self.attempts {
            let Some(expected_record_sha) = &attempt.accepted_record_sha256 else {
                continue;
            };
            let record = by_record_sha.remove(expected_record_sha).ok_or_else(|| {
                invalid("v5 accepted replay lacks the compact record named by an accepted attempt")
            })?;
            if record.record_sha256()? != *expected_record_sha
                || record.generation_index != attempt.generation_index
                || record.proposal_ordinal != attempt.proposal_ordinal
                || record.proposal_seed != attempt.proposal_seed
                || record.origin_kind != attempt.origin_kind
                || record.proposal_delta_sha256
                    != attempt
                        .proposal_delta_sha256
                        .as_deref()
                        .ok_or_else(|| invalid("accepted v5 attempt lacks proposal delta"))?
                || record.shared_authority_sha256 != attempt.shared_authority_sha256
                || record.birth_ordinal != expected_birth_ordinal
            {
                return Err(invalid(
                    "v5 accepted record does not replay its exact attempt slot",
                ));
            }
            expected_birth_ordinal = expected_birth_ordinal
                .checked_add(1)
                .ok_or_else(|| invalid("v5 accepted birth ordinal overflow"))?;
        }
        if !by_record_sha.is_empty() {
            return Err(invalid(
                "v5 accepted replay contains unreferenced compact records",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use base64::Engine;
    use flate2::read::GzDecoder;

    use super::*;

    fn actual_v5_golden() -> Value {
        let encoded = include_str!("../tests/fixtures/g0_v5_actual_journal_golden.json.gz.b64");
        let compressed = base64::engine::general_purpose::STANDARD
            .decode(encoded.split_whitespace().collect::<String>())
            .expect("decode preserved v5 journal fixture");
        let mut decoder = GzDecoder::new(compressed.as_slice());
        let mut payload = Vec::new();
        decoder
            .read_to_end(&mut payload)
            .expect("decompress preserved v5 journal fixture");
        serde_json::from_slice(&payload).expect("parse preserved v5 journal fixture")
    }

    fn shared_authority_golden() -> Value {
        let compressed = include_bytes!(
            "../../../../../tests/fixtures/temporal_qd_v5_shared_authority_oracle.json.gz"
        );
        let mut decoder = GzDecoder::new(compressed.as_slice());
        let mut payload = Vec::new();
        decoder
            .read_to_end(&mut payload)
            .expect("decompress preserved v5 shared-authority fixture");
        serde_json::from_slice(&payload).expect("parse preserved v5 shared-authority fixture")
    }

    fn first_difference(left: &Value, right: &Value, path: &str) -> Option<String> {
        match (left, right) {
            (Value::Object(left), Value::Object(right)) => {
                let keys = left.keys().chain(right.keys()).collect::<BTreeSet<_>>();
                for key in keys {
                    match (left.get(key), right.get(key)) {
                        (Some(left), Some(right)) => {
                            if let Some(found) =
                                first_difference(left, right, &format!("{path}/{key}"))
                            {
                                return Some(found);
                            }
                        }
                        _ => return Some(format!("{path}/{key}: object key presence differs")),
                    }
                }
                None
            }
            (Value::Array(left), Value::Array(right)) => {
                if left.len() != right.len() {
                    return Some(format!(
                        "{path}: array lengths {} != {}",
                        left.len(),
                        right.len()
                    ));
                }
                for (index, (left, right)) in left.iter().zip(right).enumerate() {
                    if let Some(found) = first_difference(left, right, &format!("{path}/{index}")) {
                        return Some(found);
                    }
                }
                None
            }
            _ if left == right => None,
            _ => Some(format!("{path}: left={left:?}; right={right:?}")),
        }
    }

    #[test]
    fn v5_immigrant_factory_matches_real_python_programs_without_python() {
        let fixture = actual_v5_golden();
        let seed = fixture
            .get("proposal")
            .and_then(|value| value.get("proposalSeed"))
            .and_then(Value::as_str)
            .expect("fixture proposal seed");
        let pair = fixture.get("pair").expect("fixture frozen pair");
        for side in ["long", "short"] {
            let module = pair.get(side).expect("fixture module");
            let context = module
                .get("grammarContext")
                .and_then(|value| value.get("payload"))
                .and_then(|value| value.get("context"))
                .expect("fixture frozen context");
            let budget = module
                .get("program")
                .and_then(|value| value.get("budget"))
                .expect("fixture program budget");
            let native = build_immigrant_module(side, seed, context, budget)
                .expect("native v5 immigrant module");
            assert_eq!(
                native.program,
                *module.get("program").expect("fixture program"),
                "native v5 immigrant program must be exact for {side}"
            );
            assert_eq!(
                native.program_sha256,
                module
                    .get("identities")
                    .and_then(|value| value.get("programSha256"))
                    .and_then(Value::as_str)
                    .expect("fixture program SHA-256"),
                "native v5 program identity must be exact for {side}"
            );
            let profile =
                compile_immigrant_profile(&native.program).expect("native v5 profile compilation");
            let transitions = profile
                .get("graph")
                .and_then(|graph| graph.get("transitions"))
                .and_then(Value::as_array)
                .expect("native v5 transitions");
            assert_eq!(
                transitions
                    .iter()
                    .filter(|transition| {
                        transition
                            .get("actions")
                            .and_then(Value::as_array)
                            .and_then(|actions| actions.first())
                            .and_then(|action| action.get("kind"))
                            .and_then(Value::as_str)
                            .is_some_and(is_one_shot_break_even_effect)
                    })
                    .count(),
                1,
                "native v5 module must lower one break-even request for {side}"
            );
        }
        let long = pair.get("long").expect("fixture long module");
        let short = pair.get("short").expect("fixture short module");
        let long_profile = compile_immigrant_profile(
            &build_immigrant_module(
                "long",
                seed,
                long.get("grammarContext")
                    .and_then(|value| value.get("payload"))
                    .and_then(|value| value.get("context"))
                    .expect("long context"),
                long.get("program")
                    .and_then(|value| value.get("budget"))
                    .expect("long budget"),
            )
            .expect("long native program")
            .program,
        )
        .expect("long native profile");
        let short_profile = compile_immigrant_profile(
            &build_immigrant_module(
                "short",
                seed,
                short
                    .get("grammarContext")
                    .and_then(|value| value.get("payload"))
                    .and_then(|value| value.get("context"))
                    .expect("short context"),
                short
                    .get("program")
                    .and_then(|value| value.get("budget"))
                    .expect("short budget"),
            )
            .expect("short native program")
            .program,
        )
        .expect("short native profile");
        let candidate_id = pair
            .get("validation")
            .and_then(|value| value.get("candidateId"))
            .and_then(Value::as_str)
            .expect("fixture pair candidate ID");
        let compiled = compile_bidirectional_profile(
            &long_profile,
            &short_profile,
            candidate_id,
            &ModuleSourceIdentities::from_native_report(
                long.get("nativeReport").expect("long native report"),
            )
            .expect("long identities"),
            &ModuleSourceIdentities::from_native_report(
                short.get("nativeReport").expect("short native report"),
            )
            .expect("short identities"),
        )
        .expect("native v5 pair compilation");
        assert_eq!(
            compiled.get("directionMode").and_then(Value::as_str),
            Some("both")
        );
        let modules = compiled
            .get("graph")
            .and_then(|graph| graph.get("entryArbitration"))
            .and_then(|arbitration| arbitration.get("modules"))
            .and_then(Value::as_array)
            .expect("compiled pair modules");
        assert_eq!(modules.len(), 2);
        for module in modules {
            let states = module
                .get("stateIds")
                .and_then(Value::as_array)
                .expect("compiled pair module states");
            assert!(states.iter().any(|state| {
                state
                    .as_str()
                    .is_some_and(|id| id.ends_with("position_hub_be0"))
            }));
            assert!(states.iter().any(|state| {
                state
                    .as_str()
                    .is_some_and(|id| id.ends_with("position_hub_be1"))
            }));
        }
    }

    #[test]
    fn v5_proposal_seed_matches_preserved_live_entry() {
        let fixture = actual_v5_golden();
        let config = fixture
            .get("entrySurface")
            .and_then(|value| value.get("configSha256"))
            .and_then(Value::as_str)
            .expect("fixture config identity");
        let ordinal = fixture
            .get("candidate")
            .and_then(|value| value.get("proposalOrdinal"))
            .and_then(Value::as_u64)
            .expect("fixture proposal ordinal");
        let expected = fixture
            .get("proposal")
            .and_then(|value| value.get("proposalSeed"))
            .and_then(Value::as_str)
            .expect("fixture proposal seed");
        assert_eq!(
            v5_proposal_seed(config, ordinal).expect("native v5 proposal seed"),
            expected
        );
    }

    #[test]
    fn v5_compact_delta_is_self_authenticating_and_tamper_fails_closed() {
        let fixture = actual_v5_golden();
        let seed = fixture
            .get("proposal")
            .and_then(|value| value.get("proposalSeed"))
            .and_then(Value::as_str)
            .expect("fixture proposal seed");
        let pair = fixture.get("pair").expect("fixture frozen pair");
        let module = |side: &str| {
            let row = pair.get(side).expect("fixture side");
            build_immigrant_module(
                side,
                seed,
                row.get("grammarContext")
                    .and_then(|value| value.get("payload"))
                    .and_then(|value| value.get("context"))
                    .expect("fixture frozen context"),
                row.get("program")
                    .and_then(|value| value.get("budget"))
                    .expect("fixture budget"),
            )
            .expect("native fixture module")
        };
        let delta = proposal_delta(
            0,
            seed,
            "random_immigrant",
            &module("long"),
            &module("short"),
        )
        .expect("compact delta");
        validate_proposal_delta(&delta).expect("self-authenticating compact delta");
        let mut altered = delta;
        altered
            .get_mut("proposalSeed")
            .expect("proposal seed")
            .clone_from(&Value::String("tampered".to_owned()));
        assert!(validate_proposal_delta(&altered).is_err());
    }

    #[test]
    fn v5_native_validation_identities_match_preserved_python_reports() {
        let fixture = actual_v5_golden();
        let pair = fixture.get("pair").expect("fixture frozen pair");
        for side in ["long", "short"] {
            let module = pair.get(side).expect("fixture module");
            let expected = module.get("nativeReport").expect("fixture native report");
            let actual = validate_native_profile(
                module.get("profile").expect("fixture module profile"),
                expected
                    .get("candidateId")
                    .and_then(Value::as_str)
                    .expect("fixture module candidate ID"),
            )
            .expect("native module validation");
            assert_eq!(
                actual.report, *expected,
                "native module report must be exact for {side}"
            );
        }
        let expected = pair.get("validation").expect("fixture pair validation");
        let actual = validate_native_profile(
            pair.get("profile").expect("fixture pair profile"),
            expected
                .get("candidateId")
                .and_then(Value::as_str)
                .expect("fixture pair candidate ID"),
        )
        .expect("native pair validation");
        assert_eq!(
            actual.report, *expected,
            "native pair validation must be exact"
        );
    }

    #[test]
    fn v5_factory_audit_topology_and_resource_fingerprints_match_preserved_entry() {
        let fixture = actual_v5_golden();
        let seed = fixture
            .get("proposal")
            .and_then(|value| value.get("proposalSeed"))
            .and_then(Value::as_str)
            .expect("fixture proposal seed");
        let pair = fixture.get("pair").expect("fixture frozen pair");
        let audit = fixture
            .get("proposal")
            .and_then(|value| value.get("factoryConstructionAudit"))
            .and_then(|value| value.get("sides"))
            .expect("fixture factory audit sides");
        for side in ["long", "short"] {
            let module = pair.get(side).expect("fixture module");
            let native = build_immigrant_module(
                side,
                seed,
                module
                    .get("grammarContext")
                    .and_then(|value| value.get("payload"))
                    .and_then(|value| value.get("context"))
                    .expect("fixture context"),
                module
                    .get("program")
                    .and_then(|value| value.get("budget"))
                    .expect("fixture budget"),
            )
            .expect("native v5 immigrant module");
            let expected = audit.get(side).expect("fixture side audit");
            assert_eq!(
                v5_semantic_topology_sha256(&native.program).expect("native topology signature"),
                expected
                    .get("semanticTopologySha256")
                    .and_then(Value::as_str)
                    .expect("fixture topology SHA-256"),
                "native topology signature must be exact for {side}"
            );
            assert_eq!(
                v5_resource_fingerprint_sha256(&native.program)
                    .expect("native resource fingerprint"),
                expected
                    .get("resourceFingerprintSha256")
                    .and_then(Value::as_str)
                    .expect("fixture resource SHA-256"),
                "native resource fingerprint must be exact for {side}"
            );
        }
    }

    #[test]
    fn v5_general_compiler_uses_typed_authority_and_current_one_shot_lowering() {
        let fixture = actual_v5_golden();
        let authority_fixture = shared_authority_golden();
        let authority = V5SharedConstructionAuthority::from_shared_object(
            authority_fixture
                .get("sealedAuthority")
                .expect("sealed shared authority"),
        )
        .expect("parse sealed shared authority");
        let projection = authority
            .operator_authority_projection()
            .expect("typed operator authority projection");
        let seed = fixture
            .get("proposal")
            .and_then(|value| value.get("proposalSeed"))
            .and_then(Value::as_str)
            .expect("fixture proposal seed");
        let pair = fixture.get("pair").expect("fixture frozen pair");
        for side in ["long", "short"] {
            let module = pair.get(side).expect("fixture module");
            let program = module.get("program").expect("fixture program");
            let candidate_id = v5_module_candidate_id(
                seed,
                side,
                module
                    .get("identities")
                    .and_then(|value| value.get("programSha256"))
                    .and_then(Value::as_str)
                    .expect("fixture genome program identity"),
            )
            .expect("module candidate ID");
            let compiled = compile_v5_module_profile(program, &projection, side, &candidate_id)
                .expect("authority-validated native evolved compiler");
            let identities = module.get("identities").expect("fixture identities");
            assert_eq!(
                compiled.profile,
                compile_immigrant_profile(program).expect("current compiler profile")
            );
            assert_eq!(
                compiled.genome_program_sha256,
                identities
                    .get("programSha256")
                    .and_then(Value::as_str)
                    .expect("fixture genome SHA"),
            );
            assert_eq!(
                compiled.raw_profile_sha256,
                canonical_sha256(&compiled.profile).expect("current profile identity")
            );
            assert_eq!(
                compiled
                    .native_validation_report
                    .get("candidateId")
                    .and_then(Value::as_str),
                Some(candidate_id.as_str())
            );
            assert_eq!(
                compiled
                    .native_validation_report
                    .get("candidateAcceptable")
                    .and_then(Value::as_bool),
                Some(true)
            );
        }
    }

    #[test]
    fn v5_break_even_and_zero_r_tighten_are_consumed_once_per_position() {
        let fixture = actual_v5_golden();
        for effect in [
            "move_stop_to_break_even_next_open",
            "tighten_stop_next_open",
        ] {
            let mut program = fixture
                .get("pair")
                .and_then(|pair| pair.get("long"))
                .and_then(|module| module.get("program"))
                .cloned()
                .expect("fixture long program");
            let management_edge = program
                .get_mut("edges")
                .and_then(Value::as_array_mut)
                .expect("program edges")
                .iter_mut()
                .find(|edge| {
                    edge.get("effect").and_then(Value::as_str)
                        == Some("move_stop_to_break_even_next_open")
                })
                .expect("fixture break-even edge");
            management_edge["effect"] = Value::String(effect.to_owned());

            let profile = compile_immigrant_profile(&program).expect("compile one-shot profile");
            let transitions = profile
                .get("graph")
                .and_then(|graph| graph.get("transitions"))
                .and_then(Value::as_array)
                .expect("compiled transitions");
            let requests = transitions
                .iter()
                .filter(|transition| {
                    transition
                        .get("actions")
                        .and_then(Value::as_array)
                        .and_then(|actions| actions.first())
                        .and_then(|action| action.get("kind"))
                        .and_then(Value::as_str)
                        == Some(effect)
                })
                .collect::<Vec<_>>();
            assert_eq!(requests.len(), 1, "{effect} must have one request route");
            assert_eq!(
                requests[0].get("sourceStateId").and_then(Value::as_str),
                Some("position_hub_be0")
            );
            let destination = |id: &str| {
                transitions
                    .iter()
                    .find(|transition| transition.get("id").and_then(Value::as_str) == Some(id))
                    .and_then(|transition| transition.get("destinationStateId"))
                    .and_then(Value::as_str)
            };
            assert_eq!(
                destination("e_hub_manage_applied_be0"),
                Some("position_hub_be1")
            );
            assert_eq!(
                destination("e_hub_manage_rejected_be0"),
                Some("position_hub_be0")
            );
            assert_eq!(
                destination("e_hub_manage_canceled_be0"),
                Some("position_hub_be0")
            );
            assert_eq!(destination("exit_rejected_be1"), Some("position_hub_be1"));
        }
    }

    #[test]
    fn v5_cooldowns_name_every_final_phase_specific_containing_transition() {
        fn cooldown_transition_ids(value: &Value, output: &mut Vec<String>) {
            match value {
                Value::Object(fields) => {
                    if fields.get("kind").and_then(Value::as_str) == Some("action_cooldown_elapsed")
                    {
                        output.push(
                            fields
                                .get("transitionId")
                                .and_then(Value::as_str)
                                .expect("cooldown transition ID")
                                .to_owned(),
                        );
                    }
                    for child in fields.values() {
                        cooldown_transition_ids(child, output);
                    }
                }
                Value::Array(rows) => {
                    for child in rows {
                        cooldown_transition_ids(child, output);
                    }
                }
                _ => {}
            }
        }

        let fixture = actual_v5_golden();
        let pair = fixture.get("pair").expect("fixture pair");
        let long_program = pair
            .get("long")
            .and_then(|module| module.get("program"))
            .expect("fixture long program");
        let mut short_program = pair
            .get("short")
            .and_then(|module| module.get("program"))
            .cloned()
            .expect("fixture short program");
        let cooldown_guard = |transition_id: &str| {
            object([
                ("kind", Value::String("all".to_owned())),
                (
                    "guards",
                    array([
                        object([("kind", Value::String("always".to_owned()))]),
                        object([
                            ("kind", Value::String("action_cooldown_elapsed".to_owned())),
                            ("transitionId", Value::String(transition_id.to_owned())),
                            ("actionOrdinal", Value::from(0)),
                            ("evaluations", Value::from(3)),
                        ]),
                    ]),
                ),
            ])
        };
        let edges = short_program
            .get_mut("edges")
            .and_then(Value::as_array_mut)
            .expect("short edges");
        let management_index = edges
            .iter()
            .position(|edge| edge.get("id").and_then(Value::as_str) == Some("hub_manage"))
            .expect("short management edge");
        edges[management_index]["guard"] = cooldown_guard("e_hub_manage");
        let mut repeatable = clone_value(&edges[management_index]).expect("repeatable edge clone");
        repeatable["id"] = Value::String("hub_manage_repeat".to_owned());
        repeatable["effect"] = Value::String("set_target_next_open".to_owned());
        repeatable["guard"] = cooldown_guard("e_hub_manage_repeat");
        edges.push(repeatable);

        let long_profile = compile_immigrant_profile(long_program).expect("compile long profile");
        let short_profile =
            compile_v5_profile_body(&short_program).expect("compile short cooldown profile");
        let compiled = compile_bidirectional_profile(
            &long_profile,
            &short_profile,
            "qd_cooldown_regression",
            &ModuleSourceIdentities {
                profile_snapshot_sha256: "sha256:long-profile".to_owned(),
                program_sha256: "sha256:long-program".to_owned(),
            },
            &ModuleSourceIdentities {
                profile_snapshot_sha256: "sha256:short-profile".to_owned(),
                program_sha256: "sha256:short-program".to_owned(),
            },
        )
        .expect("compile bidirectional cooldown profile");
        let transitions = compiled
            .get("graph")
            .and_then(|graph| graph.get("transitions"))
            .and_then(Value::as_array)
            .expect("compiled transitions");
        for expected_id in [
            "short_e_hub_manage_be0",
            "short_e_hub_manage_repeat_be0",
            "short_e_hub_manage_repeat_be1",
        ] {
            let request = transitions
                .iter()
                .find(|transition| {
                    transition.get("id").and_then(Value::as_str) == Some(expected_id)
                })
                .expect("final phase-specific request transition");
            let mut cooldown_ids = Vec::new();
            cooldown_transition_ids(
                request.get("guard").expect("management request guard"),
                &mut cooldown_ids,
            );
            assert_eq!(cooldown_ids, [expected_id]);
        }
    }

    #[test]
    fn v5_compiler_flattens_only_overdepth_conjunctions_and_fails_closed_otherwise() {
        let fixture = actual_v5_golden();
        let mut program = fixture
            .get("pair")
            .and_then(|pair| pair.get("long"))
            .and_then(|module| module.get("program"))
            .cloned()
            .expect("fixture long program");
        let setup = program
            .get_mut("nodes")
            .and_then(Value::as_array_mut)
            .expect("program nodes")
            .iter_mut()
            .find(|node| node.get("id").and_then(Value::as_str) == Some("setup"))
            .expect("setup node");
        setup["guard"] = object([
            ("kind", Value::String("all".to_owned())),
            (
                "guards",
                array([
                    object([
                        ("kind", Value::String("state_age_at_least".to_owned())),
                        ("events", Value::from(1)),
                    ]),
                    object([
                        ("kind", Value::String("consecutive_true".to_owned())),
                        (
                            "predicate",
                            object([
                                ("kind", Value::String("utc_time_window".to_owned())),
                                ("startMinute", Value::from(0)),
                                ("endMinute", Value::from(1439)),
                                (
                                    "weekdays",
                                    array((0_u64..=6).map(Value::from).collect::<Vec<_>>()),
                                ),
                            ]),
                        ),
                        ("evaluations", Value::from(5)),
                    ]),
                ]),
            ),
        ]);
        let entry_edge = program
            .get_mut("edges")
            .and_then(Value::as_array_mut)
            .expect("program edges")
            .iter_mut()
            .find(|edge| edge.get("id").and_then(Value::as_str) == Some("setup_entry"))
            .expect("entry edge");
        entry_edge["guard"] = object([("kind", Value::String("always".to_owned()))]);

        let profile = compile_immigrant_profile(&program).expect("compile depth regression");
        let transition = profile
            .get("graph")
            .and_then(|graph| graph.get("transitions"))
            .and_then(Value::as_array)
            .expect("compiled transitions")
            .iter()
            .find(|transition| {
                transition.get("id").and_then(Value::as_str) == Some("e_setup_entry")
            })
            .expect("compiled entry transition");
        let guard = transition.get("guard").expect("compiled entry guard");
        assert_eq!(
            compiled_guard_depth(guard).expect("valid compiled guard"),
            3
        );
        assert_eq!(
            guard
                .get("guards")
                .and_then(Value::as_array)
                .expect("flattened conjunction")
                .iter()
                .map(|item| item.get("kind").and_then(Value::as_str).unwrap_or(""))
                .collect::<Vec<_>>(),
            [
                "position_exists",
                "state_age_at_least",
                "consecutive_true",
                "always",
            ],
            "normalization must preserve left-to-right conjunction ordering",
        );

        let irreducible = object([
            ("kind", Value::String("consecutive_true".to_owned())),
            (
                "predicate",
                object([
                    ("kind", Value::String("consecutive_true".to_owned())),
                    (
                        "predicate",
                        object([
                            ("kind", Value::String("consecutive_true".to_owned())),
                            (
                                "predicate",
                                object([("kind", Value::String("always".to_owned()))]),
                            ),
                            ("evaluations", Value::from(2)),
                        ]),
                    ),
                    ("evaluations", Value::from(2)),
                ]),
            ),
            ("evaluations", Value::from(2)),
        ]);
        let error = guard_all([
            irreducible,
            object([("kind", Value::String("always".to_owned()))]),
        ])
        .expect_err("irreducible compiled depth must fail closed");
        assert!(
            error
                .to_string()
                .contains("compiled guard depth exceeds Dashboard maximum")
        );

        let malformed = object([
            ("kind", Value::String("all".to_owned())),
            ("guards", Value::String("not-an-array".to_owned())),
        ]);
        assert!(
            guard_all([malformed]).is_err(),
            "normalization must not erase malformed composite children",
        );

        let eight_leaves = object([
            ("kind", Value::String("all".to_owned())),
            (
                "guards",
                array((0..8).map(|_| object([("kind", Value::String("always".to_owned()))]))),
            ),
        ]);
        let depth_trigger = object([
            ("kind", Value::String("all".to_owned())),
            (
                "guards",
                array([
                    eight_leaves,
                    object([
                        ("kind", Value::String("all".to_owned())),
                        (
                            "guards",
                            array([
                                object([
                                    ("kind", Value::String("consecutive_true".to_owned())),
                                    (
                                        "predicate",
                                        object([("kind", Value::String("always".to_owned()))]),
                                    ),
                                    ("evaluations", Value::from(2)),
                                ]),
                                object([("kind", Value::String("always".to_owned()))]),
                            ]),
                        ),
                    ]),
                ]),
            ),
        ]);
        assert!(
            guard_all([
                object([
                    ("kind", Value::String("position_exists".to_owned())),
                    ("expected", Value::Bool(false)),
                ]),
                depth_trigger,
            ])
            .is_err(),
            "depth normalization must fail closed rather than exceed Dashboard's width cap",
        );
    }

    #[test]
    fn v5_semantic_topology_is_invariant_to_authored_resource_use_order() {
        let fixture = actual_v5_golden();
        let program = fixture
            .get("pair")
            .and_then(|value| value.get("long"))
            .and_then(|value| value.get("program"))
            .expect("fixture long program");
        let mut authored = clone_value(program).expect("clone long program");
        let nodes = authored
            .get_mut("nodes")
            .and_then(Value::as_array_mut)
            .expect("fixture nodes");
        let setup = nodes
            .iter_mut()
            .find(|node| node.get("id").and_then(Value::as_str) == Some("setup"))
            .expect("fixture setup node");
        let uses = setup
            .get_mut("resources")
            .and_then(Value::as_array_mut)
            .expect("fixture setup resource uses");
        // The fixture's setup currently has one evidence-group use.  Add an
        // otherwise topology-neutral event use so this regression exercises
        // the exact historical ordering seam (EVIDENCE_GROUP vs EVENT).
        uses.push(object([
            ("kind", Value::String("event".to_owned())),
            ("id", Value::String("ordering_probe_event".to_owned())),
        ]));
        let mut permuted = clone_value(&authored).expect("clone authored resource ordering");
        let setup = permuted
            .get_mut("nodes")
            .and_then(Value::as_array_mut)
            .expect("permuted nodes")
            .iter_mut()
            .find(|node| node.get("id").and_then(Value::as_str) == Some("setup"))
            .expect("permuted setup node");
        let uses = setup
            .get_mut("resources")
            .and_then(Value::as_array_mut)
            .expect("permuted setup resource uses");
        assert_eq!(
            uses.len(),
            2,
            "regression program must exercise two resource kinds"
        );
        uses.reverse();
        assert_eq!(
            v5_semantic_topology_sha256(&authored).expect("authored topology"),
            v5_semantic_topology_sha256(&permuted).expect("permuted topology"),
            "resource-use ordering is representational and must not affect topology identity",
        );
    }

    #[test]
    fn v5_reconstructs_the_preserved_full_identity_chain_and_compact_g0_projection() {
        let fixture = actual_v5_golden();
        let authority_fixture = shared_authority_golden();
        let authority = V5SharedConstructionAuthority::from_shared_object(
            authority_fixture
                .get("sealedAuthority")
                .expect("sealed shared authority"),
        )
        .expect("parse sealed shared authority");
        let seed = fixture
            .get("proposal")
            .and_then(|value| value.get("proposalSeed"))
            .and_then(Value::as_str)
            .expect("preserved proposal seed");
        let pair = fixture.get("pair").expect("preserved frozen pair");
        let fresh =
            reconstruct_g0_pair(&authority, seed, None).expect("reconstruct fresh native G0 pair");
        assert_eq!(
            fresh.pair_identity_sha256,
            "sha256:a0509f5516316342b240cdf6cf47419473caef32f027ea68cf59cfa9a40b1133",
            "fresh native construction must match the one-shot break-even compiler identity; \
             historical repeatable-break-even identities are import-only",
        );
        // The stopped rich entry has an old short lineage topology identity,
        // but its factory audit is the authoritative current-Python topology
        // result.  Fresh construction must use that current value; only the
        // historical FrozenModule projection below may retain the stale one.
        for (side, actual) in [("long", &fresh.long), ("short", &fresh.short)] {
            let expected_current = fixture
                .get("proposal")
                .and_then(|value| value.get("factoryConstructionAudit"))
                .and_then(|value| value.get("sides"))
                .and_then(|value| value.get(side))
                .and_then(|value| value.get("semanticTopologySha256"))
                .and_then(Value::as_str)
                .expect("current Python factory topology identity");
            assert_eq!(
                actual.semantic_topology_sha256, expected_current,
                "fresh native {side} topology must match the current Python factory audit",
            );
        }
        let reconstructed = project_preserved_g0_pair(
            &authority,
            seed,
            None,
            pair.get("long")
                .and_then(|value| value.get("lineage"))
                .expect("preserved long lineage"),
            pair.get("short")
                .and_then(|value| value.get("lineage"))
                .expect("preserved short lineage"),
        )
        .expect("project preserved historical G0 pair");
        for (side, actual) in [
            ("long", &reconstructed.long),
            ("short", &reconstructed.short),
        ] {
            let expected = pair.get(side).expect("preserved module");
            let identities = expected
                .get("identities")
                .expect("preserved module identities");
            assert_eq!(
                actual.genome_program_sha256,
                identities
                    .get("programSha256")
                    .and_then(Value::as_str)
                    .expect("preserved genome program SHA"),
                "{side} evolvable genome SHA must remain distinct and exact",
            );
            assert_ne!(
                actual.genome_program_sha256, actual.validation.program_sha256,
                "{side} test fixture must prove the two program identity namespaces differ",
            );
            assert_eq!(
                actual.validation.raw_profile_sha256,
                canonical_sha256(&actual.profile).expect("current profile SHA")
            );
        }
        let expected_executable = canonical_sha256(&object([
            (
                "schemaVersion",
                Value::String(V5_EXECUTABLE_SEMANTIC_SCHEMA.to_owned()),
            ),
            (
                "longProfileSha256",
                Value::String(reconstructed.long.validation.raw_profile_sha256.clone()),
            ),
            (
                "shortProfileSha256",
                Value::String(reconstructed.short.validation.raw_profile_sha256.clone()),
            ),
        ]))
        .expect("preserved executable semantic SHA");
        assert_eq!(
            reconstructed.executable_semantic_sha256,
            expected_executable
        );
        let projection = reconstructed
            .compact_g0_descriptor_projection(&authority, seed)
            .expect("direct compact G0 descriptor");
        let replay = project_preserved_g0_pair(
            &authority,
            seed,
            None,
            pair.get("long")
                .and_then(|value| value.get("lineage"))
                .expect("preserved long lineage"),
            pair.get("short")
                .and_then(|value| value.get("lineage"))
                .expect("preserved short lineage"),
        )
        .expect("replay preserved G0 pair")
        .compact_g0_descriptor_projection(&authority, seed)
        .expect("replay compact descriptor");
        assert_eq!(
            projection, replay,
            "current compact G0 projection must replay"
        );
    }

    #[test]
    fn v5_descriptor_origin_wrappers_reject_cross_origin_candidate_bindings() {
        let fixture = actual_v5_golden();
        let authority_fixture = shared_authority_golden();
        let authority = V5SharedConstructionAuthority::from_shared_object(
            authority_fixture
                .get("sealedAuthority")
                .expect("sealed shared authority"),
        )
        .expect("parse sealed shared authority");
        let seed = fixture
            .get("proposal")
            .and_then(|value| value.get("proposalSeed"))
            .and_then(Value::as_str)
            .expect("fixture proposal seed");
        let immigrant = reconstruct_g0_pair(&authority, seed, None)
            .expect("reconstruct sealed G0 immigrant pair");
        let immigrant_input = immigrant
            .compact_g0_descriptor_input(&authority, seed)
            .expect("derive compact G0 descriptor input");
        assert!(crate::g0::derive_descriptor_projection_from_compact_v5(&immigrant_input).is_ok());
        assert!(
            crate::g0::derive_descriptor_projection_from_evolved_compact_v5(&immigrant_input)
                .is_err()
        );

        // The descriptor projection itself is origin-neutral, but its
        // candidate envelope is not.  Build an independently finalized
        // structural candidate from the same sealed compiled pair facts to
        // prove the two wrappers cannot be substituted for one another.
        let structural_delta_sha256 = canonical_sha256(&object([(
            "schemaVersion",
            Value::String("temporal_qd_v5_descriptor_origin_test_delta_v1".to_owned()),
        )]))
        .expect("structural delta identity");
        let structural = finalize_evolved_reconstructed_pair(
            &authority,
            seed,
            "structural_offspring",
            &structural_delta_sha256,
            immigrant.long.clone(),
            immigrant.short.clone(),
            immigrant.side_targeted_lineage.clone(),
        )
        .expect("finalize sealed structural candidate");
        let structural_input = structural
            .compact_g0_descriptor_input(&authority, seed)
            .expect("derive compact structural descriptor input");
        assert!(
            crate::g0::derive_descriptor_projection_from_evolved_compact_v5(&structural_input,)
                .is_ok()
        );
        assert!(
            crate::g0::derive_descriptor_projection_from_compact_v5(&structural_input).is_err()
        );
    }

    #[test]
    fn v5_evolved_parent_snapshot_replays_without_source_archive_file() {
        let authority_fixture = shared_authority_golden();
        let authority = V5SharedConstructionAuthority::from_shared_object(
            authority_fixture
                .get("sealedAuthority")
                .expect("sealed shared authority"),
        )
        .expect("parse sealed shared authority");
        let generation_config_sha256 = canonical_sha256(&object([(
            "schemaVersion",
            Value::String("temporal_qd_v5_parent_snapshot_test_config_v1".to_owned()),
        )]))
        .expect("test generation config identity");
        let proposal_seed =
            v5_proposal_seed(&generation_config_sha256, 0).expect("derive G0 proposal seed");
        let material = build_v5_g0_accepted_material(&authority, 1, 0, 0, &proposal_seed)
            .expect("construct sealed G0 compact parent");
        let mut parent = parent_reference_from_v5_compact_record(
            &authority,
            &material.proposal_delta,
            &material.record,
        )
        .expect("construct authenticated parent reference");
        parent.selection_audit = Some(object([(
            "schemaVersion",
            Value::String("temporal_qd_v5_parent_snapshot_selection_audit_test_v1".to_owned()),
        )]));
        let source_input_binding_sha256 = canonical_sha256(&object([(
            "schemaVersion",
            Value::String("temporal_qd_v5_parent_snapshot_input_binding_test_v1".to_owned()),
        )]))
        .expect("source input binding identity");
        let source_semantic_sha256 = canonical_sha256(&object([(
            "schemaVersion",
            Value::String("temporal_qd_v5_parent_snapshot_archive_semantic_test_v1".to_owned()),
        )]))
        .expect("source archive semantic identity");
        let snapshot = V5EvolvedParentSnapshot::from_parent_reference(
            &source_input_binding_sha256,
            &source_semantic_sha256,
            &parent,
        )
        .expect("seal parent snapshot");
        let value = snapshot.to_value().expect("encode parent snapshot");
        let replay =
            V5EvolvedParentSnapshot::from_value(&value).expect("parse canonical parent snapshot");
        assert_eq!(
            replay.parent_snapshot_sha256().expect("snapshot identity"),
            snapshot
                .parent_snapshot_sha256()
                .expect("original snapshot identity"),
        );
        let binding = replay.object_binding().expect("snapshot object binding");
        binding
            .to_value()
            .expect("validate snapshot object binding");
        assert!(
            binding
                .relative_path
                .starts_with("v5-native/objects/sha256/")
        );
        let replay_parent = replay
            .parent_reference_for_replay()
            .expect("rebuild exact parent reference from snapshot");
        assert_eq!(replay_parent.pair_payload, parent.pair_payload);
        assert_eq!(replay_parent.candidate_id, parent.candidate_id);
        assert_eq!(replay_parent.selection_audit, parent.selection_audit);
        let loaded = load_v5_evolved_parent_from_snapshot(&authority, &replay)
            .expect("offline snapshot must use sealed parent loader");
        assert_eq!(loaded.accepted_record, material.record);

        // A self-hash alone cannot hide an attempt-reference substitution:
        // recompute the outer hash and require the duplicated compact record
        // to reject the altered selected-parent binding.
        let mut tampered = value;
        tampered
            .get_mut("attemptReference")
            .and_then(|value| value.get_mut("candidateId"))
            .expect("snapshot attempt candidate ID")
            .clone_from(&Value::String("qd_substituted_parent".to_owned()));
        let mut semantic = tampered.as_object().expect("snapshot is object").clone();
        semantic.remove("parentSnapshotSha256");
        tampered
            .as_object_mut()
            .expect("snapshot is mutable object")
            .insert(
                "parentSnapshotSha256".to_owned(),
                Value::String(
                    canonical_sha256(&Value::Object(semantic))
                        .expect("recompute tampered snapshot self hash"),
                ),
            );
        assert!(V5EvolvedParentSnapshot::from_value(&tampered).is_err());
    }

    #[test]
    fn v5_sha_parser_rejects_noncanonical_spellings() {
        let valid = Value::String(format!("sha256:{}", "a".repeat(64)));
        assert!(sha256_text(&valid, "test SHA").is_ok());
        let uppercase = Value::String(format!("sha256:{}", "A".repeat(64)));
        assert!(sha256_text(&uppercase, "test SHA").is_err());
        let non_hex = Value::String(format!("sha256:{}", "g".repeat(64)));
        assert!(sha256_text(&non_hex, "test SHA").is_err());
    }

    #[test]
    fn v5_attempt_journal_preserves_failed_attempts_and_rejects_replay_drift() {
        let config =
            canonical_sha256(&object([("config", Value::from("g5"))])).expect("config SHA");
        let authority = canonical_sha256(&object([("authority", Value::from("sealed"))]))
            .expect("authority SHA");
        let delta =
            canonical_sha256(&object([("delta", Value::from("candidate"))])).expect("delta SHA");
        let accepted =
            canonical_sha256(&object([("record", Value::from("accepted"))])).expect("record SHA");
        let no_refs = || V5AttemptLineageRefs {
            parent: None,
            mate: None,
            parent_selection_receipt_sha256: None,
            operator_plan_sha256: None,
            operator_application_sha256: None,
            operator_trace_sha256: None,
            step_index: None,
        };
        let attempt_and_audit = |ordinal: u64,
                                 disposition: &str,
                                 reason: &str,
                                 effect: &str,
                                 stage: &str,
                                 delta_sha: Option<String>,
                                 accepted_sha: Option<String>| {
            let lineage_refs = no_refs();
            let proposal_seed = v5_proposal_seed(&config, ordinal).expect("proposal seed");
            let audit = V5AttemptOutcomeAudit {
                generation_index: 5,
                proposal_ordinal: ordinal,
                generation_config_sha256: config.clone(),
                shared_authority_sha256: authority.clone(),
                proposal_seed: proposal_seed.clone(),
                origin_kind: "random_immigrant".to_owned(),
                disposition: disposition.to_owned(),
                reason_code: reason.to_owned(),
                stage: stage.to_owned(),
                proposal_delta_sha256: delta_sha.clone(),
                lineage_refs_sha256: canonical_sha256(&lineage_refs.to_value().expect("refs"))
                    .expect("lineage SHA"),
                identity_ledger_effect: effect.to_owned(),
                accepted_record_sha256: accepted_sha.clone(),
            };
            let attempt = V5ProposalAttemptRecord {
                generation_index: 5,
                proposal_ordinal: ordinal,
                generation_config_sha256: config.clone(),
                shared_authority_sha256: authority.clone(),
                proposal_seed,
                origin_kind: "random_immigrant".to_owned(),
                proposal_delta_sha256: delta_sha,
                disposition: disposition.to_owned(),
                reason_code: reason.to_owned(),
                lineage_refs,
                identity_ledger_effect: effect.to_owned(),
                outcome_audit_sha256: audit.audit_sha256().expect("outcome audit SHA"),
                accepted_record_sha256: accepted_sha,
            };
            (attempt, audit)
        };
        let (rejected, rejected_audit) = attempt_and_audit(
            0,
            "rejected",
            "pre_plan_rejected",
            "not_applicable",
            "pre_plan",
            None,
            None,
        );
        let (compile_rejected, compile_audit) = attempt_and_audit(
            1,
            "rejected",
            "compile_validation_rejected",
            "not_applicable",
            "compile",
            Some(delta.clone()),
            None,
        );
        let (accepted_attempt, accepted_audit) = attempt_and_audit(
            2,
            "accepted",
            "accepted",
            "inserted",
            "accepted",
            Some(delta),
            Some(accepted),
        );
        let journal = V5AttemptJournal {
            generation_index: 5,
            generation_config_sha256: config.clone(),
            shared_authority_sha256: authority.clone(),
            attempts: vec![rejected, compile_rejected, accepted_attempt],
        };
        let serialized = journal.to_value().expect("durable journal");
        let replay = V5AttemptJournal::from_value(&serialized).expect("exact replay journal");
        assert_eq!(replay, journal);
        replay
            .verify_outcome_audit_replay([rejected_audit, compile_audit, accepted_audit])
            .expect("outcome audit replay binding");

        let mut reordered_rows = replay
            .attempts
            .iter()
            .map(V5ProposalAttemptRecord::to_value)
            .collect::<Result<Vec<_>>>()
            .expect("rows");
        reordered_rows.swap(0, 1);
        assert!(V5AttemptJournal::from_rows(5, &config, &authority, &reordered_rows).is_err());
    }
}
