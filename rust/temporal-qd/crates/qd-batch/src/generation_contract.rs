//! Closed outer contract for one coarse native pair-generation invocation.
//!
//! Runtime semantics stay in `temporal-qd-runtime`/`temporal-qd-kernel`.  This
//! module binds the immutable files and values that the standalone executable
//! must reopen before it is allowed to invoke those crates.

use anyhow::{Result, anyhow, bail};
use temporal_qd_contract::{
    Map, Value, canonical_json_line, canonical_sha256, canonical_sha256_without_object_field,
};

pub const GENERATION_MANIFEST_SCHEMA: &str = "temporal_qd_native_generate_generation_manifest_v1";
pub const GENERATION_RESULT_SCHEMA: &str = "temporal_qd_native_generate_generation_result_v1";
pub const GENERATION_OPERATION: &str = "generate_generation";
pub const GENERATION_RESULT_PATH: &str = "generation-result.json";
pub const RUNTIME_AUTHORITY_SCHEMA: &str = "temporal_qd_runtime_authority_v1";
pub const PAIR_GENERATION_SCHEMA: &str = "temporal_qd_pair_generation_v2";
pub const PAIR_GENERATION_RESULT_SCHEMA: &str = "temporal_qd_pair_generation_result_v1";
pub const FRONT_GENERATION_PROGRESS_SCHEMA: &str = "temporal_qd_front_generation_progress_v1";

#[derive(Clone, Debug)]
pub struct GenerationManifest {
    pub contract_version: String,
    pub authority_sha256: String,
    pub runtime_authority: Value,
    pub runtime_authority_sha256: String,
    pub parent_archive_path: String,
    pub parent_archive_sha256: String,
    pub identity_ledger_path: String,
    pub identity_ledger_sha256: String,
    pub output_root: String,
    pub final_newline: String,
    pub generation_config: Value,
    pub generation_config_sha256: String,
    pub target_unique_candidates: u64,
    pub max_proposal_attempts: u64,
    pub max_new_proposals: Option<u64>,
    pub allow_empty_quality_bootstrap: bool,
    pub parent_schedule: Option<Value>,
    pub g0_evaluation_width: Option<u64>,
    pub frozen_construction_catalog: Option<Value>,
    pub publication_policy: Value,
    pub native_proposal_authority_sha256: String,
    pub result_path: String,
    pub manifest_sha256: String,
}

#[derive(Clone, Debug)]
pub struct GenerationResult {
    pub value: Value,
}

fn exact_keys(map: &Map<String, Value>, expected: &[&str], label: &str) -> Result<()> {
    if map.len() != expected.len() || expected.iter().any(|key| !map.contains_key(*key)) {
        bail!("{label} fields are not exact");
    }
    Ok(())
}

fn object<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| anyhow!("{label} must be an object"))
}

fn field<'a>(map: &'a Map<String, Value>, key: &str, label: &str) -> Result<&'a Value> {
    map.get(key).ok_or_else(|| anyhow!("{label} lacks {key}"))
}

fn string(map: &Map<String, Value>, key: &str, label: &str) -> Result<String> {
    field(map, key, label)?
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("{label} {key} must be a nonempty string"))
}

fn sha(value: &str, label: &str) -> Result<()> {
    if value.len() != 71
        || !value.starts_with("sha256:")
        || !value.as_bytes()[7..]
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("{label} must be a lowercase SHA-256 identity");
    }
    Ok(())
}

fn sha_field(map: &Map<String, Value>, key: &str, label: &str) -> Result<String> {
    let value = string(map, key, label)?;
    sha(&value, &format!("{label} {key}"))?;
    Ok(value)
}

fn positive_u64(map: &Map<String, Value>, key: &str, label: &str) -> Result<u64> {
    field(map, key, label)?
        .as_u64()
        .filter(|value| *value > 0)
        .ok_or_else(|| anyhow!("{label} {key} must be a positive integer"))
}

fn optional_u64(value: &Value, label: &str) -> Result<Option<u64>> {
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_u64()
        .map(Some)
        .ok_or_else(|| anyhow!("{label} must be a nonnegative integer or null"))
}

fn parse_canonical_line(raw: &[u8], label: &str) -> Result<Value> {
    let value: Value = serde_json::from_slice(raw)
        .map_err(|error| anyhow!("{label} must be UTF-8 JSON: {error}"))?;
    let canonical = canonical_json_line(&value)
        .map_err(|error| anyhow!("{label} cannot be canonically encoded: {error}"))?;
    if raw != canonical {
        bail!("{label} must be one canonical JSON document followed by exactly one LF");
    }
    Ok(value)
}

pub fn parse_generation_manifest(raw: &[u8], contract_version: &str) -> Result<GenerationManifest> {
    let value = parse_canonical_line(raw, "native generation manifest")?;
    validate_generation_manifest(&value, contract_version)
}

pub fn validate_generation_manifest(
    value: &Value,
    contract_version: &str,
) -> Result<GenerationManifest> {
    let fields = object(value, "native generation manifest")?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "contractVersion",
            "operation",
            "authoritySha256",
            "runtimeAuthority",
            "runtimeAuthoritySha256",
            "parentArchivePath",
            "parentArchiveSha256",
            "identityLedgerPath",
            "identityLedgerSha256",
            "outputRoot",
            "finalNewline",
            "generationConfig",
            "generationConfigSha256",
            "targetUniqueCandidates",
            "maxProposalAttempts",
            "maxNewProposals",
            "nativeExecutionTimeoutSeconds",
            "allowEmptyQualityBootstrap",
            "parentSchedule",
            "g0EvaluationWidth",
            "frozenConstructionCatalog",
            "frozenConstructionCatalogSha256",
            "publicationPolicy",
            "nativeProposalAuthoritySha256",
            "resultPath",
            "manifestSha256",
        ],
        "native generation manifest",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str) != Some(GENERATION_MANIFEST_SCHEMA)
        || fields.get("contractVersion").and_then(Value::as_str) != Some(contract_version)
        || fields.get("operation").and_then(Value::as_str) != Some(GENERATION_OPERATION)
        || fields.get("resultPath").and_then(Value::as_str) != Some(GENERATION_RESULT_PATH)
    {
        bail!("native generation manifest is incompatible");
    }
    let authority_sha256 = sha_field(fields, "authoritySha256", "native generation manifest")?;
    let runtime_authority =
        field(fields, "runtimeAuthority", "native generation manifest")?.clone();
    let runtime_fields = object(&runtime_authority, "runtime authority")?;
    exact_keys(
        runtime_fields,
        &[
            "schemaVersion",
            "pairRunConfig",
            "pairRunConfigSha256",
            "bidirectionalPairPolicy",
            "bidirectionalPairPolicySha256",
            "evidenceIdentityContext",
            "evidenceIdentityContextSha256",
            "generationIndex",
            "pairGenerationConfigSha256",
            "runtimeAuthoritySha256",
        ],
        "runtime authority",
    )?;
    if runtime_fields.get("schemaVersion").and_then(Value::as_str) != Some(RUNTIME_AUTHORITY_SCHEMA)
    {
        bail!("runtime authority schema is incompatible");
    }
    let pair_run_config = object(
        field(runtime_fields, "pairRunConfig", "runtime authority")?,
        "runtime authority pair run config",
    )?;
    let pair_run_config_sha256 =
        sha_field(runtime_fields, "pairRunConfigSha256", "runtime authority")?;
    if pair_run_config
        .get("pairRunConfigSha256")
        .and_then(Value::as_str)
        != Some(pair_run_config_sha256.as_str())
    {
        bail!("runtime authority pair run config embedded identity mismatch");
    }
    let mut pair_run_material = pair_run_config.clone();
    pair_run_material.remove("pairRunConfigSha256");
    if canonical_sha256(&Value::Object(pair_run_material))? != pair_run_config_sha256 {
        bail!("runtime authority pair run config self-hash mismatch");
    }
    let pair_policy = field(
        runtime_fields,
        "bidirectionalPairPolicy",
        "runtime authority",
    )?;
    object(pair_policy, "runtime authority pair policy")?;
    let pair_policy_sha256 = sha_field(
        runtime_fields,
        "bidirectionalPairPolicySha256",
        "runtime authority",
    )?;
    if canonical_sha256(pair_policy)? != pair_policy_sha256 {
        bail!("runtime authority pair policy identity mismatch");
    }
    match (
        field(
            runtime_fields,
            "evidenceIdentityContext",
            "runtime authority",
        )?,
        field(
            runtime_fields,
            "evidenceIdentityContextSha256",
            "runtime authority",
        )?,
    ) {
        (Value::Null, Value::Null) => {}
        (Value::Object(context), Value::String(identity)) => {
            sha(
                identity,
                "runtime authority evidence identity context SHA-256",
            )?;
            if context
                .get("predeclaredEvidenceContextSha256")
                .and_then(Value::as_str)
                != Some(identity.as_str())
            {
                bail!("runtime authority evidence identity context embedded identity mismatch");
            }
            let mut context_material = context.clone();
            context_material.remove("predeclaredEvidenceContextSha256");
            if canonical_sha256(&Value::Object(context_material))? != *identity {
                bail!("runtime authority evidence identity context self-hash mismatch");
            }
        }
        _ => bail!("runtime authority evidence identity context and identity must be paired"),
    }
    let runtime_authority_sha256 = sha_field(
        fields,
        "runtimeAuthoritySha256",
        "native generation manifest",
    )?;
    if runtime_fields
        .get("runtimeAuthoritySha256")
        .and_then(Value::as_str)
        != Some(runtime_authority_sha256.as_str())
    {
        bail!("runtime authority embedded identity mismatch");
    }
    let mut runtime_material = runtime_fields.clone();
    runtime_material.remove("runtimeAuthoritySha256");
    if canonical_sha256(&Value::Object(runtime_material))? != runtime_authority_sha256 {
        bail!("runtime authority identity mismatch");
    }
    let parent_archive_sha256 =
        sha_field(fields, "parentArchiveSha256", "native generation manifest")?;
    let identity_ledger_sha256 =
        sha_field(fields, "identityLedgerSha256", "native generation manifest")?;
    let final_newline = string(fields, "finalNewline", "native generation manifest")?;
    if final_newline != "lf" && final_newline != "crlf" {
        bail!("native generation manifest finalNewline must be lf or crlf");
    }
    let generation_config =
        field(fields, "generationConfig", "native generation manifest")?.clone();
    let config = object(&generation_config, "pair generation config")?;
    if config.get("schemaVersion").and_then(Value::as_str) != Some(PAIR_GENERATION_SCHEMA) {
        bail!("pair generation config schema is incompatible");
    }
    let generation_config_sha256 = sha_field(
        fields,
        "generationConfigSha256",
        "native generation manifest",
    )?;
    if config.get("configSha256").and_then(Value::as_str) != Some(generation_config_sha256.as_str())
    {
        bail!("generation config identity does not match its embedded identity");
    }
    let mut config_material = config.clone();
    config_material.remove("configSha256");
    if canonical_sha256(&Value::Object(config_material))? != generation_config_sha256 {
        bail!("generation config self-hash mismatch");
    }
    let run_config = object(
        field(config, "runConfig", "pair generation config")?,
        "run config",
    )?;
    if run_config
        .get("parentArchiveSha256")
        .and_then(Value::as_str)
        != Some(parent_archive_sha256.as_str())
    {
        bail!("generation runConfig parent archive identity mismatch");
    }
    let generation_index = field(config, "generationIndex", "pair generation config")?
        .as_u64()
        .ok_or_else(|| anyhow!("pair generation config generationIndex must be an integer"))?;
    if runtime_fields
        .get("generationIndex")
        .and_then(Value::as_u64)
        != Some(generation_index)
    {
        bail!("runtime/generation manifest generation index mismatch");
    }
    if runtime_fields
        .get("pairGenerationConfigSha256")
        .and_then(Value::as_str)
        != Some(generation_config_sha256.as_str())
    {
        bail!("runtime manifest selector seed differs from generation config identity");
    }
    let target_unique_candidates = positive_u64(
        fields,
        "targetUniqueCandidates",
        "native generation manifest",
    )?;
    let max_proposal_attempts =
        positive_u64(fields, "maxProposalAttempts", "native generation manifest")?;
    if max_proposal_attempts < target_unique_candidates
        || config.get("targetUniqueCandidates").and_then(Value::as_u64)
            != Some(target_unique_candidates)
        || config.get("maxProposalAttempts").and_then(Value::as_u64) != Some(max_proposal_attempts)
    {
        bail!("generation target/attempt fields diverge from generation config");
    }
    let max_new_proposals = optional_u64(
        field(fields, "maxNewProposals", "native generation manifest")?,
        "native generation manifest maxNewProposals",
    )?;
    let native_execution_timeout_seconds = positive_u64(
        fields,
        "nativeExecutionTimeoutSeconds",
        "native generation manifest",
    )?;
    if native_execution_timeout_seconds < 60 {
        bail!("native generation execution timeout is below 60 seconds");
    }
    let allow_empty_quality_bootstrap = field(
        fields,
        "allowEmptyQualityBootstrap",
        "native generation manifest",
    )?
    .as_bool()
    .ok_or_else(|| {
        anyhow!("native generation manifest allowEmptyQualityBootstrap must be boolean")
    })?;
    let parent_schedule = match field(fields, "parentSchedule", "native generation manifest")? {
        Value::Null => None,
        value if value.is_object() => Some(value.clone()),
        _ => bail!("native generation manifest parentSchedule must be an object or null"),
    };
    if config.get("parentSchedule") != parent_schedule.as_ref()
        && !(config.get("parentSchedule").is_none() && parent_schedule.is_none())
    {
        bail!("generation parent schedule diverges from generation config");
    }
    let g0_evaluation_width = optional_u64(
        field(fields, "g0EvaluationWidth", "native generation manifest")?,
        "native generation manifest g0EvaluationWidth",
    )?;
    if let Some(width) = g0_evaluation_width {
        let g0 = object(
            field(run_config, "g0Bootstrap", "run config")?,
            "G0 bootstrap",
        )?;
        if generation_index != 1
            || width == 0
            || width > target_unique_candidates
            || g0
                .get("initialConstructionPoolSize")
                .and_then(Value::as_u64)
                != Some(target_unique_candidates)
            || g0.get("evaluationPopulationSize").and_then(Value::as_u64) != Some(width)
        {
            bail!("native generation G0 widths diverge from runConfig");
        }
    } else if run_config.contains_key("g0Bootstrap") {
        bail!("native generation omitted its runConfig G0 width");
    }
    let frozen_construction_catalog = match field(
        fields,
        "frozenConstructionCatalog",
        "native generation manifest",
    )? {
        Value::Null => None,
        value if value.is_object() => Some(value.clone()),
        _ => {
            bail!("native generation manifest frozenConstructionCatalog must be an object or null")
        }
    };
    let frozen_construction_catalog_sha256 = match field(
        fields,
        "frozenConstructionCatalogSha256",
        "native generation manifest",
    )? {
        Value::Null => None,
        Value::String(value) => {
            sha(value, "frozen construction catalog SHA-256")?;
            Some(value.clone())
        }
        _ => {
            bail!(
                "native generation manifest frozenConstructionCatalogSha256 must be a SHA-256 or null"
            )
        }
    };
    match (
        frozen_construction_catalog.as_ref(),
        frozen_construction_catalog_sha256.as_ref(),
    ) {
        (None, None) => {}
        (Some(catalog), Some(identity)) if canonical_sha256(catalog)? == *identity => {}
        _ => bail!("native generation frozen construction catalog identity mismatch"),
    }
    let publication_policy =
        field(fields, "publicationPolicy", "native generation manifest")?.clone();
    validate_publication_policy(
        &publication_policy,
        config,
        runtime_fields,
        g0_evaluation_width,
    )?;
    let native_proposal_authority_sha256 = sha_field(
        fields,
        "nativeProposalAuthoritySha256",
        "native generation manifest",
    )?;
    let frozen_pair_run_config = object(
        field(runtime_fields, "pairRunConfig", "runtime manifest")?,
        "frozen pair run config",
    )?;
    let expected_native_authority_sha256 = canonical_sha256(field(
        frozen_pair_run_config,
        "nativeAuthority",
        "frozen pair run config",
    )?)?;
    if native_proposal_authority_sha256 != expected_native_authority_sha256 {
        bail!("native proposal authority identity diverges from frozen pair run config");
    }
    let manifest_sha256 = sha_field(fields, "manifestSha256", "native generation manifest")?;
    let mut material = fields.clone();
    material.remove("manifestSha256");
    if canonical_sha256(&Value::Object(material))? != manifest_sha256 {
        bail!("native generation manifest identity mismatch");
    }
    Ok(GenerationManifest {
        contract_version: contract_version.to_owned(),
        authority_sha256,
        runtime_authority,
        runtime_authority_sha256,
        parent_archive_path: string(fields, "parentArchivePath", "native generation manifest")?,
        parent_archive_sha256,
        identity_ledger_path: string(fields, "identityLedgerPath", "native generation manifest")?,
        identity_ledger_sha256,
        output_root: string(fields, "outputRoot", "native generation manifest")?,
        final_newline,
        generation_config,
        generation_config_sha256,
        target_unique_candidates,
        max_proposal_attempts,
        max_new_proposals,
        allow_empty_quality_bootstrap,
        parent_schedule,
        g0_evaluation_width,
        frozen_construction_catalog,
        publication_policy,
        native_proposal_authority_sha256,
        result_path: GENERATION_RESULT_PATH.to_owned(),
        manifest_sha256,
    })
}

fn validate_publication_policy(
    value: &Value,
    generation_config: &Map<String, Value>,
    runtime_authority: &Map<String, Value>,
    g0_evaluation_width: Option<u64>,
) -> Result<()> {
    let policy = object(value, "native publication policy")?;
    exact_keys(
        policy,
        &[
            "qdVersion",
            "policyName",
            "policySha256",
            "frozenPolicy",
            "pairPolicy",
            "operatorImplementationIdentity",
            "predeclaredEvidenceContextSha256",
            "publicationAuthoritySha256",
        ],
        "native publication policy",
    )?;
    let _qd_version = string(policy, "qdVersion", "native publication policy")?;
    let policy_name = string(policy, "policyName", "native publication policy")?;
    let policy_sha256 = sha_field(policy, "policySha256", "native publication policy")?;
    let frozen_policy = field(policy, "frozenPolicy", "native publication policy")?;
    if canonical_sha256(frozen_policy)? != policy_sha256
        || frozen_policy.get("policyName").and_then(Value::as_str) != Some(policy_name.as_str())
        || policy.get("pairPolicy") != generation_config.get("pairPolicy")
        || policy.get("operatorImplementationIdentity")
            != generation_config.get("operatorImplementation")
    {
        bail!("native publication policy diverges from the frozen generation config");
    }
    let publication_authority_sha256 = sha_field(
        policy,
        "publicationAuthoritySha256",
        "native publication policy",
    )?;
    let mut authority_material = policy.clone();
    authority_material.remove("publicationAuthoritySha256");
    if canonical_sha256(&Value::Object(authority_material))? != publication_authority_sha256 {
        bail!("native publication policy authority identity mismatch");
    }
    let expected_context = if g0_evaluation_width.is_some() {
        None
    } else {
        runtime_authority
            .get("evidenceIdentityContext")
            .filter(|value| !value.is_null())
            .and_then(Value::as_object)
            .and_then(|context| context.get("predeclaredEvidenceContextSha256"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    };
    match (
        policy.get("predeclaredEvidenceContextSha256"),
        expected_context,
    ) {
        (Some(Value::Null), None) => {}
        (Some(Value::String(actual)), Some(expected)) if *actual == expected => {
            sha(actual, "publication evidence context SHA-256")?
        }
        _ => bail!("native publication evidence context identity mismatch"),
    }
    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn assemble_runtime_manifest(
    manifest: &GenerationManifest,
    parent_archive: &Value,
    identity_ledger: &Value,
) -> Result<Value> {
    validate_runtime_inputs(manifest, parent_archive, identity_ledger)?;
    assemble_runtime_authority(manifest, parent_archive.clone(), identity_ledger.clone())
}

/// Production assembly consumes the file-backed JSON values after the same
/// strict identity validation as [`assemble_runtime_manifest`].  Avoiding the
/// historical clone here is material for multi-gigabyte parent archives.
pub fn assemble_runtime_manifest_owned(
    manifest: &GenerationManifest,
    parent_archive: Value,
    identity_ledger: Value,
) -> Result<Value> {
    validate_runtime_inputs(manifest, &parent_archive, &identity_ledger)?;
    assemble_runtime_authority(manifest, parent_archive, identity_ledger)
}

fn validate_runtime_inputs(
    manifest: &GenerationManifest,
    parent_archive: &Value,
    identity_ledger: &Value,
) -> Result<()> {
    let archive = object(parent_archive, "generation parent archive")?;
    if archive.get("archiveSha256").and_then(Value::as_str)
        != Some(manifest.parent_archive_sha256.as_str())
    {
        bail!("generation parent archive embedded identity mismatch");
    }
    if canonical_sha256_without_object_field(parent_archive, "archiveSha256")?
        != manifest.parent_archive_sha256
    {
        bail!("generation parent archive self-hash mismatch");
    }
    let ledger = object(identity_ledger, "generation identity ledger")?;
    if ledger.get("ledgerSha256").and_then(Value::as_str)
        != Some(manifest.identity_ledger_sha256.as_str())
    {
        bail!("generation identity ledger embedded identity mismatch");
    }
    if canonical_sha256_without_object_field(identity_ledger, "ledgerSha256")?
        != manifest.identity_ledger_sha256
    {
        bail!("generation identity ledger self-hash mismatch");
    }
    let authority = object(&manifest.runtime_authority, "runtime authority")?;
    let publication = object(&manifest.publication_policy, "native publication policy")?;
    for (archive_key, publication_key) in [
        ("qdVersion", "qdVersion"),
        ("policyName", "policyName"),
        ("policySha256", "policySha256"),
        ("frozenPolicy", "frozenPolicy"),
    ] {
        if archive.get(archive_key) != publication.get(publication_key) {
            bail!("parent archive diverges from frozen publication authority");
        }
    }
    if archive.get("bidirectionalPairPolicy") != authority.get("bidirectionalPairPolicy") {
        bail!("parent archive pair policy diverges from runtime authority");
    }
    Ok(())
}

fn assemble_runtime_authority(
    manifest: &GenerationManifest,
    parent_archive: Value,
    identity_ledger: Value,
) -> Result<Value> {
    let authority = object(&manifest.runtime_authority, "runtime authority")?;
    let mut runtime = authority.clone();
    runtime.remove("runtimeAuthoritySha256");
    runtime.insert(
        "schemaVersion".to_owned(),
        Value::String("temporal_qd_runtime_manifest_v1".to_owned()),
    );
    runtime.insert("parentArchive".to_owned(), parent_archive);
    runtime.insert(
        "parentArchiveSha256".to_owned(),
        Value::String(manifest.parent_archive_sha256.clone()),
    );
    runtime.insert("identityLedger".to_owned(), identity_ledger);
    Ok(Value::Object(runtime))
}

pub fn build_generation_result(
    manifest: &GenerationManifest,
    pair_generation_result: Value,
    output_identity_ledger_sha256: String,
) -> Result<GenerationResult> {
    validate_pair_generation_result(&pair_generation_result)?;
    sha(
        &output_identity_ledger_sha256,
        "output identity ledger SHA-256",
    )?;
    if pair_generation_result
        .get("configSha256")
        .and_then(Value::as_str)
        != Some(manifest.generation_config_sha256.as_str())
    {
        bail!("pair generation result config identity differs from its manifest");
    }
    let completed = pair_generation_result
        .get("completed")
        .and_then(Value::as_bool)
        .ok_or_else(|| anyhow!("pair generation result lacks completed"))?;
    let mut value = Value::Object(
        [
            (
                "schemaVersion",
                Value::String(GENERATION_RESULT_SCHEMA.to_owned()),
            ),
            (
                "contractVersion",
                Value::String(manifest.contract_version.clone()),
            ),
            ("operation", Value::String(GENERATION_OPERATION.to_owned())),
            (
                "status",
                Value::String(if completed { "completed" } else { "progress" }.to_owned()),
            ),
            (
                "authoritySha256",
                Value::String(manifest.authority_sha256.clone()),
            ),
            (
                "manifestSha256",
                Value::String(manifest.manifest_sha256.clone()),
            ),
            (
                "runtimeAuthoritySha256",
                Value::String(manifest.runtime_authority_sha256.clone()),
            ),
            (
                "parentArchiveSha256",
                Value::String(manifest.parent_archive_sha256.clone()),
            ),
            (
                "inputIdentityLedgerSha256",
                Value::String(manifest.identity_ledger_sha256.clone()),
            ),
            (
                "outputIdentityLedgerSha256",
                Value::String(output_identity_ledger_sha256),
            ),
            (
                "generationConfigSha256",
                Value::String(manifest.generation_config_sha256.clone()),
            ),
            ("pairGenerationResult", pair_generation_result),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect(),
    );
    let identity = canonical_sha256(&value)?;
    value
        .as_object_mut()
        .expect("generation result is an object")
        .insert("resultSha256".to_owned(), Value::String(identity));
    Ok(GenerationResult { value })
}

pub fn validate_generation_result(
    value: &Value,
    manifest: &GenerationManifest,
) -> Result<GenerationResult> {
    let fields = object(value, "native generation result")?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "contractVersion",
            "operation",
            "status",
            "authoritySha256",
            "manifestSha256",
            "runtimeAuthoritySha256",
            "parentArchiveSha256",
            "inputIdentityLedgerSha256",
            "outputIdentityLedgerSha256",
            "generationConfigSha256",
            "pairGenerationResult",
            "resultSha256",
        ],
        "native generation result",
    )?;
    let inner = field(fields, "pairGenerationResult", "native generation result")?;
    validate_pair_generation_result(inner)?;
    if inner.get("configSha256").and_then(Value::as_str)
        != Some(manifest.generation_config_sha256.as_str())
    {
        bail!("pair generation result config identity differs from its manifest");
    }
    let completed = inner.get("completed").and_then(Value::as_bool);
    let expected_status = if completed == Some(true) {
        "completed"
    } else {
        "progress"
    };
    if fields.get("schemaVersion").and_then(Value::as_str) != Some(GENERATION_RESULT_SCHEMA)
        || fields.get("contractVersion").and_then(Value::as_str)
            != Some(manifest.contract_version.as_str())
        || fields.get("operation").and_then(Value::as_str) != Some(GENERATION_OPERATION)
        || fields.get("status").and_then(Value::as_str) != Some(expected_status)
        || fields.get("authoritySha256").and_then(Value::as_str)
            != Some(manifest.authority_sha256.as_str())
        || fields.get("manifestSha256").and_then(Value::as_str)
            != Some(manifest.manifest_sha256.as_str())
        || fields.get("runtimeAuthoritySha256").and_then(Value::as_str)
            != Some(manifest.runtime_authority_sha256.as_str())
        || fields.get("parentArchiveSha256").and_then(Value::as_str)
            != Some(manifest.parent_archive_sha256.as_str())
        || fields
            .get("inputIdentityLedgerSha256")
            .and_then(Value::as_str)
            != Some(manifest.identity_ledger_sha256.as_str())
        || fields.get("generationConfigSha256").and_then(Value::as_str)
            != Some(manifest.generation_config_sha256.as_str())
    {
        bail!("native generation result is incompatible with its manifest");
    }
    sha_field(
        fields,
        "outputIdentityLedgerSha256",
        "native generation result",
    )?;
    let supplied = sha_field(fields, "resultSha256", "native generation result")?;
    let mut material = fields.clone();
    material.remove("resultSha256");
    if canonical_sha256(&Value::Object(material))? != supplied {
        bail!("native generation result identity mismatch");
    }
    Ok(GenerationResult {
        value: value.clone(),
    })
}

fn validate_pair_generation_result(value: &Value) -> Result<()> {
    let fields = object(value, "pair generation result")?;
    let schema = fields
        .get("schemaVersion")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("pair generation result lacks schemaVersion"))?;
    match schema {
        PAIR_GENERATION_RESULT_SCHEMA => {
            let required = [
                "schemaVersion",
                "configSha256",
                "populationSha256",
                "evaluationPopulationSha256",
                "journalSha256",
                "proposalCount",
                "candidateCount",
                "originProposalCounts",
                "originAcceptedCounts",
                "proposalSlots",
                "uniqueIdentityCounts",
                "duplicateCounters",
                "proposalSlotCounters",
                "nextImmigrantContinuationOrdinal",
                "completed",
            ];
            let optional = [
                "constructionPoolSize",
                "constructedAcceptedCount",
                "g0Bootstrap",
                "immigrantConstructionDistribution",
            ];
            if required.iter().any(|key| !fields.contains_key(*key))
                || fields.keys().any(|key| {
                    !required.contains(&key.as_str()) && !optional.contains(&key.as_str())
                })
                || fields.get("completed").and_then(Value::as_bool) != Some(true)
            {
                bail!("completed pair generation result fields are not exact");
            }
            for key in [
                "configSha256",
                "populationSha256",
                "evaluationPopulationSha256",
                "journalSha256",
            ] {
                sha_field(fields, key, "completed pair generation result")?;
            }
        }
        FRONT_GENERATION_PROGRESS_SCHEMA => {
            exact_keys(
                fields,
                &[
                    "schemaVersion",
                    "configSha256",
                    "proposalCount",
                    "acceptedCount",
                    "maxProposalAttempts",
                    "terminationReason",
                    "completed",
                ],
                "pair generation progress result",
            )?;
            if fields.get("completed").and_then(Value::as_bool) != Some(false) {
                bail!("pair generation progress must be incomplete");
            }
            sha_field(fields, "configSha256", "pair generation progress result")?;
        }
        _ => bail!("pair generation result schema is incompatible"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use temporal_qd_contract::{CONTRACT_VERSION, canonical_sha256_without_object_field};

    use super::*;

    fn fixture() -> (Value, Value, Value) {
        let source: Value = serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/temporal_qd_runtime_oracle/runtime-manifest.json"
        ))
        .unwrap();
        let archive = source.get("parentArchive").unwrap().clone();
        let ledger = source.get("identityLedger").unwrap().clone();
        let archive_sha = archive.get("archiveSha256").unwrap().clone();
        let mut config = json!({
            "schemaVersion": PAIR_GENERATION_SCHEMA,
            "generationIndex": 1,
            "targetUniqueCandidates": 1,
            "maxProposalAttempts": 1,
            "runConfig": {
                "parentArchiveSha256": archive_sha,
                "parameters": {},
                "evidenceIdentityContext": source.get("evidenceIdentityContext").unwrap(),
            },
            "pairPolicy": source.get("bidirectionalPairPolicy").unwrap(),
            "operatorImplementation": source.get("pairRunConfig").unwrap().get("operatorImplementation").unwrap(),
            "mutationDepthProbabilities": {"1": 0.7, "2": 0.25, "3": 0.05},
            "immigrantConstructionPolicy": source.get("pairRunConfig").unwrap().get("immigrantConstructionPolicy").unwrap(),
            "globalIdentityLedger": {
                "schemaVersion": "temporal_qd_identity_ledger_v3",
                "locationPolicy": "caller_supplied_generation_global_ledger",
            },
        });
        let config_sha = canonical_sha256(&config).unwrap();
        config
            .as_object_mut()
            .unwrap()
            .insert("configSha256".into(), Value::String(config_sha.clone()));
        let mut runtime_authority = json!({
            "schemaVersion": RUNTIME_AUTHORITY_SCHEMA,
            "pairRunConfig": source.get("pairRunConfig").unwrap(),
            "pairRunConfigSha256": source.get("pairRunConfigSha256").unwrap(),
            "bidirectionalPairPolicy": source.get("bidirectionalPairPolicy").unwrap(),
            "bidirectionalPairPolicySha256": source.get("bidirectionalPairPolicySha256").unwrap(),
            "evidenceIdentityContext": source.get("evidenceIdentityContext").unwrap(),
            "evidenceIdentityContextSha256": source.get("evidenceIdentityContextSha256").unwrap(),
            "generationIndex": 1,
            "pairGenerationConfigSha256": config_sha,
        });
        let runtime_sha = canonical_sha256(&runtime_authority).unwrap();
        runtime_authority.as_object_mut().unwrap().insert(
            "runtimeAuthoritySha256".into(),
            Value::String(runtime_sha.clone()),
        );
        let frozen_policy = archive.get("frozenPolicy").unwrap().clone();
        let mut publication = json!({
            "qdVersion": archive.get("qdVersion").unwrap(),
            "policyName": archive.get("policyName").unwrap(),
            "policySha256": archive.get("policySha256").unwrap(),
            "frozenPolicy": frozen_policy,
            "pairPolicy": config.get("pairPolicy").unwrap(),
            "operatorImplementationIdentity": config.get("operatorImplementation").unwrap(),
            "predeclaredEvidenceContextSha256": source.get("evidenceIdentityContext").unwrap().get("predeclaredEvidenceContextSha256").unwrap(),
        });
        let publication_sha = canonical_sha256(&publication).unwrap();
        publication.as_object_mut().unwrap().insert(
            "publicationAuthoritySha256".into(),
            Value::String(publication_sha),
        );
        let native_sha = canonical_sha256(
            source
                .get("pairRunConfig")
                .unwrap()
                .get("nativeAuthority")
                .unwrap(),
        )
        .unwrap();
        let mut outer = json!({
            "schemaVersion": GENERATION_MANIFEST_SCHEMA,
            "contractVersion": CONTRACT_VERSION,
            "operation": GENERATION_OPERATION,
            "authoritySha256": format!("sha256:{}", "a".repeat(64)),
            "runtimeAuthority": runtime_authority,
            "runtimeAuthoritySha256": runtime_sha,
            "parentArchivePath": "C:\\fixture\\archive.json",
            "parentArchiveSha256": archive.get("archiveSha256").unwrap(),
            "identityLedgerPath": "C:\\fixture\\identity-ledger.json",
            "identityLedgerSha256": ledger.get("ledgerSha256").unwrap(),
            "outputRoot": "C:\\fixture\\output",
            "finalNewline": "lf",
            "generationConfig": config,
            "generationConfigSha256": config_sha,
            "targetUniqueCandidates": 1,
            "maxProposalAttempts": 1,
            "maxNewProposals": 0,
            "nativeExecutionTimeoutSeconds": 3600,
            "allowEmptyQualityBootstrap": true,
            "parentSchedule": null,
            "g0EvaluationWidth": null,
            "frozenConstructionCatalog": null,
            "frozenConstructionCatalogSha256": null,
            "publicationPolicy": publication,
            "nativeProposalAuthoritySha256": native_sha,
            "resultPath": GENERATION_RESULT_PATH,
        });
        let outer_sha = canonical_sha256(&outer).unwrap();
        outer
            .as_object_mut()
            .unwrap()
            .insert("manifestSha256".into(), Value::String(outer_sha));
        (outer, archive, ledger)
    }

    #[test]
    fn real_archive_and_ledger_are_substituted_only_after_hash_verification() {
        let (outer, archive, ledger) = fixture();
        let manifest = validate_generation_manifest(&outer, CONTRACT_VERSION).unwrap();
        // The zero-copy omission path is byte-for-byte identity-compatible
        // with the historical clone/remove calculation used by Python-facing
        // archive contracts.
        let mut historical_material = archive.as_object().unwrap().clone();
        historical_material.remove("archiveSha256");
        assert_eq!(
            canonical_sha256_without_object_field(&archive, "archiveSha256").unwrap(),
            canonical_sha256(&Value::Object(historical_material)).unwrap(),
        );
        assert_eq!(
            canonical_sha256_without_object_field(&archive, "archiveSha256").unwrap(),
            manifest.parent_archive_sha256,
        );
        assert!(manifest.runtime_authority.get("parentArchive").is_none());
        assert!(manifest.runtime_authority.get("identityLedger").is_none());

        let assembled = assemble_runtime_manifest(&manifest, &archive, &ledger).unwrap();
        assert_eq!(assembled.get("parentArchive"), Some(&archive));
        assert_eq!(assembled.get("identityLedger"), Some(&ledger));
        temporal_qd_runtime::RuntimeManifest::from_value(&assembled).unwrap();

        let owned = assemble_runtime_manifest_owned(&manifest, archive.clone(), ledger.clone())
            .expect("owned assembly must preserve every public runtime byte");
        assert_eq!(owned, assembled);
        let owned_runtime = temporal_qd_runtime::RuntimeManifest::from_owned_value(owned)
            .expect("owned runtime parsing must preserve the verified archive projection");
        assert_eq!(
            owned_runtime.parent_archive.archive_sha256(),
            manifest.parent_archive_sha256
        );
        assert_eq!(
            owned_runtime.parent_archive.members().count(),
            archive["cells"]
                .as_array()
                .expect("fixture archive cells")
                .iter()
                .map(|cell| {
                    cell["members"]
                        .as_array()
                        .expect("fixture archive cell members")
                        .len()
                })
                .sum::<usize>()
        );

        let mut corrupt = archive.clone();
        corrupt
            .as_object_mut()
            .unwrap()
            .insert("candidateCountSeen".into(), Value::from(99));
        assert!(assemble_runtime_manifest(&manifest, &corrupt, &ledger).is_err());

        let mut substituted = archive.clone();
        substituted
            .as_object_mut()
            .unwrap()
            .insert("generationIndex".into(), Value::from(99));
        substituted.as_object_mut().unwrap().remove("archiveSha256");
        let substituted_sha = canonical_sha256(&substituted).unwrap();
        substituted
            .as_object_mut()
            .unwrap()
            .insert("archiveSha256".into(), Value::String(substituted_sha));
        assert!(assemble_runtime_manifest(&manifest, &substituted, &ledger).is_err());

        let mut corrupt_ledger = ledger.clone();
        corrupt_ledger
            .as_object_mut()
            .unwrap()
            .insert("records".into(), json!([{"attacker": true}]));
        assert!(assemble_runtime_manifest(&manifest, &archive, &corrupt_ledger).is_err());
    }

    #[test]
    fn generation_manifest_accepts_only_explicit_lf_or_crlf_policy() {
        for valid in ["lf", "crlf"] {
            let (mut outer, _, _) = fixture();
            outer.as_object_mut().unwrap().remove("manifestSha256");
            outer
                .as_object_mut()
                .unwrap()
                .insert("finalNewline".into(), Value::String(valid.to_owned()));
            let identity = canonical_sha256(&outer).unwrap();
            outer
                .as_object_mut()
                .unwrap()
                .insert("manifestSha256".into(), Value::String(identity));
            assert_eq!(
                validate_generation_manifest(&outer, CONTRACT_VERSION)
                    .unwrap()
                    .final_newline,
                valid
            );
        }

        let (mut outer, _, _) = fixture();
        outer.as_object_mut().unwrap().remove("manifestSha256");
        outer
            .as_object_mut()
            .unwrap()
            .insert("finalNewline".into(), Value::String("native".into()));
        let identity = canonical_sha256(&outer).unwrap();
        outer
            .as_object_mut()
            .unwrap()
            .insert("manifestSha256".into(), Value::String(identity));
        assert!(validate_generation_manifest(&outer, CONTRACT_VERSION).is_err());
    }

    #[test]
    fn frozen_publication_authority_tamper_is_rejected() {
        let (mut outer, _, _) = fixture();
        outer
            .get_mut("publicationPolicy")
            .unwrap()
            .get_mut("frozenPolicy")
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("policyName".into(), Value::String("tampered".into()));
        outer.as_object_mut().unwrap().remove("manifestSha256");
        let outer_sha = canonical_sha256(&outer).unwrap();
        outer
            .as_object_mut()
            .unwrap()
            .insert("manifestSha256".into(), Value::String(outer_sha));
        assert!(validate_generation_manifest(&outer, CONTRACT_VERSION).is_err());
    }

    #[test]
    fn runtime_authority_uses_verified_embedded_self_hashes() {
        fn resign_enclosing_authorities(value: &mut Value) {
            let authority_sha = {
                let authority = value
                    .get_mut("runtimeAuthority")
                    .unwrap()
                    .as_object_mut()
                    .unwrap();
                authority.remove("runtimeAuthoritySha256");
                let authority_sha = canonical_sha256(&Value::Object(authority.clone())).unwrap();
                authority.insert(
                    "runtimeAuthoritySha256".into(),
                    Value::String(authority_sha.clone()),
                );
                authority_sha
            };
            value.as_object_mut().unwrap().insert(
                "runtimeAuthoritySha256".into(),
                Value::String(authority_sha),
            );
            value.as_object_mut().unwrap().remove("manifestSha256");
            let manifest_sha = canonical_sha256(value).unwrap();
            value
                .as_object_mut()
                .unwrap()
                .insert("manifestSha256".into(), Value::String(manifest_sha));
        }

        let (outer, _, _) = fixture();
        let authority = outer.get("runtimeAuthority").unwrap();
        assert_eq!(
            authority.get("pairRunConfigSha256"),
            authority
                .get("pairRunConfig")
                .unwrap()
                .get("pairRunConfigSha256")
        );
        assert_eq!(
            authority.get("evidenceIdentityContextSha256"),
            authority
                .get("evidenceIdentityContext")
                .unwrap()
                .get("predeclaredEvidenceContextSha256")
        );

        for path in [
            &["pairRunConfig", "pairRunConfigSha256"][..],
            &[
                "evidenceIdentityContext",
                "predeclaredEvidenceContextSha256",
            ][..],
        ] {
            let mut tampered = outer.clone();
            let authority = tampered
                .get_mut("runtimeAuthority")
                .unwrap()
                .as_object_mut()
                .unwrap();
            authority
                .get_mut(path[0])
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert(
                    path[1].into(),
                    Value::String(format!("sha256:{}", "f".repeat(64))),
                );
            resign_enclosing_authorities(&mut tampered);
            assert!(validate_generation_manifest(&tampered, CONTRACT_VERSION).is_err());
        }

        for path in [
            &["pairRunConfig", "schemaVersion"][..],
            &["evidenceIdentityContext", "baseDecisionTimeframe"][..],
        ] {
            let mut tampered = outer.clone();
            tampered
                .get_mut("runtimeAuthority")
                .unwrap()
                .get_mut(path[0])
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert(path[1].into(), Value::String("tampered".into()));
            resign_enclosing_authorities(&mut tampered);
            assert!(validate_generation_manifest(&tampered, CONTRACT_VERSION).is_err());
        }
    }
}
