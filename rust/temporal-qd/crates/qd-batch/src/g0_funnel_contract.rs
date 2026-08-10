//! Closed manifest for the native-owned post-construction G0 transaction.
//!
//! Rich proposal rows are intentionally absent: the batch process discovers
//! the durable inventory below `outputRoot` after authenticating this compact
//! request.  That is what prevents a Python/Rust per-candidate protocol.

use anyhow::{Result, anyhow, bail};
use temporal_qd_contract::{
    CONTRACT_VERSION, Map, Value, canonical_json_line, canonical_sha256,
    canonical_sha256_without_object_field,
};

pub const G0_FUNNEL_MANIFEST_SCHEMA: &str = "temporal_qd_native_g0_funnel_manifest_v2";
pub const G0_FUNNEL_OPERATION: &str = "finalize_g0";
pub const G0_FUNNEL_RESULT_PATH: &str = "g0-funnel-result.json";

#[derive(Clone, Debug)]
pub struct G0FunnelManifest {
    pub authority_sha256: String,
    pub execution_authority: Value,
    pub output_root: String,
    pub final_newline: String,
    pub generation_config: Value,
    pub generation_config_sha256: String,
    pub generation_index: u64,
    pub construction_pool_size: u64,
    pub evaluation_population_size: u64,
    pub max_proposal_attempts: u64,
    pub publication_policy: Value,
    pub identity_ledger: Option<Value>,
    pub audit: bool,
    pub result_path: String,
    pub manifest_sha256: String,
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

fn string(map: &Map<String, Value>, key: &str, label: &str) -> Result<String> {
    map.get(key)
        .and_then(Value::as_str)
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

fn positive(map: &Map<String, Value>, key: &str, label: &str) -> Result<u64> {
    map.get(key)
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .ok_or_else(|| anyhow!("{label} {key} must be a positive integer"))
}

fn canonical_line(raw: &[u8], label: &str) -> Result<Value> {
    let value: Value = serde_json::from_slice(raw)
        .map_err(|error| anyhow!("{label} must be UTF-8 JSON: {error}"))?;
    if raw != canonical_json_line(&value)? {
        bail!("{label} must be one canonical JSON document followed by exactly one LF");
    }
    Ok(value)
}

fn validate_execution_authority(value: &Value, expected_sha: &str) -> Result<()> {
    let fields = object(value, "native G0 execution authority")?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "g0FinalizationRuntimeSha256",
            "nativeBatchAuthority",
            "nativeBatchAuthoritySha256",
            "authoritySha256",
        ],
        "native G0 execution authority",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some("temporal_qd_native_g0_execution_authority_v1")
    {
        bail!("native G0 execution authority schema is incompatible");
    }
    for key in [
        "g0FinalizationRuntimeSha256",
        "nativeBatchAuthoritySha256",
        "authoritySha256",
    ] {
        sha_field(fields, key, "native G0 execution authority")?;
    }
    if string(fields, "authoritySha256", "native G0 execution authority")? != expected_sha
        || canonical_sha256_without_object_field(value, "authoritySha256")? != expected_sha
    {
        bail!("native G0 execution authority identity mismatch");
    }
    let batch = object(
        fields
            .get("nativeBatchAuthority")
            .ok_or_else(|| anyhow!("native G0 execution authority lacks batch authority"))?,
        "native G0 batch authority",
    )?;
    exact_keys(
        batch,
        &[
            "schemaVersion",
            "contractVersion",
            "crateVersion",
            "binaryName",
            "buildProfile",
            "executableSha256",
            "sourceSha256",
            "authoritySha256",
        ],
        "native G0 batch authority",
    )?;
    if batch.get("schemaVersion").and_then(Value::as_str) != Some("temporal_qd_native_authority_v1")
        || batch.get("contractVersion").and_then(Value::as_str) != Some(CONTRACT_VERSION)
        || batch.get("binaryName").and_then(Value::as_str) != Some("temporal-qd-batch")
        || batch.get("buildProfile").and_then(Value::as_str) != Some("release")
    {
        bail!("native G0 batch authority is incompatible");
    }
    for key in ["executableSha256", "sourceSha256", "authoritySha256"] {
        sha_field(batch, key, "native G0 batch authority")?;
    }
    let batch_value = fields
        .get("nativeBatchAuthority")
        .expect("closed authority has nativeBatchAuthority");
    let batch_sha = string(batch, "authoritySha256", "native G0 batch authority")?;
    if canonical_sha256_without_object_field(batch_value, "authoritySha256")? != batch_sha
        || string(
            fields,
            "nativeBatchAuthoritySha256",
            "native G0 execution authority",
        )? != batch_sha
    {
        bail!("native G0 batch authority identity mismatch");
    }
    Ok(())
}

fn validate_identity_ledger_binding(value: &Value) -> Result<()> {
    let fields = object(value, "native G0 identity ledger binding")?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "ledgerPath",
            "policyName",
            "policySha256",
            "identityPolicy",
            "identityPolicySha256",
        ],
        "native G0 identity ledger binding",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str)
        != Some("temporal_qd_native_g0_identity_ledger_binding_v1")
        || !std::path::Path::new(&string(
            fields,
            "ledgerPath",
            "native G0 identity ledger binding",
        )?)
        .is_absolute()
        || !fields.get("identityPolicy").is_some_and(Value::is_object)
    {
        bail!("native G0 identity ledger binding is incompatible");
    }
    let policy_sha = sha_field(fields, "policySha256", "native G0 identity ledger binding")?;
    let identity_policy_sha = sha_field(
        fields,
        "identityPolicySha256",
        "native G0 identity ledger binding",
    )?;
    if string(fields, "policyName", "native G0 identity ledger binding")?.is_empty()
        || canonical_sha256(
            fields
                .get("identityPolicy")
                .expect("closed binding has identityPolicy"),
        )? != identity_policy_sha
        || policy_sha.is_empty()
    {
        bail!("native G0 identity ledger binding drifted");
    }
    Ok(())
}

pub fn parse_g0_funnel_manifest(raw: &[u8]) -> Result<G0FunnelManifest> {
    let value = canonical_line(raw, "native G0 funnel manifest")?;
    validate_g0_funnel_manifest(&value)
}

pub fn validate_g0_funnel_manifest(value: &Value) -> Result<G0FunnelManifest> {
    let fields = object(value, "native G0 funnel manifest")?;
    exact_keys(
        fields,
        &[
            "schemaVersion",
            "contractVersion",
            "operation",
            "authoritySha256",
            "executionAuthority",
            "outputRoot",
            "finalNewline",
            "generationConfig",
            "generationConfigSha256",
            "generationIndex",
            "constructionPoolSize",
            "evaluationPopulationSize",
            "maxProposalAttempts",
            "publicationPolicy",
            "identityLedger",
            "audit",
            "resultPath",
            "manifestSha256",
        ],
        "native G0 funnel manifest",
    )?;
    if fields.get("schemaVersion").and_then(Value::as_str) != Some(G0_FUNNEL_MANIFEST_SCHEMA)
        || fields.get("contractVersion").and_then(Value::as_str) != Some(CONTRACT_VERSION)
        || fields.get("operation").and_then(Value::as_str) != Some(G0_FUNNEL_OPERATION)
        || fields.get("resultPath").and_then(Value::as_str) != Some(G0_FUNNEL_RESULT_PATH)
    {
        bail!("native G0 funnel manifest is incompatible");
    }
    let authority_sha256 = sha_field(fields, "authoritySha256", "native G0 funnel manifest")?;
    let execution_authority = fields
        .get("executionAuthority")
        .cloned()
        .ok_or_else(|| anyhow!("native G0 funnel manifest lacks executionAuthority"))?;
    validate_execution_authority(&execution_authority, &authority_sha256)?;
    let generation_config = fields
        .get("generationConfig")
        .cloned()
        .ok_or_else(|| anyhow!("native G0 funnel manifest lacks generationConfig"))?;
    let generation_config_sha256 = sha_field(
        fields,
        "generationConfigSha256",
        "native G0 funnel manifest",
    )?;
    let config_fields = object(&generation_config, "native G0 funnel generation config")?;
    if config_fields.get("configSha256").and_then(Value::as_str)
        != Some(generation_config_sha256.as_str())
        || canonical_sha256_without_object_field(&generation_config, "configSha256")?
            != generation_config_sha256
    {
        bail!("native G0 funnel generation config identity mismatch");
    }
    let generation_index = positive(fields, "generationIndex", "native G0 funnel manifest")?;
    let construction_pool_size =
        positive(fields, "constructionPoolSize", "native G0 funnel manifest")?;
    let evaluation_population_size = positive(
        fields,
        "evaluationPopulationSize",
        "native G0 funnel manifest",
    )?;
    let max_proposal_attempts =
        positive(fields, "maxProposalAttempts", "native G0 funnel manifest")?;
    if generation_index != 1
        || evaluation_population_size > construction_pool_size
        || max_proposal_attempts < construction_pool_size
        || config_fields.get("generationIndex").and_then(Value::as_u64) != Some(generation_index)
        || config_fields
            .get("targetUniqueCandidates")
            .and_then(Value::as_u64)
            != Some(construction_pool_size)
        || config_fields
            .get("maxProposalAttempts")
            .and_then(Value::as_u64)
            != Some(max_proposal_attempts)
    {
        bail!("native G0 funnel manifest/config dimensions are inconsistent");
    }
    let final_newline = string(fields, "finalNewline", "native G0 funnel manifest")?;
    if !matches!(final_newline.as_str(), "lf" | "crlf") {
        bail!("native G0 funnel manifest finalNewline is invalid");
    }
    let publication_policy = fields
        .get("publicationPolicy")
        .filter(|value| value.is_object())
        .cloned()
        .ok_or_else(|| anyhow!("native G0 funnel manifest publicationPolicy is invalid"))?;
    let publication_fields = object(&publication_policy, "native G0 funnel publication policy")?;
    exact_keys(
        publication_fields,
        &[
            "qdVersion",
            "policyName",
            "policySha256",
            "pairPolicy",
            "operatorImplementationIdentity",
            "predeclaredEvidenceContextSha256",
            "archivePolicyAuthority",
        ],
        "native G0 funnel publication policy",
    )?;
    for key in ["qdVersion", "policyName", "policySha256"] {
        string(
            publication_fields,
            key,
            "native G0 funnel publication policy",
        )?;
    }
    sha_field(
        publication_fields,
        "policySha256",
        "native G0 funnel publication policy",
    )?;
    if !publication_fields
        .get("pairPolicy")
        .is_some_and(Value::is_object)
        || !publication_fields
            .get("operatorImplementationIdentity")
            .is_some_and(Value::is_object)
    {
        bail!("native G0 funnel publication policy pair/operator fields are invalid");
    }
    match publication_fields.get("predeclaredEvidenceContextSha256") {
        Some(Value::Null) => {}
        Some(Value::String(value)) => sha(value, "native G0 funnel evidence identity")?,
        _ => bail!("native G0 funnel publication policy evidence identity is invalid"),
    }
    match publication_fields.get("archivePolicyAuthority") {
        Some(Value::Null) | Some(Value::Object(_)) => {}
        _ => bail!("native G0 funnel publication policy archive authority is invalid"),
    }
    let identity_ledger = match fields
        .get("identityLedger")
        .expect("closed manifest has identityLedger")
    {
        Value::Null => None,
        Value::Object(_) => Some(
            fields
                .get("identityLedger")
                .expect("closed manifest has identityLedger")
                .clone(),
        ),
        _ => bail!("native G0 funnel manifest globalIdentityLedger is invalid"),
    };
    if let Some(binding) = &identity_ledger {
        validate_identity_ledger_binding(binding)?;
    }
    let audit = fields
        .get("audit")
        .and_then(Value::as_bool)
        .ok_or_else(|| anyhow!("native G0 funnel manifest audit must be boolean"))?;
    let manifest_sha256 = sha_field(fields, "manifestSha256", "native G0 funnel manifest")?;
    if canonical_sha256_without_object_field(value, "manifestSha256")? != manifest_sha256 {
        bail!("native G0 funnel manifest identity mismatch");
    }
    Ok(G0FunnelManifest {
        authority_sha256,
        execution_authority,
        output_root: string(fields, "outputRoot", "native G0 funnel manifest")?,
        final_newline,
        generation_config,
        generation_config_sha256,
        generation_index,
        construction_pool_size,
        evaluation_population_size,
        max_proposal_attempts,
        publication_policy,
        identity_ledger,
        audit,
        result_path: G0_FUNNEL_RESULT_PATH.to_owned(),
        manifest_sha256,
    })
}
