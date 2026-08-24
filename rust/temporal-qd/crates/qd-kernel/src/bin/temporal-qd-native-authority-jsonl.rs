//! Versioned JSONL transport for the canonical native Temporal-QD compiler.
//!
//! This binary contains no compiler or validator logic. It exposes the exact
//! `qd-kernel::v5` functions used by native construction so offline audits and
//! compatibility oracles can capture complete request/response transcripts.

use std::io::{self, BufRead, Write};

use serde_json::{Map, Value, json};
use temporal_qd_contract::{canonical_json_line, canonical_sha256};
use temporal_qd_kernel::v5::{
    ModuleSourceIdentities, V5SharedConstructionAuthority, author_v5_native_topology_study_block,
    compile_bidirectional_profile, native_profile_identity_material,
    reconstruct_v5_native_candidate_envelope, validate_native_profile,
};

const REQUEST_SCHEMA: &str = "temporal_qd_native_authority_jsonl_request_v1";
const RESPONSE_SCHEMA: &str = "temporal_qd_native_authority_jsonl_response_v1";
const AUTHORITY_SCHEMA: &str = "temporal_qd_native_compiler_authority_v1";
const VALIDATE_OPERATION: &str = "validate_native_profile";
const COMPILE_OPERATION: &str = "compile_bidirectional_profile";
const RECONSTRUCT_OPERATION: &str = "reconstruct_compact_parent";
const TOPOLOGY_BLOCK_OPERATION: &str = "author_topology_study_block";

fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str, String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("request lacks string {key}"))
}

fn object<'a>(value: &'a Value, key: &str) -> Result<&'a Value, String> {
    value
        .get(key)
        .filter(|item| item.is_object())
        .ok_or_else(|| format!("request lacks object {key}"))
}

fn identities(value: &Value, key: &str) -> Result<ModuleSourceIdentities, String> {
    let value = object(value, key)?;
    Ok(ModuleSourceIdentities {
        profile_snapshot_sha256: text(value, "profileSnapshotSha256")?.to_owned(),
        program_sha256: text(value, "programSha256")?.to_owned(),
    })
}

fn authority_manifest() -> Value {
    json!({
        "schemaVersion": AUTHORITY_SCHEMA,
        "authorityVersion": "1",
        "canonicalJsonContract": "temporal_qd_contract::canonical_json_line",
        "compiler": "temporal_qd_kernel::v5::compile_bidirectional_profile",
        "validator": "temporal_qd_kernel::v5::validate_native_profile",
        "requestSchema": REQUEST_SCHEMA,
        "responseSchema": RESPONSE_SCHEMA,
        "validationSchema": "temporal_search_candidate_validation_v1"
    })
}

fn validation_value(profile: &Value, candidate_id: &str) -> Result<Value, String> {
    let validation =
        validate_native_profile(profile, candidate_id).map_err(|error| error.to_string())?;
    Ok(json!({
        "identityMaterial": native_profile_identity_material(profile)
            .map_err(|error| error.to_string())?,
        "report": validation.report,
        "rawProfileSha256": validation.raw_profile_sha256,
        "profileSnapshotSha256": validation.profile_snapshot_sha256,
        "programSha256": validation.program_sha256,
        "validationReportSha256": validation.validation_report_sha256
    }))
}

fn execute(request: &Value) -> Result<Value, String> {
    if text(request, "schemaVersion")? != REQUEST_SCHEMA {
        return Err("request schema is not admitted by native authority v1".to_owned());
    }
    let operation = text(request, "operation")?;
    let candidate_id = text(request, "candidateId")?;
    match operation {
        VALIDATE_OPERATION => {
            let profile = object(request, "profile")?;
            Ok(json!({"validation": validation_value(profile, candidate_id)?}))
        }
        COMPILE_OPERATION => {
            let long_profile = object(request, "longProfile")?;
            let short_profile = object(request, "shortProfile")?;
            let long_identities = identities(request, "longSourceIdentities")?;
            let short_identities = identities(request, "shortSourceIdentities")?;
            let profile = compile_bidirectional_profile(
                long_profile,
                short_profile,
                candidate_id,
                &long_identities,
                &short_identities,
            )
            .map_err(|error| error.to_string())?;
            let validation = validation_value(&profile, candidate_id)?;
            Ok(json!({"compiledProfile": profile, "validation": validation}))
        }
        RECONSTRUCT_OPERATION => {
            let authority = V5SharedConstructionAuthority::from_shared_object(object(
                request,
                "sharedAuthority",
            )?)
            .map_err(|error| error.to_string())?;
            let envelope = reconstruct_v5_native_candidate_envelope(
                &authority,
                object(request, "parentMaterialRow")?,
            )
            .map_err(|error| error.to_string())?;
            Ok(json!({"candidateEnvelope": envelope}))
        }
        TOPOLOGY_BLOCK_OPERATION => {
            let authority = V5SharedConstructionAuthority::from_shared_object(object(
                request,
                "sharedAuthority",
            )?)
            .map_err(|error| error.to_string())?;
            let block = author_v5_native_topology_study_block(
                &authority,
                object(request, "parentMaterialRow")?,
                object(request, "block")?,
                object(request, "topologyRecord")?,
                object(request, "eventPrimitive")?,
                object(request, "nativeAuthority")?,
            )
            .map_err(|error| error.to_string())?;
            Ok(json!({"topologyBlock": block}))
        }
        _ => Err(format!(
            "unsupported native authority operation {operation:?}"
        )),
    }
}

fn response(request: &Value) -> Value {
    let request_id = request
        .get("requestId")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let operation = request
        .get("operation")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let request_sha256 = canonical_sha256(request).unwrap_or_else(|_| "invalid".to_owned());
    let outcome = execute(request);
    let mut fields = Map::new();
    fields.insert(
        "schemaVersion".to_owned(),
        Value::String(RESPONSE_SCHEMA.to_owned()),
    );
    fields.insert("requestId".to_owned(), Value::String(request_id));
    fields.insert("operation".to_owned(), Value::String(operation));
    fields.insert("requestSha256".to_owned(), Value::String(request_sha256));
    fields.insert("authority".to_owned(), authority_manifest());
    match outcome {
        Ok(result) => {
            fields.insert("status".to_owned(), Value::String("ok".to_owned()));
            fields.insert("result".to_owned(), result);
        }
        Err(error) => {
            fields.insert("status".to_owned(), Value::String("error".to_owned()));
            fields.insert("error".to_owned(), Value::String(error));
        }
    }
    let unsealed = Value::Object(fields.clone());
    fields.insert(
        "responseSha256".to_owned(),
        Value::String(canonical_sha256(&unsealed).unwrap_or_else(|_| "invalid".to_owned())),
    );
    Value::Object(fields)
}

fn main() {
    let stdin = io::stdin();
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    for line in stdin.lock().lines() {
        let output = match line {
            Ok(line) => match serde_json::from_str::<Value>(&line) {
                Ok(request) => response(&request),
                Err(error) => json!({
                    "schemaVersion": RESPONSE_SCHEMA,
                    "status": "error",
                    "error": format!("invalid JSON request: {error}")
                }),
            },
            Err(error) => json!({
                "schemaVersion": RESPONSE_SCHEMA,
                "status": "error",
                "error": format!("stdin read failed: {error}")
            }),
        };
        let bytes = canonical_json_line(&output).expect("native response must be canonical JSON");
        stdout
            .write_all(&bytes)
            .expect("write native JSONL response");
        stdout.flush().expect("flush native JSONL response");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unknown_operation_with_content_addressed_response() {
        let request = json!({
            "schemaVersion": REQUEST_SCHEMA,
            "requestId": "test",
            "operation": "unknown",
            "candidateId": "qd_test"
        });
        let response = response(&request);
        assert_eq!(response["status"], "error");
        assert_eq!(
            response["requestSha256"],
            canonical_sha256(&request).unwrap()
        );
        let mut unsealed = response.clone();
        let expected = unsealed
            .as_object_mut()
            .unwrap()
            .remove("responseSha256")
            .unwrap();
        assert_eq!(expected, canonical_sha256(&unsealed).unwrap());
    }
}
