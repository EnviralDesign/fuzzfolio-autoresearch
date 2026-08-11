use std::{fs, path::Path, process::Command};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use temporal_qd_contract::{
    CONTRACT_VERSION, canonical_json_bytes, canonical_json_line, canonical_sha256,
};
use temporal_qd_rotating_prefinalizer::{
    campaign_receipt::{build_to_path, validate_receipt},
    core_receipt::extract_evolved_chain_to_path,
};

fn hash_bytes(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}
fn hash(tag: &str) -> String {
    hash_bytes(tag.as_bytes())
}
fn self_hash(v: &mut Value, field: &str) {
    v[field] = json!(canonical_sha256(v).unwrap());
}
fn write_json(root: &Path, name: &str, value: &Value) -> Value {
    let path = root.join(name);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let bytes = canonical_json_line(value).unwrap();
    fs::write(&path, &bytes).unwrap();
    json!({"path":path,"rawSha256":hash_bytes(&bytes),"sizeBytes":bytes.len()})
}

/// A deliberately minimal, but canonical, v5 evolved chain.  It is kept here
/// because each tamper test must start from the same authenticated topology.
fn evolved_chain(root: &Path) -> (Value, Value, Value, Value) {
    let row = json!({"schemaVersion":"temporal_qd_v5_proposal_funnel_entry_v1","entrySha256":hash("entry"),"proposalOrdinal":0,"originKind":"immigrant","disposition":"rejected"});
    let row_bytes = canonical_json_bytes(&row).unwrap();
    let fragment = |kind: &str, count: u64, bytes: usize, digest: String| json!({"kind":kind,"fragmentSha256":digest,"encodedBytes":bytes,"rowCount":count});
    let mut fragments = json!({
        "schemaVersion":"temporal_qd_v5_evolved_publication_fragments_v2",
        "acceptedCandidateCount":1,"proposalAttemptCount":1,
        "populationCandidates":fragment("populationCandidates",1,0,hash("population")),
        "evaluationCandidates":fragment("evaluationCandidates",1,0,hash("evaluation")),
        "evaluationFunnelEntries":fragment("evaluationFunnelEntries",1,row_bytes.len(),hash_bytes(&row_bytes)),
        "generationJournalBindings":fragment("generationJournalBindings",1,0,hash("journal")),
    });
    self_hash(&mut fragments, "fragmentBundleSha256");
    let fragment_file = write_json(root, "fragments.json", &fragments);
    let evaluation = json!({"candidateCount":1,"proposalAttempts":1,"funnelEntries":[row]});
    let evaluation_file = write_json(root, "evaluation.json", &evaluation);
    let fragment_root = fragments["fragmentBundleSha256"].clone();
    let mut inventory = json!({"objectStore":{"objects":[{"relativePath":"sha256/object.json","objectSha256":fragment_root,"fileSha256":fragment_file["rawSha256"],"byteLength":fragment_file["sizeBytes"]}]}});
    self_hash(&mut inventory, "outputInventorySha256");
    let inventory_root = inventory["outputInventorySha256"].clone();
    let mut receipt = json!({"schemaVersion":"temporal_qd_native_v5_evolved_construction_receipt_v2","publicationFragmentsSha256":fragments["fragmentBundleSha256"],"outputInventory":inventory,"outputInventorySha256":inventory_root});
    self_hash(&mut receipt, "receiptSha256");
    let mut manifest =
        json!({"schemaVersion":"temporal_qd_native_v5_proposal_construction_manifest_v1"});
    self_hash(&mut manifest, "manifestSha256");
    let mut result = json!({"schemaVersion":"temporal_qd_native_v5_evolved_construction_result_v2","manifestSha256":manifest["manifestSha256"],"publicationFragmentsSha256":fragments["fragmentBundleSha256"],"outputInventorySha256":receipt["outputInventorySha256"],"receipt":receipt,"receiptSha256":receipt["receiptSha256"]});
    self_hash(&mut result, "resultSha256");
    let invocation_root = format!(
        "native-batch/v5-proposal/{}",
        manifest["manifestSha256"]
            .as_str()
            .unwrap()
            .trim_start_matches("sha256:")
    );
    let invocation_manifest =
        write_json(root, &format!("{invocation_root}/manifest.json"), &manifest);
    let invocation_result = write_json(
        root,
        &format!("{invocation_root}/v5-proposal-result.json"),
        &result,
    );
    let fragments_descriptor = json!({"schemaVersion":"temporal_qd_native_v5_evolved_publication_fragments_descriptor_v1","coreSchemaVersion":"temporal_qd_v5_evolved_publication_fragments_v2","relativePath":"fragments.json","absolutePath":fragment_file["path"],"semanticSha256":fragments["fragmentBundleSha256"],"fileSha256":fragment_file["rawSha256"],"byteLength":fragment_file["sizeBytes"]});
    let population_descriptor = json!({"relativePath":"evaluation.json","absolutePath":evaluation_file["path"],"semanticSha256":hash("evaluation-semantic"),"fileSha256":evaluation_file["rawSha256"],"byteLength":evaluation_file["sizeBytes"]});
    let mut adapter = json!({
        "schemaVersion":"temporal_qd_native_v5_evolved_generation_construction_adapter_v3","operation":"native_v5_proposal_construction","completed":true,"generationKind":"evolved","generationIndex":2,
        "generationConfigSha256":hash("config"),"authoritySha256":hash("authority"),"attemptCount":1,"acceptedCandidateCount":1,"selectedEvaluationCandidateCount":1,
        "publicationPlanSha256":hash("plan"),"publicationRequestSha256":hash("request"),"proposalResultSha256":result["resultSha256"],"proposalReceiptSha256":result["receiptSha256"],"outputInventorySha256":result["outputInventorySha256"],
        "population":population_descriptor,"evaluationPopulation":population_descriptor,"generationJournal":json!({}),"identityLedger":json!({}),"evolvedPublicationFragments":fragments_descriptor,
        "nativeV5Invocation":{"schemaVersion":"temporal_qd_native_v5_evolved_invocation_descriptor_v1","proposalManifest":{"schemaVersion":"temporal_qd_native_v5_invocation_document_descriptor_v1","documentSchemaVersion":"temporal_qd_native_v5_proposal_construction_manifest_v1","relativePath":format!("{invocation_root}/manifest.json"),"absolutePath":invocation_manifest["path"],"semanticSha256":manifest["manifestSha256"],"fileSha256":invocation_manifest["rawSha256"],"byteLength":invocation_manifest["sizeBytes"]},"proposalResult":{"schemaVersion":"temporal_qd_native_v5_invocation_document_descriptor_v1","documentSchemaVersion":"temporal_qd_native_v5_evolved_construction_result_v2","relativePath":format!("{invocation_root}/v5-proposal-result.json"),"absolutePath":invocation_result["path"],"semanticSha256":result["resultSha256"],"fileSha256":invocation_result["rawSha256"],"byteLength":invocation_result["sizeBytes"]},"proposalReceiptSha256":result["receiptSha256"],"outputInventorySha256":result["outputInventorySha256"]}
    });
    self_hash(&mut adapter, "adapterSha256");
    let manifest_file = write_json(root, "manifest.json", &manifest);
    let result_file = write_json(root, "result.json", &result);
    let adapter_file = write_json(root, "adapter.json", &adapter);
    let mut input = json!({"schemaVersion":"temporal_qd_v5_evolved_attempt_adapter_chain_input_v1","contractVersion":CONTRACT_VERSION,"manifest":manifest_file,"result":result_file,"adapter":adapter_file});
    self_hash(&mut input, "inputSha256");
    (input, manifest, result, adapter)
}

fn write_chain(root: &Path, input: &Value) -> std::path::PathBuf {
    let path = root.join("chain.json");
    fs::write(&path, canonical_json_line(input).unwrap()).unwrap();
    path
}
fn refresh_descriptor(root: &Path, input: &mut Value, key: &str, value: &Value) {
    input[key] = write_json(root, &format!("{key}-changed.json"), value);
    input.as_object_mut().unwrap().remove("inputSha256");
    self_hash(input, "inputSha256");
}

#[test]
fn evolved_chain_extraction_rejects_independent_seal_and_inventory_attacks() {
    let root = TempDir::new().unwrap();
    let (input, _manifest, result, adapter) = evolved_chain(root.path());
    let chain_path = write_chain(root.path(), &input);
    let output = root.path().join("attempts.jsonl");
    let extracted = extract_evolved_chain_to_path(&chain_path, &output).unwrap();
    assert_eq!(extracted["recordCount"], 1);
    assert!(output.is_file());
    assert_eq!(
        extract_evolved_chain_to_path(&chain_path, &output).unwrap(),
        extracted
    );

    let mut v2 = adapter.clone();
    v2["schemaVersion"] = json!("temporal_qd_native_v5_evolved_generation_construction_adapter_v2");
    v2.as_object_mut().unwrap().remove("nativeV5Invocation");
    v2.as_object_mut().unwrap().remove("adapterSha256");
    self_hash(&mut v2, "adapterSha256");
    let mut v2_input = input.clone();
    refresh_descriptor(root.path(), &mut v2_input, "adapter", &v2);
    assert!(
        extract_evolved_chain_to_path(
            &write_chain(root.path(), &v2_input),
            &root.path().join("v2.jsonl")
        )
        .is_err()
    );

    for (name, mutate) in [
        ("missing-invocation", 0usize),
        ("replaced-result", 1),
        ("wrong-invocation-schema", 2),
        ("invocation-path", 3),
        ("invocation-hash", 4),
        ("manifest-substitution", 5),
    ] {
        let mut bad = adapter.clone();
        match mutate {
            0 => {
                bad.as_object_mut().unwrap().remove("nativeV5Invocation");
            }
            1 => {
                bad["nativeV5Invocation"]["proposalResult"] =
                    bad["nativeV5Invocation"]["proposalManifest"].clone();
            }
            2 => {
                bad["nativeV5Invocation"]["schemaVersion"] =
                    json!("temporal_qd_native_v5_g0_invocation_descriptor_v1");
            }
            3 => {
                bad["nativeV5Invocation"]["proposalManifest"]["relativePath"] =
                    json!("native-batch/v5-proposal/../manifest.json");
            }
            4 => {
                bad["nativeV5Invocation"]["proposalResult"]["fileSha256"] =
                    json!(hash("wrong-invocation-file"));
            }
            _ => {
                bad["nativeV5Invocation"]["proposalManifest"]["absolutePath"] =
                    bad["nativeV5Invocation"]["proposalResult"]["absolutePath"].clone();
            }
        }
        bad.as_object_mut().unwrap().remove("adapterSha256");
        self_hash(&mut bad, "adapterSha256");
        let mut bad_input = input.clone();
        refresh_descriptor(root.path(), &mut bad_input, "adapter", &bad);
        assert!(
            extract_evolved_chain_to_path(
                &write_chain(root.path(), &bad_input),
                &root.path().join(format!("{name}.jsonl"))
            )
            .is_err(),
            "{name}"
        );
    }

    // A fresh adapter and fragment root cannot replace the result's sealed root.
    let mut changed_adapter = adapter.clone();
    let mut changed_fragments: Value = serde_json::from_slice(
        &fs::read(
            changed_adapter["evolvedPublicationFragments"]["absolutePath"]
                .as_str()
                .unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    changed_fragments["proposalAttemptCount"] = json!(2);
    changed_fragments
        .as_object_mut()
        .unwrap()
        .remove("fragmentBundleSha256");
    self_hash(&mut changed_fragments, "fragmentBundleSha256");
    let changed_file = write_json(root.path(), "changed-fragments.json", &changed_fragments);
    changed_adapter["evolvedPublicationFragments"]["absolutePath"] = changed_file["path"].clone();
    changed_adapter["evolvedPublicationFragments"]["relativePath"] =
        json!("changed-fragments.json");
    changed_adapter["evolvedPublicationFragments"]["semanticSha256"] =
        changed_fragments["fragmentBundleSha256"].clone();
    changed_adapter["evolvedPublicationFragments"]["fileSha256"] =
        changed_file["rawSha256"].clone();
    changed_adapter["evolvedPublicationFragments"]["byteLength"] =
        changed_file["sizeBytes"].clone();
    changed_adapter
        .as_object_mut()
        .unwrap()
        .remove("adapterSha256");
    self_hash(&mut changed_adapter, "adapterSha256");
    let mut changed_input = input.clone();
    refresh_descriptor(root.path(), &mut changed_input, "adapter", &changed_adapter);
    assert!(
        extract_evolved_chain_to_path(
            &write_chain(root.path(), &changed_input),
            &root.path().join("bad-adapter.jsonl")
        )
        .is_err()
    );

    let mut bad_result = result.clone();
    bad_result["resultSha256"] = json!(hash("tampered-result"));
    let mut result_input = input.clone();
    refresh_descriptor(root.path(), &mut result_input, "result", &bad_result);
    assert!(
        extract_evolved_chain_to_path(
            &write_chain(root.path(), &result_input),
            &root.path().join("bad-result.jsonl")
        )
        .is_err()
    );

    let mut bad_receipt = result.clone();
    bad_receipt["receipt"]["receiptSha256"] = json!(hash("tampered-receipt"));
    let mut receipt_input = input.clone();
    refresh_descriptor(root.path(), &mut receipt_input, "result", &bad_receipt);
    assert!(
        extract_evolved_chain_to_path(
            &write_chain(root.path(), &receipt_input),
            &root.path().join("bad-receipt.jsonl")
        )
        .is_err()
    );

    let mut bad_inventory = result.clone();
    bad_inventory["receipt"]["outputInventory"]["outputInventorySha256"] =
        json!(hash("tampered-inventory"));
    let mut inventory_input = input.clone();
    refresh_descriptor(root.path(), &mut inventory_input, "result", &bad_inventory);
    assert!(
        extract_evolved_chain_to_path(
            &write_chain(root.path(), &inventory_input),
            &root.path().join("bad-inventory.jsonl")
        )
        .is_err()
    );

    let mut missing_object = result.clone();
    missing_object["receipt"]["outputInventory"]["objectStore"]["objects"] = json!([]);
    let mut missing_input = input.clone();
    refresh_descriptor(root.path(), &mut missing_input, "result", &missing_object);
    assert!(
        extract_evolved_chain_to_path(
            &write_chain(root.path(), &missing_input),
            &root.path().join("missing-object.jsonl")
        )
        .is_err()
    );

    let mut alias_object = result.clone();
    let entry = alias_object["receipt"]["outputInventory"]["objectStore"]["objects"][0].clone();
    alias_object["receipt"]["outputInventory"]["objectStore"]["objects"]
        .as_array_mut()
        .unwrap()
        .push(entry);
    let mut alias_input = input.clone();
    refresh_descriptor(root.path(), &mut alias_input, "result", &alias_object);
    assert!(
        extract_evolved_chain_to_path(
            &write_chain(root.path(), &alias_input),
            &root.path().join("alias-object.jsonl")
        )
        .is_err()
    );

    let mut wrong_schema = adapter;
    wrong_schema["evolvedPublicationFragments"]["coreSchemaVersion"] = json!("wrong");
    wrong_schema
        .as_object_mut()
        .unwrap()
        .remove("adapterSha256");
    self_hash(&mut wrong_schema, "adapterSha256");
    let mut schema_input = input;
    refresh_descriptor(root.path(), &mut schema_input, "adapter", &wrong_schema);
    assert!(
        extract_evolved_chain_to_path(
            &write_chain(root.path(), &schema_input),
            &root.path().join("wrong-schema.jsonl")
        )
        .is_err()
    );
}

fn campaign_input(root: &Path, marker: &str) -> Value {
    let mut bindings = serde_json::Map::new();
    for name in [
        "freezeManifest",
        "freezeTransaction",
        "campaign",
        "taskManifest",
        "evaluationIdentity",
        "gatewayCompletion",
        "checkpoint",
        "campaignSeal",
        "tailResultIndex",
        "tailTransaction",
        "evaluatedMembersJsonl",
        "candidatePanelBundlesJsonl",
    ] {
        bindings.insert(
            name.into(),
            write_json(
                root,
                &format!("{marker}-{name}.json"),
                &json!({"marker":marker,"name":name}),
            ),
        );
    }
    let rows = json!({"rawSha256":hash("rows"),"sizeBytes":0,"recordCount":0,"rowSchema":"temporal_qd_evaluated_member_v1"});
    let bundles = json!({"rawSha256":hash("bundles"),"sizeBytes":0,"recordCount":0,"rowSchema":"temporal_qd_candidate_panel_evidence_bundle_v1"});
    let mut input = json!({"schemaVersion":"temporal_qd_v5_rotating_campaign_receipt_input_v2","contractVersion":CONTRACT_VERSION,"generationIndex":2,"campaignRole":"proposal_current_panel","panelId":"p1","rotatingEvidenceSha256":hash("rot"),"cohortSource":{"kind":"proposal_evaluation_population","sourceSemanticSha256":hash("source"),"candidateCount":0,"selectionSha256":Value::Null},"campaignFreeze":{"transactionSha256":hash("t"),"cohortPopulationSha256":hash("p"),"preparationSha256":hash("prep"),"authorityId":hash("a"),"evaluationIdentitySha256":hash("e"),"campaignSha256":hash("c"),"taskMatrixSha256":hash("m"),"candidateCount":0,"windowCount":0,"taskCount":0},"campaignSeal":{"directionalTailAuthoritySha256":hash("d"),"campaignSealSha256":hash("s"),"tailResultIndexSha256":hash("i"),"tailTransactionSha256":hash("tt")},"evaluatedMembers":rows,"candidatePanelBundles":bundles,"runtimeAuthoritySha256":hash("runtime"),"executionBindings":Value::Object(bindings)});
    self_hash(&mut input, "inputSha256");
    input
}

#[test]
fn campaign_receipt_is_receipt_last_and_refuses_substitution() {
    let root = TempDir::new().unwrap();
    let first = campaign_input(root.path(), "first");
    let input_path = root.path().join("campaign-input.json");
    let receipt_path = root.path().join("campaign-receipt.json");
    fs::write(&input_path, canonical_json_line(&first).unwrap()).unwrap();
    let cli = env!("CARGO_BIN_EXE_temporal-qd-rotating-prefinalizer");
    assert!(
        Command::new(cli)
            .args(["build-campaign-receipt"])
            .arg(&input_path)
            .arg(&receipt_path)
            .output()
            .unwrap()
            .status
            .success()
    );
    let receipt: Value = serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
    // The API's existing-output path accepts the identical deterministic input.
    assert_eq!(build_to_path(&input_path, &receipt_path).unwrap(), receipt);
    for binding in receipt["executionBindings"].as_object().unwrap().values() {
        fs::remove_file(binding["path"].as_str().unwrap()).unwrap();
    }
    fs::remove_file(&input_path).unwrap();
    // Receipt-last admission validates only sealed descriptor shapes; a
    // prefinalizer restart does not reopen these removed inputs or sources.
    validate_receipt(&receipt).unwrap();
    let different = campaign_input(root.path(), "different");
    let different_path = root.path().join("different-input.json");
    fs::write(&different_path, canonical_json_line(&different).unwrap()).unwrap();
    assert!(build_to_path(&different_path, &receipt_path).is_err());
}
