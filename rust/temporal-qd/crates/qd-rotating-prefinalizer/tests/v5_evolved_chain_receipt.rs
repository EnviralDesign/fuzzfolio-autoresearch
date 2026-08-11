use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use temporal_qd_contract::{
    CONTRACT_VERSION, canonical_json_bytes, canonical_json_line, canonical_sha256,
    canonical_sha256_without_object_field,
};

fn hash_bytes(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn hash(tag: &str) -> String {
    hash_bytes(tag.as_bytes())
}

fn reseal(value: &mut Value, field: &str) {
    value.as_object_mut().unwrap().remove(field);
    value[field] = json!(canonical_sha256(value).unwrap());
}

fn write_json(root: &Path, name: &str, value: &Value) -> Value {
    let path = root.join(name);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let bytes = canonical_json_line(value).unwrap();
    fs::write(&path, &bytes).unwrap();
    json!({"path":path,"rawSha256":hash_bytes(&bytes),"sizeBytes":bytes.len()})
}

fn fragment(kind: &str, rows: u64, bytes: usize, digest: String) -> Value {
    json!({"kind":kind,"fragmentSha256":digest,"encodedBytes":bytes,"rowCount":rows})
}

fn attempt(ordinal: u64, origin: &str, disposition: &str, candidate_bearing: bool) -> Value {
    let mut row = json!({
        "schemaVersion":"temporal_qd_v5_proposal_funnel_entry_v1",
        "proposalOrdinal":ordinal,
        "originKind":origin,
        "disposition":disposition,
    });
    if candidate_bearing {
        row["candidate"] = json!({
            "candidateId":"accepted-candidate",
            "sourceProfileSha256":hash("accepted-profile"),
        });
        row["proposal"] = json!({
            "candidateId":"accepted-candidate",
            "rawSourceProfileSha256":hash("accepted-profile"),
        });
        row["funnelCandidate"] = json!({
            "schemaVersion":"temporal_qd_proposal_funnel_stage_v1",
            "candidateId":"accepted-candidate",
        });
        row["acceptedCompactRecordSha256"] = json!(hash("accepted-compact-record"));
    }
    reseal(&mut row, "entrySha256");
    row
}

/// A complete v3 sealed chain, deliberately containing every terminal
/// disposition and all evolved origin families.  Rows use their real public
/// schema and canonical entry self hashes so mutation cases do not rely on
/// malformed input to fail.
fn evolved_chain(root: &Path) -> (Value, Value, Value, Value, Vec<Value>) {
    let rows = vec![
        attempt(0, "random_immigrant", "accepted", true),
        attempt(1, "structural_offspring", "rejected", false),
        attempt(2, "crossover", "no_op", false),
        attempt(3, "random_immigrant", "duplicate", false),
    ];
    let funnel_bytes = rows
        .iter()
        .enumerate()
        .fold(Vec::new(), |mut bytes, (index, row)| {
            if index != 0 {
                bytes.push(b',');
            }
            bytes.extend(canonical_json_bytes(row).unwrap());
            bytes
        });
    let mut fragments = json!({
        "schemaVersion":"temporal_qd_v5_evolved_publication_fragments_v2",
        "acceptedCandidateCount":1,
        "proposalAttemptCount":4,
        "populationCandidates":fragment("populationCandidates", 1, 0, hash("population")),
        "evaluationCandidates":fragment("evaluationCandidates", 1, 0, hash("evaluation")),
        "evaluationFunnelEntries":fragment("evaluationFunnelEntries", 4, funnel_bytes.len(), hash_bytes(&funnel_bytes)),
        "generationJournalBindings":fragment("generationJournalBindings", 1, 0, hash("journal")),
    });
    reseal(&mut fragments, "fragmentBundleSha256");
    let fragments_file = write_json(root, "fragments.json", &fragments);
    let evaluation = json!({"candidateCount":1,"proposalAttempts":4,"funnelEntries":rows});
    let evaluation_file = write_json(root, "evaluation.json", &evaluation);

    let mut inventory = json!({"objectStore":{"objects":[{
        "relativePath":"sha256/fragments.json",
        "objectSha256":fragments["fragmentBundleSha256"],
        "fileSha256":fragments_file["rawSha256"],
        "byteLength":fragments_file["sizeBytes"],
    }]}});
    reseal(&mut inventory, "outputInventorySha256");
    let mut receipt = json!({
        "schemaVersion":"temporal_qd_native_v5_evolved_construction_receipt_v2",
        "publicationFragmentsSha256":fragments["fragmentBundleSha256"],
        "outputInventory":inventory,
        "outputInventorySha256":inventory["outputInventorySha256"],
    });
    reseal(&mut receipt, "receiptSha256");
    let mut manifest =
        json!({"schemaVersion":"temporal_qd_native_v5_proposal_construction_manifest_v1"});
    reseal(&mut manifest, "manifestSha256");
    let mut result = json!({
        "schemaVersion":"temporal_qd_native_v5_evolved_construction_result_v2",
        "manifestSha256":manifest["manifestSha256"],
        "publicationFragmentsSha256":fragments["fragmentBundleSha256"],
        "outputInventorySha256":receipt["outputInventorySha256"],
        "receipt":receipt,
        "receiptSha256":receipt["receiptSha256"],
    });
    reseal(&mut result, "resultSha256");
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
    let fragments_descriptor = json!({
        "schemaVersion":"temporal_qd_native_v5_evolved_publication_fragments_descriptor_v1",
        "coreSchemaVersion":"temporal_qd_v5_evolved_publication_fragments_v2",
        "relativePath":"fragments.json","absolutePath":fragments_file["path"],
        "semanticSha256":fragments["fragmentBundleSha256"],
        "fileSha256":fragments_file["rawSha256"],"byteLength":fragments_file["sizeBytes"],
    });
    let evaluation_descriptor = json!({
        "relativePath":"evaluation.json","absolutePath":evaluation_file["path"],
        "semanticSha256":hash("evaluation-semantic"),
        "fileSha256":evaluation_file["rawSha256"],"byteLength":evaluation_file["sizeBytes"],
    });
    let mut adapter = json!({
        "schemaVersion":"temporal_qd_native_v5_evolved_generation_construction_adapter_v3",
        "operation":"native_v5_proposal_construction","completed":true,"generationKind":"evolved","generationIndex":2,
        "generationConfigSha256":hash("config"),"authoritySha256":hash("authority"),"attemptCount":4,
        "acceptedCandidateCount":1,"selectedEvaluationCandidateCount":1,
        "publicationPlanSha256":hash("plan"),"publicationRequestSha256":hash("request"),
        "proposalResultSha256":result["resultSha256"],"proposalReceiptSha256":result["receiptSha256"],
        "outputInventorySha256":result["outputInventorySha256"],
        "population":evaluation_descriptor,"evaluationPopulation":evaluation_descriptor,
        "generationJournal":{},"identityLedger":{},"evolvedPublicationFragments":fragments_descriptor,
        "nativeV5Invocation":{
            "schemaVersion":"temporal_qd_native_v5_evolved_invocation_descriptor_v1",
            "proposalManifest":{"schemaVersion":"temporal_qd_native_v5_invocation_document_descriptor_v1","documentSchemaVersion":"temporal_qd_native_v5_proposal_construction_manifest_v1","relativePath":format!("{invocation_root}/manifest.json"),"absolutePath":invocation_manifest["path"],"semanticSha256":manifest["manifestSha256"],"fileSha256":invocation_manifest["rawSha256"],"byteLength":invocation_manifest["sizeBytes"]},
            "proposalResult":{"schemaVersion":"temporal_qd_native_v5_invocation_document_descriptor_v1","documentSchemaVersion":"temporal_qd_native_v5_evolved_construction_result_v2","relativePath":format!("{invocation_root}/v5-proposal-result.json"),"absolutePath":invocation_result["path"],"semanticSha256":result["resultSha256"],"fileSha256":invocation_result["rawSha256"],"byteLength":invocation_result["sizeBytes"]},
            "proposalReceiptSha256":result["receiptSha256"],"outputInventorySha256":result["outputInventorySha256"],
        },
    });
    reseal(&mut adapter, "adapterSha256");
    let manifest_file = write_json(root, "manifest.json", &manifest);
    let result_file = write_json(root, "result.json", &result);
    let adapter_file = write_json(root, "adapter.json", &adapter);
    let mut input = json!({
        "schemaVersion":"temporal_qd_v5_evolved_attempt_adapter_chain_input_v1",
        "contractVersion":CONTRACT_VERSION,"manifest":manifest_file,"result":result_file,"adapter":adapter_file,
    });
    reseal(&mut input, "inputSha256");
    let rows = evaluation["funnelEntries"].as_array().unwrap().clone();
    (input, fragments, result, adapter, rows)
}

fn write_input(root: &Path, name: &str, input: &Value) -> PathBuf {
    let path = root.join(name);
    fs::write(&path, canonical_json_line(input).unwrap()).unwrap();
    path
}

fn refresh_bound(root: &Path, input: &mut Value, key: &str, value: &Value) {
    input[key] = write_json(root, &format!("{key}-mutated.json"), value);
    reseal(input, "inputSha256");
}

fn cli_extract(input: &Path, attempts: &Path, receipt: &Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_temporal-qd-rotating-prefinalizer"))
        .args(["extract-evolved-chain"])
        .arg(input)
        .arg(attempts)
        .arg(receipt)
        .output()
        .unwrap()
}

#[test]
fn extract_evolved_chain_receipt_seals_mixed_v3_accounting_and_restarts() {
    let root = TempDir::new().unwrap();
    let (input, fragments, result, adapter, rows) = evolved_chain(root.path());
    let input_path = write_input(root.path(), "chain.json", &input);
    let attempts = root.path().join("attempts.jsonl");
    let receipt_path = root.path().join("receipt.json");
    let first = cli_extract(&input_path, &attempts, &receipt_path);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let receipt: Value = serde_json::from_slice(&first.stdout).unwrap();

    assert_eq!(fragments["acceptedCandidateCount"], 1);
    assert_eq!(fragments["proposalAttemptCount"], 4);
    assert_eq!(fragments["evaluationFunnelEntries"]["rowCount"], 4);
    assert_eq!(
        rows.iter()
            .filter(|r| r["disposition"] == "accepted")
            .count(),
        1
    );
    assert_eq!(
        receipt["proposalAccounting"],
        json!({
            "proposalAttemptCount":4,
            "originProposalCounts":{"crossover":1,"random_immigrant":2,"structural_offspring":1},
            "dispositionCounts":{"accepted":1,"duplicate":1,"no_op":1,"rejected":1},
        })
    );
    assert_eq!(receipt["inputSha256"], input["inputSha256"]);
    assert_eq!(receipt["proposalResultSha256"], result["resultSha256"]);
    assert_eq!(receipt["proposalReceiptSha256"], result["receiptSha256"]);
    assert_eq!(
        receipt["outputInventorySha256"],
        result["outputInventorySha256"]
    );
    assert_eq!(
        receipt["fragmentBundleSha256"],
        fragments["fragmentBundleSha256"]
    );
    assert_eq!(
        receipt["evaluationPopulationSha256"],
        adapter["evaluationPopulation"]["semanticSha256"]
    );
    assert_eq!(
        canonical_sha256_without_object_field(&receipt, "receiptSha256").unwrap(),
        receipt["receiptSha256"]
    );

    let second = cli_extract(&input_path, &attempts, &receipt_path);
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&second.stdout).unwrap(),
        receipt
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&fs::read(&receipt_path).unwrap()).unwrap(),
        receipt
    );
}

#[test]
fn extract_evolved_chain_receipt_rejects_drift_and_substitution() {
    let root = TempDir::new().unwrap();
    let (input, fragments, result, adapter, rows) = evolved_chain(root.path());

    // Fragment accounting cannot claim a different transcript length, even if
    // its local self hash, descriptor, adapter, and input are refreshed.
    let mut count_drift = fragments.clone();
    count_drift["proposalAttemptCount"] = json!(5);
    reseal(&mut count_drift, "fragmentBundleSha256");
    let mut count_adapter = adapter.clone();
    let count_file = write_json(root.path(), "count-fragments.json", &count_drift);
    count_adapter["evolvedPublicationFragments"]["absolutePath"] = count_file["path"].clone();
    count_adapter["evolvedPublicationFragments"]["relativePath"] = json!("count-fragments.json");
    count_adapter["evolvedPublicationFragments"]["semanticSha256"] =
        count_drift["fragmentBundleSha256"].clone();
    count_adapter["evolvedPublicationFragments"]["fileSha256"] = count_file["rawSha256"].clone();
    count_adapter["evolvedPublicationFragments"]["byteLength"] = count_file["sizeBytes"].clone();
    reseal(&mut count_adapter, "adapterSha256");
    let mut count_input = input.clone();
    refresh_bound(root.path(), &mut count_input, "adapter", &count_adapter);
    assert!(
        !cli_extract(
            &write_input(root.path(), "count-chain.json", &count_input),
            &root.path().join("count.jsonl"),
            &root.path().join("count-receipt.json")
        )
        .status
        .success()
    );

    // Changing the stored row sequence or membership changes the fragment
    // bytes. The immutable funnel commitment rejects all three variants.
    for (name, changed_rows) in [
        (
            "reordered",
            vec![
                rows[1].clone(),
                rows[0].clone(),
                rows[2].clone(),
                rows[3].clone(),
            ],
        ),
        (
            "missing",
            vec![rows[0].clone(), rows[1].clone(), rows[2].clone()],
        ),
        (
            "extra",
            vec![
                rows[0].clone(),
                rows[1].clone(),
                rows[2].clone(),
                rows[3].clone(),
                rows[3].clone(),
            ],
        ),
    ] {
        let changed_eval = json!({"candidateCount":1,"proposalAttempts":changed_rows.len(),"funnelEntries":changed_rows});
        let file = write_json(
            root.path(),
            &format!("{name}-evaluation.json"),
            &changed_eval,
        );
        let mut changed_adapter = adapter.clone();
        changed_adapter["evaluationPopulation"]["absolutePath"] = file["path"].clone();
        changed_adapter["evaluationPopulation"]["relativePath"] =
            json!(format!("{name}-evaluation.json"));
        changed_adapter["evaluationPopulation"]["fileSha256"] = file["rawSha256"].clone();
        changed_adapter["evaluationPopulation"]["byteLength"] = file["sizeBytes"].clone();
        reseal(&mut changed_adapter, "adapterSha256");
        let mut changed_input = input.clone();
        refresh_bound(root.path(), &mut changed_input, "adapter", &changed_adapter);
        assert!(
            !cli_extract(
                &write_input(root.path(), &format!("{name}-chain.json"), &changed_input),
                &root.path().join(format!("{name}.jsonl")),
                &root.path().join(format!("{name}-receipt.json"))
            )
            .status
            .success(),
            "{name}"
        );
    }

    // A semantic origin/disposition rewrite with fresh entry, fragment, and
    // adapter hashes is still rejected by the outer sealed fragment root.
    for (name, field, replacement) in [
        ("origin", "originKind", "crossover"),
        ("disposition", "disposition", "rejected"),
    ] {
        let mut changed_rows = rows.clone();
        changed_rows[3][field] = json!(replacement);
        reseal(&mut changed_rows[3], "entrySha256");
        let funnel = changed_rows
            .iter()
            .enumerate()
            .fold(Vec::new(), |mut bytes, (index, row)| {
                if index != 0 {
                    bytes.push(b',');
                }
                bytes.extend(canonical_json_bytes(row).unwrap());
                bytes
            });
        let mut changed_fragments = fragments.clone();
        changed_fragments["evaluationFunnelEntries"]["fragmentSha256"] = json!(hash_bytes(&funnel));
        changed_fragments["evaluationFunnelEntries"]["encodedBytes"] = json!(funnel.len());
        reseal(&mut changed_fragments, "fragmentBundleSha256");
        let changed_eval =
            json!({"candidateCount":1,"proposalAttempts":4,"funnelEntries":changed_rows});
        let fragment_file = write_json(
            root.path(),
            &format!("{name}-fragments.json"),
            &changed_fragments,
        );
        let evaluation_file = write_json(
            root.path(),
            &format!("{name}-evaluation.json"),
            &changed_eval,
        );
        let mut changed_adapter = adapter.clone();
        changed_adapter["evolvedPublicationFragments"]["absolutePath"] =
            fragment_file["path"].clone();
        changed_adapter["evolvedPublicationFragments"]["relativePath"] =
            json!(format!("{name}-fragments.json"));
        changed_adapter["evolvedPublicationFragments"]["semanticSha256"] =
            changed_fragments["fragmentBundleSha256"].clone();
        changed_adapter["evolvedPublicationFragments"]["fileSha256"] =
            fragment_file["rawSha256"].clone();
        changed_adapter["evolvedPublicationFragments"]["byteLength"] =
            fragment_file["sizeBytes"].clone();
        changed_adapter["evaluationPopulation"]["absolutePath"] = evaluation_file["path"].clone();
        changed_adapter["evaluationPopulation"]["relativePath"] =
            json!(format!("{name}-evaluation.json"));
        changed_adapter["evaluationPopulation"]["fileSha256"] =
            evaluation_file["rawSha256"].clone();
        changed_adapter["evaluationPopulation"]["byteLength"] =
            evaluation_file["sizeBytes"].clone();
        reseal(&mut changed_adapter, "adapterSha256");
        let mut changed_input = input.clone();
        refresh_bound(root.path(), &mut changed_input, "adapter", &changed_adapter);
        assert!(
            !cli_extract(
                &write_input(root.path(), &format!("{name}-chain.json"), &changed_input),
                &root.path().join(format!("{name}.jsonl")),
                &root.path().join(format!("{name}-receipt.json"))
            )
            .status
            .success(),
            "self-rehashed {name} substitution"
        );
    }

    // The outer input cannot swap documents; the root and inventory cannot be
    // substituted beneath an otherwise self-consistent adapter/result pair.
    let mut input_swap = input.clone();
    input_swap["manifest"] = input["result"].clone();
    reseal(&mut input_swap, "inputSha256");
    assert!(
        !cli_extract(
            &write_input(root.path(), "input-swap.json", &input_swap),
            &root.path().join("input-swap.jsonl"),
            &root.path().join("input-swap-receipt.json")
        )
        .status
        .success()
    );
    let mut root_swap = adapter.clone();
    root_swap["evolvedPublicationFragments"]["semanticSha256"] = json!(hash("substituted-root"));
    reseal(&mut root_swap, "adapterSha256");
    let mut root_input = input.clone();
    refresh_bound(root.path(), &mut root_input, "adapter", &root_swap);
    assert!(
        !cli_extract(
            &write_input(root.path(), "root-swap.json", &root_input),
            &root.path().join("root-swap.jsonl"),
            &root.path().join("root-swap-receipt.json")
        )
        .status
        .success()
    );
    let mut inventory_swap = result.clone();
    inventory_swap["receipt"]["outputInventory"]["objectStore"]["objects"] = json!([]);
    reseal(
        &mut inventory_swap["receipt"]["outputInventory"],
        "outputInventorySha256",
    );
    inventory_swap["receipt"]["outputInventorySha256"] =
        inventory_swap["receipt"]["outputInventory"]["outputInventorySha256"].clone();
    reseal(&mut inventory_swap["receipt"], "receiptSha256");
    inventory_swap["receiptSha256"] = inventory_swap["receipt"]["receiptSha256"].clone();
    inventory_swap["outputInventorySha256"] =
        inventory_swap["receipt"]["outputInventorySha256"].clone();
    reseal(&mut inventory_swap, "resultSha256");
    let mut inventory_input = input.clone();
    refresh_bound(root.path(), &mut inventory_input, "result", &inventory_swap);
    assert!(
        !cli_extract(
            &write_input(root.path(), "inventory-swap.json", &inventory_input),
            &root.path().join("inventory-swap.jsonl"),
            &root.path().join("inventory-swap-receipt.json")
        )
        .status
        .success()
    );

    let canonical_input = write_input(root.path(), "canonical-chain.json", &input);
    let attempts = root.path().join("canonical.jsonl");
    let receipt_path = root.path().join("canonical-receipt.json");
    assert!(
        cli_extract(&canonical_input, &attempts, &receipt_path)
            .status
            .success()
    );
    let original_stream = fs::read(&attempts).unwrap();
    fs::write(&attempts, canonical_json_line(&rows[0]).unwrap()).unwrap();
    assert!(
        !cli_extract(&canonical_input, &attempts, &receipt_path)
            .status
            .success(),
        "divergent existing stream"
    );
    fs::write(&attempts, &original_stream).unwrap();
    let mut divergent_receipt: Value =
        serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
    divergent_receipt["proposalAccounting"]["proposalAttemptCount"] = json!(99);
    reseal(&mut divergent_receipt, "receiptSha256");
    fs::write(
        &receipt_path,
        canonical_json_line(&divergent_receipt).unwrap(),
    )
    .unwrap();
    assert!(
        !cli_extract(&canonical_input, &attempts, &receipt_path)
            .status
            .success(),
        "divergent existing receipt"
    );
}
