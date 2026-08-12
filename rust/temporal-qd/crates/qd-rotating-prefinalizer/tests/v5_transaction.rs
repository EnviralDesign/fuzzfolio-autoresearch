use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use serde_json::{Value, json};
use sha2::Digest;
use tempfile::TempDir;
use temporal_qd_contract::{CONTRACT_VERSION, canonical_json_line, canonical_sha256};
use temporal_qd_rotating_prefinalizer::{
    campaign_receipt::validate_receipt, execute_manifest, funnel_source,
};

fn sha(tag: &str) -> String {
    format!("sha256:{:0<64}", tag)
}
fn add(v: &mut Value, field: &str) {
    let h = canonical_sha256(v).unwrap();
    v.as_object_mut().unwrap().insert(field.into(), json!(h));
}
fn write(root: &Path, name: &str, v: &Value) -> Value {
    let p = root.join(name);
    let bytes = canonical_json_line(v).unwrap();
    fs::write(&p, &bytes).unwrap();
    json!({"path":p,"rawSha256":format!("sha256:{:x}",sha2::Sha256::digest(&bytes)),"sizeBytes":bytes.len()})
}
fn write_rows(root: &Path, name: &str, rows: &[Value]) -> Value {
    let p = root.join(name);
    let bytes = rows
        .iter()
        .flat_map(|r| canonical_json_line(r).unwrap())
        .collect::<Vec<_>>();
    fs::write(&p, &bytes).unwrap();
    json!({"path":p,"rawSha256":format!("sha256:{:x}",sha2::Sha256::digest(&bytes)),"sizeBytes":bytes.len(),"recordCount":rows.len()})
}
fn descriptor(root: &Path, name: &str) -> Value {
    write(root, name, &json!({"sidecar":name}))
}
fn invocation_descriptor(
    root: &Path,
    name: &str,
    schema: &str,
    hash_field: &str,
    extra: Option<Value>,
) -> Value {
    let mut document = json!({"schemaVersion":schema,"document":"fixture"});
    if let Some(extra) = extra {
        document.as_object_mut().unwrap().extend(
            extra
                .as_object()
                .expect("invocation fixture extra object")
                .clone(),
        );
    }
    add(&mut document, hash_field);
    let path = root.join(name);
    let bytes = canonical_json_line(&document).unwrap();
    fs::write(&path, &bytes).unwrap();
    json!({
        "schemaVersion":"temporal_qd_native_v5_invocation_document_descriptor_v1",
        "documentSchemaVersion":schema,
        "relativePath":name,
        "absolutePath":fs::canonicalize(&path).unwrap(),
        "semanticSha256":document[hash_field],
        "fileSha256":format!("sha256:{:x}", sha2::Sha256::digest(&bytes)),
        "byteLength":bytes.len(),
    })
}
fn funnel_pair(
    root: &Path,
    result: &Value,
    receipt_sha: &Value,
    inventory: &Value,
) -> (Value, Value) {
    let output = root.join("funnel-fixture");
    fs::create_dir_all(&output).unwrap();
    let script =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/build_v5_funnel_oracle_fixture.py");
    let python = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(4)
        .expect("repository root")
        .join(".venv/Scripts/python.exe");
    let status = Command::new(python)
        .arg(script)
        .arg(&output)
        .status()
        .expect("run native v5 funnel fixture");
    assert!(status.success());
    let mut input: Value =
        serde_json::from_slice(&fs::read(output.join("input.json")).unwrap()).unwrap();
    let receipt_path = PathBuf::from(
        input["proposalAttemptAuthority"]["receiptPath"]
            .as_str()
            .unwrap(),
    );
    let mut attempt: Value = serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
    attempt["proposalResultSha256"] = result.clone();
    attempt["proposalReceiptSha256"] = receipt_sha.clone();
    attempt["outputInventorySha256"] = inventory.clone();
    attempt.as_object_mut().unwrap().remove("receiptSha256");
    add(&mut attempt, "receiptSha256");
    let bytes = canonical_json_line(&attempt).unwrap();
    fs::write(&receipt_path, &bytes).unwrap();
    input["proposalAttemptAuthority"]["receiptFileSha256"] =
        json!(format!("sha256:{:x}", sha2::Sha256::digest(&bytes)));
    input["proposalAttemptAuthority"]["receiptSizeBytes"] = json!(bytes.len());
    input["proposalAttemptAuthority"]["receiptSha256"] = attempt["receiptSha256"].clone();
    input.as_object_mut().unwrap().remove("inputSha256");
    add(&mut input, "inputSha256");
    let source = funnel_source::assemble(&input).unwrap();
    (input, source)
}
fn candidate(id: &str) -> Value {
    json!({"candidateId":id,"candidateIdentitySha256":sha(&format!("id-{id}")),"programSha256":sha(&format!("program-{id}")),"profileSnapshotSha256":sha(&format!("profile-{id}")),"cellId":"cell","currentPanelRank":1.0})
}
fn rotating_sha() -> String {
    let windows = json!([{"windowId":"w","analysisWindowStart":"2024-01-01T00:00:00Z","analysisWindowEnd":"2024-02-01T00:00:00Z"}]);
    let mut rotating = json!({"schemaVersion":"temporal_qd_rotating_evidence_v1","panels":[{"panelId":"p1","windows":windows},{"panelId":"p2","windows":windows}],"absoluteGenerationMapping":{"cycleLength":2},"provisionalReduction":{"maxCandidates":4},"robustSelection":{"breederWidth":1}});
    add(&mut rotating, "rotatingEvidenceSha256");
    rotating["rotatingEvidenceSha256"]
        .as_str()
        .unwrap()
        .to_owned()
}
fn receipt(
    root: &Path,
    role: &str,
    panel: &str,
    members: Vec<Value>,
    bundles: Vec<Value>,
) -> Value {
    let members = members
        .into_iter()
        .map(|member| {
            if member.get("schemaVersion").is_some() {
                member
            } else {
                json!({"schemaVersion":"temporal_qd_evaluated_member_v1","candidate":member["candidate"].clone()})
            }
        })
        .collect::<Vec<_>>();
    let mem = write_rows(root, &format!("{role}-{panel}-members.jsonl"), &members);
    let bun = write_rows(root, &format!("{role}-{panel}-bundles.jsonl"), &bundles);
    let mut exec = serde_json::Map::new();
    for k in [
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
    ] {
        exec.insert(
            k.into(),
            descriptor(root, &format!("{role}-{panel}-{k}.json")),
        );
    }
    let mut md = mem.clone();
    md.as_object_mut().unwrap().remove("recordCount");
    exec.insert("evaluatedMembersJsonl".into(), md);
    let mut bd = bun.clone();
    bd.as_object_mut().unwrap().remove("recordCount");
    exec.insert("candidatePanelBundlesJsonl".into(), bd);
    let cohort = json!({"kind":"proposal_evaluation_population","sourceSemanticSha256":sha("proposal"),"candidateCount":members.len(),"selectionSha256":Value::Null});
    let freeze = json!({"transactionSha256":sha("freeze-t"),"cohortPopulationSha256":sha("pop"),"preparationSha256":sha("prep"),"authorityId":sha("auth"),"evaluationIdentitySha256":sha("eval"),"campaignSha256":sha("campaign"),"taskMatrixSha256":sha("matrix"),"candidateCount":members.len(),"windowCount":1,"taskCount":members.len()});
    let seal = json!({"directionalTailAuthoritySha256":sha("tailauth"),"campaignSealSha256":sha("seal"),"tailResultIndexSha256":sha("index"),"tailTransactionSha256":sha("tailtx")});
    let mut r = json!({"schemaVersion":"temporal_qd_v5_rotating_campaign_receipt_v2","contractVersion":CONTRACT_VERSION,"generationIndex":2,"campaignRole":role,"panelId":panel,"rotatingEvidenceSha256":rotating_sha(),"cohortSource":cohort,"campaignFreeze":freeze,"campaignSeal":seal,"evaluatedMembers":{"rawSha256":mem["rawSha256"],"sizeBytes":mem["sizeBytes"],"recordCount":mem["recordCount"],"rowSchema":"temporal_qd_evaluated_member_v1"},"candidatePanelBundles":{"rawSha256":bun["rawSha256"],"sizeBytes":bun["sizeBytes"],"recordCount":bun["recordCount"],"rowSchema":"temporal_qd_candidate_panel_evidence_bundle_v1"}});
    let semantic = canonical_sha256(&r).unwrap();
    r.as_object_mut()
        .unwrap()
        .insert("semanticReceiptSha256".into(), json!(semantic));
    r.as_object_mut()
        .unwrap()
        .insert("runtimeAuthoritySha256".into(), json!(sha("runtime")));
    r.as_object_mut()
        .unwrap()
        .insert("executionBindings".into(), Value::Object(exec));
    add(&mut r, "receiptSha256");
    r
}
fn bind_receipt_to_task(receipt: &mut Value, task_root: &Path, task: &Value) {
    let document: Value = serde_json::from_slice(
        &fs::read(task_root.join(task["selectionDocumentRelativePath"].as_str().unwrap())).unwrap(),
    )
    .unwrap();
    receipt["rotatingEvidenceSha256"] = task["rotatingEvidenceSha256"].clone();
    receipt["cohortSource"] = json!({"kind":"sealed_cohort_selection","sourceSemanticSha256":sha("task-source"),"candidateCount":task["candidateCount"],"selectionSha256":document["selectionDocumentSha256"]});
    let semantic = json!({"schemaVersion":receipt["schemaVersion"],"contractVersion":receipt["contractVersion"],"generationIndex":receipt["generationIndex"],"campaignRole":receipt["campaignRole"],"panelId":receipt["panelId"],"rotatingEvidenceSha256":receipt["rotatingEvidenceSha256"],"cohortSource":receipt["cohortSource"],"campaignFreeze":receipt["campaignFreeze"],"campaignSeal":receipt["campaignSeal"],"evaluatedMembers":receipt["evaluatedMembers"],"candidatePanelBundles":receipt["candidatePanelBundles"]});
    receipt["semanticReceiptSha256"] = json!(canonical_sha256(&semantic).unwrap());
    let mut full = semantic.as_object().unwrap().clone();
    full.insert(
        "semanticReceiptSha256".into(),
        receipt["semanticReceiptSha256"].clone(),
    );
    full.insert(
        "runtimeAuthoritySha256".into(),
        receipt["runtimeAuthoritySha256"].clone(),
    );
    full.insert(
        "executionBindings".into(),
        receipt["executionBindings"].clone(),
    );
    receipt["receiptSha256"] = json!(canonical_sha256(&Value::Object(full)).unwrap());
}
fn bind(root: &Path, name: &str, v: &Value) -> Value {
    write(root, name, v)
}
fn base(root: &Path, proposal: &Value, parent: &Value) -> Value {
    let finalizer_root = root.join("immutable-generation-finalizer");
    fs::create_dir_all(&finalizer_root).unwrap();
    let finalizer_root = fs::canonicalize(finalizer_root).unwrap();
    let windows = json!([{"windowId":"w","analysisWindowStart":"2024-01-01T00:00:00Z","analysisWindowEnd":"2024-02-01T00:00:00Z"}]);
    let mut rotating = json!({"schemaVersion":"temporal_qd_rotating_evidence_v1","panels":[{"panelId":"p1","windows":windows},{"panelId":"p2","windows":windows}],"absoluteGenerationMapping":{"cycleLength":2},"provisionalReduction":{"maxCandidates":4},"robustSelection":{"breederWidth":1}});
    add(&mut rotating, "rotatingEvidenceSha256");
    let policy_frozen = json!({"archive":{"defaultCellCapacity":1}});
    let mut policy = json!({"schemaVersion":"temporal_qd_archive_policy_binding_v1","policyName":"x","policySha256":sha("policy"),"frozenPolicy":policy_frozen});
    add(&mut policy, "policyBindingSha256");
    let completed = json!([]);
    let mut state = json!({"schemaVersion":"temporal_qd_v5_generation_state_basis_v1","configSha256":sha("config"),"generationIndex":2,"completedGenerationsSha256":canonical_sha256(&completed).unwrap(),"uniqueCandidatesEvaluated":0,"workerTasksCompleted":0,"nextImmigrantContinuationOrdinal":0,"uniqueIdentityCounts":{},"duplicateCounters":{},"proposalSlotCounters":{}});
    add(&mut state, "stateBasisSha256");
    let supervisor = json!({"supervisorConfigSha256":sha("super"),"generationConfigSha256":sha("gen"),"rotatingEvidence":rotating,"archivePolicy":policy});
    let invocation_manifest = invocation_descriptor(
        root,
        "proposal-manifest.json",
        "temporal_qd_native_v5_proposal_construction_manifest_v1",
        "manifestSha256",
        None,
    );
    let invocation_result = invocation_descriptor(
        root,
        "proposal-result.json",
        "temporal_qd_native_v5_evolved_construction_result_v3",
        "resultSha256",
        Some(json!({"manifestSha256":invocation_manifest["semanticSha256"]})),
    );
    let inventory = json!(sha("inventory"));
    let (funnel_input, funnel) = funnel_pair(
        root,
        &invocation_result["semanticSha256"],
        &proposal["semanticReceiptSha256"],
        &inventory,
    );
    let authority = json!({"generationKind":"evolved","proposalManifestSha256":invocation_manifest["semanticSha256"],"proposalReceiptSha256":proposal["semanticReceiptSha256"],"generationJournalSha256":sha("journal"),"inputIdentityLedgerSha256":sha("input-ledger"),"outputIdentityLedgerRelativePath":"proposal/v5-native/identity-ledger.json","outputIdentityLedgerSha256":sha("ledger"),"outputIdentityLedgerFileSha256":sha("ledger-file")});
    let construction = json!({"proposalSemanticRoots":{"root":sha("root"),"generationJournalSha256":sha("journal"),"proposalReceiptSha256":proposal["semanticReceiptSha256"]},"identityLedgerSha256":sha("ledger"),"proposalReceiptSha256":proposal["semanticReceiptSha256"],"inputIdentityLedgerSha256":sha("input-ledger"),"identityLedger":{"semanticSha256":sha("ledger"),"fileSha256":sha("ledger-file")},"nativeV5Invocation":{"schemaVersion":"temporal_qd_native_v5_evolved_invocation_descriptor_v1","proposalManifest":invocation_manifest,"proposalResult":invocation_result,"proposalReceiptSha256":proposal["semanticReceiptSha256"],"outputInventorySha256":inventory},"funnelReductionInput":funnel_input,"funnelReductionSource":funnel});
    let pb = bind(root, "proposal-receipt.json", proposal);
    let mut par = bind(root, "parent.json", parent);
    par.as_object_mut()
        .unwrap()
        .insert("archiveSha256".into(), parent["archiveSha256"].clone());
    let mut v = json!({"schemaVersion":"temporal_qd_v5_rotating_prefinalizer_manifest_v1","contractVersion":CONTRACT_VERSION,"operation":"prepare_native_v5_rotating_generation","generationIndex":2,"supervisorConfigBinding":supervisor,"stateBasis":state,"completedGenerationRecords":completed,"proposalStateAuthority":authority,"proposalConstructionBinding":construction,"previousParentArchiveBinding":par,"previousCumulativeArchiveBinding":Value::Null,"proposalCampaignReceiptBinding":pb,"finalizerOutputRoot":finalizer_root,"runtimeAuthoritySha256":sha("runtime")});
    let projection = json!({"schemaVersion":"temporal_qd_v5_rotating_prefinalizer_semantic_authority_v1","generationIndex":2,"supervisorConfigSha256":sha("super"),"generationConfigSha256":sha("gen"),"stateBasisSha256":v["stateBasis"]["stateBasisSha256"],"completedGenerationRecordsSha256":canonical_sha256(&v["completedGenerationRecords"]).unwrap(),"proposalStateAuthority":v["proposalStateAuthority"],"proposalSemanticRoots":v["proposalConstructionBinding"]["proposalSemanticRoots"],"identityLedgerSha256":sha("ledger"),"previousParentArchiveSha256":v["previousParentArchiveBinding"]["archiveSha256"],"previousCumulativeArchiveSha256":sha(""),"proposalCampaignSemanticReceiptSha256":proposal["semanticReceiptSha256"]});
    v.as_object_mut().unwrap().insert(
        "semanticAuthoritySha256".into(),
        json!(canonical_sha256(&projection).unwrap()),
    );
    add(&mut v, "manifestSha256");
    v
}
fn resume(root: &Path, base: &Value, previous: &Value, receipts: &[Value]) -> Value {
    let b = bind(root, "base.json", base);
    let p = bind(root, "previous.json", previous);
    let rs = receipts
        .iter()
        .enumerate()
        .map(|(i, r)| bind(root, &format!("receipt-{i}.json"), r))
        .collect::<Vec<_>>();
    let mut v = json!({"schemaVersion":"temporal_qd_v5_rotating_prefinalizer_resume_manifest_v1","contractVersion":CONTRACT_VERSION,"operation":"resume_native_v5_rotating_generation","baseManifestBinding":b,"roundIndex":previous["roundIndex"].as_u64().unwrap()+1,"previousResultBinding":p,"newCampaignReceiptBindings":rs,"runtimeAuthoritySha256":sha("runtime")});
    add(&mut v, "manifestSha256");
    v
}
fn bundles(ids: &[&str], panel: &str) -> Vec<Value> {
    ids.iter()
        .map(|id| {
            let candidate = candidate(id);
            let resolved_profile = sha(&format!("{id}-resolved-profile"));
            let resolved_program = sha(&format!("{id}-resolved-program"));
            let mut record = json!({
                "schemaVersion":"temporal_qd_candidate_window_evidence_v1",
                "candidateId":id,
                "panelId":panel,
                "candidateIdentitySha256":candidate["candidateIdentitySha256"],
                "programSha256":candidate["programSha256"],
                "metrics":{"sourceProfileSnapshotSha256":candidate["profileSnapshotSha256"],"resolvedProfileSnapshotSha256":resolved_profile,"resolvedProgramSha256":resolved_program},
                "windowId":"w",
            });
            add(&mut record, "recordSha256");
            let mut bundle = json!({
                "schemaVersion":"temporal_qd_candidate_panel_evidence_bundle_v1",
                "rotatingEvidenceSha256":rotating_sha(),
                "candidateId":id,
                "candidateIdentitySha256":candidate["candidateIdentitySha256"],
                "programSha256":candidate["programSha256"],
                "normalizedProfileSnapshotSha256":candidate["profileSnapshotSha256"],
                "panelId":panel,
                "windowEvidence":[record],
            });
            add(&mut bundle, "bundleSha256");
            bundle
        })
        .collect()
}
fn variant_bundle(id: &str, panel: &str) -> Value {
    let mut bundle = bundles(&[id], panel).pop().unwrap();
    bundle["windowEvidence"][0]["distinctEvidenceTag"] = json!("independently-sealed-current");
    bundle["windowEvidence"][0]
        .as_object_mut()
        .unwrap()
        .remove("recordSha256");
    add(&mut bundle["windowEvidence"][0], "recordSha256");
    bundle.as_object_mut().unwrap().remove("bundleSha256");
    add(&mut bundle, "bundleSha256");
    bundle
}
fn consume_task_cohort(root: &Path, result: &Value, expected: &[&str]) {
    let task = &result["taskPlan"]["tasks"][0];
    let descriptor = &task["cohortSelection"]["candidateRows"];
    assert_eq!(
        task["cohortSelection"]["schemaVersion"],
        "temporal_qd_v5_native_rich_candidate_selection_v2"
    );
    assert_eq!(
        descriptor["rowSchema"],
        "temporal_qd_selected_rich_candidate_v1"
    );
    let bytes = fs::read(root.join(descriptor["path"].as_str().unwrap())).unwrap();
    assert_eq!(
        descriptor["rawSha256"],
        format!("sha256:{:x}", sha2::Sha256::digest(&bytes))
    );
    let rows = bytes
        .split_inclusive(|byte| *byte == b'\n')
        .map(|line| serde_json::from_slice::<Value>(&line[..line.len() - 1]).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        rows.len(),
        descriptor["recordCount"].as_u64().unwrap() as usize
    );
    assert_eq!(
        rows.iter()
            .map(|row| row["candidateId"].as_str().unwrap())
            .collect::<Vec<_>>(),
        expected
    );
}

#[test]
fn v5_retained_parent_resume_backfill_ready_and_tamper_gates() {
    let root = TempDir::new().unwrap();
    let a = candidate("a");
    let p = candidate("p");
    let proposal = receipt(
        root.path(),
        "proposal_current_panel",
        "p2",
        vec![json!({"candidate":a})],
        bundles(&["a"], "p2"),
    );
    let mut parent = json!({"schemaVersion":"temporal_qd_archive_v3","candidateCountSeen":1,"memberCount":1,"cells":[{"cellId":"cell","members":[p]}]});
    add(&mut parent, "archiveSha256");
    let mut first = base(root.path(), &proposal, &parent);
    // G2 mixed coverage: the retained parent already has p1 in its sealed
    // cumulative archive, while the new proposal must still schedule p1.
    let mut cumulative = json!({
        "schemaVersion":"temporal_qd_cumulative_breeder_archive_v1",
        "generationIndex":1,
        "candidatePanelBundles":bundles(&["p"], "p1"),
    });
    add(&mut cumulative, "archiveSha256");
    let mut cumulative_binding = bind(root.path(), "g2-cumulative.json", &cumulative);
    cumulative_binding
        .as_object_mut()
        .unwrap()
        .insert("archiveSha256".into(), cumulative["archiveSha256"].clone());
    first["previousCumulativeArchiveBinding"] = cumulative_binding;
    resemantic(&mut first);
    let mut parent_descriptor_mismatch = first.clone();
    parent_descriptor_mismatch["previousParentArchiveBinding"]["archiveSha256"] =
        json!(sha("other-parent"));
    resemantic(&mut parent_descriptor_mismatch);
    assert!(
        execute_base_in(
            &root,
            "parent-descriptor-mismatch",
            &parent_descriptor_mismatch
        )
        .is_err()
    );
    let mut cumulative_descriptor_mismatch = first.clone();
    cumulative_descriptor_mismatch["previousCumulativeArchiveBinding"]["archiveSha256"] =
        json!(sha("other-cumulative"));
    resemantic(&mut cumulative_descriptor_mismatch);
    assert!(
        execute_base_in(
            &root,
            "cumulative-descriptor-mismatch",
            &cumulative_descriptor_mismatch
        )
        .is_err()
    );
    let d0 = root.path().join("r0");
    fs::create_dir(&d0).unwrap();
    fs::write(
        d0.join("manifest.json"),
        canonical_json_line(&first).unwrap(),
    )
    .unwrap();
    let r0 = execute_manifest(&d0.join("manifest.json")).unwrap()["result"].clone();
    assert_eq!(r0["status"], "awaiting_retained_parent_current_panel");
    consume_task_cohort(&d0, &r0, &["p"]);
    let task_selection = &r0["taskPlan"]["tasks"][0];
    let document_path = d0.join(
        task_selection["selectionDocumentRelativePath"]
            .as_str()
            .unwrap(),
    );
    let receipt_path = d0.join(
        task_selection["selectionReceiptRelativePath"]
            .as_str()
            .unwrap(),
    );
    assert!(document_path.is_file() && receipt_path.is_file());
    assert_eq!(
        serde_json::from_slice::<Value>(&fs::read(&document_path).unwrap()).unwrap()["schemaVersion"],
        "temporal_qd_v5_non_proposal_task_selection_v2"
    );
    assert_eq!(
        execute_manifest(&d0.join("manifest.json")).unwrap()["result"],
        r0
    );
    // A mutually self-rehashed result/task/source chain is still not an
    // authority: restart must rederive it from the sealed base manifest.
    let result_path = d0.join("result.json");
    let original_result_bytes = fs::read(&result_path).unwrap();
    let mut forged = r0.clone();
    forged["taskPlan"]["tasks"][0]["sourceAuthority"]["previousParentArchiveSha256"] =
        json!(sha("forged-parent-archive"));
    forged["taskPlan"]["tasks"][0]
        .as_object_mut()
        .unwrap()
        .remove("taskSha256");
    add(&mut forged["taskPlan"]["tasks"][0], "taskSha256");
    forged["taskPlan"]
        .as_object_mut()
        .unwrap()
        .remove("taskPlanSha256");
    add(&mut forged["taskPlan"], "taskPlanSha256");
    forged.as_object_mut().unwrap().remove("resultSha256");
    add(&mut forged, "resultSha256");
    fs::write(&result_path, canonical_json_line(&forged).unwrap()).unwrap();
    assert!(execute_manifest(&d0.join("manifest.json")).is_err());
    fs::write(&result_path, original_result_bytes).unwrap();
    assert_eq!(
        execute_manifest(&d0.join("manifest.json")).unwrap()["result"],
        r0
    );
    let mut parent_current = receipt(
        root.path(),
        "retained_parent_current_panel",
        "p2",
        vec![json!({"candidate":candidate("p")})],
        bundles(&["p"], "p2"),
    );
    bind_receipt_to_task(&mut parent_current, &d0, &r0["taskPlan"]["tasks"][0]);
    let d1 = root.path().join("r1");
    fs::create_dir(&d1).unwrap();
    fs::create_dir(d1.join("task-selections")).unwrap();
    fs::create_dir(d1.join("task-candidates")).unwrap();
    for name in [
        "round-0-task-0.selection.json",
        "round-0-task-0.receipt.json",
    ] {
        fs::copy(
            d0.join("task-selections").join(name),
            d1.join("task-selections").join(name),
        )
        .unwrap();
    }
    fs::copy(
        d0.join("task-candidates/round-0-task-0.jsonl"),
        d1.join("task-candidates/round-0-task-0.jsonl"),
    )
    .unwrap();
    let m1 = resume(&d1, &first, &r0, &[parent_current]);
    fs::write(d1.join("manifest.json"), canonical_json_line(&m1).unwrap()).unwrap();
    let r1 = execute_manifest(&d1.join("manifest.json")).unwrap()["result"].clone();
    assert_eq!(r1["status"], "awaiting_prior_panel_backfill");
    assert_eq!(r1["cohort"]["newProposalCandidateIds"], json!(["a"]));
    assert_eq!(
        r1["cohort"]["retainedParentEvaluationCandidateIds"],
        json!(["p"])
    );
    assert_eq!(r1["cohort"]["candidates"][0]["cohortRole"], "new_proposal");
    assert_eq!(
        r1["cohort"]["candidates"][1]["cohortRole"],
        "retained_parent_current_panel"
    );
    consume_task_cohort(&d1, &r1, &["a"]);
    let sidecar_path = d1.join(
        r1["taskPlan"]["tasks"][0]["cohortSelection"]["candidateRows"]["path"]
            .as_str()
            .unwrap(),
    );
    let sidecar_bytes = fs::read(&sidecar_path).unwrap();
    fs::remove_file(&sidecar_path).unwrap();
    assert!(execute_manifest(&d1.join("manifest.json")).is_err());
    fs::write(&sidecar_path, &sidecar_bytes).unwrap();
    let mut extra = sidecar_bytes.clone();
    extra.extend_from_slice(
        &sidecar_bytes[..sidecar_bytes
            .iter()
            .position(|byte| *byte == b'\n')
            .unwrap()
            + 1],
    );
    fs::write(&sidecar_path, &extra).unwrap();
    assert!(execute_manifest(&d1.join("manifest.json")).is_err());
    fs::write(&sidecar_path, &sidecar_bytes).unwrap();
    let mut wrong: Value = serde_json::from_slice(
        &sidecar_bytes[..sidecar_bytes
            .iter()
            .position(|byte| *byte == b'\n')
            .unwrap()],
    )
    .unwrap();
    wrong["candidateId"] = json!("substituted");
    let mut wrong_bytes = canonical_json_line(&wrong).unwrap();
    wrong_bytes.extend_from_slice(
        &sidecar_bytes[sidecar_bytes
            .iter()
            .position(|byte| *byte == b'\n')
            .unwrap()
            + 1..],
    );
    fs::write(&sidecar_path, &wrong_bytes).unwrap();
    assert!(execute_manifest(&d1.join("manifest.json")).is_err());
    fs::write(&sidecar_path, &sidecar_bytes).unwrap();
    let mut path_alias = r1.clone();
    let descriptor = &mut path_alias["taskPlan"]["tasks"][0]["cohortSelection"]["candidateRows"];
    descriptor["path"] = json!("task-candidates/alias.jsonl");
    descriptor
        .as_object_mut()
        .unwrap()
        .remove("descriptorSha256");
    add(descriptor, "descriptorSha256");
    path_alias["taskPlan"]["tasks"][0]
        .as_object_mut()
        .unwrap()
        .remove("taskSha256");
    add(&mut path_alias["taskPlan"]["tasks"][0], "taskSha256");
    path_alias["taskPlan"]
        .as_object_mut()
        .unwrap()
        .remove("taskPlanSha256");
    add(&mut path_alias["taskPlan"], "taskPlanSha256");
    path_alias.as_object_mut().unwrap().remove("resultSha256");
    add(&mut path_alias, "resultSha256");
    fs::write(
        d1.join("result.json"),
        canonical_json_line(&path_alias).unwrap(),
    )
    .unwrap();
    assert!(execute_manifest(&d1.join("manifest.json")).is_err());
    fs::write(d1.join("result.json"), canonical_json_line(&r1).unwrap()).unwrap();
    let mut backfill = receipt(
        root.path(),
        "prior_panel_backfill",
        "p1",
        vec![json!({
            "schemaVersion":"temporal_qd_evaluated_member_v1",
            "candidate":candidate("a"),
            "descriptor":{"cellId":"prior-panel-cell"},
            "aggregate":{"totalConservativeNetR":-7.0}
        })],
        bundles(&["a"], "p1"),
    );
    bind_receipt_to_task(&mut backfill, &d1, &r1["taskPlan"]["tasks"][0]);
    let d2 = root.path().join("r2");
    fs::create_dir(&d2).unwrap();
    fs::create_dir(d2.join("task-selections")).unwrap();
    fs::create_dir(d2.join("task-candidates")).unwrap();
    for name in [
        "round-1-task-0.selection.json",
        "round-1-task-0.receipt.json",
    ] {
        fs::copy(
            d1.join("task-selections").join(name),
            d2.join("task-selections").join(name),
        )
        .unwrap();
    }
    fs::copy(
        d1.join("task-candidates/round-1-task-0.jsonl"),
        d2.join("task-candidates/round-1-task-0.jsonl"),
    )
    .unwrap();
    let m2 = resume(&d2, &first, &r1, &[backfill]);
    fs::write(d2.join("manifest.json"), canonical_json_line(&m2).unwrap()).unwrap();
    let ready = execute_manifest(&d2.join("manifest.json")).unwrap()["result"].clone();
    assert_eq!(ready["status"], "ready_for_finalizer");
    assert_eq!(
        ready["admittedCampaignLedger"]["campaigns"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(ready["selectedRichMembers"]["memberCount"], 2);
    assert_eq!(
        ready["panelCoverage"]["coverage"]["a"]["panelIds"],
        json!(["p1", "p2"])
    );
    let fixed_finalizer_root = root.path().join("immutable-generation-finalizer");
    assert!(
        fixed_finalizer_root.join("source.json").is_file()
            && fixed_finalizer_root.join("manifest.json").is_file()
    );
    assert_eq!(
        ready["finalizerSource"]["path"],
        json!(fs::canonicalize(fixed_finalizer_root.join("source.json")).unwrap())
    );
    let ready_receipt: Value =
        serde_json::from_slice(&fs::read(d2.join("execution-receipt.json")).unwrap()).unwrap();
    assert_eq!(ready_receipt["status"], "ready_for_finalizer");
    assert_eq!(ready_receipt["taskCount"], 0);
    assert!(
        ready_receipt["taskSelections"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        ready_receipt["finalizerSource"]["path"],
        ready["finalizerSource"]["path"]
    );
    assert_eq!(
        ready_receipt["finalizerManifest"]["path"],
        ready["finalizerManifest"]["path"]
    );
    // Restart is input-free: delete raw evaluated members from a sealed campaign.
    let source: Value =
        serde_json::from_slice(&fs::read(fixed_finalizer_root.join("source.json")).unwrap())
            .unwrap();
    assert_eq!(
        source["schemaVersion"],
        "temporal_qd_generation_finalization_source_v2"
    );
    assert_eq!(source.as_object().unwrap().len(), 20);
    assert_eq!(
        source["completedGenerationRecords"],
        first["completedGenerationRecords"]
    );
    assert_eq!(
        source["proposalStateAuthority"],
        first["proposalStateAuthority"]
    );
    assert!(
        source["baselineCandidatePanelBundles"]
            .as_array()
            .unwrap()
            .iter()
            .any(|bundle| bundle["candidateId"] == "p" && bundle["panelId"] == "p1")
    );
    let rich_p = source["selectedRichMembers"]["members"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["candidateId"] == "p")
        .unwrap();
    assert_eq!(
        rich_p["candidateIdentitySha256"],
        rich_p["candidate"]["candidateIdentitySha256"]
    );
    assert_eq!(
        rich_p["programSha256"],
        rich_p["candidate"]["programSha256"]
    );
    assert_eq!(
        rich_p["profileSnapshotSha256"],
        rich_p["candidate"]["profileSnapshotSha256"]
    );
    let mut bad = m2.clone();
    bad.as_object_mut()
        .unwrap()
        .insert("unknown".into(), json!(1));
    assert!(execute_manifest(&write_manifest(&root, "bad.json", &bad)).is_err());
    let mut traversal = m2.clone();
    traversal["baseManifestBinding"]["path"] = json!("../base.json");
    traversal.as_object_mut().unwrap().remove("manifestSha256");
    add(&mut traversal, "manifestSha256");
    assert!(execute_manifest(&write_manifest(&root, "traversal.json", &traversal)).is_err());
    let mut relabel = proposal.clone();
    relabel["campaignRole"] = json!("prior_panel_backfill");
    assert!(validate_receipt(&relabel).is_err());
    let mut old = proposal.clone();
    old["generationIndex"] = json!(1);
    assert!(validate_receipt(&old).is_err());
    let duplicate_dir = root.path().join("duplicate");
    fs::create_dir(&duplicate_dir).unwrap();
    let duplicate = resume(
        &duplicate_dir,
        &first,
        &r0,
        &[proposal.clone(), proposal.clone()],
    );
    fs::write(
        duplicate_dir.join("manifest.json"),
        canonical_json_line(&duplicate).unwrap(),
    )
    .unwrap();
    assert!(execute_manifest(&duplicate_dir.join("manifest.json")).is_err());
    let extra_dir = root.path().join("extra-bundle");
    fs::create_dir(&extra_dir).unwrap();
    let extra = receipt(
        root.path(),
        "prior_panel_backfill",
        "p1",
        vec![
            json!({"candidate":candidate("a")}),
            json!({"candidate":candidate("p")}),
        ],
        bundles(&["a", "p", "not-in-cohort"], "p1"),
    );
    let extra_manifest = resume(&extra_dir, &first, &r1, &[extra]);
    fs::write(
        extra_dir.join("manifest.json"),
        canonical_json_line(&extra_manifest).unwrap(),
    )
    .unwrap();
    assert!(execute_manifest(&extra_dir.join("manifest.json")).is_err());
    let drift_dir = root.path().join("drift");
    fs::create_dir(&drift_dir).unwrap();
    let mut state_drift = first.clone();
    state_drift["stateBasis"]["workerTasksCompleted"] = json!(1);
    state_drift["stateBasis"]
        .as_object_mut()
        .unwrap()
        .remove("stateBasisSha256");
    add(&mut state_drift["stateBasis"], "stateBasisSha256");
    state_drift
        .as_object_mut()
        .unwrap()
        .remove("manifestSha256");
    add(&mut state_drift, "manifestSha256");
    fs::write(
        drift_dir.join("manifest.json"),
        canonical_json_line(&state_drift).unwrap(),
    )
    .unwrap();
    assert!(execute_manifest(&drift_dir.join("manifest.json")).is_err());
    // Bound archive bytes are reopened, not trusted from their manifest hash.
    let parent_path = first["previousParentArchiveBinding"]["path"]
        .as_str()
        .unwrap();
    let original_parent = fs::read(parent_path).unwrap();
    fs::write(parent_path, b"{\"tampered\":true}\n").unwrap();
    let parent_tamper = root.path().join("parent-tamper");
    fs::create_dir(&parent_tamper).unwrap();
    fs::write(
        parent_tamper.join("manifest.json"),
        canonical_json_line(&first).unwrap(),
    )
    .unwrap();
    assert!(execute_manifest(&parent_tamper.join("manifest.json")).is_err());
    fs::write(parent_path, original_parent).unwrap();
    // An incomplete backfill remains awaiting rather than being promoted.
    let missing_dir = root.path().join("missing");
    fs::create_dir(&missing_dir).unwrap();
    let incomplete = receipt(
        root.path(),
        "prior_panel_backfill",
        "p1",
        vec![json!({"candidate":candidate("a")})],
        bundles(&["a"], "p1"),
    );
    let incomplete_manifest = resume(&missing_dir, &first, &r1, &[incomplete]);
    fs::write(
        missing_dir.join("manifest.json"),
        canonical_json_line(&incomplete_manifest).unwrap(),
    )
    .unwrap();
    assert!(execute_manifest(&missing_dir.join("manifest.json")).is_err());
    // Moving an operational receipt file does not change the derived semantic authority.
    let op_dir = root.path().join("operational");
    fs::create_dir(&op_dir).unwrap();
    let copied = op_dir.join("proposal-copy.json");
    fs::copy(root.path().join("proposal-receipt.json"), &copied).unwrap();
    let mut operational = first.clone();
    let bytes = fs::read(&copied).unwrap();
    operational["proposalCampaignReceiptBinding"] = json!({"path":copied,"rawSha256":format!("sha256:{:x}",sha2::Sha256::digest(&bytes)),"sizeBytes":bytes.len()});
    operational
        .as_object_mut()
        .unwrap()
        .remove("manifestSha256");
    add(&mut operational, "manifestSha256");
    fs::write(
        op_dir.join("manifest.json"),
        canonical_json_line(&operational).unwrap(),
    )
    .unwrap();
    let op = execute_manifest(&op_dir.join("manifest.json")).unwrap()["result"].clone();
    assert_eq!(op["semanticAuthoritySha256"], r0["semanticAuthoritySha256"]);
    let mut cumulative =
        json!({"schemaVersion":"temporal_qd_cumulative_breeder_archive_v1","generationIndex":1});
    add(&mut cumulative, "archiveSha256");
    let mut cumulative_binding = write(root.path(), "cumulative.json", &cumulative);
    cumulative_binding
        .as_object_mut()
        .unwrap()
        .insert("archiveSha256".into(), cumulative["archiveSha256"].clone());
    let mut with_cumulative = first.clone();
    with_cumulative["previousCumulativeArchiveBinding"] = cumulative_binding.clone();
    resemantic(&mut with_cumulative);
    assert_ne!(
        with_cumulative["semanticAuthoritySha256"],
        first["semanticAuthoritySha256"]
    );
    let cumulative_dir = root.path().join("cumulative");
    fs::create_dir(&cumulative_dir).unwrap();
    fs::write(
        cumulative_dir.join("manifest.json"),
        canonical_json_line(&with_cumulative).unwrap(),
    )
    .unwrap();
    assert_eq!(
        execute_manifest(&cumulative_dir.join("manifest.json")).unwrap()["result"]["status"],
        "awaiting_retained_parent_current_panel"
    );
    let cumulative_path = cumulative_binding["path"].as_str().unwrap();
    let original = fs::read(cumulative_path).unwrap();
    fs::write(cumulative_path, b"{\"tampered\":true}\n").unwrap();
    let cumulative_tamper = root.path().join("cumulative-tamper");
    fs::create_dir(&cumulative_tamper).unwrap();
    fs::write(
        cumulative_tamper.join("manifest.json"),
        canonical_json_line(&with_cumulative).unwrap(),
    )
    .unwrap();
    assert!(execute_manifest(&cumulative_tamper.join("manifest.json")).is_err());
    fs::write(cumulative_path, original).unwrap();
    let mut changed = first.clone();
    changed["proposalConstructionBinding"]["proposalSemanticRoots"]["semanticCap"] = json!(2);
    resemantic(&mut changed);
    assert_ne!(
        changed["semanticAuthoritySha256"],
        first["semanticAuthoritySha256"]
    );
}
fn write_manifest(root: &TempDir, name: &str, v: &Value) -> PathBuf {
    let p = root.path().join(name);
    fs::write(&p, canonical_json_line(v).unwrap()).unwrap();
    p
}

#[test]
fn v5_finalizer_source_excludes_authenticated_non_survivor_bundles() {
    let root = TempDir::new().unwrap();
    let ids = ["a", "b", "c", "d", "e"];
    let proposal = receipt(
        root.path(),
        "proposal_current_panel",
        "p2",
        ids.iter()
            .map(|id| json!({"candidate":candidate(id)}))
            .collect(),
        bundles(&ids, "p2"),
    );
    let mut parent = json!({"schemaVersion":"temporal_qd_archive_v3","candidateCountSeen":0,"memberCount":0,"cells":[]});
    add(&mut parent, "archiveSha256");
    let base_manifest = base(root.path(), &proposal, &parent);

    let round0 = root.path().join("survivor-round-0");
    fs::create_dir(&round0).unwrap();
    fs::write(
        round0.join("manifest.json"),
        canonical_json_line(&base_manifest).unwrap(),
    )
    .unwrap();
    let first = execute_manifest(&round0.join("manifest.json")).unwrap()["result"].clone();
    assert_eq!(first["status"], "awaiting_prior_panel_backfill");
    let selected = first["provisional"]["candidates"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["candidateId"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(selected.len(), 4);
    let selected_refs = selected.iter().map(String::as_str).collect::<Vec<_>>();

    let task = &first["taskPlan"]["tasks"][0];
    let mut backfill = receipt(
        root.path(),
        "prior_panel_backfill",
        "p1",
        selected
            .iter()
            .map(|id| json!({"candidate":candidate(id)}))
            .collect(),
        bundles(&selected_refs, "p1"),
    );
    bind_receipt_to_task(&mut backfill, &round0, task);

    let round1 = root.path().join("survivor-round-1");
    fs::create_dir(&round1).unwrap();
    for relative in [
        task["selectionDocumentRelativePath"].as_str().unwrap(),
        task["selectionReceiptRelativePath"].as_str().unwrap(),
        task["cohortSelection"]["candidateRows"]["path"]
            .as_str()
            .unwrap(),
    ] {
        let source = round0.join(relative);
        let destination = round1.join(relative);
        fs::create_dir_all(destination.parent().unwrap()).unwrap();
        fs::copy(source, destination).unwrap();
    }
    let resumed = resume(&round1, &base_manifest, &first, &[backfill]);
    fs::write(
        round1.join("manifest.json"),
        canonical_json_line(&resumed).unwrap(),
    )
    .unwrap();
    let ready = execute_manifest(&round1.join("manifest.json")).unwrap()["result"].clone();
    assert_eq!(ready["status"], "ready_for_finalizer");

    let source: Value = serde_json::from_slice(
        &fs::read(
            root.path()
                .join("immutable-generation-finalizer/source.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let final_bundles = source["baselineCandidatePanelBundles"].as_array().unwrap();
    assert_eq!(final_bundles.len(), selected.len() * 2);
    assert!(final_bundles.iter().all(|bundle| {
        selected
            .iter()
            .any(|id| bundle["candidateId"].as_str() == Some(id.as_str()))
    }));
    let excluded = ids
        .iter()
        .find(|id| !selected.iter().any(|selected_id| selected_id == **id))
        .unwrap();
    assert!(
        final_bundles
            .iter()
            .all(|bundle| bundle["candidateId"].as_str() != Some(excluded))
    );
}

#[test]
fn v5_rejects_conflicting_current_bundle_over_retained_cumulative_bundle() {
    let root = TempDir::new().unwrap();
    let proposal = receipt(
        root.path(),
        "proposal_current_panel",
        "p2",
        vec![json!({"candidate":candidate("a")})],
        bundles(&["a"], "p2"),
    );
    let mut parent = json!({"schemaVersion":"temporal_qd_archive_v3","candidateCountSeen":1,"memberCount":1,"cells":[{"cellId":"cell","members":[candidate("p")]}]});
    add(&mut parent, "archiveSha256");
    let mut base_value = base(root.path(), &proposal, &parent);
    let mut cumulative = json!({"schemaVersion":"temporal_qd_cumulative_breeder_archive_v1","generationIndex":1,"candidatePanelBundles":[variant_bundle("p", "p2")]});
    add(&mut cumulative, "archiveSha256");
    let mut cumulative_binding = bind(root.path(), "conflict-cumulative.json", &cumulative);
    cumulative_binding
        .as_object_mut()
        .unwrap()
        .insert("archiveSha256".into(), cumulative["archiveSha256"].clone());
    base_value["previousCumulativeArchiveBinding"] = cumulative_binding;
    resemantic(&mut base_value);
    let r0_dir = root.path().join("conflict-r0");
    fs::create_dir(&r0_dir).unwrap();
    fs::write(
        r0_dir.join("manifest.json"),
        canonical_json_line(&base_value).unwrap(),
    )
    .unwrap();
    let r0 = execute_manifest(&r0_dir.join("manifest.json")).unwrap()["result"].clone();
    let mut current = receipt(
        root.path(),
        "retained_parent_current_panel",
        "p2",
        vec![json!({"candidate":candidate("p")})],
        bundles(&["p"], "p2"),
    );
    bind_receipt_to_task(&mut current, &r0_dir, &r0["taskPlan"]["tasks"][0]);
    let r1_dir = root.path().join("conflict-r1");
    fs::create_dir(&r1_dir).unwrap();
    fs::create_dir(r1_dir.join("task-selections")).unwrap();
    fs::create_dir(r1_dir.join("task-candidates")).unwrap();
    for name in [
        "round-0-task-0.selection.json",
        "round-0-task-0.receipt.json",
    ] {
        fs::copy(
            r0_dir.join("task-selections").join(name),
            r1_dir.join("task-selections").join(name),
        )
        .unwrap();
    }
    fs::copy(
        r0_dir.join("task-candidates/round-0-task-0.jsonl"),
        r1_dir.join("task-candidates/round-0-task-0.jsonl"),
    )
    .unwrap();
    let manifest = resume(&r1_dir, &base_value, &r0, &[current]);
    fs::write(
        r1_dir.join("manifest.json"),
        canonical_json_line(&manifest).unwrap(),
    )
    .unwrap();
    assert!(execute_manifest(&r1_dir.join("manifest.json")).is_err());
    assert!(!r1_dir.join("result.json").exists());
    assert!(
        !root
            .path()
            .join("immutable-generation-finalizer/source.json")
            .exists()
    );
}

#[test]
fn v5_rejects_two_panel_resolved_identity_drift_before_ready() {
    let root = TempDir::new().unwrap();
    let proposal = receipt(
        root.path(),
        "proposal_current_panel",
        "p2",
        vec![json!({"candidate":candidate("a")})],
        bundles(&["a"], "p2"),
    );
    let mut parent = json!({"schemaVersion":"temporal_qd_archive_v3","candidateCountSeen":1,"memberCount":1,"cells":[{"cellId":"cell","members":[candidate("p")]}]});
    add(&mut parent, "archiveSha256");
    let mut base_value = base(root.path(), &proposal, &parent);
    let mut cumulative = json!({"schemaVersion":"temporal_qd_cumulative_breeder_archive_v1","generationIndex":1,"candidatePanelBundles":bundles(&["p"], "p1")});
    add(&mut cumulative, "archiveSha256");
    let mut binding = bind(root.path(), "resolved-cumulative.json", &cumulative);
    binding
        .as_object_mut()
        .unwrap()
        .insert("archiveSha256".into(), cumulative["archiveSha256"].clone());
    base_value["previousCumulativeArchiveBinding"] = binding;
    resemantic(&mut base_value);
    let d0 = root.path().join("resolved-r0");
    fs::create_dir(&d0).unwrap();
    fs::write(
        d0.join("manifest.json"),
        canonical_json_line(&base_value).unwrap(),
    )
    .unwrap();
    let r0 = execute_manifest(&d0.join("manifest.json")).unwrap()["result"].clone();
    let mut drift = bundles(&["p"], "p2").pop().unwrap();
    drift["windowEvidence"][0]["metrics"]["resolvedProfileSnapshotSha256"] =
        json!(sha("other-profile"));
    drift["windowEvidence"][0]["metrics"]["resolvedProgramSha256"] = json!(sha("other-program"));
    drift["windowEvidence"][0]
        .as_object_mut()
        .unwrap()
        .remove("recordSha256");
    add(&mut drift["windowEvidence"][0], "recordSha256");
    drift.as_object_mut().unwrap().remove("bundleSha256");
    add(&mut drift, "bundleSha256");
    let mut current = receipt(
        root.path(),
        "retained_parent_current_panel",
        "p2",
        vec![json!({"candidate":candidate("p")})],
        vec![drift],
    );
    bind_receipt_to_task(&mut current, &d0, &r0["taskPlan"]["tasks"][0]);
    let d1 = root.path().join("resolved-r1");
    fs::create_dir(&d1).unwrap();
    fs::create_dir(d1.join("task-selections")).unwrap();
    fs::create_dir(d1.join("task-candidates")).unwrap();
    for name in [
        "round-0-task-0.selection.json",
        "round-0-task-0.receipt.json",
    ] {
        fs::copy(
            d0.join("task-selections").join(name),
            d1.join("task-selections").join(name),
        )
        .unwrap();
    }
    fs::copy(
        d0.join("task-candidates/round-0-task-0.jsonl"),
        d1.join("task-candidates/round-0-task-0.jsonl"),
    )
    .unwrap();
    let manifest = resume(&d1, &base_value, &r0, &[current]);
    fs::write(
        d1.join("manifest.json"),
        canonical_json_line(&manifest).unwrap(),
    )
    .unwrap();
    assert!(execute_manifest(&d1.join("manifest.json")).is_err());
    assert!(!d1.join("result.json").exists());
}

fn resemantic(v: &mut Value) {
    let receipt: Value = serde_json::from_slice(
        &fs::read(
            v["proposalCampaignReceiptBinding"]["path"]
                .as_str()
                .unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    let cumulative = if v["previousCumulativeArchiveBinding"].is_null() {
        sha("")
    } else {
        v["previousCumulativeArchiveBinding"]["archiveSha256"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let projection = json!({"schemaVersion":"temporal_qd_v5_rotating_prefinalizer_semantic_authority_v1","generationIndex":v["generationIndex"],"supervisorConfigSha256":v["supervisorConfigBinding"]["supervisorConfigSha256"],"generationConfigSha256":v["supervisorConfigBinding"]["generationConfigSha256"],"stateBasisSha256":v["stateBasis"]["stateBasisSha256"],"completedGenerationRecordsSha256":canonical_sha256(&v["completedGenerationRecords"]).unwrap(),"proposalStateAuthority":v["proposalStateAuthority"],"proposalSemanticRoots":v["proposalConstructionBinding"]["proposalSemanticRoots"],"identityLedgerSha256":v["proposalConstructionBinding"]["identityLedgerSha256"],"previousParentArchiveSha256":v["previousParentArchiveBinding"]["archiveSha256"],"previousCumulativeArchiveSha256":cumulative,"proposalCampaignSemanticReceiptSha256":receipt["semanticReceiptSha256"]});
    v["semanticAuthoritySha256"] = json!(canonical_sha256(&projection).unwrap());
    v.as_object_mut().unwrap().remove("manifestSha256");
    add(v, "manifestSha256");
}

fn rehash_state_and_base(v: &mut Value) {
    v["stateBasis"]["completedGenerationsSha256"] =
        json!(canonical_sha256(&v["completedGenerationRecords"]).unwrap());
    v["stateBasis"]
        .as_object_mut()
        .unwrap()
        .remove("stateBasisSha256");
    add(&mut v["stateBasis"], "stateBasisSha256");
    resemantic(v);
}

#[test]
fn v5_compact_execution_receipt_bounds_stdout_and_closes_committed_restart() {
    let root = TempDir::new().unwrap();
    let ids = (0..128)
        .map(|i| format!("candidate-{i:03}"))
        .collect::<Vec<_>>();
    let members = ids
        .iter()
        .map(|id| json!({"candidate":candidate(id)}))
        .collect::<Vec<_>>();
    let id_refs = ids.iter().map(String::as_str).collect::<Vec<_>>();
    let proposal = receipt(
        root.path(),
        "proposal_current_panel",
        "p2",
        members,
        bundles(&id_refs, "p2"),
    );
    let mut parent = json!({"schemaVersion":"temporal_qd_archive_v3","candidateCountSeen":0,"memberCount":0,"cells":[]});
    add(&mut parent, "archiveSha256");
    let manifest = base(root.path(), &proposal, &parent);

    let committed = root.path().join("compact-awaiting");
    fs::create_dir(&committed).unwrap();
    let manifest_path = committed.join("manifest.json");
    fs::write(&manifest_path, canonical_json_line(&manifest).unwrap()).unwrap();
    let precommit = root.path().join("precommit-missing-source");
    fs::create_dir(&precommit).unwrap();
    fs::write(
        precommit.join("manifest.json"),
        canonical_json_line(&manifest).unwrap(),
    )
    .unwrap();

    let first = Command::new(env!("CARGO_BIN_EXE_temporal-qd-rotating-prefinalizer"))
        .arg(&manifest_path)
        .output()
        .unwrap();
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert!(
        first.stdout.len() < 16 * 1024,
        "compact stdout grew with candidates"
    );
    let execution: Value = serde_json::from_slice(&first.stdout).unwrap();
    assert_eq!(
        execution["schemaVersion"],
        "temporal_qd_v5_rotating_prefinalizer_execution_v2"
    );
    assert_eq!(execution["restart"], false);
    assert!(execution.get("result").is_none());
    let compact = &execution["receipt"];
    assert_eq!(
        compact["schemaVersion"],
        "temporal_qd_v5_rotating_prefinalizer_execution_receipt_v2"
    );
    assert_eq!(compact["status"], "awaiting_prior_panel_backfill");
    assert_eq!(compact["taskCount"], 1);
    assert_eq!(compact["taskSelections"][0]["candidateCount"], 4);
    assert!(
        fs::metadata(committed.join("execution-receipt.json"))
            .unwrap()
            .len()
            < 16 * 1024
    );
    assert!(fs::metadata(committed.join("result.json")).unwrap().len() > first.stdout.len() as u64);

    let selection_receipt = PathBuf::from(
        compact["taskSelections"][0]["selectionReceipt"]["path"]
            .as_str()
            .unwrap(),
    );
    let execution_receipt_path = committed.join("execution-receipt.json");
    let execution_receipt_bytes = fs::read(&execution_receipt_path).unwrap();
    let mut self_rehashed = compact.clone();
    self_rehashed["taskSelections"][0]["candidateCount"] = json!(3);
    self_rehashed
        .as_object_mut()
        .unwrap()
        .remove("receiptSha256");
    add(&mut self_rehashed, "receiptSha256");
    fs::write(
        &execution_receipt_path,
        canonical_json_line(&self_rehashed).unwrap(),
    )
    .unwrap();
    let forged = Command::new(env!("CARGO_BIN_EXE_temporal-qd-rotating-prefinalizer"))
        .arg(&manifest_path)
        .output()
        .unwrap();
    assert!(!forged.status.success());
    fs::write(&execution_receipt_path, &execution_receipt_bytes).unwrap();

    let mut traversal = compact.clone();
    traversal["internalResult"]["path"] = json!("../result.json");
    traversal.as_object_mut().unwrap().remove("receiptSha256");
    add(&mut traversal, "receiptSha256");
    fs::write(
        &execution_receipt_path,
        canonical_json_line(&traversal).unwrap(),
    )
    .unwrap();
    let traversed = Command::new(env!("CARGO_BIN_EXE_temporal-qd-rotating-prefinalizer"))
        .arg(&manifest_path)
        .output()
        .unwrap();
    assert!(!traversed.status.success());
    fs::write(&execution_receipt_path, &execution_receipt_bytes).unwrap();

    let internal_result: Value = serde_json::from_slice(
        &fs::read(compact["internalResult"]["path"].as_str().unwrap()).unwrap(),
    )
    .unwrap();
    let candidate_sidecar = committed.join(
        internal_result["taskPlan"]["tasks"][0]["cohortSelection"]["candidateRows"]["path"]
            .as_str()
            .unwrap(),
    );
    let sidecar_bytes = fs::read(&candidate_sidecar).unwrap();
    let mut sidecar_lines = sidecar_bytes
        .split_inclusive(|byte| *byte == b'\n')
        .map(|line| line.to_vec())
        .collect::<Vec<_>>();
    sidecar_lines.reverse();
    fs::write(&candidate_sidecar, sidecar_lines.concat()).unwrap();
    let reordered = Command::new(env!("CARGO_BIN_EXE_temporal-qd-rotating-prefinalizer"))
        .arg(&manifest_path)
        .output()
        .unwrap();
    assert!(!reordered.status.success());
    fs::write(&candidate_sidecar, &sidecar_bytes).unwrap();

    let selection_bytes = fs::read(&selection_receipt).unwrap();
    fs::write(&selection_receipt, b"{}\n").unwrap();
    let tampered = Command::new(env!("CARGO_BIN_EXE_temporal-qd-rotating-prefinalizer"))
        .arg(&manifest_path)
        .output()
        .unwrap();
    assert!(!tampered.status.success());
    fs::write(&selection_receipt, selection_bytes).unwrap();

    let proposal_receipt_path = PathBuf::from(
        manifest["proposalCampaignReceiptBinding"]["path"]
            .as_str()
            .unwrap(),
    );
    let funnel_attempt_path = PathBuf::from(
        manifest["proposalConstructionBinding"]["funnelReductionInput"]["proposalAttemptAuthority"]
            ["receiptPath"]
            .as_str()
            .unwrap(),
    );
    fs::remove_file(proposal_receipt_path).unwrap();
    fs::remove_file(funnel_attempt_path).unwrap();

    let restart = Command::new(env!("CARGO_BIN_EXE_temporal-qd-rotating-prefinalizer"))
        .arg(&manifest_path)
        .output()
        .unwrap();
    assert!(
        restart.status.success(),
        "{}",
        String::from_utf8_lossy(&restart.stderr)
    );
    let restarted: Value = serde_json::from_slice(&restart.stdout).unwrap();
    assert_eq!(restarted["restart"], true);
    assert_eq!(restarted["receipt"], compact.clone());

    let rejected = Command::new(env!("CARGO_BIN_EXE_temporal-qd-rotating-prefinalizer"))
        .arg(precommit.join("manifest.json"))
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert!(!precommit.join("execution-receipt.json").exists());
}

#[test]
fn v5_base_manifest_reopens_funnel_assembly_receipt_and_reassembles_source() {
    let root = TempDir::new().unwrap();
    let proposal = receipt(
        root.path(),
        "proposal_current_panel",
        "p2",
        vec![json!({"candidate":candidate("a")})],
        bundles(&["a"], "p2"),
    );
    let mut parent = json!({"schemaVersion":"temporal_qd_archive_v3","candidateCountSeen":0,"memberCount":0,"cells":[]});
    add(&mut parent, "archiveSha256");
    let mut manifest = base(root.path(), &proposal, &parent);
    let assembly_root = root.path().join("current-funnel-assembly");
    fs::create_dir(&assembly_root).unwrap();
    let input_path = assembly_root.join("input.json");
    let source_path = assembly_root.join("source.json");
    let input = manifest["proposalConstructionBinding"]["funnelReductionInput"].clone();
    fs::write(&input_path, canonical_json_line(&input).unwrap()).unwrap();
    let execution = funnel_source::assemble_to_path_compact(&input_path, &source_path).unwrap();
    let receipt_path = assembly_root.join("funnel-assembly-receipt.json");
    let receipt_bytes = fs::read(&receipt_path).unwrap();
    manifest["schemaVersion"] = json!("temporal_qd_v5_rotating_prefinalizer_manifest_v2");
    manifest.as_object_mut().unwrap().remove("manifestSha256");
    add(&mut manifest, "manifestSha256");
    let inline_v2_root = root.path().join("inline-v2-rejected");
    fs::create_dir(&inline_v2_root).unwrap();
    fs::write(
        inline_v2_root.join("manifest.json"),
        canonical_json_line(&manifest).unwrap(),
    )
    .unwrap();
    assert!(execute_manifest(&inline_v2_root.join("manifest.json")).is_err());
    manifest["proposalConstructionBinding"]
        .as_object_mut()
        .unwrap()
        .remove("funnelReductionSource");
    manifest["proposalConstructionBinding"]["funnelAssemblyReceiptBinding"] = json!({
        "schemaVersion":"temporal_qd_v5_native_funnel_assembly_receipt_binding_v1",
        "path":fs::canonicalize(&receipt_path).unwrap(),
        "rawSha256":format!("sha256:{:x}",sha2::Sha256::digest(&receipt_bytes)),
        "sizeBytes":receipt_bytes.len(),
        "receiptSha256":execution["receipt"]["receiptSha256"],
    });
    manifest.as_object_mut().unwrap().remove("manifestSha256");
    add(&mut manifest, "manifestSha256");
    let output_root = root.path().join("receipt-bound-base");
    fs::create_dir(&output_root).unwrap();
    let manifest_path = output_root.join("manifest.json");
    fs::write(&manifest_path, canonical_json_line(&manifest).unwrap()).unwrap();
    assert!(execute_manifest(&manifest_path).is_ok());

    let source_bytes = fs::read(&source_path).unwrap();
    fs::write(&source_path, b"{}\n").unwrap();
    let tampered_root = root.path().join("receipt-bound-tampered");
    fs::create_dir(&tampered_root).unwrap();
    fs::write(
        tampered_root.join("manifest.json"),
        canonical_json_line(&manifest).unwrap(),
    )
    .unwrap();
    assert!(execute_manifest(&tampered_root.join("manifest.json")).is_err());
    fs::write(source_path, source_bytes).unwrap();
}

fn execute_base_in(root: &TempDir, name: &str, manifest: &Value) -> anyhow::Result<Value> {
    let dir = root.path().join(name);
    fs::create_dir(&dir).unwrap();
    let path = dir.join("manifest.json");
    fs::write(&path, canonical_json_line(manifest).unwrap()).unwrap();
    execute_manifest(&path)
}

#[test]
fn v5_source_v2_completed_records_and_proposal_state_authority_gates() {
    let root = TempDir::new().unwrap();
    let proposal = receipt(
        root.path(),
        "proposal_current_panel",
        "p2",
        vec![json!({"candidate":candidate("a")})],
        bundles(&["a"], "p2"),
    );
    let mut parent = json!({"schemaVersion":"temporal_qd_archive_v3","candidateCountSeen":0,"memberCount":0,"cells":[]});
    add(&mut parent, "archiveSha256");
    let evolved = base(root.path(), &proposal, &parent);
    assert!(execute_base_in(&root, "evolved", &evolved).is_ok());

    // A wholly self-consistent same-generation evolved attempt receipt is
    // still foreign when its proposal roots differ from this invocation.
    let mut foreign_attempt = evolved.clone();
    let input = &mut foreign_attempt["proposalConstructionBinding"]["funnelReductionInput"];
    let path = PathBuf::from(
        input["proposalAttemptAuthority"]["receiptPath"]
            .as_str()
            .unwrap(),
    );
    let original_attempt = fs::read(&path).unwrap();
    let mut attempt: Value = serde_json::from_slice(&original_attempt).unwrap();
    attempt["proposalResultSha256"] = json!(sha("foreign-result"));
    attempt["proposalReceiptSha256"] = json!(sha("foreign-receipt"));
    attempt["outputInventorySha256"] = json!(sha("foreign-inventory"));
    attempt.as_object_mut().unwrap().remove("receiptSha256");
    add(&mut attempt, "receiptSha256");
    let bytes = canonical_json_line(&attempt).unwrap();
    fs::write(&path, &bytes).unwrap();
    input["proposalAttemptAuthority"]["receiptFileSha256"] =
        json!(format!("sha256:{:x}", sha2::Sha256::digest(&bytes)));
    input["proposalAttemptAuthority"]["receiptSizeBytes"] = json!(bytes.len());
    input["proposalAttemptAuthority"]["receiptSha256"] = attempt["receiptSha256"].clone();
    input.as_object_mut().unwrap().remove("inputSha256");
    add(input, "inputSha256");
    foreign_attempt["proposalConstructionBinding"]["funnelReductionSource"] =
        funnel_source::assemble(input).unwrap();
    resemantic(&mut foreign_attempt);
    assert!(execute_base_in(&root, "foreign-evolved-attempt", &foreign_attempt).is_err());
    fs::write(&path, original_attempt).unwrap();

    let mut g0 = evolved.clone();
    g0["proposalStateAuthority"]["generationKind"] = json!("g0");
    g0["proposalStateAuthority"]["inputIdentityLedgerSha256"] = Value::Null;
    g0["proposalConstructionBinding"]["nativeV5Invocation"]["schemaVersion"] =
        json!("temporal_qd_native_v5_g0_invocation_descriptor_v1");
    g0["proposalConstructionBinding"]
        .as_object_mut()
        .unwrap()
        .remove("inputIdentityLedgerSha256");
    resemantic(&mut g0);
    // An evolved attempt receipt cannot be relabelled as a G0 authority.
    assert!(execute_base_in(&root, "g0", &g0).is_err());

    let mut record_one =
        json!({"schemaVersion":"temporal_qd_generation_record_v2","generationIndex":0});
    add(&mut record_one, "generationRecordSha256");
    let mut record_two =
        json!({"schemaVersion":"temporal_qd_generation_record_v2","generationIndex":1});
    add(&mut record_two, "generationRecordSha256");
    let mut completed = evolved.clone();
    completed["completedGenerationRecords"] = json!([record_one.clone(), record_two.clone()]);
    rehash_state_and_base(&mut completed);
    assert!(execute_base_in(&root, "completed-positive", &completed).is_ok());

    // The array root in state basis authenticates both cardinality and order.
    let mut reordered = completed.clone();
    reordered["completedGenerationRecords"] = json!([record_two.clone(), record_one.clone()]);
    resemantic(&mut reordered);
    assert!(execute_base_in(&root, "record-order", &reordered).is_err());

    // Rehashing the enclosing state cannot repair a substituted record's own hash.
    let mut self_rehashed = completed.clone();
    self_rehashed["completedGenerationRecords"][0]["generationIndex"] = json!(99);
    rehash_state_and_base(&mut self_rehashed);
    assert!(execute_base_in(&root, "record-self-hash", &self_rehashed).is_err());

    for (name, field, replacement) in [
        (
            "input-ledger",
            "inputIdentityLedgerSha256",
            json!(sha("other-input")),
        ),
        (
            "output-path",
            "outputIdentityLedgerRelativePath",
            json!("proposal/v5-native/other-ledger.json"),
        ),
        (
            "manifest",
            "proposalManifestSha256",
            json!(sha("other-manifest")),
        ),
        (
            "receipt",
            "proposalReceiptSha256",
            json!(sha("other-receipt")),
        ),
        (
            "journal",
            "generationJournalSha256",
            json!(sha("other-journal")),
        ),
        (
            "output-ledger",
            "outputIdentityLedgerSha256",
            json!(sha("other-ledger")),
        ),
        (
            "output-ledger-file",
            "outputIdentityLedgerFileSha256",
            json!(sha("other-ledger-file")),
        ),
    ] {
        let mut mutated = evolved.clone();
        mutated["proposalStateAuthority"][field] = replacement;
        resemantic(&mut mutated);
        assert!(
            execute_base_in(&root, name, &mutated).is_err(),
            "{name} substitution was accepted"
        );
    }

    let mut invocation_substitution = evolved.clone();
    invocation_substitution["proposalConstructionBinding"]["nativeV5Invocation"]["proposalManifest"]
        ["semanticSha256"] = json!(sha("substituted-manifest"));
    resemantic(&mut invocation_substitution);
    assert!(execute_base_in(&root, "invocation-substitution", &invocation_substitution).is_err());

    // The former minimal shape (semantic hints only, no reopenable documents
    // and no sealed funnel input) is deliberately not a compatibility ABI.
    let mut old_minimal = evolved.clone();
    old_minimal["proposalConstructionBinding"]
        .as_object_mut()
        .unwrap()
        .remove("funnelReductionInput");
    old_minimal["proposalConstructionBinding"]
        .as_object_mut()
        .unwrap()
        .remove("funnelReductionSource");
    old_minimal["proposalConstructionBinding"]["nativeV5Invocation"]["proposalManifest"] =
        json!({"semanticSha256":evolved["proposalStateAuthority"]["proposalManifestSha256"]});
    resemantic(&mut old_minimal);
    assert!(execute_base_in(&root, "old-minimal", &old_minimal).is_err());

    let mut roots_substitution = evolved.clone();
    roots_substitution["proposalConstructionBinding"]["proposalSemanticRoots"]["proposalReceiptSha256"] =
        json!(sha("substituted-receipt"));
    resemantic(&mut roots_substitution);
    assert!(execute_base_in(&root, "semantic-roots-substitution", &roots_substitution).is_err());
}
