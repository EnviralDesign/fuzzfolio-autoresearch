//! G0 compact-contract parity gates.
//!
//! The hash fixture is exported by `autoresearch.temporal_qd_g0_bootstrap`.
//! Rich-entry descriptor derivation is checked against Python goldens, while
//! the compact-reference tests exercise the persisted Rust boundary.

use serde_json::{Map, Value};
use temporal_qd_contract::{canonical_json_bytes, canonical_sha256};
use temporal_qd_kernel::g0;

fn object<K: Into<String>>(entries: impl IntoIterator<Item = (K, Value)>) -> Value {
    let mut result = Value::Object(Map::new());
    let values = result.as_object_mut().expect("object");
    for (key, value) in entries {
        values.insert(key.into(), value);
    }
    result
}

fn hash(value: &Value) -> String {
    canonical_sha256(value).expect("canonical test fixture")
}

fn sha_token(token: &str) -> String {
    hash(&Value::from(token))
}

fn insert_hash(value: &mut Value, field: &str) {
    let hash = hash(value);
    value
        .as_object_mut()
        .expect("self-hashed object")
        .insert(field.to_owned(), Value::from(hash));
}

fn descriptor(candidate_id: &str, candidate_identity: &str, family: &str) -> Value {
    let vector = object(g0::DESCRIPTOR_AXES.iter().map(|axis| {
        let value = if *axis == "staticLongShortActivationPotential" {
            "long:true|short:true".to_owned()
        } else {
            format!("{family}:{axis}")
        };
        (*axis, Value::from(value))
    }));
    let liveness = || {
        object([
            ("entryActionRouteCount", Value::from(1_u64)),
            ("reachableEntryActionRouteCount", Value::from(1_u64)),
            ("potential", Value::Bool(true)),
        ])
    };
    let mut result = object([
        (
            "schemaVersion",
            Value::from(g0::DESCRIPTOR_PROJECTION_SCHEMA),
        ),
        ("candidateId", Value::from(candidate_id)),
        ("candidateIdentitySha256", Value::from(candidate_identity)),
        ("pairIdentitySha256", Value::from(sha_token("pair"))),
        ("longCatalogSha256", Value::from(sha_token("long catalog"))),
        (
            "shortCatalogSha256",
            Value::from(sha_token("short catalog")),
        ),
        (
            "nativeValidationReportSha256",
            Value::from(sha_token("native validation")),
        ),
        (
            "staticReachabilityReportSha256",
            Value::from(sha_token("static reachability")),
        ),
        (
            "perSideLivenessProof",
            object([("long", liveness()), ("short", liveness())]),
        ),
        ("descriptorVector", vector),
    ]);
    insert_hash(&mut result, "descriptorProjectionSha256");
    result
}

fn reference(ordinal: u64, identity_token: &str, family: &str) -> Value {
    let pool_identity = sha_token("pool");
    let candidate_identity = sha_token(identity_token);
    let candidate_id = format!("qd_{}", &candidate_identity[7..35]);
    let entry_sha = sha_token(&format!("entry-{ordinal}"));
    let mut lineage = object([
        (
            "schemaVersion",
            Value::from("temporal_qd_g0_construction_lineage_v1"),
        ),
        ("entrySha256", Value::from(entry_sha.clone())),
        ("proposalOrdinal", Value::from(ordinal)),
        ("generationIndex", Value::from(0_u64)),
        ("birthOrdinal", Value::from(ordinal)),
        ("originKind", Value::from("random_immigrant")),
        ("candidateId", Value::from(candidate_id.clone())),
        (
            "candidateIdentitySha256",
            Value::from(candidate_identity.clone()),
        ),
    ]);
    insert_hash(&mut lineage, "constructionLineageSha256");
    let projection = descriptor(&candidate_id, &candidate_identity, family);
    let projection_sha = projection
        .get("descriptorProjectionSha256")
        .expect("descriptor hash")
        .clone();
    let mut result = object([
        ("schemaVersion", Value::from(g0::ACCEPTED_REFERENCE_SCHEMA)),
        ("constructionPoolIdentitySha256", Value::from(pool_identity)),
        ("proposalOrdinal", Value::from(ordinal)),
        (
            "journalReference",
            object([
                (
                    "schemaVersion",
                    Value::from("temporal_qd_g0_journal_reference_v1"),
                ),
                (
                    "journalRelativePath",
                    Value::from(format!("proposal-journal/{ordinal:08}.json")),
                ),
                ("entrySha256", Value::from(entry_sha.clone())),
            ]),
        ),
        ("acceptedPairEntrySha256", Value::from(entry_sha)),
        ("candidateId", Value::from(candidate_id)),
        ("candidateIdentitySha256", Value::from(candidate_identity)),
        ("constructionLineage", lineage),
        ("descriptorProjection", projection),
        ("descriptorProjectionSha256", projection_sha),
    ]);
    insert_hash(&mut result, "referenceSha256");
    result
}

fn pool(references: &[Value]) -> Value {
    let identity = sha_token("pool");
    g0::build_accepted_pool(&identity, references).expect("valid compact pool")
}

fn fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/g0_python_golden_hashes.json"))
        .expect("Python golden hash fixture JSON")
}

fn python_single_reference() -> Value {
    serde_json::from_str(include_str!("fixtures/g0_python_single_reference.json"))
        .expect("Python compact-reference fixture JSON")
}

#[test]
fn python_default_policy_hash_is_exact() {
    let fixture = fixture();
    let expected = fixture["defaultPolicySha256"]
        .as_str()
        .expect("fixture hash");
    assert_eq!(hash(&g0::default_policy()), expected);
}

#[test]
fn python_compact_reference_pool_ledger_and_selection_hashes_are_exact() {
    let fixture = fixture();
    let expected = &fixture["singleReference"];
    let reference = python_single_reference();
    let accepted = g0::validate_accepted_reference(&reference).expect("Python reference is valid");
    assert_eq!(
        accepted.reference_sha256,
        expected["referenceSha256"]
            .as_str()
            .expect("fixture reference hash")
    );
    let pool = g0::build_accepted_pool(
        reference["constructionPoolIdentitySha256"]
            .as_str()
            .expect("pool identity"),
        std::slice::from_ref(&reference),
    )
    .expect("Python pool shape");
    assert_eq!(
        pool["acceptedPoolSha256"],
        expected["acceptedPoolSha256"].clone()
    );
    let selection = g0::select_g0_bootstrap(&pool, 1, None).expect("Python selection shape");
    assert_eq!(
        selection["selectionSha256"],
        expected["selectionSha256"].clone()
    );
    let selected = vec![
        selection["selected"][0]["referenceSha256"]
            .as_str()
            .expect("selected reference")
            .to_owned(),
    ];
    let ledger = g0::materialize_campaign_ledger(&pool, &selected).expect("Python ledger shape");
    assert_eq!(ledger["ledgerSha256"], expected["ledgerSha256"].clone());
}

#[test]
fn compact_pool_ledger_and_selection_are_input_order_invariant() {
    let refs = vec![
        reference(0, "identity-z", "same"),
        reference(1, "identity-a", "other"),
        reference(2, "identity-m", "same"),
    ];
    let accepted_pool = pool(&refs);
    let mut reversed = refs.clone();
    reversed.reverse();
    let reversed_pool = pool(&reversed);

    let selection = g0::select_g0_bootstrap(&accepted_pool, 2, None).expect("selection");
    assert_eq!(
        selection,
        g0::select_g0_bootstrap(&reversed_pool, 2, None).expect("reversed selection")
    );
    assert_eq!(selection["marketEvidenceRead"], Value::Bool(false));
    let selected = selection["selected"].as_array().expect("selected");
    assert_eq!(selected.len(), 2);
    let selected_hashes: Vec<String> = selected
        .iter()
        .map(|row| {
            row["referenceSha256"]
                .as_str()
                .expect("reference hash")
                .to_owned()
        })
        .collect();
    let ledger = g0::materialize_campaign_ledger(&accepted_pool, &selected_hashes).expect("ledger");
    assert_eq!(
        g0::verify_campaign_ledger(&ledger, &accepted_pool, &selected_hashes)
            .expect("verify ledger"),
        ledger
    );
    assert_eq!(
        g0::verify_g0_bootstrap_selection(&selection, &accepted_pool).expect("verify selection"),
        selection
    );
}

#[test]
fn tie_break_is_lower_frequency_then_canonical_identity() {
    let refs = vec![
        reference(0, "identity-z", "shared"),
        reference(1, "identity-a", "shared"),
        reference(2, "identity-m", "distinct"),
    ];
    let result = g0::select_g0_bootstrap(&pool(&refs), 2, None).expect("selection");
    let trace = result["selectionTrace"].as_array().expect("trace");
    // `distinct` is the sole member of 18 buckets.  The activation bucket is
    // intentionally shared by all three references, so its total cost is 21.
    // It still wins equal 19-axis gains via the lower frequency cost.
    assert_eq!(trace[0]["globalBucketFrequencyCost"], Value::from(21_u64));
    assert_eq!(trace[0]["marginalCoverage"], Value::from(19_u64));

    let all_shared = vec![
        reference(0, "identity-z", "shared"),
        reference(1, "identity-a", "shared"),
    ];
    let shared_result =
        g0::select_g0_bootstrap(&pool(&all_shared), 1, None).expect("tie selection");
    let expected = all_shared
        .iter()
        .map(|reference| reference["candidateIdentitySha256"].as_str().unwrap())
        .min()
        .unwrap();
    assert_eq!(
        shared_result["selected"][0]["candidateIdentitySha256"],
        Value::from(expected)
    );
}

#[test]
fn compact_contract_rejects_unknown_fields_and_identity_drift() {
    let mut candidate_reference = reference(0, "identity", "family");
    candidate_reference["descriptorProjection"]
        .as_object_mut()
        .expect("projection")
        .insert("EconomicScore".to_owned(), Value::from(1_u64));
    let projection = candidate_reference["descriptorProjection"].clone();
    let projection_hash = hash(&{
        let mut without = projection.clone();
        without
            .as_object_mut()
            .unwrap()
            .remove("descriptorProjectionSha256");
        without
    });
    candidate_reference["descriptorProjection"]["descriptorProjectionSha256"] =
        Value::from(projection_hash.clone());
    candidate_reference["descriptorProjectionSha256"] = Value::from(projection_hash);
    let mut without_reference_hash = candidate_reference.clone();
    without_reference_hash
        .as_object_mut()
        .unwrap()
        .remove("referenceSha256");
    candidate_reference["referenceSha256"] = Value::from(hash(&without_reference_hash));
    assert!(g0::validate_accepted_reference(&candidate_reference).is_err());

    let good = reference(0, "identity", "family");
    let mut pool = pool(&[good]);
    pool["acceptedPoolSha256"] = Value::from(sha_token("forged"));
    assert!(g0::validate_accepted_pool(&pool).is_err());
}

fn python_rich_projection_fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/g0_python_rich_projection.json"))
        .expect("Python rich-entry projection fixture JSON")
}

fn python_dead_side_fixture() -> Value {
    serde_json::from_str(include_str!("fixtures/g0_python_dead_side_entry.json"))
        .expect("Python dead-side entry fixture JSON")
}

fn refresh_entry_hash(entry: &mut Value) {
    entry
        .as_object_mut()
        .expect("entry object")
        .remove("entrySha256");
    insert_hash(entry, "entrySha256");
}

#[test]
fn rich_projection_matches_python_golden_and_admits_compact_reference() {
    let fixture = python_rich_projection_fixture();
    let entry = &fixture["entry"];
    let expected = &fixture["projection"];
    let projection = g0::derive_descriptor_projection_from_rich_entry(entry)
        .expect("Python rich entry projects under native Rust authority");
    assert_eq!(&projection, expected);
    assert_eq!(
        canonical_json_bytes(&projection).expect("actual projection canonical bytes"),
        canonical_json_bytes(expected).expect("Python golden projection canonical bytes"),
    );

    let compact = g0::project_accepted_pair_entry(
        &sha_token("pool"),
        0,
        "proposal-journal/00000000.json",
        entry,
    )
    .expect("rich projection can be persisted as a compact reference");
    g0::validate_accepted_reference(&compact).expect("valid rich compact reference");
    assert_eq!(compact["descriptorProjection"], *expected);
}

#[test]
fn rich_projection_rejects_tampered_authority_and_schema() {
    let fixture = python_rich_projection_fixture();
    let mut tampered = fixture["entry"].clone();
    tampered["candidate"]["sourceProfile"]["name"] = Value::from("forged-source");
    refresh_entry_hash(&mut tampered);
    assert!(g0::derive_descriptor_projection_from_rich_entry(&tampered).is_err());

    let mut schema_drift = fixture["entry"].clone();
    schema_drift
        .as_object_mut()
        .expect("entry object")
        .insert("unexpectedAuthorityField".to_owned(), Value::Bool(true));
    refresh_entry_hash(&mut schema_drift);
    assert!(g0::derive_descriptor_projection_from_rich_entry(&schema_drift).is_err());

    let mut forged_descriptor = fixture["projection"].clone();
    forged_descriptor["descriptorVector"]["long.graphSize"] =
        Value::from("states:99|transitions:99");
    let mut descriptor_material = forged_descriptor.clone();
    descriptor_material
        .as_object_mut()
        .expect("descriptor object")
        .remove("descriptorProjectionSha256");
    forged_descriptor["descriptorProjectionSha256"] = Value::from(hash(&descriptor_material));
    assert!(
        g0::project_accepted_pair_entry_with_descriptor(
            &sha_token("pool"),
            0,
            "proposal-journal/00000000.json",
            &fixture["entry"],
            &forged_descriptor,
        )
        .is_err()
    );
}

#[test]
fn rich_projection_rejects_python_dead_side_entry() {
    let entry = python_dead_side_fixture();
    let error = g0::derive_descriptor_projection_from_rich_entry(&entry)
        .expect_err("Python liveness-dead entry must not project");
    assert!(
        error
            .to_string()
            .contains("per-side entry liveness proof is incomplete")
    );
}
