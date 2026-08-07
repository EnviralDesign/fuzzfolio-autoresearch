use temporal_qd_contract::{Value, canonical_sha256};
use temporal_qd_kernel::construction::{
    ConstructionCatalog, DIRECTION_FLIP,
    GRAPH_BOUND_TIMEFRAME as CONSTRUCTION_GRAPH_BOUND_TIMEFRAME, GeneratorV3ConstructionRegistry,
    MANAGEMENT_PLAN, SCALAR_DYNAMIC_MANAGEMENT, inspect_construction_reachability,
};
use temporal_qd_kernel::indicator::{
    EVIDENCE_MEMBERSHIP, FAMILY_SUBSTITUTION, GRAPH_BOUND_TIMEFRAME, IndicatorCatalog,
    IndicatorLearningRegistry, TA_PERIOD,
};
use temporal_qd_kernel::protection::{
    apply_initial_protection_plan, default_initial_protection_policy,
    enumerate_initial_protection_plans,
};

const SHA_A: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SHA_B: &str = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn fixture(source: &str) -> Value {
    serde_json::from_str(source).unwrap()
}

fn profile() -> Value {
    serde_json::from_str(
        r#"{
          "executionConfig":{"managementLibrary":{
            "defaultPlanId":"base",
            "plans":[{"id":"base","initialStop":{"kind":"fixed_percent","percent":1.0},"initialTarget":{"kind":"reward_multiple","multiple":2.0}}],
            "scalarBindings":[
              {"id":"atr_distance","indicatorInstanceId":"atr","outputKey":"atr","valueKind":"price_distance","availability":"completed_bar"},
              {"id":"ma","indicatorInstanceId":"ma","outputKey":"ma","valueKind":"price_level","availability":"completed_bar"}
            ]
          }},
          "graph":{"transitions":[]}
        }"#,
    )
    .unwrap()
}

#[test]
fn initial_protection_matches_frozen_python_oracle_vector() {
    let golden: Value =
        serde_json::from_str(include_str!("fixtures/indicator_protection_golden.json")).unwrap();
    let policy = default_initial_protection_policy();
    let source = profile();
    let plans = enumerate_initial_protection_plans(&source, &policy).unwrap();

    assert_eq!(plans.len(), golden["planCount"].as_u64().unwrap() as usize);
    assert_eq!(
        plans.first().unwrap()["planSha256"],
        golden["firstPlanSha256"]
    );
    assert_eq!(
        plans.last().unwrap()["planSha256"],
        golden["lastPlanSha256"]
    );

    let (child, audit) =
        apply_initial_protection_plan(&source, plans.first().unwrap(), &policy).unwrap();
    assert_eq!(audit["applicationSha256"], golden["applicationSha256"]);
    assert_eq!(
        canonical_sha256(&child).unwrap(),
        golden["childSha256"].as_str().unwrap()
    );
    assert_eq!(
        child["executionConfig"]["managementLibrary"]["scalarBindings"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn graph_bound_timeframe_plan_identity_matches_python_oracle_vector() {
    let golden: Value =
        serde_json::from_str(include_str!("fixtures/indicator_protection_golden.json")).unwrap();
    let catalog: Value = serde_json::from_str(r#"{
      "timeframes":{"M5":{},"M15":{},"H1":{}},
      "indicators":[{"meta":{"id":"RSI","signalPersistence":"state","valueRange":{"min":0,"max":100,"step":5,"minRange":10},"requiredPaddingBars":1,"usesRangeConfiguration":true,"talibMeta":[{"name":"timeperiod","uiType":"integer_slider","default":14,"min":2,"max":50,"marks":[{"value":7},{"value":14},{"value":21}]}]},"config":{"timeframe":"M5","lookbackBars":1,"isActive":true,"useFormingBar":false,"weight":1.0,"ranges":{"buy":[30,70],"sell":[30,70]},"talibConfig":[{"name":"timeperiod","value":14}]}}]
    }"#).unwrap();
    let profile: Value = serde_json::from_str(r#"{
      "directionMode":"long",
      "indicators":[{"meta":{"id":"RSI","instanceId":"rsi","signalPersistence":"state","valueRange":{"min":0,"max":100,"step":5,"minRange":10},"requiredPaddingBars":1,"usesRangeConfiguration":true,"talibMeta":[{"name":"timeperiod","uiType":"integer_slider","default":14,"min":2,"max":50,"marks":[{"value":7},{"value":14},{"value":21}]}]},"config":{"timeframe":"M5","lookbackBars":1,"isActive":true,"useFormingBar":false,"weight":1.0,"ranges":{"buy":[30,70],"sell":[30,70]},"talibConfig":[{"name":"timeperiod","value":14}]}}],
      "graph":{"evidenceGroups":[{"id":"g","indicatorInstanceIds":["rsi"]}],"eventBindings":[]},
      "executionConfig":{"managementLibrary":{"scalarBindings":[]}}
    }"#).unwrap();
    let catalog = IndicatorCatalog::new(&catalog).unwrap();
    assert_eq!(
        catalog.catalog_sha256(),
        golden["indicator"]["catalogSha256"].as_str().unwrap()
    );
    let registry = IndicatorLearningRegistry::new(catalog).unwrap();
    let plans = registry
        .get(GRAPH_BOUND_TIMEFRAME)
        .unwrap()
        .enumerate_plans(&profile)
        .unwrap();
    assert_eq!(
        plans.len(),
        golden["indicator"]["planCount"].as_u64().unwrap() as usize
    );
    assert_eq!(
        plans.first().unwrap()["planSha256"],
        golden["indicator"]["firstPlanSha256"]
    );
    assert_eq!(
        plans.last().unwrap()["planSha256"],
        golden["indicator"]["lastPlanSha256"]
    );
}

#[test]
fn family_substitution_preserves_ranges_only_with_the_same_numeric_domain() {
    // This is the shape-64 triggering case: a state-score replacement with
    // the same [-1, 1] domain must keep its parent thresholds, rather than
    // silently reverting to the replacement's catalog defaults.
    let catalog: Value = serde_json::from_str(
        r#"{
          "timeframes":{"M5":{},"M15":{},"H1":{}},
          "indicators":[
            {"meta":{"id":"SOURCE","signalPersistence":"state","valueRange":{"min":-1,"max":1,"step":0.05,"minRange":0.1},"requiredPaddingBars":1,"usesRangeConfiguration":true},"config":{"timeframe":"M5","lookbackBars":5,"isActive":true,"useFormingBar":false,"weight":1,"ranges":{"buy":[0.2,1.0],"sell":[-1.0,-0.2]},"talibConfig":[]}},
            {"meta":{"id":"SAME_DOMAIN","signalPersistence":"state","valueRange":{"min":-1,"max":1,"step":0.05,"minRange":0.1},"requiredPaddingBars":1,"usesRangeConfiguration":true},"config":{"timeframe":"M5","lookbackBars":5,"isActive":true,"useFormingBar":false,"weight":1,"ranges":{"buy":[0.4,0.8],"sell":[-0.8,-0.4]},"talibConfig":[]}},
            {"meta":{"id":"OTHER_DOMAIN","signalPersistence":"state","valueRange":{"min":0,"max":100,"step":5,"minRange":10},"requiredPaddingBars":1,"usesRangeConfiguration":true},"config":{"timeframe":"M5","lookbackBars":5,"isActive":true,"useFormingBar":false,"weight":1,"ranges":{"buy":[20,40],"sell":[60,80]},"talibConfig":[]}}
          ]
        }"#,
    )
    .unwrap();
    let profile: Value = serde_json::from_str(
        r#"{
          "directionMode":"long",
          "indicators":[{"meta":{"id":"SOURCE","instanceId":"entry_score","signalPersistence":"state","valueRange":{"min":-1,"max":1,"step":0.05,"minRange":0.1},"requiredPaddingBars":1,"usesRangeConfiguration":true},"config":{"timeframe":"M5","lookbackBars":5,"isActive":true,"useFormingBar":false,"weight":1,"ranges":{"buy":[0.2,1.0],"sell":[-1.0,-0.2]},"talibConfig":[]}}],
          "graph":{"evidenceGroups":[{"id":"entry","indicatorInstanceIds":["entry_score"]}],"eventBindings":[]},
          "executionConfig":{"managementLibrary":{"scalarBindings":[]}}
        }"#,
    )
    .unwrap();
    let registry =
        IndicatorLearningRegistry::new(IndicatorCatalog::new(&catalog).unwrap()).unwrap();
    let operator = registry.get(FAMILY_SUBSTITUTION).unwrap();
    let plans = operator.enumerate_plans(&profile).unwrap();
    let same_domain = plans
        .iter()
        .find(|plan| plan["construction"]["afterIndicatorId"] == "SAME_DOMAIN")
        .unwrap();
    let changed = operator.preview(&profile, same_domain).unwrap();
    assert_eq!(
        changed["indicators"][0]["config"]["ranges"], profile["indicators"][0]["config"]["ranges"],
        "same score domain keeps parent-owned thresholds"
    );

    // The contrast prevents over-broad threshold copying: semantic-domain
    // changes intentionally start at the replacement catalog defaults.
    let other_domain = plans
        .iter()
        .find(|plan| plan["construction"]["afterIndicatorId"] == "OTHER_DOMAIN")
        .unwrap();
    let changed = operator.preview(&profile, other_domain).unwrap();
    assert_eq!(
        changed["indicators"][0]["config"]["ranges"],
        catalog["indicators"][2]["config"]["ranges"]
    );
}

#[test]
fn scalar_bound_fuzzy_evidence_member_cannot_seed_membership_plans() {
    // Python permits this pre-existing fuzzy+scalar topology, but refuses to
    // use it as a membership source or to add a candidate alongside it.
    let catalog: Value = serde_json::from_str(
        r#"{
          "timeframes":{"M1":{},"M5":{},"M15":{},"H1":{}},
          "indicators":[
            {"meta":{"id":"SCALAR_SCORE","signalPersistence":"state","valueRange":{"min":0,"max":100,"step":5,"minRange":10},"requiredPaddingBars":1,"usesRangeConfiguration":true,"managementScalarOutputs":[{"outputKey":"level","valueKind":"price_level","unit":"price"}],"talibMeta":[{"name":"timeperiod","uiType":"integer_slider","default":14,"min":2,"max":50,"marks":[{"value":7},{"value":14},{"value":21}]}]},"config":{"timeframe":"M5","lookbackBars":1,"isActive":true,"useFormingBar":false,"weight":1.0,"ranges":{"buy":[30,70],"sell":[30,70]},"talibConfig":[{"name":"timeperiod","value":14}]}},
            {"meta":{"id":"CANDIDATE_SCORE","signalPersistence":"state","valueRange":{"min":0,"max":100,"step":5,"minRange":10},"requiredPaddingBars":1,"usesRangeConfiguration":true,"talibMeta":[{"name":"timeperiod","uiType":"integer_slider","default":14,"min":2,"max":50,"marks":[{"value":7},{"value":14},{"value":21}]}]},"config":{"timeframe":"M5","lookbackBars":1,"isActive":true,"useFormingBar":false,"weight":1.0,"ranges":{"buy":[30,70],"sell":[30,70]},"talibConfig":[{"name":"timeperiod","value":14}]}}
          ]
        }"#,
    )
    .unwrap();
    let profile: Value = serde_json::from_str(
        r#"{
          "directionMode":"short",
          "indicators":[
            {"meta":{"id":"SCALAR_SCORE","instanceId":"scalar_score","signalPersistence":"state","valueRange":{"min":0,"max":100,"step":5,"minRange":10},"requiredPaddingBars":1,"usesRangeConfiguration":true,"managementScalarOutputs":[{"outputKey":"level","valueKind":"price_level","unit":"price"}],"talibMeta":[{"name":"timeperiod","uiType":"integer_slider","default":14,"min":2,"max":50,"marks":[{"value":7},{"value":14},{"value":21}]}]},"config":{"timeframe":"M5","lookbackBars":1,"isActive":true,"useFormingBar":false,"weight":1.0,"ranges":{"buy":[30,70],"sell":[30,70]},"talibConfig":[{"name":"timeperiod","value":14}]}},
            {"meta":{"id":"CANDIDATE_SCORE","instanceId":"candidate_score","signalPersistence":"state","valueRange":{"min":0,"max":100,"step":5,"minRange":10},"requiredPaddingBars":1,"usesRangeConfiguration":true,"talibMeta":[{"name":"timeperiod","uiType":"integer_slider","default":14,"min":2,"max":50,"marks":[{"value":7},{"value":14},{"value":21}]}]},"config":{"timeframe":"M5","lookbackBars":1,"isActive":true,"useFormingBar":false,"weight":1.0,"ranges":{"buy":[30,70],"sell":[30,70]},"talibConfig":[{"name":"timeperiod","value":14}]}}
          ],
          "graph":{"initialStateId":"s0","evidenceGroups":[{"id":"g","indicatorInstanceIds":["scalar_score"]}],"eventBindings":[],"transitions":[{"sourceStateId":"s0","destinationStateId":"s1","guard":{"kind":"evidence_at_least","groupId":"g"}}]},
          "executionConfig":{"managementLibrary":{"scalarBindings":[{"indicatorInstanceId":"scalar_score","outputKey":"level","valueKind":"price_level"}]}}
        }"#,
    )
    .unwrap();

    let registry =
        IndicatorLearningRegistry::new(IndicatorCatalog::new(&catalog).unwrap()).unwrap();
    assert!(
        registry
            .get(EVIDENCE_MEMBERSHIP)
            .unwrap()
            .enumerate_plans(&profile)
            .unwrap()
            .is_empty(),
        "a scalar-bound member must not enter the rich-immigrant membership plan order"
    );
    assert!(
        registry
            .get(TA_PERIOD)
            .unwrap()
            .enumerate_plans(&profile)
            .unwrap()
            .iter()
            .any(|plan| plan["construction"]["indicatorInstanceId"] == "scalar_score"),
        "the next seeded operator can still use the scalar-bound indicator's TA period surface"
    );
}

#[test]
fn runtime_oracle_rich_module_indicator_plans_match_python() {
    let manifest: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/temporal_qd_runtime_oracle/runtime-manifest.json"
    ))
    .unwrap();
    let transcript: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/temporal_qd_runtime_oracle/dashboard-jsonl-transcript.json"
    ))
    .unwrap();
    let catalog =
        IndicatorCatalog::new(&manifest["pairRunConfig"]["longModule"]["catalog"]).unwrap();
    let registry = IndicatorLearningRegistry::new(catalog).unwrap();
    let profile = &transcript["records"][0]["request"]["sourceProfile"];

    let plans = registry.enumerate_plans(profile).unwrap();
    assert_eq!(plans.len(), 217);
    assert_eq!(
        canonical_sha256(&Value::Array(
            plans
                .iter()
                .map(|plan| plan["planSha256"].clone())
                .collect(),
        ))
        .unwrap(),
        "sha256:e811344bf48bfb3d008f009fea613a10ff3cd7de8d7a799a59c2a97b531c2ee3"
    );

    let period_plans = registry
        .get(TA_PERIOD)
        .unwrap()
        .enumerate_plans(profile)
        .unwrap();
    let donchian = period_plans
        .iter()
        .filter(|plan| plan["construction"]["indicatorId"] == "DONCHIAN_CHANNEL_BREAKOUT")
        .collect::<Vec<_>>();
    assert_eq!(donchian.len(), 2);
    assert_eq!(
        donchian
            .iter()
            .map(|plan| {
                (
                    plan["construction"]["change"]["choice"].clone(),
                    plan["construction"]["change"]["after"].clone(),
                    plan["planSha256"].clone(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (
                Value::String("fast".into()),
                Value::from(10),
                Value::String(
                    "sha256:2e453904917758c503442bda12eca37de5da054c8ea4976b761baa3a9cf2ec3d"
                        .into(),
                ),
            ),
            (
                Value::String("slow".into()),
                Value::from(55),
                Value::String(
                    "sha256:a6a6fff6e07687816befc39337311e3f7598a1e4dc90f708111ede1e989c4479"
                        .into(),
                ),
            ),
        ]
    );
    let selected = donchian
        .iter()
        .find(|plan| plan["construction"]["change"]["choice"] == "slow")
        .unwrap();
    let selected_child = registry
        .get(TA_PERIOD)
        .unwrap()
        .preview(profile, selected)
        .unwrap();
    assert_eq!(
        canonical_sha256(&selected_child).unwrap(),
        "sha256:36e7ab72de56bc48e48bf9546bb98b9dbb99f84267fa983751456fa27ba32564"
    );

    let short_catalog =
        IndicatorCatalog::new(&manifest["pairRunConfig"]["shortModule"]["catalog"]).unwrap();
    let short_registry = IndicatorLearningRegistry::new(short_catalog).unwrap();
    let short_profile = &transcript["records"][1]["request"]["sourceProfile"];
    let short_plans = short_registry.enumerate_plans(short_profile).unwrap();
    assert_eq!(short_plans.len(), 165);
    assert_eq!(
        canonical_sha256(&Value::Array(
            short_plans
                .iter()
                .map(|plan| plan["planSha256"].clone())
                .collect(),
        ))
        .unwrap(),
        "sha256:71b09c9eaffcfe6a9b8d924b61bd4b29637dc272e7485bc3ba9ffd1fca6713e5"
    );
}

struct ConstructionOracle<'a> {
    operator_id: &'a str,
    plans: &'a [&'a str],
    child_sha: &'a str,
    application_sha: &'a str,
    audit_sha: &'a str,
    reachability_sha: &'a str,
}

fn assert_construction_oracle(
    registry: &GeneratorV3ConstructionRegistry,
    profile: &Value,
    expected: ConstructionOracle<'_>,
) {
    let operator = registry.get(expected.operator_id).unwrap();
    let actual = operator.enumerate_plans(profile).unwrap();
    assert_eq!(
        actual
            .iter()
            .map(|plan| plan["planSha256"].as_str().unwrap())
            .collect::<Vec<_>>(),
        expected.plans
    );
    let (child, application) = operator.apply(profile, &actual[0], SHA_A, SHA_B).unwrap();
    assert_eq!(canonical_sha256(&child).unwrap(), expected.child_sha);
    assert_eq!(application["applicationSha256"], expected.application_sha);
    assert_eq!(
        operator.audit(profile, &child, &application).unwrap()["auditSha256"],
        expected.audit_sha
    );
    assert_eq!(
        inspect_construction_reachability(&child).unwrap()["reachabilitySha256"],
        expected.reachability_sha
    );
}

#[test]
fn construction_enabled_families_match_frozen_python_oracle_vectors() {
    let catalog =
        ConstructionCatalog::new(&fixture(include_str!("fixtures/construction_catalog.json")))
            .unwrap();
    assert_eq!(
        catalog.catalog_sha256(),
        "sha256:561e568122fdf1b9171c2178c0faaac94a928250debe3722e9b3c7d39fcf795e"
    );
    let registry = GeneratorV3ConstructionRegistry::new(catalog).unwrap();
    assert_eq!(
        registry.policy()["policySha256"],
        "sha256:2079322041f2bd46eb7e1511dad11918a37b3b8f1679acbe035bd3e74529e1e9"
    );

    let created = fixture(include_str!("fixtures/construction_create_direction.json"));
    assert_construction_oracle(
        &registry,
        &created,
        ConstructionOracle {
            operator_id: DIRECTION_FLIP,
            plans: &["sha256:87bb867eafad95d2a7f8e3a19d50a5cef6a7960236db78dc66a830049d4c47b4"],
            child_sha: "sha256:a20025ac0e8aeb281571ba1b4b057a996651d51d5f859cac3d130d40b11909aa",
            application_sha: "sha256:636c638ebe564b97524d4395d98a076124b02d1512374d77f64628c464efea01",
            audit_sha: "sha256:98d44b9ff00e094831f8799d0c6fe68a8f8095e10ef40b2e339c9bb1a4cc1966",
            reachability_sha: "sha256:7dda6cb166daf4567630bf039d8cf0ab1bc235d8538f22c0492a8086000a5144",
        },
    );
    assert_construction_oracle(
        &registry,
        &created,
        ConstructionOracle {
            operator_id: MANAGEMENT_PLAN,
            plans: &["sha256:68a71c76d57b42bea19f48edcfa4e78f8d00a0eb606c61a092fea20efba3deab"],
            child_sha: "sha256:1ef8b783b94f9b4a4fd6cd8cf1c0986d80c7c0db6c32e6a5e5f3b62d341eca2f",
            application_sha: "sha256:7c689eb546fc0474d5e9a3b82f5b552e8500045a5c75e066a4f235b6d8d0a3e5",
            audit_sha: "sha256:a12c888369ba67e372a01c6c1dbbb5521dfb80f71be61ba32b510ef79ec07503",
            reachability_sha: "sha256:e3cb2823d3e291403ac857d0ebf6af38f520b444514830a9114b022def165971",
        },
    );

    let deleted = fixture(include_str!("fixtures/construction_delete.json"));
    assert_construction_oracle(
        &registry,
        &deleted,
        ConstructionOracle {
            operator_id: MANAGEMENT_PLAN,
            plans: &[
                "sha256:126d664ba002465d67dc97acc0be9045ee2269223fa0f75262ea1004e0c9e3ef",
                "sha256:89d3844461e7e1d42dca3534cc87e6fb3d12b79a67d13a55bd3f2c5fbb5e8cbe",
                "sha256:a56bfdf27c36c3b1db5f33d7ff17ac0ed743bc2e6561fa4b7e0f7fcf179d4893",
            ],
            child_sha: "sha256:29acc21c8c1db318d7265d375a32e7d880e36c6c28e47ba09231ebe02ff2c336",
            application_sha: "sha256:735eeabdb9be9805e6bde855a177326470fcc0de537d322e577c9aec1b99f49f",
            audit_sha: "sha256:39a977932d750e86125b9d9d065566c2473671171137fb59a49fb3122b7328a7",
            reachability_sha: "sha256:c1cb23609ac566ffd8093dbba99ac82a00237f890f3514dd1466cd98ea394c3e",
        },
    );

    let scalar = fixture(include_str!("fixtures/construction_scalar_timeframe.json"));
    assert_construction_oracle(
        &registry,
        &scalar,
        ConstructionOracle {
            operator_id: CONSTRUCTION_GRAPH_BOUND_TIMEFRAME,
            plans: &["sha256:fbadd6c34a452cb884198d9a4e2536035530b2d5a80857c2e454876faf8be89f"],
            child_sha: "sha256:2631c02131b5e0f09d25765a14915ef6a33ed3e01cbace84ed34125c034a00d8",
            application_sha: "sha256:271e2ebf405ebe7adcecaa17094125830c4555f60b35b96775cfb9c2498123ea",
            audit_sha: "sha256:5716804b4b81b60423a4bde1db38c54afad5f8f35d248111d39e18073bf7d1ca",
            reachability_sha: "sha256:7dda6cb166daf4567630bf039d8cf0ab1bc235d8538f22c0492a8086000a5144",
        },
    );
    assert_construction_oracle(
        &registry,
        &scalar,
        ConstructionOracle {
            operator_id: SCALAR_DYNAMIC_MANAGEMENT,
            plans: &[
                "sha256:3fdbc4e356b98bff5bee79e906913459dd2a1979c77ac0cf705b695f94e3d19a",
                "sha256:aa4536fbcd8abb8fe13833c4e3c8d260687654c9d755837f00385548184accb1",
                "sha256:fc86db3e62d94b713ca42ddc47da084fcca1de6fe7f689cdca48faf6bc349a49",
            ],
            child_sha: "sha256:40d0d03002cc66c74baeff23a1471c68a305dd530cfcb17fc7fccf1ccb664357",
            application_sha: "sha256:03894cd1e24a64145c5f476418d74d7cbe96a70b4951b48f16b816438b97c8f0",
            audit_sha: "sha256:f76ce8ab1bfa102edf9d7aa437b7e26a5c1c6eb5a9ecd67bf29ed2be5a407d2d",
            reachability_sha: "sha256:34ee4ecdf45ce7ec31b9994dc6517e410e1e69e2a77920fec629af2a15aaf406",
        },
    );
}
