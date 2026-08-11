//! Independent regression coverage for the stopped fresh-v5 construction run.
//!
//! The fixture is a small, checked-in extraction of proposal ordinal zero; it
//! contains the real frozen contexts and expected programs but deliberately
//! excludes the old rich pair/candidate blobs.  This proves that native
//! construction is exact without normalizing a 4,000-entry rich journal in a
//! test process.

use serde_json::Value;
use temporal_qd_contract::canonical_sha256;
use temporal_qd_kernel::v5::{
    V5_PROPOSAL_DELTA_SCHEMA, build_immigrant_module, proposal_delta, v5_proposal_seed,
    validate_proposal_delta,
};

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/temporal_qd_v5_stopped_run_oracle.json"
    ))
    .expect("parse stopped-run v5 oracle fixture")
}

fn field<'a>(value: &'a Value, key: &str) -> &'a Value {
    value
        .get(key)
        .unwrap_or_else(|| panic!("fixture lacks {key}"))
}

#[test]
fn stopped_run_fixture_is_self_authenticating() {
    let fixture = fixture();
    assert_eq!(
        fixture.get("schemaVersion").and_then(Value::as_str),
        Some("temporal_qd_v5_stopped_run_oracle_fixture_v1")
    );
    let supplied = field(&fixture, "fixtureSha256")
        .as_str()
        .expect("fixture SHA-256");
    let mut material = fixture.as_object().expect("fixture object").clone();
    material.remove("fixtureSha256");
    assert_eq!(
        canonical_sha256(&Value::Object(material)).expect("hash fixture"),
        supplied
    );
}

#[test]
fn native_immigrant_programs_match_real_stopped_v5_oracle_without_rich_rehydration() {
    let fixture = fixture();
    let construction = field(&fixture, "construction");
    assert_eq!(
        field(construction, "originKind").as_str(),
        Some("random_immigrant")
    );
    let ordinal = field(construction, "proposalOrdinal")
        .as_u64()
        .expect("proposal ordinal");
    let seed = field(construction, "proposalSeed")
        .as_str()
        .expect("proposal seed");
    assert_eq!(
        v5_proposal_seed(
            field(construction, "generationConfigSha256")
                .as_str()
                .expect("generation config SHA-256"),
            ordinal,
        )
        .expect("derive stopped-run proposal seed"),
        seed,
    );
    let sides = field(construction, "sides");
    let mut modules = Vec::new();
    for side in ["long", "short"] {
        let expected = field(sides, side);
        let actual = build_immigrant_module(
            side,
            seed,
            field(expected, "context"),
            field(expected, "budget"),
        )
        .unwrap_or_else(|error| panic!("build {side} module: {error}"));
        assert_eq!(actual.program, *field(expected, "program"));
        assert_eq!(
            actual.program_sha256,
            field(expected, "programSha256")
                .as_str()
                .expect("expected program SHA-256")
        );
        modules.push(actual);
    }
    let delta = proposal_delta(ordinal, seed, "random_immigrant", &modules[0], &modules[1])
        .expect("construct compact stopped-run delta");
    assert_eq!(
        delta.get("schemaVersion").and_then(Value::as_str),
        Some(V5_PROPOSAL_DELTA_SCHEMA)
    );
    validate_proposal_delta(&delta).expect("compact stopped-run delta validates");
    assert_eq!(
        delta.get("longProgram"),
        Some(field(field(sides, "long"), "program"))
    );
    assert_eq!(
        delta.get("shortProgram"),
        Some(field(field(sides, "short"), "program"))
    );
}
