use std::{
    fs,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde_json::{Value, json};
use temporal_qd_contract::canonical_sha256 as contract_canonical_sha256;
use temporal_qd_dashboard_jsonl::{
    CompileBidirectionalRequest, DashboardJsonlConfig, DashboardJsonlTransport, JsonObject,
    ValidateCandidateOutcome, ValidateCandidateRequest, canonical_sha256,
};

fn helper_path() -> &'static str {
    env!("CARGO_BIN_EXE_qd-dashboard-jsonl-mock-helper")
}

fn temporary_path(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("qd-dashboard-jsonl-{name}-{nonce}"))
}

fn config(mode: &str) -> DashboardJsonlConfig {
    DashboardJsonlConfig::new([helper_path().to_owned()])
        .unwrap()
        .with_timeout(Duration::from_secs(1))
        .unwrap()
        .with_max_line_bytes(4096)
        .unwrap()
        .with_stderr_limit_bytes(1024)
        .unwrap()
        .with_environment("QD_JSONL_TEST_MODE", mode)
        .unwrap()
}

fn object(value: Value) -> JsonObject {
    value.as_object().cloned().unwrap()
}

fn validate(candidate_id: &str) -> ValidateCandidateRequest {
    ValidateCandidateRequest {
        candidate_id: candidate_id.to_owned(),
        expected_raw_source_profile_sha256: Some("sha256:".to_owned() + &"a".repeat(64)),
        source_profile: object(json!({"fixture": candidate_id})),
    }
}

#[test]
fn ordered_validate_requests_reuse_one_child_and_honor_environment_cwd_and_flags() {
    let cwd = temporary_path("cwd");
    fs::create_dir_all(&cwd).unwrap();
    let config = config("happy")
        .with_current_dir(&cwd)
        .with_environment("QD_JSONL_TEST_VALUE", "present")
        .unwrap();
    let mut transport = DashboardJsonlTransport::spawn(config).unwrap();
    let first = transport.validate_candidate(validate("first")).unwrap();
    let second = transport.validate_candidate(validate("second")).unwrap();
    transport.close();

    assert_eq!(first.outcome, ValidateCandidateOutcome::Accepted);
    assert_eq!(first.report["fixture"], "first");
    assert_eq!(second.report["fixture"], "second");
    assert_eq!(first.report["cwd"], cwd.display().to_string());
    assert_eq!(first.report["testEnvironment"], "present");
    assert_eq!(first.report["protocolFlagsPresent"], true);
    fs::remove_dir_all(cwd).unwrap();
}

#[test]
fn compile_bidirectional_verifies_closed_result_and_identity_bindings() {
    let long_profile = object(json!({
        "version": "v2",
        "directionMode": "long",
        "threshold": 0.00001,
        "negativeZero": -0.0,
    }));
    let short_profile = object(json!({
        "version": "v2",
        "directionMode": "short",
        "threshold": 1e16,
    }));
    let mut transport = DashboardJsonlTransport::spawn(config("happy")).unwrap();
    let response = transport
        .compile_bidirectional(CompileBidirectionalRequest {
            candidate_id: "pair-one".to_owned(),
            expected_long_raw_source_profile_sha256: contract_canonical_sha256(&Value::Object(
                long_profile.clone(),
            ))
            .unwrap(),
            expected_short_raw_source_profile_sha256: contract_canonical_sha256(&Value::Object(
                short_profile.clone(),
            ))
            .unwrap(),
            long_profile,
            short_profile,
        })
        .unwrap();
    transport.close();

    assert_eq!(response.result.candidate_id, "pair-one");
    assert_eq!(response.result.profile["version"], "v3");
    assert_eq!(response.result.profile["directionMode"], "both");
    assert_eq!(response.result.report["candidateAcceptable"], true);
}

#[test]
fn post_write_timeout_is_fail_closed_and_is_not_retried() {
    let log = temporary_path("timeout-log");
    let config = config("timeout")
        .with_environment("QD_JSONL_TEST_LOG", log.display().to_string())
        .unwrap();
    let mut transport = DashboardJsonlTransport::spawn(config).unwrap();
    let error = transport
        .validate_candidate(validate("ambiguous"))
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("timed out; request outcome is ambiguous")
    );
    assert!(transport.is_closed());
    assert_eq!(fs::read_to_string(&log).unwrap(), "ambiguous\n");
    assert!(
        transport
            .validate_candidate(validate("must-not-send"))
            .is_err()
    );
    assert_eq!(fs::read_to_string(&log).unwrap(), "ambiguous\n");
    fs::remove_file(log).unwrap();
}

#[test]
fn response_protocol_violations_close_the_session_and_capture_bounded_stderr() {
    let mut transport = DashboardJsonlTransport::spawn(config("stderr-wrong-id")).unwrap();
    let error = transport.validate_candidate(validate("bad")).unwrap_err();
    let text = error.to_string();

    assert!(text.contains("request ID mismatch"));
    assert!(text.contains("tail"));
    assert!(!text.contains("head:"));
    assert!(transport.is_closed());
}

#[test]
fn strict_response_schema_and_response_line_limit_fail_closed() {
    let mut foreign = DashboardJsonlTransport::spawn(config("foreign")).unwrap();
    let foreign_error = foreign.validate_candidate(validate("foreign")).unwrap_err();
    assert!(foreign_error.to_string().contains("fields are not exact"));
    assert!(foreign.is_closed());

    let mut oversized =
        DashboardJsonlTransport::spawn(config("oversize").with_max_line_bytes(1024).unwrap())
            .unwrap();
    let oversized_error = oversized
        .validate_candidate(validate("oversize"))
        .unwrap_err();
    assert!(
        oversized_error
            .to_string()
            .contains("response exceeds JSONL line limit")
    );
    assert!(oversized.is_closed());
}

#[test]
fn prewrite_line_limit_rejects_without_poisoning_or_dispatching_the_child() {
    let log = temporary_path("prewrite-log");
    let config = config("happy")
        .with_max_line_bytes(1024)
        .unwrap()
        .with_environment("QD_JSONL_TEST_LOG", log.display().to_string())
        .unwrap();
    let mut transport = DashboardJsonlTransport::spawn(config).unwrap();
    let mut oversized = validate("too-big");
    oversized
        .source_profile
        .insert("large".to_owned(), json!("x".repeat(2048)));

    assert!(
        transport
            .validate_candidate(oversized)
            .unwrap_err()
            .to_string()
            .contains("line limit")
    );
    assert!(!transport.is_closed());
    assert_eq!(
        transport
            .validate_candidate(validate("later"))
            .unwrap()
            .report["fixture"],
        "later"
    );
    transport.close();
    assert_eq!(fs::read_to_string(&log).unwrap(), "later\n");
    fs::remove_file(log).unwrap();
}

#[test]
fn canonical_identity_uses_python_ascii_key_order_and_float_exponents() {
    let value = json!({"z": "😀", "a": -0.0, "small": 0.00001, "large": 1e16});
    let hash = canonical_sha256(&value).unwrap();
    assert_eq!(hash, contract_canonical_sha256(&value).unwrap());
    assert_eq!(
        hash,
        "sha256:91266d0525042c8ac2f10104e92c6b1b7643b2100603f18ff7346d4cc14ed78a"
    );
}
