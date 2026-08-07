use std::{
    env,
    io::{self, BufRead, Write},
    thread,
    time::Duration,
};

use serde_json::{Map, Value, json};
use temporal_qd_dashboard_jsonl::{
    COMPILE_BIDIRECTIONAL_OPERATION, COMPILE_BIDIRECTIONAL_RESPONSE_SCHEMA,
    COMPILE_BIDIRECTIONAL_RESULT_SCHEMA, VALIDATE_CANDIDATE_OPERATION,
    VALIDATE_CANDIDATE_RESPONSE_SCHEMA, VALIDATION_REPORT_SCHEMA, canonical_sha256,
};

fn main() {
    let mode = env::var("QD_JSONL_TEST_MODE").unwrap_or_else(|_| "happy".to_owned());
    let log = env::var("QD_JSONL_TEST_LOG").ok();
    let cwd = env::current_dir().unwrap_or_default().display().to_string();
    let flags_ok = env::args().any(|argument| argument == "--jsonl-server")
        && env::args().any(|argument| argument == "--jsonl-max-line-bytes");
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else { return };
        let request: Value = serde_json::from_str(&line).expect("mock child receives JSON");
        let candidate = request["candidateId"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        if let Some(log) = &log {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(log)
                .unwrap();
            writeln!(file, "{candidate}").unwrap();
        }
        match mode.as_str() {
            "timeout" => thread::sleep(Duration::from_secs(2)),
            "crash" => std::process::exit(7),
            "oversize" => {
                print!("{}", "x".repeat(4096));
                io::stdout().flush().unwrap();
                continue;
            }
            "stderr-wrong-id" => {
                eprint!("head:{}tail", "x".repeat(2048));
                io::stderr().flush().unwrap();
                emit(validate_response(
                    &request, "wrong", &candidate, &cwd, flags_ok,
                ));
                continue;
            }
            _ => {}
        }
        if request["operation"] == COMPILE_BIDIRECTIONAL_OPERATION {
            emit(compile_response(&request));
        } else {
            let request_id = if mode == "wrong-id" {
                "wrong"
            } else {
                request["requestId"].as_str().unwrap()
            };
            let mut response = validate_response(&request, request_id, &candidate, &cwd, flags_ok);
            if mode == "foreign" {
                response
                    .as_object_mut()
                    .unwrap()
                    .insert("foreign".to_owned(), json!(true));
            }
            if mode == "bool-exit" {
                response
                    .as_object_mut()
                    .unwrap()
                    .insert("semanticExitCode".to_owned(), json!(false));
            }
            emit(response);
        }
    }
}

fn validate_response(
    request: &Value,
    request_id: &str,
    candidate: &str,
    cwd: &str,
    flags_ok: bool,
) -> Value {
    json!({
        "schemaVersion": VALIDATE_CANDIDATE_RESPONSE_SCHEMA,
        "requestId": request_id,
        "operation": request["operation"].as_str().unwrap_or(VALIDATE_CANDIDATE_OPERATION),
        "semanticExitCode": 0,
        "report": {
            "schemaVersion": VALIDATION_REPORT_SCHEMA,
            "candidateAcceptable": true,
            "fixture": candidate,
            "cwd": cwd,
            "testEnvironment": env::var("QD_JSONL_TEST_VALUE").ok(),
            "protocolFlagsPresent": flags_ok,
        },
    })
}

fn compile_response(request: &Value) -> Value {
    let candidate = request["candidateId"].as_str().unwrap();
    let profile = json!({"version": "v3", "directionMode": "both", "fixture": candidate});
    let raw = canonical_sha256(&profile).unwrap();
    let snapshot = digest("snapshot", &profile);
    let program = digest("program", &profile);
    let validation = digest("validation", &profile);
    let report = json!({
        "schemaVersion": VALIDATION_REPORT_SCHEMA,
        "candidateId": candidate,
        "rawSourceProfileSha256": raw,
        "profileSnapshotSha256": snapshot,
        "programSha256": program,
        "validationReportSha256": validation,
        "evaluatorId": "mock-evaluator",
        "status": "valid_evaluable",
        "candidateAcceptable": true,
    });
    json!({
        "schemaVersion": COMPILE_BIDIRECTIONAL_RESPONSE_SCHEMA,
        "requestId": request["requestId"],
        "operation": COMPILE_BIDIRECTIONAL_OPERATION,
        "semanticExitCode": 0,
        "result": {
            "schemaVersion": COMPILE_BIDIRECTIONAL_RESULT_SCHEMA,
            "candidateId": candidate,
            "longRawSourceProfileSha256": request["expectedLongRawSourceProfileSha256"],
            "shortRawSourceProfileSha256": request["expectedShortRawSourceProfileSha256"],
            "rawSourceProfileSha256": raw,
            "profileSnapshotSha256": snapshot,
            "programSha256": program,
            "validationReportSha256": validation,
            "evaluatorId": "mock-evaluator",
            "profile": profile,
            "report": report,
        }
    })
}

fn digest(label: &str, profile: &Value) -> String {
    canonical_sha256(&Value::Object(Map::from_iter([(
        label.to_owned(),
        profile.clone(),
    )])))
    .unwrap()
}

fn emit(value: Value) {
    println!("{}", serde_json::to_string(&value).unwrap());
    io::stdout().flush().unwrap();
}
