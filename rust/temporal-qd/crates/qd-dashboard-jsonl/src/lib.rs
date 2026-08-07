//! Ordered, fail-closed transport for Dashboard's persistent JSONL authority.
//!
//! The Dashboard process remains the only interpreter of temporal profiles.
//! This crate owns just the child lifecycle and the strict request/response
//! boundary.  A response is accepted only when it is the next response for the
//! request that was written, has the exact schema for its operation, and binds
//! all identity-bearing compile fields back to the request.

use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use temporal_qd_contract::canonical_json_line;
use thiserror::Error;

pub use temporal_qd_contract::canonical_sha256;

pub const DASHBOARD_JSONL_PROTOCOL_VERSION: &str = "temporal_search_candidate_validation_jsonl_v1";
pub const VALIDATE_CANDIDATE_REQUEST_SCHEMA: &str =
    "temporal_search_candidate_validation_jsonl_request_v1";
pub const VALIDATE_CANDIDATE_RESPONSE_SCHEMA: &str =
    "temporal_search_candidate_validation_jsonl_response_v1";
pub const VALIDATE_CANDIDATE_OPERATION: &str = "validate_candidate";
pub const COMPILE_BIDIRECTIONAL_REQUEST_SCHEMA: &str =
    "temporal_search_bidirectional_compile_jsonl_request_v1";
pub const COMPILE_BIDIRECTIONAL_RESPONSE_SCHEMA: &str =
    "temporal_search_bidirectional_compile_jsonl_response_v1";
pub const COMPILE_BIDIRECTIONAL_RESULT_SCHEMA: &str =
    "temporal_search_bidirectional_compile_result_v1";
pub const COMPILE_BIDIRECTIONAL_OPERATION: &str = "compile_bidirectional";
pub const VALIDATION_REPORT_SCHEMA: &str = "temporal_search_candidate_validation_v1";

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_LINE_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_STDERR_LIMIT_BYTES: usize = 64 * 1024;
const MIN_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_TIMEOUT: Duration = Duration::from_secs(300);
const MIN_LINE_BYTES: usize = 1024;
const MAX_LINE_BYTES: usize = 64 * 1024 * 1024;
const MIN_STDERR_LIMIT_BYTES: usize = 1024;
const MAX_STDERR_LIMIT_BYTES: usize = 4 * 1024 * 1024;

pub type JsonObject = Map<String, Value>;

/// Process configuration.  Environment entries override (rather than replace)
/// the inherited process environment, matching the existing Python client.
#[derive(Clone, Debug)]
pub struct DashboardJsonlConfig {
    command: Vec<String>,
    timeout: Duration,
    max_line_bytes: usize,
    stderr_limit_bytes: usize,
    current_dir: Option<PathBuf>,
    environment: BTreeMap<String, String>,
}

impl DashboardJsonlConfig {
    pub fn new<I, S>(command: I) -> Result<Self, DashboardJsonlError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let config = Self {
            command: command.into_iter().map(Into::into).collect(),
            timeout: DEFAULT_TIMEOUT,
            max_line_bytes: DEFAULT_MAX_LINE_BYTES,
            stderr_limit_bytes: DEFAULT_STDERR_LIMIT_BYTES,
            current_dir: None,
            environment: BTreeMap::new(),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn command(&self) -> &[String] {
        &self.command
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn max_line_bytes(&self) -> usize {
        self.max_line_bytes
    }

    pub fn stderr_limit_bytes(&self) -> usize {
        self.stderr_limit_bytes
    }

    pub fn current_dir(&self) -> Option<&Path> {
        self.current_dir.as_deref()
    }

    pub fn environment(&self) -> &BTreeMap<String, String> {
        &self.environment
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Result<Self, DashboardJsonlError> {
        self.timeout = timeout;
        self.validate()?;
        Ok(self)
    }

    pub fn with_max_line_bytes(
        mut self,
        max_line_bytes: usize,
    ) -> Result<Self, DashboardJsonlError> {
        self.max_line_bytes = max_line_bytes;
        self.validate()?;
        Ok(self)
    }

    pub fn with_stderr_limit_bytes(
        mut self,
        stderr_limit_bytes: usize,
    ) -> Result<Self, DashboardJsonlError> {
        self.stderr_limit_bytes = stderr_limit_bytes;
        self.validate()?;
        Ok(self)
    }

    pub fn with_current_dir(mut self, current_dir: impl Into<PathBuf>) -> Self {
        self.current_dir = Some(current_dir.into());
        self
    }

    pub fn with_environment(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, DashboardJsonlError> {
        let name = name.into();
        let value = value.into();
        validate_environment_entry(&name, &value)?;
        self.environment.insert(name, value);
        Ok(self)
    }

    fn validate(&self) -> Result<(), DashboardJsonlError> {
        if self.command.is_empty() || self.command.iter().any(|part| part.trim().is_empty()) {
            return Err(DashboardJsonlError::Configuration(
                "command must be a non-empty argument array".to_owned(),
            ));
        }
        if self.timeout < MIN_TIMEOUT || self.timeout > MAX_TIMEOUT {
            return Err(DashboardJsonlError::Configuration(
                "timeout must be between 1 and 300 seconds".to_owned(),
            ));
        }
        if !(MIN_LINE_BYTES..=MAX_LINE_BYTES).contains(&self.max_line_bytes) {
            return Err(DashboardJsonlError::Configuration(format!(
                "maximum JSONL line bytes must be between {MIN_LINE_BYTES} and {MAX_LINE_BYTES}"
            )));
        }
        if !(MIN_STDERR_LIMIT_BYTES..=MAX_STDERR_LIMIT_BYTES).contains(&self.stderr_limit_bytes) {
            return Err(DashboardJsonlError::Configuration(format!(
                "stderr limit bytes must be between {MIN_STDERR_LIMIT_BYTES} and {MAX_STDERR_LIMIT_BYTES}"
            )));
        }
        for (name, value) in &self.environment {
            validate_environment_entry(name, value)?;
        }
        Ok(())
    }
}

/// A typed validate request.  `source_profile` remains opaque to Rust; its
/// object shape is deliberately owned by Dashboard.
#[derive(Clone, Debug)]
pub struct ValidateCandidateRequest {
    pub candidate_id: String,
    pub expected_raw_source_profile_sha256: Option<String>,
    pub source_profile: JsonObject,
}

/// Dashboard's valid/rejected semantic result for a validate request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValidateCandidateOutcome {
    Accepted,
    Rejected,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ValidateCandidateResponse {
    pub outcome: ValidateCandidateOutcome,
    pub report: JsonObject,
}

/// A typed v2-long + v2-short compilation request.
#[derive(Clone, Debug)]
pub struct CompileBidirectionalRequest {
    pub candidate_id: String,
    pub long_profile: JsonObject,
    pub short_profile: JsonObject,
    pub expected_long_raw_source_profile_sha256: String,
    pub expected_short_raw_source_profile_sha256: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CompileBidirectionalResult {
    pub candidate_id: String,
    pub long_raw_source_profile_sha256: String,
    pub short_raw_source_profile_sha256: String,
    pub raw_source_profile_sha256: String,
    pub profile_snapshot_sha256: String,
    pub program_sha256: String,
    pub validation_report_sha256: String,
    pub evaluator_id: String,
    pub profile: JsonObject,
    pub report: JsonObject,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CompileBidirectionalResponse {
    pub result: CompileBidirectionalResult,
}

#[derive(Debug, Error)]
pub enum DashboardJsonlError {
    #[error("dashboard JSONL configuration is invalid: {0}")]
    Configuration(String),
    #[error("dashboard JSONL request is invalid: {0}")]
    Request(String),
    #[error("could not spawn Dashboard JSONL child: {source}")]
    Spawn {
        #[source]
        source: std::io::Error,
    },
    #[error("{message}{stderr_suffix}")]
    Session {
        message: String,
        stderr_suffix: String,
    },
}

enum OutputEvent {
    Line(Vec<u8>),
    End,
}

/// A single sequential Dashboard JSONL child process.
///
/// Methods require `&mut self`, so a response cannot be consumed by a later
/// request.  Once request bytes have been attempted, every operational or
/// protocol error closes this instance.  Callers that want a new child must
/// explicitly spawn a new transport for a later logical request.
pub struct DashboardJsonlTransport {
    config: DashboardJsonlConfig,
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    stdout_events: Receiver<OutputEvent>,
    stderr: Arc<Mutex<BoundedBytes>>,
    stdout_thread: Option<JoinHandle<()>>,
    stderr_thread: Option<JoinHandle<()>>,
    next_request_sequence: u64,
    closed: bool,
}

impl DashboardJsonlTransport {
    pub fn spawn(config: DashboardJsonlConfig) -> Result<Self, DashboardJsonlError> {
        config.validate()?;
        let mut command = Command::new(&config.command[0]);
        command.args(&config.command[1..]);
        command
            .arg("--jsonl-server")
            .arg("--jsonl-max-line-bytes")
            .arg(config.max_line_bytes.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .envs(&config.environment);
        if let Some(current_dir) = &config.current_dir {
            command.current_dir(current_dir);
        }
        let mut child = command
            .spawn()
            .map_err(|source| DashboardJsonlError::Spawn { source })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| DashboardJsonlError::Session {
                message: "Dashboard JSONL child has no stdin pipe".to_owned(),
                stderr_suffix: String::new(),
            })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| DashboardJsonlError::Session {
                message: "Dashboard JSONL child has no stdout pipe".to_owned(),
                stderr_suffix: String::new(),
            })?;
        let stderr_pipe = child
            .stderr
            .take()
            .ok_or_else(|| DashboardJsonlError::Session {
                message: "Dashboard JSONL child has no stderr pipe".to_owned(),
                stderr_suffix: String::new(),
            })?;

        // One queued response bounds memory if a bad child writes output before
        // the caller can consume it.  The protocol is strictly one-in/one-out.
        let (sender, receiver) = mpsc::sync_channel(1);
        let max_line_bytes = config.max_line_bytes;
        let stdout_thread = match thread::Builder::new()
            .name("qd-dashboard-jsonl-stdout".to_owned())
            .spawn(move || drain_stdout(stdout, max_line_bytes, sender))
        {
            Ok(thread) => thread,
            Err(error) => {
                drop(stdin);
                let _ = child.kill();
                let _ = child.wait();
                return Err(DashboardJsonlError::Session {
                    message: format!("could not start Dashboard stdout reader: {error}"),
                    stderr_suffix: String::new(),
                });
            }
        };

        let stderr = Arc::new(Mutex::new(BoundedBytes::new(config.stderr_limit_bytes)));
        let stderr_for_thread = Arc::clone(&stderr);
        let stderr_thread = match thread::Builder::new()
            .name("qd-dashboard-jsonl-stderr".to_owned())
            .spawn(move || drain_stderr(stderr_pipe, stderr_for_thread))
        {
            Ok(thread) => thread,
            Err(error) => {
                drop(stdin);
                let _ = child.kill();
                let _ = child.wait();
                drop(stdout_thread);
                return Err(DashboardJsonlError::Session {
                    message: format!("could not start Dashboard stderr reader: {error}"),
                    stderr_suffix: String::new(),
                });
            }
        };

        Ok(Self {
            config,
            child: Some(child),
            stdin: Some(stdin),
            stdout_events: receiver,
            stderr,
            stdout_thread: Some(stdout_thread),
            stderr_thread: Some(stderr_thread),
            next_request_sequence: 1,
            closed: false,
        })
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Terminate and reap the child.  This operation is idempotent.
    pub fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        drop(self.stdin.take());
        if let Some(mut child) = self.child.take() {
            match child.try_wait() {
                Ok(Some(_)) => {}
                Ok(None) => {
                    // `Child::kill` is forced termination on the supported
                    // platforms, so the following wait reaps the process.
                    let _ = child.kill();
                    let _ = child.wait();
                }
                Err(_) => {
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }
        }
        // Reader threads own only the now-closed pipe handles.  Do not join
        // them here: a malicious child can fill the one-slot response channel
        // just before shutdown, and waiting for that sender would turn
        // fail-closed cleanup into a deadlock.  Dropping a JoinHandle detaches
        // it; after the child is reaped both readers reach EOF promptly.
        drop(self.stdout_thread.take());
        drop(self.stderr_thread.take());
    }

    pub fn validate_candidate(
        &mut self,
        request: ValidateCandidateRequest,
    ) -> Result<ValidateCandidateResponse, DashboardJsonlError> {
        let request_id = self.next_request_id()?;
        let wire = ValidateCandidateWireRequest {
            schema_version: VALIDATE_CANDIDATE_REQUEST_SCHEMA,
            operation: VALIDATE_CANDIDATE_OPERATION,
            request_id: &request_id,
            candidate_id: &request.candidate_id,
            expected_raw_source_profile_sha256: request
                .expected_raw_source_profile_sha256
                .as_deref(),
            source_profile: &request.source_profile,
        };
        let encoded = self.encode_request(&wire, "candidate validator")?;
        let response =
            self.dispatch_and_receive(&request_id, &encoded, "persistent candidate validator")?;
        self.verify_validate_response(response, &request_id)
    }

    pub fn compile_bidirectional(
        &mut self,
        request: CompileBidirectionalRequest,
    ) -> Result<CompileBidirectionalResponse, DashboardJsonlError> {
        require_sha256(
            &request.expected_long_raw_source_profile_sha256,
            "expected long raw source profile SHA-256",
        )?;
        require_sha256(
            &request.expected_short_raw_source_profile_sha256,
            "expected short raw source profile SHA-256",
        )?;
        if canonical_request_sha256(&Value::Object(request.long_profile.clone()))?
            != request.expected_long_raw_source_profile_sha256
            || canonical_request_sha256(&Value::Object(request.short_profile.clone()))?
                != request.expected_short_raw_source_profile_sha256
        {
            return Err(DashboardJsonlError::Request(
                "bidirectional compiler request profile identity mismatch".to_owned(),
            ));
        }

        let request_id = self.next_request_id()?;
        let wire = CompileBidirectionalWireRequest {
            schema_version: COMPILE_BIDIRECTIONAL_REQUEST_SCHEMA,
            operation: COMPILE_BIDIRECTIONAL_OPERATION,
            request_id: &request_id,
            candidate_id: &request.candidate_id,
            long_profile: &request.long_profile,
            short_profile: &request.short_profile,
            expected_long_raw_source_profile_sha256: &request
                .expected_long_raw_source_profile_sha256,
            expected_short_raw_source_profile_sha256: &request
                .expected_short_raw_source_profile_sha256,
        };
        let encoded = self.encode_request(&wire, "bidirectional compiler")?;
        let response =
            self.dispatch_and_receive(&request_id, &encoded, "persistent bidirectional compiler")?;
        self.verify_compile_response(response, &request_id, &request)
    }

    fn next_request_id(&mut self) -> Result<String, DashboardJsonlError> {
        if self.closed {
            return Err(DashboardJsonlError::Session {
                message: "Dashboard JSONL transport is closed".to_owned(),
                stderr_suffix: self.stderr_suffix(),
            });
        }
        let sequence = self.next_request_sequence;
        self.next_request_sequence =
            self.next_request_sequence.checked_add(1).ok_or_else(|| {
                DashboardJsonlError::Session {
                    message: "Dashboard JSONL request ID sequence exhausted".to_owned(),
                    stderr_suffix: self.stderr_suffix(),
                }
            })?;
        Ok(format!("temporal-qd-jsonl-{sequence:016x}"))
    }

    fn encode_request<T: Serialize>(
        &mut self,
        request: &T,
        subject: &str,
    ) -> Result<Vec<u8>, DashboardJsonlError> {
        let value = serde_json::to_value(request).map_err(|error| {
            DashboardJsonlError::Request(format!(
                "{subject} request cannot be represented as finite JSON: {error}"
            ))
        })?;
        let encoded = canonical_json_line(&value).map_err(|error| {
            DashboardJsonlError::Request(format!(
                "{subject} request cannot be represented as finite JSON: {error}"
            ))
        })?;
        if encoded.len() > self.config.max_line_bytes {
            return Err(DashboardJsonlError::Request(format!(
                "{subject} request exceeds persistent JSONL line limit"
            )));
        }
        Ok(encoded)
    }

    fn dispatch_and_receive(
        &mut self,
        request_id: &str,
        encoded: &[u8],
        subject: &str,
    ) -> Result<Value, DashboardJsonlError> {
        self.ensure_child_running(subject)?;
        let write_result = match self.stdin.as_mut() {
            Some(stdin) => stdin.write_all(encoded).and_then(|_| stdin.flush()),
            None => {
                return Err(self.session_failure(format!("{subject} stdin is unavailable")));
            }
        };
        if let Err(error) = write_result {
            return Err(self.session_failure(format!(
                "{subject} failed while dispatching request: {error}"
            )));
        }

        let line = match self.stdout_events.recv_timeout(self.config.timeout) {
            Ok(OutputEvent::Line(line)) => line,
            Ok(OutputEvent::End) => {
                return Err(self.session_failure(format!(
                    "{subject} exited before responding to request {request_id}"
                )));
            }
            Err(RecvTimeoutError::Timeout) => {
                return Err(self.session_failure(format!(
                    "{subject} timed out; request outcome is ambiguous"
                )));
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(self.session_failure(format!(
                    "{subject} stdout reader disconnected before responding"
                )));
            }
        };
        if line.len() > self.config.max_line_bytes || !line.ends_with(b"\n") {
            return Err(
                self.session_failure(format!("{subject} response exceeds JSONL line limit"))
            );
        }
        serde_json::from_slice(&line).map_err(|error| {
            self.session_failure(format!(
                "{subject} returned malformed JSONL response: {error}"
            ))
        })
    }

    fn ensure_child_running(&mut self, subject: &str) -> Result<(), DashboardJsonlError> {
        if self.closed {
            return Err(DashboardJsonlError::Session {
                message: format!("{subject} transport is closed"),
                stderr_suffix: self.stderr_suffix(),
            });
        }
        match self.child.as_mut() {
            Some(child) => match child.try_wait() {
                Ok(Some(status)) => Err(self.session_failure(format!(
                    "{subject} exited before request dispatch with status {status}"
                ))),
                Ok(None) => Ok(()),
                Err(error) => Err(self
                    .session_failure(format!("could not inspect {subject} child status: {error}"))),
            },
            None => Err(self.session_failure(format!("{subject} child is unavailable"))),
        }
    }

    fn verify_validate_response(
        &mut self,
        response: Value,
        request_id: &str,
    ) -> Result<ValidateCandidateResponse, DashboardJsonlError> {
        let object = self.verify_envelope(
            response,
            request_id,
            VALIDATE_CANDIDATE_RESPONSE_SCHEMA,
            VALIDATE_CANDIDATE_OPERATION,
            "persistent candidate validator",
        )?;
        let semantic_exit = object.get("semanticExitCode").and_then(Value::as_i64);
        if semantic_exit == Some(1) {
            let _: ErrorWireResponse =
                serde_json::from_value(Value::Object(object)).map_err(|error| {
                    self.session_failure(format!(
                    "persistent candidate validator error response fields are not exact: {error}"
                ))
                })?;
            return Err(self.session_failure(
                "persistent candidate validator rejected its request".to_owned(),
            ));
        }
        let wire: ValidateCandidateWireResponse = serde_json::from_value(Value::Object(object))
            .map_err(|error| {
                self.session_failure(format!(
                    "persistent candidate validator response fields are not exact: {error}"
                ))
            })?;
        let outcome = match wire.semantic_exit_code {
            0 => ValidateCandidateOutcome::Accepted,
            2 => ValidateCandidateOutcome::Rejected,
            _ => {
                return Err(self.session_failure(
                    "persistent candidate validator returned invalid semantic exit code".to_owned(),
                ));
            }
        };
        let report = value_into_object(
            wire.report,
            "persistent candidate validator response lacks report",
        )
        .map_err(|message| self.session_failure(message))?;
        if report.get("schemaVersion").and_then(Value::as_str) != Some(VALIDATION_REPORT_SCHEMA) {
            return Err(
                self.session_failure("candidate validator returned an unknown schema".to_owned())
            );
        }
        match outcome {
            ValidateCandidateOutcome::Accepted
                if report.get("candidateAcceptable") != Some(&Value::Bool(true)) =>
            {
                return Err(
                    self.session_failure("validator exit code and acceptance disagree".to_owned())
                );
            }
            ValidateCandidateOutcome::Rejected
                if report.get("candidateAcceptable") != Some(&Value::Bool(false)) =>
            {
                return Err(self.session_failure(
                    "validator rejection exit code and report disagree".to_owned(),
                ));
            }
            _ => {}
        }
        Ok(ValidateCandidateResponse { outcome, report })
    }

    fn verify_compile_response(
        &mut self,
        response: Value,
        request_id: &str,
        request: &CompileBidirectionalRequest,
    ) -> Result<CompileBidirectionalResponse, DashboardJsonlError> {
        let object = self.verify_envelope(
            response,
            request_id,
            COMPILE_BIDIRECTIONAL_RESPONSE_SCHEMA,
            COMPILE_BIDIRECTIONAL_OPERATION,
            "persistent bidirectional compiler",
        )?;
        let semantic_exit = object.get("semanticExitCode").and_then(Value::as_i64);
        if semantic_exit == Some(1) {
            let _: ErrorWireResponse =
                serde_json::from_value(Value::Object(object)).map_err(|error| {
                    self.session_failure(format!(
                    "persistent bidirectional compiler error response fields are not exact: {error}"
                ))
                })?;
            return Err(self.session_failure(
                "persistent bidirectional compiler rejected its request".to_owned(),
            ));
        }
        let wire: CompileBidirectionalWireResponse = serde_json::from_value(Value::Object(object))
            .map_err(|error| {
                self.session_failure(format!(
                    "persistent bidirectional compiler response fields are not exact: {error}"
                ))
            })?;
        if wire.semantic_exit_code != 0 {
            return Err(self.session_failure(
                "persistent bidirectional compiler returned invalid semantic exit code".to_owned(),
            ));
        }
        let result = wire.result;
        if result.schema_version != COMPILE_BIDIRECTIONAL_RESULT_SCHEMA {
            return Err(self.session_failure(
                "persistent bidirectional compiler result fields are not exact".to_owned(),
            ));
        }
        if result.candidate_id != request.candidate_id
            || result.long_raw_source_profile_sha256
                != request.expected_long_raw_source_profile_sha256
            || result.short_raw_source_profile_sha256
                != request.expected_short_raw_source_profile_sha256
        {
            return Err(self.session_failure(
                "persistent bidirectional compiler result identity mismatch".to_owned(),
            ));
        }
        if result.profile.get("version").and_then(Value::as_str) != Some("v3")
            || result.profile.get("directionMode").and_then(Value::as_str) != Some("both")
        {
            return Err(self.session_failure(
                "persistent bidirectional compiler did not return a v3/both profile".to_owned(),
            ));
        }
        for (name, value) in [
            ("rawSourceProfileSha256", &result.raw_source_profile_sha256),
            ("profileSnapshotSha256", &result.profile_snapshot_sha256),
            ("programSha256", &result.program_sha256),
            ("validationReportSha256", &result.validation_report_sha256),
        ] {
            require_sha256(value, &format!("bidirectional compiler {name}"))
                .map_err(|error| self.session_failure(error.to_string()))?;
        }
        let profile_sha =
            canonical_sha256(&Value::Object(result.profile.clone())).map_err(|error| {
                self.session_failure(format!(
                "persistent bidirectional compiler result profile cannot be canonicalized: {error}"
            ))
            })?;
        if profile_sha != result.raw_source_profile_sha256 {
            return Err(self.session_failure(
                "persistent bidirectional compiler result report mismatch".to_owned(),
            ));
        }
        if result.report.get("schemaVersion").and_then(Value::as_str)
            != Some(VALIDATION_REPORT_SCHEMA)
            || result.report.get("candidateId").and_then(Value::as_str)
                != Some(&request.candidate_id)
        {
            return Err(self.session_failure(
                "persistent bidirectional compiler result report mismatch".to_owned(),
            ));
        }
        for (wire_name, report_name) in [
            (&result.raw_source_profile_sha256, "rawSourceProfileSha256"),
            (&result.profile_snapshot_sha256, "profileSnapshotSha256"),
            (&result.program_sha256, "programSha256"),
            (&result.validation_report_sha256, "validationReportSha256"),
            (&result.evaluator_id, "evaluatorId"),
        ] {
            if result.report.get(report_name).and_then(Value::as_str) != Some(wire_name) {
                return Err(self.session_failure(
                    "persistent bidirectional compiler report identities mismatch".to_owned(),
                ));
            }
        }
        if result.report.get("candidateAcceptable") != Some(&Value::Bool(true))
            || result.report.get("status").and_then(Value::as_str) != Some("valid_evaluable")
        {
            return Err(self.session_failure(
                "persistent bidirectional compiler did not admit its pair".to_owned(),
            ));
        }
        Ok(CompileBidirectionalResponse {
            result: CompileBidirectionalResult {
                candidate_id: result.candidate_id,
                long_raw_source_profile_sha256: result.long_raw_source_profile_sha256,
                short_raw_source_profile_sha256: result.short_raw_source_profile_sha256,
                raw_source_profile_sha256: result.raw_source_profile_sha256,
                profile_snapshot_sha256: result.profile_snapshot_sha256,
                program_sha256: result.program_sha256,
                validation_report_sha256: result.validation_report_sha256,
                evaluator_id: result.evaluator_id,
                profile: result.profile,
                report: result.report,
            },
        })
    }

    fn verify_envelope(
        &mut self,
        response: Value,
        request_id: &str,
        expected_schema: &str,
        expected_operation: &str,
        subject: &str,
    ) -> Result<JsonObject, DashboardJsonlError> {
        let object = value_into_object(response, &format!("{subject} response must be an object"))
            .map_err(|message| self.session_failure(message))?;
        if object.get("requestId").and_then(Value::as_str) != Some(request_id) {
            return Err(self.session_failure(format!("{subject} response request ID mismatch")));
        }
        if object.get("schemaVersion").and_then(Value::as_str) != Some(expected_schema) {
            return Err(
                self.session_failure(format!("{subject} returned an unknown protocol schema"))
            );
        }
        if object.get("operation").and_then(Value::as_str) != Some(expected_operation) {
            return Err(self.session_failure(format!("{subject} response operation mismatch")));
        }
        Ok(object)
    }

    fn session_failure(&mut self, message: String) -> DashboardJsonlError {
        let stderr_suffix = self.stderr_suffix();
        self.close();
        DashboardJsonlError::Session {
            message,
            stderr_suffix,
        }
    }

    fn stderr_suffix(&self) -> String {
        let bytes = self
            .stderr
            .lock()
            .map(|value| value.as_slice().to_vec())
            .unwrap_or_default();
        let stderr = String::from_utf8_lossy(&bytes).trim().to_owned();
        if stderr.is_empty() {
            String::new()
        } else {
            format!("; stderr={stderr:?}")
        }
    }
}

impl Drop for DashboardJsonlTransport {
    fn drop(&mut self) {
        self.close();
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ValidateCandidateWireRequest<'a> {
    schema_version: &'static str,
    operation: &'static str,
    request_id: &'a str,
    candidate_id: &'a str,
    expected_raw_source_profile_sha256: Option<&'a str>,
    source_profile: &'a JsonObject,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CompileBidirectionalWireRequest<'a> {
    schema_version: &'static str,
    operation: &'static str,
    request_id: &'a str,
    candidate_id: &'a str,
    long_profile: &'a JsonObject,
    short_profile: &'a JsonObject,
    expected_long_raw_source_profile_sha256: &'a str,
    expected_short_raw_source_profile_sha256: &'a str,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ErrorWireResponse {
    #[allow(dead_code)]
    schema_version: String,
    #[allow(dead_code)]
    request_id: String,
    #[allow(dead_code)]
    operation: String,
    #[allow(dead_code)]
    semantic_exit_code: i64,
    #[allow(dead_code)]
    error: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ValidateCandidateWireResponse {
    #[allow(dead_code)]
    schema_version: String,
    #[allow(dead_code)]
    request_id: String,
    #[allow(dead_code)]
    operation: String,
    semantic_exit_code: i64,
    report: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CompileBidirectionalWireResponse {
    #[allow(dead_code)]
    schema_version: String,
    #[allow(dead_code)]
    request_id: String,
    #[allow(dead_code)]
    operation: String,
    semantic_exit_code: i64,
    result: CompileBidirectionalWireResult,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CompileBidirectionalWireResult {
    #[allow(dead_code)]
    schema_version: String,
    candidate_id: String,
    long_raw_source_profile_sha256: String,
    short_raw_source_profile_sha256: String,
    raw_source_profile_sha256: String,
    profile_snapshot_sha256: String,
    program_sha256: String,
    validation_report_sha256: String,
    evaluator_id: String,
    profile: JsonObject,
    report: JsonObject,
}

fn value_into_object(value: Value, message: &str) -> Result<JsonObject, String> {
    value.as_object().cloned().ok_or_else(|| message.to_owned())
}

fn validate_environment_entry(name: &str, value: &str) -> Result<(), DashboardJsonlError> {
    if name.is_empty() || name.contains('=') || name.contains('\0') || value.contains('\0') {
        return Err(DashboardJsonlError::Configuration(
            "environment must be a string mapping with valid non-empty names".to_owned(),
        ));
    }
    Ok(())
}

fn require_sha256(value: &str, name: &str) -> Result<(), DashboardJsonlError> {
    if value.len() != 71
        || !value.starts_with("sha256:")
        || !value[7..]
            .bytes()
            .all(|character| character.is_ascii_hexdigit() && !character.is_ascii_uppercase())
    {
        return Err(DashboardJsonlError::Request(format!(
            "{name} must be a canonical sha256 digest"
        )));
    }
    Ok(())
}

fn canonical_request_sha256(value: &Value) -> Result<String, DashboardJsonlError> {
    canonical_sha256(value).map_err(|error| {
        DashboardJsonlError::Request(format!(
            "profile cannot be represented as canonical finite JSON: {error}"
        ))
    })
}

fn drain_stdout(stdout: impl Read, max_line_bytes: usize, sender: SyncSender<OutputEvent>) {
    let mut reader = BufReader::new(stdout);
    loop {
        let mut line = Vec::with_capacity(max_line_bytes.min(8 * 1024));
        let result = reader
            .by_ref()
            .take(max_line_bytes as u64 + 1)
            .read_until(b'\n', &mut line);
        match result {
            Ok(0) => {
                let _ = sender.try_send(OutputEvent::End);
                return;
            }
            Ok(_) => {
                // A child that emits more than one response before the owner
                // consumes one must not make the reader unbounded or block
                // shutdown.  The next request cannot safely proceed, so it is
                // enough to retain the first line and stop this reader.
                if sender.try_send(OutputEvent::Line(line)).is_err() {
                    return;
                }
            }
            Err(_) => {
                let _ = sender.try_send(OutputEvent::End);
                return;
            }
        }
    }
}

fn drain_stderr(stderr: impl Read, target: Arc<Mutex<BoundedBytes>>) {
    let mut stderr = stderr;
    let mut chunk = [0_u8; 4096];
    loop {
        match stderr.read(&mut chunk) {
            Ok(0) | Err(_) => return,
            Ok(count) => {
                if let Ok(mut target) = target.lock() {
                    target.append(&chunk[..count]);
                } else {
                    return;
                }
            }
        }
    }
}

struct BoundedBytes {
    maximum: usize,
    bytes: Vec<u8>,
}

impl BoundedBytes {
    fn new(maximum: usize) -> Self {
        Self {
            maximum,
            bytes: Vec::with_capacity(maximum),
        }
    }

    fn append(&mut self, bytes: &[u8]) {
        if bytes.len() >= self.maximum {
            self.bytes.clear();
            self.bytes
                .extend_from_slice(&bytes[bytes.len() - self.maximum..]);
            return;
        }
        let excess = self
            .bytes
            .len()
            .saturating_add(bytes.len())
            .saturating_sub(self.maximum);
        if excess > 0 {
            self.bytes.drain(..excess);
        }
        self.bytes.extend_from_slice(bytes);
    }

    fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}
