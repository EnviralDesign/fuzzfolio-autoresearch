//! Bounded native dispatcher for the existing local Lab gateway contract.
//!
//! The task matrix remains immutable.  This crate writes each task exactly
//! once into a content-addressed sidecar, keeps only compact task-index and
//! completion-journal metadata in memory, and writes the legacy Python
//! checkpoint a single time after every task has a durable terminal record.
//! A result is fsynced and journaled before its gateway lease is acknowledged.

#![recursion_limit = "256"]

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    fmt,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use flate2::{Compression, GzBuilder, read::GzDecoder};
use reqwest::{StatusCode, blocking::Client};
use serde::Deserializer as _;
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use temporal_qd_campaign_seal::{
    CandidateWindowResultAdmission, admit_candidate_window_task_result,
};
use temporal_qd_contract::{
    JsonNewline, canonical_json_bytes, canonical_json_line, canonical_sha256,
    canonical_sha256_without_object_field, python_pretty_json_line, sha256_prefixed,
};

pub const DISPATCH_SCHEMA: &str = "temporal_qd_native_gateway_dispatch_result_v1";
pub const TASK_INDEX_SCHEMA: &str = "temporal_qd_native_gateway_task_index_v1";
pub const TASK_INDEX_ENTRY_SCHEMA: &str = "temporal_qd_native_gateway_task_index_entry_v1";
pub const TASK_OBJECT_SCHEMA: &str = "temporal_qd_native_gateway_task_object_v1";
pub const COMPLETION_JOURNAL_SCHEMA: &str =
    "temporal_qd_native_gateway_completion_journal_entry_v1";
pub const FAILURE_RECEIPT_SCHEMA: &str = "temporal_qd_native_gateway_failure_receipt_v1";
pub const TELEMETRY_SCHEMA: &str = "temporal_qd_native_gateway_dispatch_telemetry_v1";
/// Current-runtime receipt ABI.  Version one deliberately remains historical:
/// v5 callers must never fall back to its embedded O(T) result inventory.
pub const EXECUTION_RECEIPT_SCHEMA: &str = "temporal_qd_native_gateway_execution_receipt_v2";
pub const RESULT_INVENTORY_ENTRY_SCHEMA: &str =
    "temporal_qd_native_gateway_result_inventory_entry_v2";
pub const RESULT_INVENTORY_ROOT_SCHEMA: &str =
    "temporal_qd_native_gateway_result_inventory_root_v2";

const TASK_MANIFEST_SCHEMA: &str = "temporal_graph_candidate_window_manifest_v1";
const CHECKPOINT_SCHEMA: &str = "temporal_graph_candidate_window_checkpoint_v1";
const TASK_KIND: &str = "temporal_graph_candidate_window";
#[cfg(test)]
const ADMITTED_SCHEMA: &str = "temporal_graph_candidate_window_result_v1";
const REJECTED_SCHEMA: &str = "temporal_graph_candidate_window_rejected_result_v1";
const SIDECAR_DIR: &str = ".native-gateway-dispatch";
const TASK_INDEX_NAME: &str = "task-index.jsonl";
const TASK_INDEX_ROOT_NAME: &str = "task-index.json";
const COMPLETION_JOURNAL_NAME: &str = "completion-journal.jsonl";
const RESULT_INVENTORY_NAME: &str = "result-inventory.jsonl";
const RESULT_INVENTORY_ROOT_NAME: &str = "result-inventory.json";
const TASK_OBJECT_DIR: &str = "tasks";
const RESULTS_DIR: &str = "results";
const FAILURES_DIR: &str = "failures";
const CHECKPOINT_NAME: &str = "checkpoint.json";
const EXECUTION_RECEIPT_NAME: &str = "execution-receipt.json";
const MAX_SAFE_BATCH: usize = 1_000;
const MAX_RESULT_BATCH: usize = 1_024;
const MAX_HTTP_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DispatchMode {
    Fresh,
    Resume,
}

/// Immutable local inputs and explicit bounded-work controls.
#[derive(Clone, Debug)]
pub struct GatewayDispatchRequest {
    pub task_manifest_path: PathBuf,
    pub output_root: PathBuf,
    pub mode: DispatchMode,
    pub timeout: Duration,
    pub poll_interval: Duration,
    pub enqueue_batch_size: usize,
    pub result_batch_size: usize,
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    /// Delay between the single shared dependency-health probes while the
    /// gateway reports lake maintenance. No task is re-enqueued during this
    /// interval and the pause is excluded from the scientific wait budget.
    pub maintenance_probe_interval: Duration,
    /// Upper bound for one continuous infrastructure-maintenance pause. This
    /// is independent from the candidate-window completion timeout.
    pub maintenance_timeout: Duration,
}

impl GatewayDispatchRequest {
    pub fn bounded(
        task_manifest_path: impl Into<PathBuf>,
        output_root: impl Into<PathBuf>,
        mode: DispatchMode,
    ) -> Self {
        Self {
            task_manifest_path: task_manifest_path.into(),
            output_root: output_root.into(),
            mode,
            timeout: Duration::from_secs(900),
            poll_interval: Duration::from_millis(250),
            enqueue_batch_size: 128,
            result_batch_size: 128,
            max_request_bytes: MAX_HTTP_BYTES,
            max_response_bytes: MAX_HTTP_BYTES,
            maintenance_probe_interval: Duration::from_secs(30),
            maintenance_timeout: Duration::from_secs(12 * 60 * 60),
        }
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            !self.task_manifest_path.as_os_str().is_empty(),
            "task manifest path is required"
        );
        ensure!(
            !self.output_root.as_os_str().is_empty(),
            "dispatcher output root is required"
        );
        ensure!(
            self.timeout >= Duration::from_secs(1),
            "dispatcher timeout must be at least one second"
        );
        ensure!(
            self.poll_interval >= Duration::from_millis(10),
            "dispatcher poll interval must be at least 10 milliseconds"
        );
        ensure!(
            self.maintenance_probe_interval >= Duration::from_millis(10),
            "maintenance probe interval must be at least 10 milliseconds"
        );
        ensure!(
            self.maintenance_timeout >= self.maintenance_probe_interval,
            "maintenance timeout must cover at least one probe interval"
        );
        ensure!(
            (1..=MAX_SAFE_BATCH).contains(&self.enqueue_batch_size),
            "enqueue batch size must be between 1 and {MAX_SAFE_BATCH}"
        );
        ensure!(
            (1..=MAX_RESULT_BATCH).contains(&self.result_batch_size),
            "result batch size must be between 1 and {MAX_RESULT_BATCH}"
        );
        for (label, value) in [
            ("maximum request bytes", self.max_request_bytes),
            ("maximum response bytes", self.max_response_bytes),
        ] {
            ensure!(
                (1..=MAX_HTTP_BYTES).contains(&value),
                "{label} must be between 1 and {MAX_HTTP_BYTES}"
            );
        }
        Ok(())
    }
}

/// Runtime-only gateway authority.  The bearer token is never copied to the
/// task sidecar, result journal, checkpoint, or returned telemetry.
#[derive(Clone, Debug)]
pub struct GatewayRuntimeOptions {
    pub base_url: String,
    pub bearer_token: Option<String>,
    pub request_timeout: Duration,
}

impl GatewayRuntimeOptions {
    pub fn new(base_url: impl Into<String>, bearer_token: Option<String>) -> Self {
        Self {
            base_url: base_url.into(),
            bearer_token,
            request_timeout: Duration::from_secs(30),
        }
    }
}

/// Compact execution counters.  Batches are capped by the request and no rich
/// task/result values are retained after their durable transaction completes.
#[derive(Clone, Debug, Default)]
pub struct GatewayDispatchTelemetry {
    pub task_count: u64,
    pub completed_task_count: u64,
    pub enqueue_batches: u64,
    pub enqueued_task_count: u64,
    pub received_completion_count: u64,
    pub acknowledged_completion_count: u64,
    pub recovered_completion_count: u64,
    pub duplicate_redelivery_count: u64,
    pub rejected_completion_count: u64,
    pub peak_live_task_batch: usize,
    pub peak_live_completion_batch: usize,
    pub peak_completion_bytes: usize,
    pub checkpoint_compacted: bool,
    pub maintenance_activations: u64,
    pub maintenance_probe_count: u64,
    pub maintenance_pause_millis: u64,
    pub infrastructure_availability_events: u64,
}

impl GatewayDispatchTelemetry {
    fn to_value(&self) -> Value {
        json!({
            "schemaVersion": TELEMETRY_SCHEMA,
            "taskCount": self.task_count,
            "completedTaskCount": self.completed_task_count,
            "enqueueBatches": self.enqueue_batches,
            "enqueuedTaskCount": self.enqueued_task_count,
            "receivedCompletionCount": self.received_completion_count,
            "acknowledgedCompletionCount": self.acknowledged_completion_count,
            "recoveredCompletionCount": self.recovered_completion_count,
            "duplicateRedeliveryCount": self.duplicate_redelivery_count,
            "rejectedCompletionCount": self.rejected_completion_count,
            "peakLiveTaskBatch": self.peak_live_task_batch,
            "peakLiveCompletionBatch": self.peak_live_completion_batch,
            "peakCompletionBytes": self.peak_completion_bytes,
            "checkpointCompacted": self.checkpoint_compacted,
            "maintenanceActivations": self.maintenance_activations,
            "maintenanceProbeCount": self.maintenance_probe_count,
            "maintenancePauseMillis": self.maintenance_pause_millis,
            "infrastructureAvailabilityEvents": self.infrastructure_availability_events,
        })
    }
}

/// Exactly the three production gateway endpoint interactions.  A test may
/// supply a local fake implementation without starting a worker fleet.
pub trait GatewayClient {
    fn enqueue_tasks(&mut self, tasks: &[Value]) -> Result<Value>;
    fn read_results(&mut self, limit: usize) -> Result<Vec<Value>>;
    fn ack_results(&mut self, lease_ids: &[String]) -> Result<u64>;
}

/// Blocking HTTP client for `/tasks`, `/results`, and `/results/ack`.
pub struct HttpGatewayClient {
    client: Client,
    base_url: String,
    bearer_token: Option<String>,
    max_request_bytes: usize,
    max_response_bytes: usize,
}

impl HttpGatewayClient {
    pub fn new(runtime: &GatewayRuntimeOptions, request: &GatewayDispatchRequest) -> Result<Self> {
        let base_url = runtime.base_url.trim().trim_end_matches('/').to_owned();
        ensure!(!base_url.is_empty(), "gateway URL is required");
        let parsed = reqwest::Url::parse(&base_url).context("gateway URL is invalid")?;
        ensure!(
            matches!(parsed.scheme(), "http" | "https"),
            "gateway URL must use HTTP or HTTPS"
        );
        ensure!(
            runtime.request_timeout >= Duration::from_secs(1),
            "gateway request timeout must be at least one second"
        );
        let client = Client::builder()
            .timeout(runtime.request_timeout)
            .build()
            .context("build gateway HTTP client")?;
        Ok(Self {
            client,
            base_url,
            bearer_token: runtime
                .bearer_token
                .as_deref()
                .map(str::trim)
                .filter(|token| !token.is_empty())
                .map(ToOwned::to_owned),
            max_request_bytes: request.max_request_bytes,
            max_response_bytes: request.max_response_bytes,
        })
    }

    fn endpoint(&self, suffix: &str) -> String {
        format!("{}/{}", self.base_url, suffix)
    }

    fn with_auth(
        &self,
        request: reqwest::blocking::RequestBuilder,
    ) -> reqwest::blocking::RequestBuilder {
        if let Some(token) = &self.bearer_token {
            request.bearer_auth(token)
        } else {
            request
        }
    }

    fn post_json(&self, suffix: &str, body: Value) -> Result<Value> {
        let encoded = canonical_json_bytes(&body)?;
        ensure!(
            encoded.len() <= self.max_request_bytes,
            "gateway request exceeds configured byte bound"
        );
        let response = self
            .with_auth(
                self.client
                    .post(self.endpoint(suffix))
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(encoded),
            )
            .send()
            .with_context(|| format!("POST gateway {suffix}"))?;
        parse_http_json(response, self.max_response_bytes, suffix)
    }

    fn get_json(&self, suffix: &str, query: &[(&str, String)]) -> Result<Value> {
        let response = self
            .with_auth(self.client.get(self.endpoint(suffix)).query(query))
            .send()
            .with_context(|| format!("GET gateway {suffix}"))?;
        parse_http_json(response, self.max_response_bytes, suffix)
    }
}

#[derive(Debug)]
struct GatewayMaintenance {
    endpoint: String,
    status: StatusCode,
    retry_after: Option<Duration>,
}

impl fmt::Display for GatewayMaintenance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "gateway {} is unavailable for shared maintenance (HTTP {})",
            self.endpoint, self.status
        )
    }
}

impl StdError for GatewayMaintenance {}

fn maintenance_error(error: &anyhow::Error) -> Option<&GatewayMaintenance> {
    error.downcast_ref::<GatewayMaintenance>()
}

#[derive(Debug, Default)]
struct MaintenanceGate {
    active_since: Option<Instant>,
    total_pause: Duration,
}

impl MaintenanceGate {
    fn run<T>(
        &mut self,
        request: &GatewayDispatchRequest,
        telemetry: &mut GatewayDispatchTelemetry,
        operation: &str,
        mut attempt: impl FnMut() -> Result<T>,
    ) -> Result<T> {
        loop {
            match attempt() {
                Ok(value) => {
                    self.close(telemetry)?;
                    return Ok(value);
                }
                Err(error) => {
                    let Some(maintenance) = maintenance_error(&error) else {
                        return Err(error);
                    };
                    telemetry.infrastructure_availability_events = telemetry
                        .infrastructure_availability_events
                        .checked_add(1)
                        .ok_or_else(|| anyhow!("infrastructure availability event overflow"))?;
                    if self.active_since.is_none() {
                        self.active_since = Some(Instant::now());
                        telemetry.maintenance_activations = telemetry
                            .maintenance_activations
                            .checked_add(1)
                            .ok_or_else(|| anyhow!("maintenance activation count overflow"))?;
                    } else {
                        telemetry.maintenance_probe_count = telemetry
                            .maintenance_probe_count
                            .checked_add(1)
                            .ok_or_else(|| anyhow!("maintenance probe count overflow"))?;
                    }
                    let active = self
                        .active_since
                        .expect("maintenance gate was opened before measuring pause");
                    let elapsed = active.elapsed();
                    ensure!(
                        elapsed < request.maintenance_timeout,
                        "gateway {operation} remained in shared maintenance longer than {:?}",
                        request.maintenance_timeout
                    );
                    let remaining = request.maintenance_timeout.saturating_sub(elapsed);
                    let requested_delay = maintenance
                        .retry_after
                        .unwrap_or(request.maintenance_probe_interval)
                        .max(request.maintenance_probe_interval);
                    let delay = requested_delay.min(remaining);
                    ensure!(
                        !delay.is_zero(),
                        "gateway {operation} remained in shared maintenance longer than {:?}",
                        request.maintenance_timeout
                    );
                    thread::sleep(delay);
                }
            }
        }
    }

    fn close(&mut self, telemetry: &mut GatewayDispatchTelemetry) -> Result<()> {
        let Some(started) = self.active_since.take() else {
            return Ok(());
        };
        let pause = started.elapsed();
        self.total_pause = self
            .total_pause
            .checked_add(pause)
            .ok_or_else(|| anyhow!("maintenance pause duration overflow"))?;
        let millis = u64::try_from(pause.as_millis()).unwrap_or(u64::MAX);
        telemetry.maintenance_pause_millis = telemetry
            .maintenance_pause_millis
            .checked_add(millis)
            .ok_or_else(|| anyhow!("maintenance pause telemetry overflow"))?;
        Ok(())
    }

    fn paused_duration(&self) -> Duration {
        self.total_pause.saturating_add(
            self.active_since
                .map_or(Duration::ZERO, |started| started.elapsed()),
        )
    }
}

impl GatewayClient for HttpGatewayClient {
    fn enqueue_tasks(&mut self, tasks: &[Value]) -> Result<Value> {
        self.post_json("tasks", json!({"tasks": tasks}))
    }

    fn read_results(&mut self, limit: usize) -> Result<Vec<Value>> {
        let body = self.get_json("results", &[("limit", limit.to_string())])?;
        let map = object(&body, "gateway results response")?;
        let results = field(map, "results")?
            .as_array()
            .ok_or_else(|| anyhow!("gateway results response lacks results array"))?;
        ensure!(
            results.len() <= limit,
            "gateway returned more results than requested"
        );
        Ok(results.clone())
    }

    fn ack_results(&mut self, lease_ids: &[String]) -> Result<u64> {
        let body = self.post_json("results/ack", json!({"lease_ids": lease_ids}))?;
        unsigned(object(&body, "gateway acknowledgement response")?, "acked")
    }
}

fn parse_http_json(
    response: reqwest::blocking::Response,
    max_response_bytes: usize,
    endpoint: &str,
) -> Result<Value> {
    let status = response.status();
    if matches!(
        status,
        StatusCode::CONFLICT | StatusCode::SERVICE_UNAVAILABLE
    ) {
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_secs);
        return Err(GatewayMaintenance {
            endpoint: endpoint.to_owned(),
            status,
            retry_after,
        }
        .into());
    }
    if status != StatusCode::OK {
        bail!("gateway {endpoint} returned HTTP {status}");
    }
    let mut reader = response;
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 64 * 1024];
    loop {
        let count = reader.read(&mut chunk)?;
        if count == 0 {
            break;
        }
        ensure!(
            bytes
                .len()
                .checked_add(count)
                .is_some_and(|size| size <= max_response_bytes),
            "gateway {endpoint} response exceeds configured byte bound"
        );
        bytes.extend_from_slice(&chunk[..count]);
    }
    serde_json::from_slice(&bytes).with_context(|| format!("parse gateway {endpoint} response"))
}

/// Dispatch over the production HTTP gateway.
pub fn execute_gateway_dispatch(
    request: &GatewayDispatchRequest,
    runtime: &GatewayRuntimeOptions,
) -> Result<Value> {
    let mut client = HttpGatewayClient::new(runtime, request)?;
    execute_gateway_dispatch_with_client(request, &mut client)
}

/// Dispatch using a gateway implementation.  The injection point exists for
/// deterministic crash/restart tests and does not alter the production path.
pub fn execute_gateway_dispatch_with_client(
    request: &GatewayDispatchRequest,
    client: &mut dyn GatewayClient,
) -> Result<Value> {
    request.validate()?;
    fs::create_dir_all(&request.output_root).with_context(|| {
        format!(
            "create dispatcher output root: {}",
            request.output_root.display()
        )
    })?;
    let paths = DispatchPaths::new(&request.output_root);
    paths.ensure_directories()?;
    let (index, created_sidecar) = open_or_build_task_index(request, &paths)?;
    let mut journal = load_completion_journal(&paths, &index)?;
    if paths.execution_receipt.exists() {
        ensure!(
            matches!(request.mode, DispatchMode::Resume),
            "fresh dispatcher run cannot reuse a committed execution receipt"
        );
        return load_gateway_execution_receipt(&paths, &index, &journal, created_sidecar);
    }
    validate_mode_and_checkpoint(request, &paths, &index, &journal)?;
    let mut telemetry = GatewayDispatchTelemetry {
        task_count: index.task_count,
        completed_task_count: journal.len() as u64,
        ..Default::default()
    };
    let mut maintenance = MaintenanceGate::default();

    // A deterministic failure could have been acknowledged before the process
    // reached its completion journal append. Recover it from its independent
    // fsynced receipt before any enqueue can cause a redelivery.
    recover_durable_failures(&paths, &index, &mut journal, &mut telemetry)?;

    // Consume prior deliveries before enqueue. This preserves the controller's
    // no-second-economic-evaluation restart rule.
    drain_available_results(
        client,
        request,
        &paths,
        &index,
        &mut journal,
        &mut telemetry,
        &mut maintenance,
    )?;

    enqueue_missing_tasks(
        client,
        request,
        &paths,
        &index,
        &journal,
        &mut telemetry,
        &mut maintenance,
    )?;
    // Only productive completion-wait time consumes the scientific timeout.
    // A shared dependency outage has its own explicit maintenance bound and
    // never burns task attempts or forces a supervisor restart.
    let wait_started = Instant::now();
    let maintenance_before_wait = maintenance.paused_duration();
    while journal.len() as u64 != index.task_count {
        let maintenance_in_wait = maintenance
            .paused_duration()
            .saturating_sub(maintenance_before_wait);
        let productive_elapsed = wait_started.elapsed().saturating_sub(maintenance_in_wait);
        ensure!(
            productive_elapsed < request.timeout,
            "timed out waiting for gateway candidate-window completions"
        );
        let consumed = drain_available_results(
            client,
            request,
            &paths,
            &index,
            &mut journal,
            &mut telemetry,
            &mut maintenance,
        )?;
        if consumed == 0 {
            thread::sleep(request.poll_interval);
        }
    }
    compact_legacy_checkpoint_once(&paths, &index, &journal)?;
    telemetry.completed_task_count = journal.len() as u64;
    telemetry.checkpoint_compacted = true;
    let receipt = commit_gateway_execution_receipt(&paths, &index, &journal)?;
    Ok(json!({
        "schemaVersion": DISPATCH_SCHEMA,
        "authorityId": index.authority_id,
        "taskMatrixSha256": index.task_matrix_sha256,
        "taskCount": index.task_count,
        "completedTaskCount": journal.len(),
        "taskIndexRootSha256": index.root_sha256,
        "checkpointPath": paths.checkpoint.to_string_lossy(),
        "sidecarRoot": paths.sidecar_root.to_string_lossy(),
        "createdTaskSidecar": created_sidecar,
        "executionReceiptSha256": receipt.receipt_sha256,
        "semanticExecutionReceiptSha256": receipt.semantic_receipt_sha256,
        "executionReceiptPath": paths.execution_receipt.to_string_lossy(),
        "telemetry": telemetry.to_value(),
    }))
}

#[derive(Clone, Debug)]
struct DispatchPaths {
    sidecar_root: PathBuf,
    task_objects: PathBuf,
    task_index: PathBuf,
    task_index_root: PathBuf,
    completion_journal: PathBuf,
    result_inventory: PathBuf,
    result_inventory_root: PathBuf,
    results: PathBuf,
    failures: PathBuf,
    checkpoint: PathBuf,
    execution_receipt: PathBuf,
}

impl DispatchPaths {
    fn new(output_root: &Path) -> Self {
        let sidecar_root = output_root.join(SIDECAR_DIR);
        Self {
            task_objects: sidecar_root.join(TASK_OBJECT_DIR),
            task_index: sidecar_root.join(TASK_INDEX_NAME),
            task_index_root: sidecar_root.join(TASK_INDEX_ROOT_NAME),
            completion_journal: sidecar_root.join(COMPLETION_JOURNAL_NAME),
            result_inventory: sidecar_root.join(RESULT_INVENTORY_NAME),
            result_inventory_root: sidecar_root.join(RESULT_INVENTORY_ROOT_NAME),
            results: output_root.join(RESULTS_DIR),
            failures: output_root.join(FAILURES_DIR),
            checkpoint: output_root.join(CHECKPOINT_NAME),
            execution_receipt: sidecar_root.join(EXECUTION_RECEIPT_NAME),
            sidecar_root,
        }
    }

    fn ensure_directories(&self) -> Result<()> {
        for (path, label) in [
            (&self.sidecar_root, "dispatcher sidecar root"),
            (&self.task_objects, "dispatcher task object root"),
            (&self.results, "dispatcher result root"),
            (&self.failures, "dispatcher failure root"),
        ] {
            fs::create_dir_all(path)
                .with_context(|| format!("create {label}: {}", path.display()))?;
            ensure_real_directory(path, label)?;
        }
        Ok(())
    }

    fn task_object_path(&self, task_id: &str) -> Result<PathBuf> {
        safe_identifier_value(task_id, "task id")?;
        Ok(self.task_objects.join(format!("{task_id}.json")))
    }

    fn result_path(&self, task_id: &str) -> Result<PathBuf> {
        safe_identifier_value(task_id, "task id")?;
        Ok(self.results.join(format!("{task_id}.json.gz")))
    }

    fn failure_path(&self, task_id: &str) -> Result<PathBuf> {
        safe_identifier_value(task_id, "task id")?;
        Ok(self.failures.join(format!("{task_id}.json")))
    }
}

#[derive(Clone, Debug)]
struct GatewayExecutionReceipt {
    receipt_sha256: String,
    semantic_receipt_sha256: String,
}

fn gateway_runtime_role_sha256() -> Result<String> {
    Ok(canonical_sha256(&json!({
        "schemaVersion": "temporal_qd_native_gateway_runtime_role_v1",
        "runtimeEpoch": "temporal_qd_native_gateway_dispatch_epoch_v1",
        "binaryRole": "temporal-qd-gateway-dispatch",
    }))?)
}

#[derive(Clone, Debug)]
struct ResultInventoryRoot {
    sha256: String,
    size_bytes: u64,
    count: u64,
    root_sha256: String,
}

fn result_inventory_entry(
    ordinal: u64,
    task_id: &str,
    entry: &CompletionJournalEntry,
    paths: &DispatchPaths,
) -> Result<Value> {
    let record = object(&entry.record, "receipt completion record")?;
    let blob_sha256 = sha_file(&paths.result_path(task_id)?)?;
    ensure!(
        blob_sha256 == sha_field(record, "resultBlobSha256")?,
        "receipt result blob identity drifted"
    );
    let mut value = json!({
        "schemaVersion": RESULT_INVENTORY_ENTRY_SCHEMA,
        "ordinal": ordinal,
        "taskId": task_id,
        "taskSha256": entry.task_sha256,
        "completionSha256": entry.completion_sha256,
        "resultBlobSha256": blob_sha256,
        "resultSemanticSha256": sha_field(record, "resultSemanticSha256")?,
        "outcome": record.get("outcome").cloned().unwrap_or(Value::String("admitted".into())),
    });
    let entry_sha256 = canonical_sha256(&value)?;
    value["entrySha256"] = Value::String(entry_sha256);
    Ok(value)
}

/// Persist the O(T) inventory outside the receipt.  The receipt binds only a
/// small self-hashed descriptor, so Python can pass it through without parsing
/// candidate/result-scale material.
fn write_result_inventory(
    paths: &DispatchPaths,
    journal: &CompletionJournal,
) -> Result<ResultInventoryRoot> {
    let staging = paths.sidecar_root.join(".result-inventory.staging");
    if staging.exists() {
        ensure_real_file(&staging, "stale result inventory staging file")?;
        fs::remove_file(&staging).context("remove stale result inventory staging file")?;
    }
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging)?;
    let mut writer = BufWriter::new(file);
    for (ordinal, (task_id, entry)) in journal.iter().enumerate() {
        writer.write_all(&canonical_json_line(&result_inventory_entry(
            ordinal as u64,
            task_id,
            entry,
            paths,
        )?)?)?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    drop(writer);
    let sha256 = sha_file(&staging)?;
    let size_bytes = fs::metadata(&staging)?.len();
    publish_staging_once(&staging, &paths.result_inventory)?;
    let mut root = json!({
        "schemaVersion": RESULT_INVENTORY_ROOT_SCHEMA,
        "inventoryRelativePath": RESULT_INVENTORY_NAME,
        "inventorySha256": sha256,
        "inventorySizeBytes": size_bytes,
        "inventoryCount": journal.len(),
    });
    let root_sha256 = canonical_sha256(&root)?;
    root["resultInventoryRootSha256"] = Value::String(root_sha256.clone());
    write_immutable_canonical(&paths.result_inventory_root, &root)?;
    Ok(ResultInventoryRoot {
        sha256: sha_field(object(&root, "result inventory root")?, "inventorySha256")?,
        size_bytes,
        count: journal.len() as u64,
        root_sha256,
    })
}

fn load_result_inventory(
    paths: &DispatchPaths,
    journal: &CompletionJournal,
) -> Result<ResultInventoryRoot> {
    let root_value = read_canonical_line(&paths.result_inventory_root, "result inventory root")?;
    let root = object(&root_value, "result inventory root")?;
    exact_keys(
        root,
        &[
            "schemaVersion",
            "inventoryRelativePath",
            "inventorySha256",
            "inventorySizeBytes",
            "inventoryCount",
            "resultInventoryRootSha256",
        ],
        "result inventory root",
    )?;
    let root_sha256 = sha_field(root, "resultInventoryRootSha256")?;
    ensure!(
        text(root, "schemaVersion")? == RESULT_INVENTORY_ROOT_SCHEMA
            && text(root, "inventoryRelativePath")? == RESULT_INVENTORY_NAME
            && canonical_sha256_without_object_field(&root_value, "resultInventoryRootSha256")?
                == root_sha256,
        "result inventory root identity drifted"
    );
    ensure_real_file(&paths.result_inventory, "result inventory")?;
    ensure!(
        sha_file(&paths.result_inventory)? == sha_field(root, "inventorySha256")?
            && fs::metadata(&paths.result_inventory)?.len()
                == unsigned(root, "inventorySizeBytes")?,
        "result inventory bytes drifted"
    );
    let reader = BufReader::new(File::open(&paths.result_inventory)?);
    let mut count = 0_u64;
    let mut seen = BTreeSet::new();
    for line in reader.lines() {
        let line = line?;
        ensure!(!line.is_empty(), "result inventory has an empty line");
        let value: Value = serde_json::from_str(&line)?;
        ensure!(
            canonical_json_bytes(&value)? == line.as_bytes(),
            "result inventory line is not canonical"
        );
        let map = object(&value, "result inventory entry")?;
        exact_keys(
            map,
            &[
                "schemaVersion",
                "ordinal",
                "taskId",
                "taskSha256",
                "completionSha256",
                "resultBlobSha256",
                "resultSemanticSha256",
                "outcome",
                "entrySha256",
            ],
            "result inventory entry",
        )?;
        let task_id = safe_identifier(field(map, "taskId")?, "result inventory task id")?;
        ensure!(
            text(map, "schemaVersion")? == RESULT_INVENTORY_ENTRY_SCHEMA
                && unsigned(map, "ordinal")? == count
                && canonical_sha256_without_object_field(&value, "entrySha256")?
                    == sha_field(map, "entrySha256")?,
            "result inventory entry identity drifted"
        );
        let journal_entry = journal
            .get(&task_id)
            .ok_or_else(|| anyhow!("result inventory task is absent from completion journal"))?;
        let expected = result_inventory_entry(count, &task_id, journal_entry, paths)?;
        ensure!(
            value == expected,
            "result inventory/journal binding drifted"
        );
        ensure!(
            seen.insert(task_id),
            "result inventory has duplicate task ids"
        );
        count = count
            .checked_add(1)
            .ok_or_else(|| anyhow!("result inventory count overflow"))?;
    }
    ensure!(
        count == journal.len() as u64 && count == unsigned(root, "inventoryCount")?,
        "result inventory count drifted"
    );
    Ok(ResultInventoryRoot {
        sha256: sha_field(root, "inventorySha256")?,
        size_bytes: unsigned(root, "inventorySizeBytes")?,
        count,
        root_sha256,
    })
}

fn completion_journal_semantic_sha256(journal: &CompletionJournal) -> Result<String> {
    let mut rows = Vec::with_capacity(journal.len());
    for entry in journal.values() {
        let mut value = entry.to_value();
        value
            .as_object_mut()
            .ok_or_else(|| anyhow!("receipt completion journal entry must be an object"))?
            .get_mut("record")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| anyhow!("receipt completion journal record is missing"))?
            .remove("resultPath");
        rows.push(value);
    }
    Ok(canonical_sha256(&Value::Array(rows))?)
}

fn checkpoint_semantic_sha256(index: &TaskIndex, journal: &CompletionJournal) -> Result<String> {
    Ok(canonical_sha256(&json!({
        "schemaVersion": CHECKPOINT_SCHEMA,
        "authorityId": index.authority_id,
        "taskMatrixSha256": index.task_matrix_sha256,
        "completionJournalSemanticSha256": completion_journal_semantic_sha256(journal)?,
    }))?)
}

fn commit_gateway_execution_receipt(
    paths: &DispatchPaths,
    index: &TaskIndex,
    journal: &CompletionJournal,
) -> Result<GatewayExecutionReceipt> {
    ensure!(
        journal.len() as u64 == index.task_count && paths.checkpoint.is_file(),
        "gateway execution receipt requires complete journal and checkpoint"
    );
    let inventory = write_result_inventory(paths, journal)?;
    let completion_journal_semantic_sha256 = completion_journal_semantic_sha256(journal)?;
    let checkpoint_semantic_sha256 = checkpoint_semantic_sha256(index, journal)?;
    let semantic = json!({
        "schemaVersion": EXECUTION_RECEIPT_SCHEMA,
        "runtimeRoleSha256": gateway_runtime_role_sha256()?,
        "authorityId": index.authority_id,
        "taskMatrixSha256": index.task_matrix_sha256,
        "sourceTaskManifestSha256": index.source_manifest_sha256,
        "taskIndexRootSha256": index.root_sha256,
        "completionJournalSemanticSha256": completion_journal_semantic_sha256,
        "checkpointSemanticSha256": checkpoint_semantic_sha256,
        "taskCount": index.task_count,
        "completedTaskCount": journal.len(),
        "resultInventoryRootSha256": inventory.root_sha256,
        "resultInventorySha256": inventory.sha256,
        "resultInventorySizeBytes": inventory.size_bytes,
        "resultInventoryCount": inventory.count,
    });
    let semantic_receipt_sha256 = canonical_sha256(&semantic)?;
    let mut value = semantic;
    value["completionJournalSha256"] = Value::String(sha_file(&paths.completion_journal)?);
    value["checkpointSha256"] = Value::String(sha_file(&paths.checkpoint)?);
    value["semanticReceiptSha256"] = Value::String(semantic_receipt_sha256.clone());
    let receipt_sha256 = canonical_sha256(&value)?;
    value["receiptSha256"] = Value::String(receipt_sha256.clone());
    write_immutable_canonical(&paths.execution_receipt, &value)?;
    Ok(GatewayExecutionReceipt {
        receipt_sha256,
        semantic_receipt_sha256,
    })
}

fn load_gateway_execution_receipt(
    paths: &DispatchPaths,
    index: &TaskIndex,
    journal: &CompletionJournal,
    created_sidecar: bool,
) -> Result<Value> {
    let value = read_canonical_line(&paths.execution_receipt, "gateway execution receipt")?;
    let map = object(&value, "gateway execution receipt")?;
    exact_keys(
        map,
        &[
            "schemaVersion",
            "runtimeRoleSha256",
            "authorityId",
            "taskMatrixSha256",
            "sourceTaskManifestSha256",
            "taskIndexRootSha256",
            "completionJournalSemanticSha256",
            "checkpointSemanticSha256",
            "completionJournalSha256",
            "checkpointSha256",
            "taskCount",
            "completedTaskCount",
            "resultInventoryRootSha256",
            "resultInventorySha256",
            "resultInventorySizeBytes",
            "resultInventoryCount",
            "semanticReceiptSha256",
            "receiptSha256",
        ],
        "gateway execution receipt",
    )?;
    let receipt_sha256 = sha_field(map, "receiptSha256")?;
    ensure!(
        text(map, "schemaVersion")? == EXECUTION_RECEIPT_SCHEMA
            && canonical_sha256_without_object_field(&value, "receiptSha256")? == receipt_sha256
            && sha_field(map, "runtimeRoleSha256")? == gateway_runtime_role_sha256()?
            && sha_field(map, "authorityId")? == index.authority_id
            && sha_field(map, "taskMatrixSha256")? == index.task_matrix_sha256
            && sha_field(map, "sourceTaskManifestSha256")? == index.source_manifest_sha256
            && sha_field(map, "taskIndexRootSha256")? == index.root_sha256
            && sha_field(map, "completionJournalSemanticSha256")?
                == completion_journal_semantic_sha256(journal)?
            && sha_field(map, "checkpointSemanticSha256")?
                == checkpoint_semantic_sha256(index, journal)?
            && sha_field(map, "completionJournalSha256")? == sha_file(&paths.completion_journal)?
            && sha_field(map, "checkpointSha256")? == sha_file(&paths.checkpoint)?
            && unsigned(map, "taskCount")? == index.task_count
            && unsigned(map, "completedTaskCount")? == journal.len() as u64,
        "gateway execution receipt identity/output binding drifted"
    );
    let inventory = load_result_inventory(paths, journal)?;
    ensure!(
        sha_field(map, "resultInventoryRootSha256")? == inventory.root_sha256
            && sha_field(map, "resultInventorySha256")? == inventory.sha256
            && unsigned(map, "resultInventorySizeBytes")? == inventory.size_bytes
            && unsigned(map, "resultInventoryCount")? == inventory.count,
        "gateway receipt result inventory binding drifted"
    );
    let semantic = json!({
        "schemaVersion": EXECUTION_RECEIPT_SCHEMA,
        "runtimeRoleSha256": gateway_runtime_role_sha256()?,
        "authorityId": index.authority_id,
        "taskMatrixSha256": index.task_matrix_sha256,
        "sourceTaskManifestSha256": index.source_manifest_sha256,
        "taskIndexRootSha256": index.root_sha256,
        "completionJournalSemanticSha256": sha_field(map, "completionJournalSemanticSha256")?,
        "checkpointSemanticSha256": sha_field(map, "checkpointSemanticSha256")?,
        "taskCount": index.task_count,
        "completedTaskCount": journal.len(),
        "resultInventoryRootSha256": inventory.root_sha256,
        "resultInventorySha256": inventory.sha256,
        "resultInventorySizeBytes": inventory.size_bytes,
        "resultInventoryCount": inventory.count,
    });
    ensure!(
        canonical_sha256(&semantic)? == sha_field(map, "semanticReceiptSha256")?,
        "gateway execution receipt semantic identity drifted"
    );
    Ok(json!({
        "schemaVersion": DISPATCH_SCHEMA,
        "authorityId": index.authority_id,
        "taskMatrixSha256": index.task_matrix_sha256,
        "taskCount": index.task_count,
        "completedTaskCount": journal.len(),
        "taskIndexRootSha256": index.root_sha256,
        "checkpointPath": paths.checkpoint.to_string_lossy(),
        "sidecarRoot": paths.sidecar_root.to_string_lossy(),
        "createdTaskSidecar": created_sidecar,
        "executionReceiptSha256": receipt_sha256,
        "semanticExecutionReceiptSha256": sha_field(map, "semanticReceiptSha256")?,
        "executionReceiptPath": paths.execution_receipt.to_string_lossy(),
        // Telemetry is intentionally not a durable semantic claim.
        "telemetry": GatewayDispatchTelemetry { task_count: index.task_count, completed_task_count: journal.len() as u64, checkpoint_compacted: true, ..Default::default() }.to_value(),
    }))
}

#[derive(Clone, Debug)]
struct TaskIndexEntry {
    ordinal: u64,
    task_id: String,
    task_sha256: String,
    task_object_sha256: String,
    relative_path: String,
    entry_sha256: String,
}

impl TaskIndexEntry {
    fn new(
        ordinal: u64,
        task_id: String,
        task_sha256: String,
        task_object_sha256: String,
    ) -> Result<Self> {
        let relative_path = format!("{TASK_OBJECT_DIR}/{task_id}.json");
        let mut value = json!({
            "schemaVersion": TASK_INDEX_ENTRY_SCHEMA,
            "ordinal": ordinal,
            "taskId": task_id,
            "taskSha256": task_sha256,
            "taskObjectSha256": task_object_sha256,
            "relativePath": relative_path,
        });
        let entry_sha256 = canonical_sha256(&value)?;
        value["entrySha256"] = Value::String(entry_sha256.clone());
        Self::from_value(&value)
    }

    fn to_value(&self) -> Value {
        json!({
            "schemaVersion": TASK_INDEX_ENTRY_SCHEMA,
            "ordinal": self.ordinal,
            "taskId": self.task_id,
            "taskSha256": self.task_sha256,
            "taskObjectSha256": self.task_object_sha256,
            "relativePath": self.relative_path,
            "entrySha256": self.entry_sha256,
        })
    }

    fn from_value(value: &Value) -> Result<Self> {
        let map = object(value, "task index entry")?;
        exact_keys(
            map,
            &[
                "schemaVersion",
                "ordinal",
                "taskId",
                "taskSha256",
                "taskObjectSha256",
                "relativePath",
                "entrySha256",
            ],
            "task index entry",
        )?;
        ensure!(
            text(map, "schemaVersion")? == TASK_INDEX_ENTRY_SCHEMA,
            "task index entry schema is incompatible"
        );
        let entry_sha256 = sha_field(map, "entrySha256")?;
        ensure!(
            canonical_sha256_without_object_field(value, "entrySha256")? == entry_sha256,
            "task index entry self identity mismatch"
        );
        let task_id = safe_identifier(field(map, "taskId")?, "task index task id")?;
        let relative_path = text(map, "relativePath")?;
        ensure!(
            relative_path == format!("{TASK_OBJECT_DIR}/{task_id}.json"),
            "task index entry path is unsafe or inconsistent"
        );
        Ok(Self {
            ordinal: unsigned(map, "ordinal")?,
            task_id,
            task_sha256: sha_field(map, "taskSha256")?,
            task_object_sha256: sha_field(map, "taskObjectSha256")?,
            relative_path,
            entry_sha256,
        })
    }
}

#[derive(Clone, Debug)]
struct TaskIndex {
    authority_id: String,
    task_matrix_sha256: String,
    source_manifest_sha256: String,
    task_count: u64,
    index_sha256: String,
    index_size_bytes: u64,
    root_sha256: String,
    /// Compact per-task identities/path metadata only. Rich task values stay
    /// in individual sidecar objects and are loaded one at a time.
    entries: BTreeMap<String, TaskIndexEntry>,
}

impl TaskIndex {
    fn from_root_value(value: &Value) -> Result<Self> {
        let map = object(value, "task index root")?;
        exact_keys(
            map,
            &[
                "schemaVersion",
                "authorityId",
                "taskMatrixSha256",
                "sourceTaskManifestSha256",
                "taskCount",
                "taskIndexRelativePath",
                "taskIndexSha256",
                "taskIndexSizeBytes",
                "taskIndexRootSha256",
            ],
            "task index root",
        )?;
        ensure!(
            text(map, "schemaVersion")? == TASK_INDEX_SCHEMA,
            "task index root schema is incompatible"
        );
        ensure!(
            text(map, "taskIndexRelativePath")? == TASK_INDEX_NAME,
            "task index root path is incompatible"
        );
        let root_sha256 = sha_field(map, "taskIndexRootSha256")?;
        ensure!(
            canonical_sha256_without_object_field(value, "taskIndexRootSha256")? == root_sha256,
            "task index root self identity mismatch"
        );
        Ok(Self {
            authority_id: sha_field(map, "authorityId")?,
            task_matrix_sha256: sha_field(map, "taskMatrixSha256")?,
            source_manifest_sha256: sha_field(map, "sourceTaskManifestSha256")?,
            task_count: unsigned(map, "taskCount")?,
            index_sha256: sha_field(map, "taskIndexSha256")?,
            index_size_bytes: unsigned(map, "taskIndexSizeBytes")?,
            root_sha256,
            entries: BTreeMap::new(),
        })
    }
}

fn open_or_build_task_index(
    request: &GatewayDispatchRequest,
    paths: &DispatchPaths,
) -> Result<(TaskIndex, bool)> {
    if paths.task_index_root.exists() {
        ensure!(
            paths.task_index.is_file(),
            "dispatcher task sidecar root commits a missing task index"
        );
        let index = load_task_index(paths)?;
        if request.task_manifest_path.exists() {
            ensure!(
                sha_file(&request.task_manifest_path)? == index.source_manifest_sha256,
                "immutable task manifest bytes drifted from dispatcher sidecar"
            );
        } else {
            ensure!(
                matches!(request.mode, DispatchMode::Resume),
                "fresh dispatcher run requires the immutable task manifest"
            );
        }
        return Ok((index, false));
    }
    if paths.task_index.exists() {
        // An index without its immutable root can only be an interrupted first
        // sidecar publication. No completed journal can bind to it yet, so
        // rebuild from the task manifest while retaining/verifying individual
        // immutable task objects.
        ensure_real_file(&paths.task_index, "interrupted task index")?;
        fs::remove_file(&paths.task_index).context("discard interrupted task index")?;
    }
    ensure!(
        matches!(request.mode, DispatchMode::Fresh | DispatchMode::Resume),
        "dispatcher mode is invalid"
    );
    let index = build_task_index(&request.task_manifest_path, paths)?;
    Ok((index, true))
}

fn build_task_index(task_manifest_path: &Path, paths: &DispatchPaths) -> Result<TaskIndex> {
    let manifest_path = existing_file(task_manifest_path, "immutable task manifest")?;
    let source_manifest_sha256 = sha_file(&manifest_path)?;
    let staging = paths.sidecar_root.join(".task-index.staging");
    if staging.exists() {
        ensure_real_file(&staging, "stale task-index staging file")?;
        fs::remove_file(&staging).context("remove stale task-index staging file")?;
    }
    let index_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging)
        .context("create task-index staging file")?;
    let mut writer = BufWriter::new(index_file);
    let mut entries_seen = BTreeSet::new();
    let mut entries = BTreeMap::new();
    let mut entry_count = 0_u64;
    let header = stream_task_manifest(&manifest_path, |task| {
        let task_map = object(&task, "task manifest task")?;
        let task_id = safe_identifier(field(task_map, "task_id")?, "task manifest task id")?;
        ensure!(
            entries_seen.insert(task_id.clone()),
            "immutable task manifest has duplicate task ids"
        );
        let task_sha256 = canonical_sha256(&task)?;
        let task_object = task_object_value(&task, &task_sha256)?;
        let task_object_sha256 =
            sha_field(object(&task_object, "task object")?, "taskObjectSha256")?;
        let task_object_path = paths.task_object_path(&task_id)?;
        write_immutable_canonical(&task_object_path, &task_object)?;
        let entry = TaskIndexEntry::new(entry_count, task_id, task_sha256, task_object_sha256)?;
        writer.write_all(&canonical_json_line(&entry.to_value())?)?;
        ensure!(
            entries.insert(entry.task_id.clone(), entry).is_none(),
            "immutable task manifest task id is duplicated"
        );
        entry_count = entry_count
            .checked_add(1)
            .ok_or_else(|| anyhow!("task index entry count overflow"))?;
        Ok(())
    })?;
    ensure!(
        header.schema_version == TASK_MANIFEST_SCHEMA,
        "immutable task manifest schema is incompatible"
    );
    ensure!(
        header.task_count == entry_count,
        "immutable task manifest task count drifted"
    );
    writer.flush()?;
    writer
        .get_ref()
        .sync_all()
        .context("fsync task-index staging file")?;
    drop(writer);
    let index_sha256 = sha_file(&staging)?;
    let index_size_bytes = fs::metadata(&staging)?.len();
    let mut root_value = json!({
        "schemaVersion": TASK_INDEX_SCHEMA,
        "authorityId": header.authority_id,
        "taskMatrixSha256": header.task_matrix_sha256,
        "sourceTaskManifestSha256": source_manifest_sha256,
        "taskCount": header.task_count,
        "taskIndexRelativePath": TASK_INDEX_NAME,
        "taskIndexSha256": index_sha256,
        "taskIndexSizeBytes": index_size_bytes,
    });
    let root_sha256 = canonical_sha256(&root_value)?;
    root_value["taskIndexRootSha256"] = Value::String(root_sha256.clone());
    publish_staging_once(&staging, &paths.task_index)?;
    write_immutable_canonical(&paths.task_index_root, &root_value)?;
    let mut index = TaskIndex::from_root_value(&root_value)?;
    index.entries = entries;
    Ok(index)
}

fn load_task_index(paths: &DispatchPaths) -> Result<TaskIndex> {
    let root_value = read_canonical_line(&paths.task_index_root, "task index root")?;
    let mut index = TaskIndex::from_root_value(&root_value)?;
    ensure!(
        fs::metadata(&paths.task_index)?.len() == index.index_size_bytes,
        "task index byte count drifted"
    );
    ensure!(
        sha_file(&paths.task_index)? == index.index_sha256,
        "task index identity drifted"
    );
    let mut previous_ordinal = None;
    let mut count = 0_u64;
    stream_task_index(paths, |entry| {
        if let Some(previous) = previous_ordinal {
            ensure!(
                entry.ordinal == previous + 1,
                "task index ordinals are not contiguous"
            );
        } else {
            ensure!(entry.ordinal == 0, "task index must begin at ordinal zero");
        }
        previous_ordinal = Some(entry.ordinal);
        ensure!(
            index.entries.insert(entry.task_id.clone(), entry).is_none(),
            "task index contains duplicate task ids"
        );
        count += 1;
        Ok(())
    })?;
    ensure!(count == index.task_count, "task index task count drifted");
    Ok(index)
}

fn stream_task_index(
    paths: &DispatchPaths,
    mut visitor: impl FnMut(TaskIndexEntry) -> Result<()>,
) -> Result<()> {
    let file = File::open(&paths.task_index)
        .with_context(|| format!("open task index: {}", paths.task_index.display()))?;
    let reader = BufReader::new(file);
    for (line_number, line) in reader.lines().enumerate() {
        let line = line?;
        ensure!(!line.is_empty(), "task index has an empty line");
        let value: Value = serde_json::from_str(&line)
            .with_context(|| format!("parse task index line {}", line_number + 1))?;
        ensure!(
            canonical_json_bytes(&value)? == line.as_bytes(),
            "task index line {} is not canonical JSON",
            line_number + 1
        );
        visitor(TaskIndexEntry::from_value(&value)?)?;
    }
    Ok(())
}

fn task_object_value(task: &Value, task_sha256: &str) -> Result<Value> {
    let mut object = json!({
        "schemaVersion": TASK_OBJECT_SCHEMA,
        "task": task,
        "taskSha256": task_sha256,
    });
    let object_sha256 = canonical_sha256(&object)?;
    object["taskObjectSha256"] = Value::String(object_sha256);
    Ok(object)
}

fn load_task_object(paths: &DispatchPaths, entry: &TaskIndexEntry) -> Result<Value> {
    let path = paths.task_object_path(&entry.task_id)?;
    let value = read_canonical_line(&path, "task object")?;
    let map = object(&value, "task object")?;
    exact_keys(
        map,
        &["schemaVersion", "task", "taskSha256", "taskObjectSha256"],
        "task object",
    )?;
    ensure!(
        text(map, "schemaVersion")? == TASK_OBJECT_SCHEMA,
        "task object schema is incompatible"
    );
    ensure!(
        sha_field(map, "taskObjectSha256")? == entry.task_object_sha256
            && canonical_sha256_without_object_field(&value, "taskObjectSha256")?
                == entry.task_object_sha256,
        "task object self identity drifted"
    );
    let task = field(map, "task")?.clone();
    ensure!(
        canonical_sha256(&task)? == entry.task_sha256
            && sha_field(map, "taskSha256")? == entry.task_sha256,
        "task object task identity drifted"
    );
    let task_id = safe_identifier(
        field(object(&task, "task object task")?, "task_id")?,
        "task object task id",
    )?;
    ensure!(task_id == entry.task_id, "task object task id drifted");
    Ok(task)
}

#[derive(Clone, Debug)]
struct TaskManifestHeader {
    schema_version: String,
    authority_id: String,
    task_matrix_sha256: String,
    task_count: u64,
}

/// Deserialize just the manifest envelope while feeding each individual task
/// to `on_task`. The `tasks` array is never materialized as a `Vec<Value>`.
fn stream_task_manifest(
    path: &Path,
    on_task: impl FnMut(Value) -> Result<()>,
) -> Result<TaskManifestHeader> {
    let file =
        File::open(path).with_context(|| format!("open task manifest: {}", path.display()))?;
    let mut callback = on_task;
    let mut state = TaskManifestStreamState {
        callback: &mut callback,
        schema_version: None,
        authority_id: None,
        task_matrix_sha256: None,
        task_count: None,
        seen: BTreeSet::new(),
        task_hasher: {
            let mut hasher = Sha256::new();
            hasher.update(b"[");
            hasher
        },
        seen_task_count: 0,
    };
    let mut deserializer = serde_json::Deserializer::from_reader(BufReader::new(file));
    (&mut deserializer)
        .deserialize_map(TaskManifestVisitor { state: &mut state })
        .context("parse streaming task manifest")?;
    deserializer
        .end()
        .context("task manifest has trailing data")?;
    state.finish()
}

struct TaskManifestStreamState<'a, F>
where
    F: FnMut(Value) -> Result<()>,
{
    callback: &'a mut F,
    schema_version: Option<String>,
    authority_id: Option<String>,
    task_matrix_sha256: Option<String>,
    task_count: Option<u64>,
    seen: BTreeSet<String>,
    task_hasher: Sha256,
    seen_task_count: u64,
}

impl<F> TaskManifestStreamState<'_, F>
where
    F: FnMut(Value) -> Result<()>,
{
    fn finish(&self) -> Result<TaskManifestHeader> {
        let expected_keys: BTreeSet<String> = [
            "authorityId",
            "schemaVersion",
            "taskCount",
            "taskMatrixSha256",
            "tasks",
        ]
        .into_iter()
        .map(ToOwned::to_owned)
        .collect();
        ensure!(
            self.seen == expected_keys,
            "immutable task manifest fields are not exact"
        );
        let schema_version = self
            .schema_version
            .clone()
            .ok_or_else(|| anyhow!("task manifest lacks schemaVersion"))?;
        let authority_id = self
            .authority_id
            .clone()
            .ok_or_else(|| anyhow!("task manifest lacks authorityId"))?;
        let task_matrix_sha256 = self
            .task_matrix_sha256
            .clone()
            .ok_or_else(|| anyhow!("task manifest lacks taskMatrixSha256"))?;
        let task_count = self
            .task_count
            .ok_or_else(|| anyhow!("task manifest lacks taskCount"))?;
        ensure!(
            task_count == self.seen_task_count,
            "task manifest task count drifted"
        );
        let mut matrix_hasher = self.task_hasher.clone();
        matrix_hasher.update(b"]");
        let expected_matrix = digest_sha256_prefixed(matrix_hasher.finalize());
        ensure!(
            task_matrix_sha256 == expected_matrix,
            "task manifest task matrix identity drifted"
        );
        Ok(TaskManifestHeader {
            schema_version,
            authority_id,
            task_matrix_sha256,
            task_count,
        })
    }
}

struct TaskManifestVisitor<'state, 'callback, F>
where
    F: FnMut(Value) -> Result<()>,
{
    state: &'state mut TaskManifestStreamState<'callback, F>,
}

impl<'de, F> Visitor<'de> for TaskManifestVisitor<'_, '_, F>
where
    F: FnMut(Value) -> Result<()>,
{
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an immutable native task manifest object")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some(key) = map.next_key::<String>()? {
            if !self.state.seen.insert(key.clone()) {
                return Err(de::Error::custom(format!("task manifest repeats {key}")));
            }
            match key.as_str() {
                "schemaVersion" => {
                    self.state.schema_version = Some(map.next_value::<String>()?);
                }
                "authorityId" => {
                    self.state.authority_id = Some(map.next_value::<String>()?);
                }
                "taskMatrixSha256" => {
                    self.state.task_matrix_sha256 = Some(map.next_value::<String>()?);
                }
                "taskCount" => {
                    self.state.task_count = Some(map.next_value::<u64>()?);
                }
                "tasks" => {
                    map.next_value_seed(TaskArraySeed {
                        state: &mut *self.state,
                    })?;
                }
                _ => {
                    let _: IgnoredAny = map.next_value()?;
                    return Err(de::Error::custom(format!(
                        "task manifest has unexpected {key}"
                    )));
                }
            }
        }
        Ok(())
    }
}

struct TaskArraySeed<'state, 'callback, F>
where
    F: FnMut(Value) -> Result<()>,
{
    state: &'state mut TaskManifestStreamState<'callback, F>,
}

impl<'de, F> DeserializeSeed<'de> for TaskArraySeed<'_, '_, F>
where
    F: FnMut(Value) -> Result<()>,
{
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(TaskArrayVisitor { state: self.state })
    }
}

struct TaskArrayVisitor<'state, 'callback, F>
where
    F: FnMut(Value) -> Result<()>,
{
    state: &'state mut TaskManifestStreamState<'callback, F>,
}

impl<'de, F> Visitor<'de> for TaskArrayVisitor<'_, '_, F>
where
    F: FnMut(Value) -> Result<()>,
{
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a streamed immutable task array")
    }

    fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(task) = sequence.next_element::<Value>()? {
            let canonical = canonical_json_bytes(&task).map_err(de::Error::custom)?;
            if self.state.seen_task_count > 0 {
                self.state.task_hasher.update(b",");
            }
            self.state.task_hasher.update(&canonical);
            (self.state.callback)(task).map_err(de::Error::custom)?;
            self.state.seen_task_count = self
                .state
                .seen_task_count
                .checked_add(1)
                .ok_or_else(|| de::Error::custom("task manifest task count overflow"))?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct CompletionJournalEntry {
    ordinal: u64,
    task_id: String,
    task_sha256: String,
    completion_sha256: String,
    record: Value,
    entry_sha256: String,
}

impl CompletionJournalEntry {
    fn new(
        ordinal: u64,
        entry: &TaskIndexEntry,
        completion_sha256: String,
        record: Value,
    ) -> Result<Self> {
        let mut value = json!({
            "schemaVersion": COMPLETION_JOURNAL_SCHEMA,
            "ordinal": ordinal,
            "taskId": entry.task_id,
            "taskSha256": entry.task_sha256,
            "completionSha256": completion_sha256,
            "record": record,
        });
        let entry_sha256 = canonical_sha256(&value)?;
        value["entrySha256"] = Value::String(entry_sha256.clone());
        Self::from_value(&value)
    }

    fn to_value(&self) -> Value {
        json!({
            "schemaVersion": COMPLETION_JOURNAL_SCHEMA,
            "ordinal": self.ordinal,
            "taskId": self.task_id,
            "taskSha256": self.task_sha256,
            "completionSha256": self.completion_sha256,
            "record": self.record,
            "entrySha256": self.entry_sha256,
        })
    }

    fn from_value(value: &Value) -> Result<Self> {
        let map = object(value, "completion journal entry")?;
        exact_keys(
            map,
            &[
                "schemaVersion",
                "ordinal",
                "taskId",
                "taskSha256",
                "completionSha256",
                "record",
                "entrySha256",
            ],
            "completion journal entry",
        )?;
        ensure!(
            text(map, "schemaVersion")? == COMPLETION_JOURNAL_SCHEMA,
            "completion journal entry schema is incompatible"
        );
        let entry_sha256 = sha_field(map, "entrySha256")?;
        ensure!(
            canonical_sha256_without_object_field(value, "entrySha256")? == entry_sha256,
            "completion journal entry self identity mismatch"
        );
        Ok(Self {
            ordinal: unsigned(map, "ordinal")?,
            task_id: safe_identifier(field(map, "taskId")?, "completion journal task id")?,
            task_sha256: sha_field(map, "taskSha256")?,
            completion_sha256: sha_field(map, "completionSha256")?,
            record: field(map, "record")?.clone(),
            entry_sha256,
        })
    }
}

type CompletionJournal = BTreeMap<String, CompletionJournalEntry>;

fn load_completion_journal(paths: &DispatchPaths, index: &TaskIndex) -> Result<CompletionJournal> {
    if !paths.completion_journal.exists() {
        return Ok(BTreeMap::new());
    }
    ensure_real_file(&paths.completion_journal, "completion journal")?;
    let file = File::open(&paths.completion_journal)?;
    let reader = BufReader::new(file);
    let mut journal = BTreeMap::new();
    let mut ordinal = 0_u64;
    for (line_number, line) in reader.lines().enumerate() {
        let line = line?;
        ensure!(!line.is_empty(), "completion journal has an empty line");
        let value: Value = serde_json::from_str(&line)
            .with_context(|| format!("parse completion journal line {}", line_number + 1))?;
        ensure!(
            canonical_json_bytes(&value)? == line.as_bytes(),
            "completion journal line {} is not canonical JSON",
            line_number + 1
        );
        let entry = CompletionJournalEntry::from_value(&value)?;
        ensure!(
            entry.ordinal == ordinal,
            "completion journal ordinals are not contiguous"
        );
        let index_entry = find_index_entry(paths, index, &entry.task_id)?;
        ensure!(
            entry.task_sha256 == index_entry.task_sha256,
            "completion journal task identity drifted"
        );
        validate_completion_record(paths, &index_entry, &entry.record)?;
        ensure!(
            journal.insert(entry.task_id.clone(), entry).is_none(),
            "completion journal contains duplicate task ids"
        );
        ordinal = ordinal
            .checked_add(1)
            .ok_or_else(|| anyhow!("completion journal ordinal overflow"))?;
    }
    ensure!(
        journal.len() as u64 <= index.task_count,
        "completion journal exceeds immutable task count"
    );
    Ok(journal)
}

fn append_completion_journal(
    paths: &DispatchPaths,
    journal: &mut CompletionJournal,
    entry: CompletionJournalEntry,
) -> Result<()> {
    ensure!(
        !journal.contains_key(&entry.task_id),
        "refusing to append a duplicate completion journal task"
    );
    let ordinal = journal.len() as u64;
    ensure!(
        entry.ordinal == ordinal,
        "completion journal append ordinal drifted"
    );
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.completion_journal)
        .with_context(|| {
            format!(
                "open completion journal: {}",
                paths.completion_journal.display()
            )
        })?;
    file.write_all(&canonical_json_line(&entry.to_value())?)?;
    file.flush()?;
    file.sync_data()
        .context("fsync completion journal append")?;
    journal.insert(entry.task_id.clone(), entry);
    Ok(())
}

fn find_index_entry(
    _paths: &DispatchPaths,
    index: &TaskIndex,
    task_id: &str,
) -> Result<TaskIndexEntry> {
    let entry = index
        .entries
        .get(task_id)
        .cloned()
        .ok_or_else(|| anyhow!("task is not in immutable task index: {task_id}"))?;
    ensure!(
        entry.ordinal < index.task_count,
        "task index ordinal exceeds task count"
    );
    Ok(entry)
}

fn validate_completion_record(
    paths: &DispatchPaths,
    entry: &TaskIndexEntry,
    record: &Value,
) -> Result<()> {
    let record_map = object(record, "completion record")?;
    let mut expected: BTreeSet<&str> = [
        "resultSha256",
        "resultPath",
        "candidateId",
        "resultCodec",
        "resultSemanticSha256",
        "resultSemanticSizeBytes",
        "resultUncompressedSha256",
        "resultUncompressedSizeBytes",
        "resultBlobSha256",
        "resultBlobSizeBytes",
    ]
    .into_iter()
    .collect();
    let rejected = record_map.get("outcome") == Some(&Value::String("rejected".into()));
    if rejected {
        expected.insert("outcome");
        expected.insert("rejectionCode");
    }
    ensure!(
        record_map.len() == expected.len()
            && expected.iter().all(|key| record_map.contains_key(*key)),
        "completion record fields are not exact"
    );
    for key in [
        "resultSha256",
        "resultSemanticSha256",
        "resultUncompressedSha256",
        "resultBlobSha256",
    ] {
        sha_field(record_map, key)?;
    }
    for key in [
        "resultSemanticSizeBytes",
        "resultUncompressedSizeBytes",
        "resultBlobSizeBytes",
    ] {
        unsigned(record_map, key)?;
    }
    ensure!(
        text(record_map, "resultCodec")? == "gzip-json-v1",
        "completion record codec is incompatible"
    );
    ensure!(
        text(record_map, "resultPath")? == paths.result_path(&entry.task_id)?.to_string_lossy(),
        "completion record result path drifted"
    );
    let task = load_task_object(paths, entry)?;
    let task_payload = object(
        field(object(&task, "completion record task")?, "payload")?,
        "completion record task payload",
    )?;
    ensure!(
        field(record_map, "candidateId")? == field(task_payload, "candidate_id")?,
        "completion record candidate identity drifted"
    );
    if rejected {
        ensure!(
            text(record_map, "rejectionCode")? == "aligned_scoring_warmup_insufficient"
                || text(record_map, "rejectionCode")? == "duplicate_break_even_execution_invariant",
            "completion record rejection code is incompatible"
        );
    }
    let result_path = paths.result_path(&entry.task_id)?;
    let (material, metadata) = read_gzip_json_value(&result_path)?;
    ensure!(
        canonical_sha256(&material)? == sha_field(record_map, "resultSha256")?
            && metadata.semantic_sha256 == sha_field(record_map, "resultSemanticSha256")?
            && metadata.semantic_size_bytes == unsigned(record_map, "resultSemanticSizeBytes")?
            && metadata.uncompressed_sha256 == sha_field(record_map, "resultUncompressedSha256")?
            && metadata.uncompressed_size_bytes
                == unsigned(record_map, "resultUncompressedSizeBytes")?
            && metadata.blob_sha256 == sha_field(record_map, "resultBlobSha256")?
            && metadata.blob_size_bytes == unsigned(record_map, "resultBlobSizeBytes")?,
        "completion record deterministic result representation drifted"
    );
    let admission = admit_candidate_window_task_result(&task, &material)?;
    ensure!(
        (rejected && admission == CandidateWindowResultAdmission::Rejected)
            || (!rejected && admission == CandidateWindowResultAdmission::Admitted),
        "completion record outcome does not match admitted material"
    );
    if rejected {
        ensure!(
            field(
                object(
                    field(
                        object(&material, "rejected completion material")?,
                        "evaluation_outcome"
                    )?,
                    "rejected completion material outcome",
                )?,
                "reason_code",
            )? == field(record_map, "rejectionCode")?,
            "completion record rejection code drifted"
        );
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct ResultCodecMetadata {
    semantic_sha256: String,
    semantic_size_bytes: u64,
    uncompressed_sha256: String,
    uncompressed_size_bytes: u64,
    blob_sha256: String,
    blob_size_bytes: u64,
}

impl ResultCodecMetadata {
    fn record_fields(&self) -> Value {
        json!({
            "resultCodec": "gzip-json-v1",
            "resultSemanticSha256": self.semantic_sha256,
            "resultSemanticSizeBytes": self.semantic_size_bytes,
            "resultUncompressedSha256": self.uncompressed_sha256,
            "resultUncompressedSizeBytes": self.uncompressed_size_bytes,
            "resultBlobSha256": self.blob_sha256,
            "resultBlobSizeBytes": self.blob_size_bytes,
        })
    }
}

fn encode_gzip_json(value: &Value) -> Result<(Vec<u8>, ResultCodecMetadata)> {
    let semantic = canonical_json_bytes(value)?;
    let uncompressed = python_pretty_json_line(value, JsonNewline::Lf)?;
    let mut encoder = GzBuilder::new()
        .mtime(0)
        .operating_system(255)
        .write(Vec::new(), Compression::best());
    encoder.write_all(&uncompressed)?;
    let blob = encoder
        .finish()
        .context("finish deterministic result gzip")?;
    Ok((
        blob.clone(),
        ResultCodecMetadata {
            semantic_sha256: sha256_prefixed(&semantic),
            semantic_size_bytes: semantic.len() as u64,
            uncompressed_sha256: sha256_prefixed(&uncompressed),
            uncompressed_size_bytes: uncompressed.len() as u64,
            blob_sha256: sha256_prefixed(&blob),
            blob_size_bytes: blob.len() as u64,
        },
    ))
}

fn write_gzip_json_once(path: &Path, value: &Value) -> Result<ResultCodecMetadata> {
    let (blob, metadata) = encode_gzip_json(value)?;
    if path.exists() {
        ensure!(
            fs::read(path)? == blob,
            "refusing to overwrite divergent immutable result blob: {}",
            path.display()
        );
    } else {
        let parent = path
            .parent()
            .ok_or_else(|| anyhow!("result path has no parent"))?;
        let staging = parent.join(format!(
            ".{}.{}.staging",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("result"),
            std::process::id()
        ));
        if staging.exists() {
            fs::remove_file(&staging).context("remove stale result staging file")?;
        }
        let mut staging_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staging)
            .with_context(|| format!("create result staging file: {}", staging.display()))?;
        staging_file.write_all(&blob)?;
        staging_file.flush()?;
        staging_file
            .sync_all()
            .context("fsync result staging file")?;
        drop(staging_file);
        match fs::hard_link(&staging, path) {
            Ok(()) => {}
            Err(error) if path.exists() => {
                ensure!(
                    fs::read(path)? == blob,
                    "refusing to overwrite divergent immutable result blob: {}",
                    path.display()
                );
                let _ = error;
            }
            Err(error) => {
                return Err(error).with_context(|| format!("publish result: {}", path.display()));
            }
        }
        fs::remove_file(&staging).context("remove result staging hard link")?;
    }
    let (_, verified) = read_gzip_json_value(path)?;
    ensure!(
        verified.semantic_sha256 == metadata.semantic_sha256
            && verified.semantic_size_bytes == metadata.semantic_size_bytes
            && verified.uncompressed_sha256 == metadata.uncompressed_sha256
            && verified.uncompressed_size_bytes == metadata.uncompressed_size_bytes
            && verified.blob_sha256 == metadata.blob_sha256
            && verified.blob_size_bytes == metadata.blob_size_bytes,
        "persisted deterministic result representation drifted"
    );
    Ok(metadata)
}

fn read_gzip_json_value(path: &Path) -> Result<(Value, ResultCodecMetadata)> {
    let blob = fs::read(path).with_context(|| format!("read result blob: {}", path.display()))?;
    let mut decoder = GzDecoder::new(blob.as_slice());
    let mut uncompressed = Vec::new();
    decoder
        .read_to_end(&mut uncompressed)
        .with_context(|| format!("inflate result blob: {}", path.display()))?;
    let mut inner = decoder.into_inner();
    ensure!(
        inner.read(&mut [0_u8; 1])? == 0,
        "result blob has trailing gzip bytes"
    );
    let value: Value = serde_json::from_slice(&uncompressed)
        .with_context(|| format!("parse result JSON: {}", path.display()))?;
    ensure!(
        python_pretty_json_line(&value, JsonNewline::Lf)? == uncompressed,
        "result blob uncompressed JSON is not Python-compatible deterministic pretty JSON"
    );
    let (expected_blob, metadata) = encode_gzip_json(&value)?;
    ensure!(
        expected_blob == blob,
        "result blob is not canonical deterministic gzip"
    );
    Ok((value, metadata))
}

fn completion_record(
    paths: &DispatchPaths,
    entry: &TaskIndexEntry,
    material: &Value,
) -> Result<Value> {
    let result_path = paths.result_path(&entry.task_id)?;
    let metadata = write_gzip_json_once(&result_path, material)?;
    let task = load_task_object(paths, entry)?;
    let payload = object(
        field(object(&task, "completion record task")?, "payload")?,
        "completion record task payload",
    )?;
    let mut record = json!({
        "resultSha256": metadata.semantic_sha256,
        "resultPath": result_path.to_string_lossy(),
        "candidateId": field(payload, "candidate_id")?,
    });
    let codec_field_value = metadata.record_fields();
    let codec_fields = object(&codec_field_value, "result codec fields")?;
    for (key, value) in codec_fields {
        record[key] = value.clone();
    }
    if object(material, "completion material")?.get("schema_version")
        == Some(&Value::String(REJECTED_SCHEMA.into()))
    {
        record["outcome"] = Value::String("rejected".into());
        record["rejectionCode"] = field(
            object(
                field(object(material, "rejected material")?, "evaluation_outcome")?,
                "rejected evaluation outcome",
            )?,
            "reason_code",
        )?
        .clone();
    }
    validate_completion_record(paths, entry, &record)?;
    Ok(record)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeterministicRejection {
    Warmup,
    BreakEven,
}

impl DeterministicRejection {
    fn reason_code(self) -> &'static str {
        match self {
            Self::Warmup => "aligned_scoring_warmup_insufficient",
            Self::BreakEven => "duplicate_break_even_execution_invariant",
        }
    }

    fn replay_executed(self) -> bool {
        matches!(self, Self::BreakEven)
    }
}

#[derive(Clone, Debug)]
struct FailureReceipt {
    task_id: String,
    task_sha256: String,
    completion: Value,
    completion_sha256: String,
    classification: Option<DeterministicRejection>,
    error: Option<Value>,
}

impl FailureReceipt {
    fn from_completion(entry: &TaskIndexEntry, completion: &Value) -> Result<Self> {
        let (classification, error) = classify_deterministic_rejection(completion)?;
        Ok(Self {
            task_id: entry.task_id.clone(),
            task_sha256: entry.task_sha256.clone(),
            completion: completion.clone(),
            completion_sha256: canonical_sha256(completion)?,
            classification,
            error,
        })
    }

    fn to_value(&self) -> Result<Value> {
        let mut value = json!({
            "schemaVersion": FAILURE_RECEIPT_SCHEMA,
            "taskId": self.task_id,
            "taskSha256": self.task_sha256,
            "completion": self.completion,
            "completionSha256": self.completion_sha256,
            "classification": match self.classification {
                Some(DeterministicRejection::Warmup) => Value::String("warmup".into()),
                Some(DeterministicRejection::BreakEven) => Value::String("break_even".into()),
                None => Value::Null,
            },
            "workerError": self.error,
        });
        let sha = canonical_sha256(&value)?;
        value["failureReceiptSha256"] = Value::String(sha);
        Ok(value)
    }

    fn from_value(value: &Value) -> Result<Self> {
        let map = object(value, "failure receipt")?;
        exact_keys(
            map,
            &[
                "schemaVersion",
                "taskId",
                "taskSha256",
                "completion",
                "completionSha256",
                "classification",
                "workerError",
                "failureReceiptSha256",
            ],
            "failure receipt",
        )?;
        ensure!(
            text(map, "schemaVersion")? == FAILURE_RECEIPT_SCHEMA,
            "failure receipt schema is incompatible"
        );
        let supplied = sha_field(map, "failureReceiptSha256")?;
        ensure!(
            canonical_sha256_without_object_field(value, "failureReceiptSha256")? == supplied,
            "failure receipt self identity mismatch"
        );
        let completion = field(map, "completion")?.clone();
        let completion_sha256 = sha_field(map, "completionSha256")?;
        ensure!(
            canonical_sha256(&completion)? == completion_sha256,
            "failure receipt completion identity mismatch"
        );
        let classification = match field(map, "classification")? {
            Value::String(value) if value == "warmup" => Some(DeterministicRejection::Warmup),
            Value::String(value) if value == "break_even" => {
                Some(DeterministicRejection::BreakEven)
            }
            Value::Null => None,
            _ => bail!("failure receipt classification is incompatible"),
        };
        let error = match field(map, "workerError")? {
            Value::Null => None,
            value => Some(value.clone()),
        };
        ensure!(
            classification.is_some() == error.is_some(),
            "failure receipt classification/error presence drifted"
        );
        Ok(Self {
            task_id: safe_identifier(field(map, "taskId")?, "failure receipt task id")?,
            task_sha256: sha_field(map, "taskSha256")?,
            completion,
            completion_sha256,
            classification,
            error,
        })
    }
}

fn classify_deterministic_rejection(
    completion: &Value,
) -> Result<(Option<DeterministicRejection>, Option<Value>)> {
    let completion_map = object(completion, "failed worker completion")?;
    if let Some(nested) = completion_map.get("result").and_then(Value::as_object) {
        if nested.get("status") == Some(&Value::String("failed".into()))
            && nested.get("error_type")
                == Some(&Value::String(
                    "AlignedScoringWarmupInsufficientError".into(),
                ))
        {
            return Ok((
                Some(DeterministicRejection::Warmup),
                Some(Value::Object(nested.clone())),
            ));
        }
        if nested.get("status") == Some(&Value::String("failed".into()))
            && nested.get("error_type")
                == Some(&Value::String("TemporalExecutionInvariantError".into()))
            && nested.get("error")
                == Some(&Value::String(
                    "TemporalExecutionInvariantError: break-even may be applied only once".into(),
                ))
        {
            return Ok((
                Some(DeterministicRejection::BreakEven),
                Some(Value::Object(nested.clone())),
            ));
        }
    }
    if let Some(error) = completion_map.get("error").and_then(Value::as_object)
        && error.get("type")
            == Some(&Value::String(
                "AlignedScoringWarmupInsufficientError".into(),
            ))
    {
        return Ok((
            Some(DeterministicRejection::Warmup),
            Some(Value::Object(error.clone())),
        ));
    }
    Ok((None, None))
}

fn write_failure_receipt_once(paths: &DispatchPaths, receipt: &FailureReceipt) -> Result<()> {
    write_immutable_canonical(&paths.failure_path(&receipt.task_id)?, &receipt.to_value()?)
}

fn read_failure_receipt(paths: &DispatchPaths, entry: &TaskIndexEntry) -> Result<FailureReceipt> {
    let value = read_canonical_line(&paths.failure_path(&entry.task_id)?, "failure receipt")?;
    let receipt = FailureReceipt::from_value(&value)?;
    ensure!(
        receipt.task_id == entry.task_id && receipt.task_sha256 == entry.task_sha256,
        "failure receipt task binding drifted"
    );
    require_completion_routing(&load_task_object(paths, entry)?, &receipt.completion)?;
    Ok(receipt)
}

fn rejected_material(
    task: &Value,
    completion: &Value,
    classification: DeterministicRejection,
    worker_error: &Value,
) -> Result<Value> {
    require_completion_routing(task, completion)?;
    let completion_map = object(completion, "failed worker completion")?;
    let lease_id = safe_identifier(field(completion_map, "lease_id")?, "completion lease id")?;
    let attempt_id = safe_identifier(
        field(completion_map, "attempt_id")?,
        "completion attempt id",
    )?;
    let payload = object(
        field(object(task, "rejection task")?, "payload")?,
        "rejection task payload",
    )?;
    let mut outcome = json!({
        "schema_version": if classification.replay_executed() {
            "temporal_candidate_window_rejection_v2"
        } else {
            "temporal_candidate_window_rejection_v1"
        },
        "disposition": "rejected",
        "reason_code": classification.reason_code(),
        "replay_executed": classification.replay_executed(),
        "worker_attempt_id": attempt_id,
        "worker_lease_id": lease_id,
        "worker_error": worker_error,
        "worker_error_sha256": canonical_sha256(worker_error)?,
        "worker_completion_sha256": canonical_sha256(completion)?,
    });
    if classification.replay_executed() {
        outcome["replay_completed"] = Value::Bool(false);
    }
    let evidence = object(field(payload, "evidence_plan")?, "rejection evidence plan")?;
    let mut material = json!({
        "schema_version": REJECTED_SCHEMA,
        "task_kind": TASK_KIND,
        "job_id": field(payload, "job_id")?,
        "authority_id": field(payload, "authority_id")?,
        "candidate_id": field(payload, "candidate_id")?,
        "evidence_plan_id": field(evidence, "plan_id")?,
        "lake_window_semantic_sha256": field(payload, "lake_window_semantic_sha256")?,
        "shared_observation_stream_id": field(payload, "shared_observation_stream_id")?,
        "analysis_window_start": field(payload, "analysis_window_start")?,
        "analysis_window_end": field(payload, "analysis_window_end")?,
        "evaluation_outcome": outcome,
    });
    material["artifact_sha256"] = Value::String(canonical_sha256(&material)?);
    let mut size = 1_u64;
    for _ in 0..16 {
        material["artifact_size_bytes"] = Value::Number(size.into());
        let next = canonical_json_bytes(&material)?.len() as u64;
        if next == size {
            return Ok(material);
        }
        size = next;
    }
    bail!("could not stabilize rejected material byte count")
}

fn require_completion_routing(task: &Value, completion: &Value) -> Result<()> {
    let task_map = object(task, "immutable task")?;
    let completion_map = object(completion, "gateway completion")?;
    ensure!(
        field(completion_map, "task_id")? == field(task_map, "task_id")?
            && field(completion_map, "lane_id")? == field(task_map, "lane_id")?
            && field(completion_map, "attempt_id")? == field(task_map, "attempt_id")?,
        "completion routing identity mismatch"
    );
    safe_identifier(field(completion_map, "lease_id")?, "completion lease id")?;
    Ok(())
}

fn recover_durable_failures(
    paths: &DispatchPaths,
    index: &TaskIndex,
    journal: &mut CompletionJournal,
    telemetry: &mut GatewayDispatchTelemetry,
) -> Result<()> {
    for entry in index.entries.values() {
        if journal.contains_key(&entry.task_id) {
            continue;
        }
        let failure_path = paths.failure_path(&entry.task_id)?;
        if !failure_path.exists() {
            continue;
        }
        let receipt = read_failure_receipt(paths, entry)?;
        let Some(classification) = receipt.classification else {
            bail!(
                "previously acknowledged unclassified worker failure for {} requires operator review",
                entry.task_id
            );
        };
        let worker_error = receipt
            .error
            .as_ref()
            .ok_or_else(|| anyhow!("deterministic failure receipt lacks worker error"))?;
        let task = load_task_object(paths, entry)?;
        let material = rejected_material(&task, &receipt.completion, classification, worker_error)?;
        ensure!(
            admit_candidate_window_task_result(&task, &material)?
                == CandidateWindowResultAdmission::Rejected,
            "recovered deterministic failure did not produce an admitted rejection"
        );
        let record = completion_record(paths, entry, &material)?;
        append_completion_journal(
            paths,
            journal,
            CompletionJournalEntry::new(
                journal.len() as u64,
                entry,
                receipt.completion_sha256,
                record,
            )?,
        )?;
        telemetry.recovered_completion_count = telemetry
            .recovered_completion_count
            .checked_add(1)
            .ok_or_else(|| anyhow!("recovered completion count overflow"))?;
        telemetry.rejected_completion_count = telemetry
            .rejected_completion_count
            .checked_add(1)
            .ok_or_else(|| anyhow!("rejected completion count overflow"))?;
    }
    telemetry.completed_task_count = journal.len() as u64;
    Ok(())
}

fn drain_available_results(
    client: &mut dyn GatewayClient,
    request: &GatewayDispatchRequest,
    paths: &DispatchPaths,
    index: &TaskIndex,
    journal: &mut CompletionJournal,
    telemetry: &mut GatewayDispatchTelemetry,
    maintenance: &mut MaintenanceGate,
) -> Result<usize> {
    let completions = maintenance.run(request, telemetry, "result probe", || {
        client.read_results(request.result_batch_size)
    })?;
    ensure!(
        completions.len() <= request.result_batch_size,
        "gateway client exceeded configured completion batch bound"
    );
    telemetry.peak_live_completion_batch =
        telemetry.peak_live_completion_batch.max(completions.len());
    if completions.is_empty() {
        return Ok(0);
    }
    let mut leases = BTreeSet::new();
    let mut fatal_after_ack = None::<String>;
    let mut completion_batch_bytes = 0_usize;
    for completion in &completions {
        let size = canonical_json_bytes(completion)?.len();
        completion_batch_bytes = completion_batch_bytes
            .checked_add(size)
            .ok_or_else(|| anyhow!("gateway completion batch byte count overflow"))?;
        ensure!(
            completion_batch_bytes <= request.max_response_bytes,
            "gateway completion exceeds configured response byte bound"
        );
        telemetry.peak_completion_bytes = telemetry.peak_completion_bytes.max(size);
        match process_completion(paths, index, journal, completion, telemetry)? {
            ProcessedCompletion::Durable { lease } => {
                leases.insert(lease);
            }
            ProcessedCompletion::FatalAfterReceipt { lease, detail } => {
                leases.insert(lease);
                fatal_after_ack = Some(detail);
                break;
            }
        }
    }
    if !leases.is_empty() {
        let lease_ids: Vec<String> = leases.into_iter().collect();
        let acknowledged = maintenance.run(request, telemetry, "result acknowledgement", || {
            client.ack_results(&lease_ids)
        })?;
        ensure!(
            acknowledged == lease_ids.len() as u64,
            "gateway did not acknowledge the exact durable completion lease batch"
        );
        telemetry.acknowledged_completion_count = telemetry
            .acknowledged_completion_count
            .checked_add(acknowledged)
            .ok_or_else(|| anyhow!("acknowledged completion count overflow"))?;
    }
    telemetry.completed_task_count = journal.len() as u64;
    if let Some(detail) = fatal_after_ack {
        bail!("{detail}");
    }
    Ok(completions.len())
}

enum ProcessedCompletion {
    Durable {
        lease: String,
    },
    /// The exact failure receipt is already fsynced, so it is safe and
    /// necessary to acknowledge before surfacing the terminal controller
    /// error. Resume will fail closed from that receipt rather than enqueue.
    FatalAfterReceipt {
        lease: String,
        detail: String,
    },
}

fn process_completion(
    paths: &DispatchPaths,
    index: &TaskIndex,
    journal: &mut CompletionJournal,
    completion: &Value,
    telemetry: &mut GatewayDispatchTelemetry,
) -> Result<ProcessedCompletion> {
    let completion_map = object(completion, "gateway completion")?;
    let task_id = safe_identifier(field(completion_map, "task_id")?, "completion task id")?;
    let entry = find_index_entry(paths, index, &task_id)?;
    let task = load_task_object(paths, &entry)?;
    require_completion_routing(&task, completion)?;
    let lease = safe_identifier(field(completion_map, "lease_id")?, "completion lease id")?;
    let completion_sha256 = canonical_sha256(completion)?;
    telemetry.received_completion_count = telemetry
        .received_completion_count
        .checked_add(1)
        .ok_or_else(|| anyhow!("received completion count overflow"))?;

    let status = text(completion_map, "status")?;
    let material = if status.eq_ignore_ascii_case("success") {
        let envelope = object(field(completion_map, "result")?, "worker envelope")?;
        ensure!(
            envelope.get("status") == Some(&Value::String("success".into()))
                && envelope.get("job_kind") == Some(&Value::String(TASK_KIND.into())),
            "worker envelope does not prove a successful temporal candidate/window job"
        );
        let material = field(envelope, "result")?.clone();
        ensure!(
            admit_candidate_window_task_result(&task, &material)?
                == CandidateWindowResultAdmission::Admitted,
            "successful worker completion is not an admitted candidate-window material"
        );
        material
    } else {
        let receipt = FailureReceipt::from_completion(&entry, completion)?;
        // A failure receipt is independently immutable and fsynced before any
        // acknowledgement. A crash now cannot silently resurrect this task.
        write_failure_receipt_once(paths, &receipt)?;
        let Some(classification) = receipt.classification else {
            return Ok(ProcessedCompletion::FatalAfterReceipt {
                lease,
                detail: format!(
                    "worker completion failed for {} and is not a deterministic rejection",
                    entry.task_id
                ),
            });
        };
        let error = receipt
            .error
            .as_ref()
            .ok_or_else(|| anyhow!("deterministic failure classification lacks worker error"))?;
        let material = rejected_material(&task, completion, classification, error)?;
        ensure!(
            admit_candidate_window_task_result(&task, &material)?
                == CandidateWindowResultAdmission::Rejected,
            "deterministic failure did not produce an admitted rejection"
        );
        telemetry.rejected_completion_count = telemetry
            .rejected_completion_count
            .checked_add(1)
            .ok_or_else(|| anyhow!("rejected completion count overflow"))?;
        material
    };

    let semantic_sha256 = canonical_sha256(&material)?;
    if let Some(prior) = journal.get(&task_id) {
        let prior_record = object(&prior.record, "prior completion record")?;
        ensure!(
            sha_field(prior_record, "resultSha256")? == semantic_sha256,
            "conflicting duplicate gateway completion material"
        );
        telemetry.duplicate_redelivery_count = telemetry
            .duplicate_redelivery_count
            .checked_add(1)
            .ok_or_else(|| anyhow!("duplicate redelivery count overflow"))?;
        return Ok(ProcessedCompletion::Durable { lease });
    }
    let record = completion_record(paths, &entry, &material)?;
    append_completion_journal(
        paths,
        journal,
        CompletionJournalEntry::new(journal.len() as u64, &entry, completion_sha256, record)?,
    )?;
    Ok(ProcessedCompletion::Durable { lease })
}

fn enqueue_missing_tasks(
    client: &mut dyn GatewayClient,
    request: &GatewayDispatchRequest,
    paths: &DispatchPaths,
    index: &TaskIndex,
    journal: &CompletionJournal,
    telemetry: &mut GatewayDispatchTelemetry,
    maintenance: &mut MaintenanceGate,
) -> Result<()> {
    let mut batch = Vec::with_capacity(request.enqueue_batch_size);
    let mut batch_payload_bytes = 12_usize; // `{"tasks":[]}` framing.
    for entry in index.entries.values() {
        if journal.contains_key(&entry.task_id) {
            continue;
        }
        let task = load_task_object(paths, entry)?;
        let task_bytes = canonical_json_bytes(&task)?;
        ensure!(
            task_bytes
                .len()
                .checked_add(12)
                .is_some_and(|size| size <= request.max_request_bytes),
            "one immutable task exceeds configured gateway request byte bound"
        );
        let delimiter = usize::from(!batch.is_empty());
        if !batch.is_empty()
            && (batch.len() == request.enqueue_batch_size
                || batch_payload_bytes
                    .checked_add(delimiter)
                    .and_then(|size| size.checked_add(task_bytes.len()))
                    .is_none_or(|size| size > request.max_request_bytes))
        {
            enqueue_batch(client, &batch, request, telemetry, maintenance)?;
            batch.clear();
            batch_payload_bytes = 12;
        }
        batch_payload_bytes = batch_payload_bytes
            .checked_add(usize::from(!batch.is_empty()))
            .and_then(|size| size.checked_add(task_bytes.len()))
            .ok_or_else(|| anyhow!("gateway enqueue batch byte count overflow"))?;
        batch.push(task);
        telemetry.peak_live_task_batch = telemetry.peak_live_task_batch.max(batch.len());
    }
    if !batch.is_empty() {
        enqueue_batch(client, &batch, request, telemetry, maintenance)?;
    }
    Ok(())
}

fn enqueue_batch(
    client: &mut dyn GatewayClient,
    tasks: &[Value],
    request: &GatewayDispatchRequest,
    telemetry: &mut GatewayDispatchTelemetry,
    maintenance: &mut MaintenanceGate,
) -> Result<()> {
    let receipt = maintenance.run(request, telemetry, "task enqueue", || {
        client.enqueue_tasks(tasks)
    })?;
    let receipt_map = object(&receipt, "gateway enqueue receipt")?;
    let expected = tasks.len() as u64;
    let submitted = unsigned(receipt_map, "submitted")?;
    let enqueued = unsigned(receipt_map, "enqueued")?;
    let rejected = unsigned(receipt_map, "rejected")?;
    ensure!(
        submitted == expected && enqueued <= expected && rejected == expected - enqueued,
        "gateway enqueue receipt does not bind the exact pending task batch"
    );
    if matches!(request.mode, DispatchMode::Fresh) {
        ensure!(
            enqueued == expected,
            "fresh dispatcher run requires every pending task to be newly enqueued"
        );
    }
    telemetry.enqueue_batches = telemetry
        .enqueue_batches
        .checked_add(1)
        .ok_or_else(|| anyhow!("gateway enqueue batch count overflow"))?;
    telemetry.enqueued_task_count = telemetry
        .enqueued_task_count
        .checked_add(enqueued)
        .ok_or_else(|| anyhow!("gateway enqueued task count overflow"))?;
    Ok(())
}

#[derive(Clone, Debug)]
struct CheckpointOverview {
    authority_id: String,
    task_matrix_sha256: String,
    completed_count: u64,
    journal_count: u64,
}

fn validate_mode_and_checkpoint(
    request: &GatewayDispatchRequest,
    paths: &DispatchPaths,
    index: &TaskIndex,
    journal: &CompletionJournal,
) -> Result<()> {
    let Some(checkpoint) = read_checkpoint_overview_if_exists(&paths.checkpoint)? else {
        ensure!(
            !matches!(request.mode, DispatchMode::Fresh) || journal.is_empty(),
            "fresh dispatcher run has an existing completion journal"
        );
        return Ok(());
    };
    ensure!(
        checkpoint.authority_id == index.authority_id
            && checkpoint.task_matrix_sha256 == index.task_matrix_sha256,
        "existing checkpoint does not bind the immutable task matrix"
    );
    if matches!(request.mode, DispatchMode::Fresh) {
        ensure!(
            checkpoint.completed_count == 0 && checkpoint.journal_count == 0 && journal.is_empty(),
            "fresh dispatcher run already has completed tasks"
        );
    } else if checkpoint.completed_count > 0 || checkpoint.journal_count > 0 {
        ensure!(
            checkpoint.completed_count == journal.len() as u64
                && checkpoint.journal_count == journal.len() as u64,
            "legacy checkpoint cannot be resumed without the matching native completion journal"
        );
    }
    Ok(())
}

fn compact_legacy_checkpoint_once(
    paths: &DispatchPaths,
    index: &TaskIndex,
    journal: &CompletionJournal,
) -> Result<()> {
    ensure!(
        journal.len() as u64 == index.task_count,
        "legacy checkpoint compaction requires every immutable task completion"
    );
    let staging = paths.sidecar_root.join(".checkpoint.staging");
    if staging.exists() {
        ensure_real_file(&staging, "stale checkpoint staging file")?;
        fs::remove_file(&staging).context("remove stale checkpoint staging file")?;
    }
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging)
        .context("create checkpoint staging file")?;
    let mut writer = BufWriter::new(file);
    write_legacy_checkpoint(&mut writer, paths, index, journal)?;
    writer.flush()?;
    writer
        .get_ref()
        .sync_all()
        .context("fsync legacy checkpoint staging file")?;
    drop(writer);
    if paths.checkpoint.exists() {
        if files_equal(&staging, &paths.checkpoint)? {
            fs::remove_file(&staging)?;
            return Ok(());
        }
        let existing = read_checkpoint_overview_if_exists(&paths.checkpoint)?
            .ok_or_else(|| anyhow!("legacy checkpoint disappeared during compaction"))?;
        ensure!(
            existing.authority_id == index.authority_id
                && existing.task_matrix_sha256 == index.task_matrix_sha256
                && existing.completed_count == 0
                && existing.journal_count == 0,
            "refusing to overwrite divergent legacy checkpoint"
        );
        // The freezer owns the initial empty checkpoint. It is the one mutable
        // legacy artifact this dispatcher is allowed to replace, exactly once,
        // after all terminal durable journal entries exist.
        fs::remove_file(&paths.checkpoint)?;
    }
    fs::rename(&staging, &paths.checkpoint)
        .with_context(|| format!("publish legacy checkpoint: {}", paths.checkpoint.display()))?;
    Ok(())
}

fn write_legacy_checkpoint(
    writer: &mut impl Write,
    paths: &DispatchPaths,
    index: &TaskIndex,
    journal: &CompletionJournal,
) -> Result<()> {
    // This is the exact Python sorted-key checkpoint layout, streamed rather
    // than assembled as an O(T) `completed` map or O(T) journal vector.
    writer.write_all(b"{\n  \"authorityId\": ")?;
    write_pretty_scalar(writer, &Value::String(index.authority_id.clone()))?;
    writer.write_all(b",\n  \"completed\": {")?;
    let mut first = true;
    for (task_id, entry) in journal {
        if !first {
            writer.write_all(b",")?;
        }
        first = false;
        writer.write_all(b"\n    ")?;
        write_pretty_scalar(writer, &Value::String(task_id.clone()))?;
        writer.write_all(b": ")?;
        write_nested_pretty(writer, &entry.record, 4)?;
    }
    if !journal.is_empty() {
        writer.write_all(b"\n  ")?;
    }
    writer.write_all(b"},\n  \"journal\": [")?;
    let file = File::open(&paths.completion_journal)?;
    let reader = BufReader::new(file);
    let mut expected_ordinal = 0_u64;
    let mut first = true;
    for line in reader.lines() {
        let line = line?;
        let value: Value = serde_json::from_str(&line)?;
        let entry = CompletionJournalEntry::from_value(&value)?;
        ensure!(
            entry.ordinal == expected_ordinal,
            "completion journal ordinal drifted during compaction"
        );
        expected_ordinal += 1;
        if !first {
            writer.write_all(b",")?;
        }
        first = false;
        writer.write_all(b"\n    ")?;
        let mut row = object(&entry.record, "completion journal record")?.clone();
        row.insert("taskId".into(), Value::String(entry.task_id));
        write_nested_pretty(writer, &Value::Object(row), 4)?;
    }
    ensure!(
        expected_ordinal == index.task_count,
        "completion journal task count drifted during compaction"
    );
    if index.task_count > 0 {
        writer.write_all(b"\n  ")?;
    }
    writer.write_all(b"],\n  \"schemaVersion\": \"temporal_graph_candidate_window_checkpoint_v1\",\n  \"taskMatrixSha256\": ")?;
    write_pretty_scalar(writer, &Value::String(index.task_matrix_sha256.clone()))?;
    writer.write_all(b"\n}\n")?;
    Ok(())
}

fn read_checkpoint_overview_if_exists(path: &Path) -> Result<Option<CheckpointOverview>> {
    if !path.exists() {
        return Ok(None);
    }
    ensure_real_file(path, "legacy checkpoint")?;
    let file = File::open(path)?;
    let mut state = CheckpointStreamState::default();
    let mut deserializer = serde_json::Deserializer::from_reader(BufReader::new(file));
    (&mut deserializer)
        .deserialize_map(CheckpointVisitor { state: &mut state })
        .context("parse legacy checkpoint overview")?;
    deserializer
        .end()
        .context("legacy checkpoint has trailing data")?;
    Ok(Some(state.finish()?))
}

#[derive(Default)]
struct CheckpointStreamState {
    schema_version: Option<String>,
    authority_id: Option<String>,
    task_matrix_sha256: Option<String>,
    completed_count: Option<u64>,
    journal_count: Option<u64>,
    seen: BTreeSet<String>,
}

impl CheckpointStreamState {
    fn finish(self) -> Result<CheckpointOverview> {
        let expected: BTreeSet<String> = [
            "authorityId",
            "completed",
            "journal",
            "schemaVersion",
            "taskMatrixSha256",
        ]
        .into_iter()
        .map(ToOwned::to_owned)
        .collect();
        ensure!(
            self.seen == expected,
            "legacy checkpoint fields are not exact"
        );
        ensure!(
            self.schema_version.as_deref() == Some(CHECKPOINT_SCHEMA),
            "legacy checkpoint schema is incompatible"
        );
        Ok(CheckpointOverview {
            authority_id: self
                .authority_id
                .ok_or_else(|| anyhow!("legacy checkpoint lacks authorityId"))?,
            task_matrix_sha256: self
                .task_matrix_sha256
                .ok_or_else(|| anyhow!("legacy checkpoint lacks taskMatrixSha256"))?,
            completed_count: self
                .completed_count
                .ok_or_else(|| anyhow!("legacy checkpoint lacks completed"))?,
            journal_count: self
                .journal_count
                .ok_or_else(|| anyhow!("legacy checkpoint lacks journal"))?,
        })
    }
}

struct CheckpointVisitor<'a> {
    state: &'a mut CheckpointStreamState,
}

impl<'de> Visitor<'de> for CheckpointVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a legacy candidate-window checkpoint object")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        while let Some(key) = map.next_key::<String>()? {
            if !self.state.seen.insert(key.clone()) {
                return Err(de::Error::custom(format!(
                    "legacy checkpoint repeats {key}"
                )));
            }
            match key.as_str() {
                "schemaVersion" => self.state.schema_version = Some(map.next_value::<String>()?),
                "authorityId" => self.state.authority_id = Some(map.next_value::<String>()?),
                "taskMatrixSha256" => {
                    self.state.task_matrix_sha256 = Some(map.next_value::<String>()?)
                }
                "completed" => {
                    self.state.completed_count = Some(map.next_value_seed(CountMapSeed)?);
                }
                "journal" => {
                    self.state.journal_count = Some(map.next_value_seed(CountSeqSeed)?);
                }
                _ => {
                    let _: IgnoredAny = map.next_value()?;
                    return Err(de::Error::custom(format!(
                        "legacy checkpoint has unexpected {key}"
                    )));
                }
            }
        }
        Ok(())
    }
}

struct CountMapSeed;

impl<'de> DeserializeSeed<'de> for CountMapSeed {
    type Value = u64;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(CountMapVisitor)
    }
}

struct CountMapVisitor;

impl<'de> Visitor<'de> for CountMapVisitor {
    type Value = u64;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a checkpoint completed object")
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut count = 0_u64;
        let mut last = None::<String>;
        while let Some(key) = map.next_key::<String>()? {
            safe_identifier_value(&key, "checkpoint completed task id")
                .map_err(de::Error::custom)?;
            if let Some(previous) = &last
                && key <= *previous
            {
                return Err(de::Error::custom(
                    "checkpoint completed task ids are not canonical",
                ));
            }
            let _: IgnoredAny = map.next_value()?;
            count = count
                .checked_add(1)
                .ok_or_else(|| de::Error::custom("checkpoint completed count overflow"))?;
            last = Some(key);
        }
        Ok(count)
    }
}

struct CountSeqSeed;

impl<'de> DeserializeSeed<'de> for CountSeqSeed {
    type Value = u64;

    fn deserialize<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_seq(CountSeqVisitor)
    }
}

struct CountSeqVisitor;

impl<'de> Visitor<'de> for CountSeqVisitor {
    type Value = u64;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a checkpoint journal array")
    }

    fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut count = 0_u64;
        while sequence.next_element::<IgnoredAny>()?.is_some() {
            count = count
                .checked_add(1)
                .ok_or_else(|| de::Error::custom("checkpoint journal count overflow"))?;
        }
        Ok(count)
    }
}

fn write_pretty_scalar(writer: &mut impl Write, value: &Value) -> Result<()> {
    let bytes = python_pretty_json_line(value, JsonNewline::Lf)?;
    writer.write_all(bytes.strip_suffix(b"\n").unwrap_or(&bytes))?;
    Ok(())
}

fn write_nested_pretty(writer: &mut impl Write, value: &Value, base_indent: usize) -> Result<()> {
    let bytes = python_pretty_json_line(value, JsonNewline::Lf)?;
    let body = bytes.strip_suffix(b"\n").unwrap_or(&bytes);
    for (line_index, line) in body.split(|byte| *byte == b'\n').enumerate() {
        if line_index > 0 {
            writer.write_all(b"\n")?;
            writer.write_all(&vec![b' '; base_indent])?;
        }
        writer.write_all(line)?;
    }
    Ok(())
}

fn write_immutable_canonical(path: &Path, value: &Value) -> Result<()> {
    let bytes = canonical_json_line(value)?;
    if path.exists() {
        ensure!(
            fs::read(path)? == bytes,
            "refusing to overwrite divergent immutable artifact: {}",
            path.display()
        );
        return Ok(());
    }
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("immutable artifact path has no parent"))?;
    let staging = parent.join(format!(
        ".{}.{}.staging",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("artifact"),
        std::process::id()
    ));
    if staging.exists() {
        fs::remove_file(&staging).context("remove stale immutable artifact staging")?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging)
        .with_context(|| format!("create immutable artifact staging: {}", staging.display()))?;
    file.write_all(&bytes)?;
    file.flush()?;
    file.sync_all()
        .context("fsync immutable artifact staging")?;
    drop(file);
    match fs::hard_link(&staging, path) {
        Ok(()) => {}
        Err(error) if path.exists() => {
            ensure!(
                fs::read(path)? == bytes,
                "refusing to overwrite divergent immutable artifact: {}",
                path.display()
            );
            let _ = error;
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("publish immutable artifact: {}", path.display()));
        }
    }
    fs::remove_file(&staging).context("remove immutable artifact staging hard link")?;
    Ok(())
}

fn publish_staging_once(staging: &Path, destination: &Path) -> Result<()> {
    if destination.exists() {
        ensure!(
            files_equal(staging, destination)?,
            "refusing to overwrite divergent immutable artifact: {}",
            destination.display()
        );
        fs::remove_file(staging)?;
        return Ok(());
    }
    fs::rename(staging, destination)
        .with_context(|| format!("publish immutable artifact: {}", destination.display()))
}

fn read_canonical_line(path: &Path, label: &str) -> Result<Value> {
    ensure_real_file(path, label)?;
    let raw = fs::read(path).with_context(|| format!("read {label}: {}", path.display()))?;
    let value: Value = serde_json::from_slice(&raw).with_context(|| format!("parse {label}"))?;
    ensure!(
        canonical_json_line(&value)? == raw,
        "{label} must be canonical JSON plus LF"
    );
    Ok(value)
}

fn files_equal(left: &Path, right: &Path) -> Result<bool> {
    if fs::metadata(left)?.len() != fs::metadata(right)?.len() {
        return Ok(false);
    }
    let mut left = File::open(left)?;
    let mut right = File::open(right)?;
    let mut left_buffer = [0_u8; 64 * 1024];
    let mut right_buffer = [0_u8; 64 * 1024];
    loop {
        let left_count = left.read(&mut left_buffer)?;
        let right_count = right.read(&mut right_buffer)?;
        if left_count != right_count || left_buffer[..left_count] != right_buffer[..right_count] {
            return Ok(false);
        }
        if left_count == 0 {
            return Ok(true);
        }
    }
}

fn sha_file(path: &Path) -> Result<String> {
    ensure_real_file(path, "SHA-256 input")?;
    let mut file =
        File::open(path).with_context(|| format!("open for SHA-256: {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(digest_sha256_prefixed(hasher.finalize()))
}

fn digest_sha256_prefixed(digest: impl AsRef<[u8]>) -> String {
    let mut output = String::from("sha256:");
    for byte in digest.as_ref() {
        use std::fmt::Write as _;
        write!(output, "{byte:02x}").expect("write SHA-256 digest into String");
    }
    output
}

fn existing_file(path: &Path, label: &str) -> Result<PathBuf> {
    ensure_real_file(path, label)?;
    path.canonicalize()
        .with_context(|| format!("resolve {label}: {}", path.display()))
}

fn ensure_real_file(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("{label} is unavailable: {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file() && !metadata.file_type().is_symlink(),
        "{label} must be a real regular file"
    );
    Ok(())
}

fn ensure_real_directory(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("{label} is unavailable: {}", path.display()))?;
    ensure!(
        metadata.file_type().is_dir() && !metadata.file_type().is_symlink(),
        "{label} must be a real directory"
    );
    Ok(())
}

fn object<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| anyhow!("{label} must be an object"))
}

fn field<'a>(map: &'a Map<String, Value>, key: &str) -> Result<&'a Value> {
    map.get(key).ok_or_else(|| anyhow!("object lacks {key}"))
}

fn text(map: &Map<String, Value>, key: &str) -> Result<String> {
    field(map, key)?
        .as_str()
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("{key} must be a nonempty string"))
}

fn unsigned(map: &Map<String, Value>, key: &str) -> Result<u64> {
    field(map, key)?
        .as_u64()
        .ok_or_else(|| anyhow!("{key} must be a nonnegative integer"))
}

fn sha_field(map: &Map<String, Value>, key: &str) -> Result<String> {
    let value = text(map, key)?;
    ensure!(
        value.len() == 71
            && value.starts_with("sha256:")
            && value.as_bytes()[7..]
                .iter()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte)),
        "{key} must be lowercase SHA-256"
    );
    Ok(value)
}

fn exact_keys(map: &Map<String, Value>, expected: &[&str], label: &str) -> Result<()> {
    ensure!(
        map.len() == expected.len() && expected.iter().all(|key| map.contains_key(*key)),
        "{label} fields are not exact"
    );
    Ok(())
}

fn safe_identifier(value: &Value, label: &str) -> Result<String> {
    let value = value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("{label} must be a safe explicit identifier"))?;
    safe_identifier_value(value, label)
}

fn safe_identifier_value(value: &str, label: &str) -> Result<String> {
    ensure!(
        value.len() <= 160
            && value
                .as_bytes()
                .first()
                .is_some_and(u8::is_ascii_alphanumeric)
            && value.as_bytes().iter().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-')
            }),
        "{label} must be a safe explicit identifier"
    );
    Ok(value.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::VecDeque,
        net::{TcpListener, TcpStream},
        sync::{Arc, Mutex},
    };

    use tempfile::TempDir;

    const SHA_A: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const SHA_B: &str = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const SHA_C: &str = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    const AUTHORITY: &str =
        "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";

    struct FakeLocalGateway {
        results: VecDeque<Value>,
        enqueued_ids: BTreeSet<String>,
        reject_enqueues: bool,
        fail_ack_once: bool,
        journal_path: PathBuf,
        saw_durable_journal_before_ack: bool,
        max_enqueue_batch: usize,
        max_read_limit: usize,
        acknowledgement_calls: u64,
        maintenance_reads_remaining: usize,
        maintenance_enqueues_remaining: usize,
        maintenance_acks_remaining: usize,
    }

    impl FakeLocalGateway {
        fn new(root: &Path, results: impl IntoIterator<Item = Value>) -> Self {
            Self {
                results: results.into_iter().collect(),
                enqueued_ids: BTreeSet::new(),
                reject_enqueues: false,
                fail_ack_once: false,
                journal_path: root.join(SIDECAR_DIR).join(COMPLETION_JOURNAL_NAME),
                saw_durable_journal_before_ack: false,
                max_enqueue_batch: 0,
                max_read_limit: 0,
                acknowledgement_calls: 0,
                maintenance_reads_remaining: 0,
                maintenance_enqueues_remaining: 0,
                maintenance_acks_remaining: 0,
            }
        }
    }

    impl GatewayClient for FakeLocalGateway {
        fn enqueue_tasks(&mut self, tasks: &[Value]) -> Result<Value> {
            if self.maintenance_enqueues_remaining > 0 {
                self.maintenance_enqueues_remaining -= 1;
                return Err(GatewayMaintenance {
                    endpoint: "tasks".into(),
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    retry_after: None,
                }
                .into());
            }
            self.max_enqueue_batch = self.max_enqueue_batch.max(tasks.len());
            let mut accepted = 0_u64;
            for task in tasks {
                let id = safe_identifier(
                    field(object(task, "fake gateway task")?, "task_id")?,
                    "fake gateway task id",
                )?;
                if !self.reject_enqueues && self.enqueued_ids.insert(id) {
                    accepted += 1;
                }
            }
            Ok(json!({
                "status":"accepted",
                "submitted":tasks.len(),
                "accepted":accepted,
                "enqueued":accepted,
                "rejected":tasks.len() as u64 - accepted,
            }))
        }

        fn read_results(&mut self, limit: usize) -> Result<Vec<Value>> {
            if self.maintenance_reads_remaining > 0 {
                self.maintenance_reads_remaining -= 1;
                return Err(GatewayMaintenance {
                    endpoint: "results".into(),
                    status: StatusCode::CONFLICT,
                    retry_after: None,
                }
                .into());
            }
            self.max_read_limit = self.max_read_limit.max(limit);
            Ok(self.results.iter().take(limit).cloned().collect())
        }

        fn ack_results(&mut self, lease_ids: &[String]) -> Result<u64> {
            if self.maintenance_acks_remaining > 0 {
                self.maintenance_acks_remaining -= 1;
                return Err(GatewayMaintenance {
                    endpoint: "results/ack".into(),
                    status: StatusCode::SERVICE_UNAVAILABLE,
                    retry_after: None,
                }
                .into());
            }
            self.acknowledgement_calls += 1;
            self.saw_durable_journal_before_ack =
                self.journal_path.is_file() && fs::metadata(&self.journal_path)?.len() > 0;
            if self.fail_ack_once {
                self.fail_ack_once = false;
                bail!("injected acknowledgement crash")
            }
            let requested: BTreeSet<&str> = lease_ids.iter().map(String::as_str).collect();
            let mut acknowledged = 0_u64;
            self.results.retain(|completion| {
                let lease = completion
                    .as_object()
                    .and_then(|map| map.get("lease_id"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if requested.contains(lease) {
                    acknowledged += 1;
                    false
                } else {
                    true
                }
            });
            Ok(acknowledged)
        }
    }

    #[derive(Clone, Default)]
    struct HttpFixtureState {
        endpoint_log: Vec<String>,
        results: VecDeque<Value>,
    }

    struct HttpFixtureRequest {
        method: String,
        target: String,
        headers: BTreeMap<String, String>,
        body: Vec<u8>,
        stream: TcpStream,
    }

    fn spawn_http_gateway_fixture()
    -> Result<(String, std::thread::JoinHandle<Result<HttpFixtureState>>)> {
        let listener = TcpListener::bind("127.0.0.1:0").context("bind fake gateway listener")?;
        let address = listener.local_addr()?;
        let state = Arc::new(Mutex::new(HttpFixtureState::default()));
        let server_state = Arc::clone(&state);
        let handle = std::thread::spawn(move || -> Result<HttpFixtureState> {
            // One empty pre-drain, one enqueue, one delivery, and one durable
            // acknowledgement exercise the actual three HTTP endpoint paths.
            for _ in 0..4 {
                let (stream, _) = listener.accept().context("accept fake gateway request")?;
                let HttpFixtureRequest {
                    method,
                    target,
                    headers,
                    body,
                    mut stream,
                } = read_http_request(stream)?;
                ensure!(
                    headers.get("authorization").map(String::as_str)
                        == Some("Bearer fixture-token"),
                    "fake gateway did not receive its runtime bearer token"
                );
                let path = target.split('?').next().unwrap_or_default();
                let response = match (method.as_str(), path) {
                    ("GET", "/results") => {
                        let mut state = server_state
                            .lock()
                            .map_err(|_| anyhow!("fake gateway mutex poisoned"))?;
                        state.endpoint_log.push("GET /results".into());
                        json!({"results":state.results.iter().cloned().collect::<Vec<_>>()})
                    }
                    ("POST", "/tasks") => {
                        let payload: Value = serde_json::from_slice(&body)?;
                        let tasks = field(object(&payload, "fake gateway enqueue body")?, "tasks")?
                            .as_array()
                            .ok_or_else(|| anyhow!("fake gateway enqueue tasks are invalid"))?;
                        ensure!(tasks.len() == 1, "fake gateway expected one bounded task");
                        let completion = warmup_failure_completion(&tasks[0], "http-lease")?;
                        let mut state = server_state
                            .lock()
                            .map_err(|_| anyhow!("fake gateway mutex poisoned"))?;
                        state.endpoint_log.push("POST /tasks".into());
                        state.results.push_back(completion);
                        json!({"status":"accepted","submitted":1,"accepted":1,"enqueued":1,"rejected":0})
                    }
                    ("POST", "/results/ack") => {
                        let payload: Value = serde_json::from_slice(&body)?;
                        let leases = field(
                            object(&payload, "fake gateway acknowledgement body")?,
                            "lease_ids",
                        )?
                        .as_array()
                        .ok_or_else(|| {
                            anyhow!("fake gateway acknowledgement leases are invalid")
                        })?;
                        let requested: BTreeSet<&str> =
                            leases.iter().filter_map(Value::as_str).collect();
                        let mut state = server_state
                            .lock()
                            .map_err(|_| anyhow!("fake gateway mutex poisoned"))?;
                        state.endpoint_log.push("POST /results/ack".into());
                        let mut acknowledged = 0_u64;
                        state.results.retain(|completion| {
                            let lease = completion
                                .as_object()
                                .and_then(|map| map.get("lease_id"))
                                .and_then(Value::as_str)
                                .unwrap_or_default();
                            if requested.contains(lease) {
                                acknowledged += 1;
                                false
                            } else {
                                true
                            }
                        });
                        json!({"acked":acknowledged})
                    }
                    _ => bail!("unexpected fake gateway endpoint: {method} {target}"),
                };
                write_http_response(&mut stream, &response)?;
            }
            server_state
                .lock()
                .map_err(|_| anyhow!("fake gateway mutex poisoned"))
                .map(|state| state.clone())
        });
        Ok((format!("http://{address}"), handle))
    }

    fn read_http_request(stream: TcpStream) -> Result<HttpFixtureRequest> {
        let mut reader = BufReader::new(stream);
        let mut request_line = String::new();
        reader.read_line(&mut request_line)?;
        let mut request = request_line.split_whitespace();
        let method = request
            .next()
            .ok_or_else(|| anyhow!("fake gateway request lacks method"))?
            .to_owned();
        let target = request
            .next()
            .ok_or_else(|| anyhow!("fake gateway request lacks target"))?
            .to_owned();
        ensure!(
            request.next().is_some(),
            "fake gateway request lacks HTTP version"
        );
        let mut headers = BTreeMap::new();
        loop {
            let mut line = String::new();
            reader.read_line(&mut line)?;
            if line == "\r\n" || line == "\n" {
                break;
            }
            let (name, value) = line
                .split_once(':')
                .ok_or_else(|| anyhow!("fake gateway request header is malformed"))?;
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
        let content_length = headers
            .get("content-length")
            .map(|value| value.parse::<usize>())
            .transpose()
            .context("fake gateway content length is invalid")?
            .unwrap_or(0);
        let mut body = vec![0_u8; content_length];
        reader.read_exact(&mut body)?;
        Ok(HttpFixtureRequest {
            method,
            target,
            headers,
            body,
            stream: reader.into_inner(),
        })
    }

    fn write_http_response(stream: &mut TcpStream, value: &Value) -> Result<()> {
        let body = canonical_json_bytes(value)?;
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )?;
        stream.write_all(&body)?;
        stream.flush()?;
        Ok(())
    }

    fn task(task_id: &str, candidate_id: &str) -> Value {
        json!({
            "task_id":task_id,
            "lane_id":candidate_id,
            "attempt_id":task_id,
            "task_kind":TASK_KIND,
            "payload":{
                "job_id":task_id,
                "candidate_id":candidate_id,
                "authority_id":AUTHORITY,
                "evidence_plan":{"plan_id":SHA_A},
                "lake_window_semantic_sha256":SHA_B,
                "shared_observation_stream_id":"fixture-stream",
                "analysis_window_start":"2024-01-01T00:00:00Z",
                "analysis_window_end":"2024-02-01T00:00:00Z",
                "bar_limit":100
            },
            "required_worker_capabilities":[],
            "deadline_seconds":30,
            "max_attempts":1
        })
    }

    fn write_fixture_manifest(root: &Path, tasks: &[Value]) -> Result<PathBuf> {
        let matrix = canonical_sha256(&Value::Array(tasks.to_vec()))?;
        let manifest = json!({
            "authorityId":AUTHORITY,
            "schemaVersion":TASK_MANIFEST_SCHEMA,
            "taskCount":tasks.len(),
            "taskMatrixSha256":matrix,
            "tasks":tasks,
        });
        let path = root.join("task-manifest.json");
        fs::write(&path, python_pretty_json_line(&manifest, JsonNewline::Lf)?)?;
        fs::write(
            root.join(CHECKPOINT_NAME),
            python_pretty_json_line(
                &json!({
                    "schemaVersion":CHECKPOINT_SCHEMA,
                    "authorityId":AUTHORITY,
                    "taskMatrixSha256":matrix,
                    "completed":{},"journal":[]
                }),
                JsonNewline::Lf,
            )?,
        )?;
        Ok(path)
    }

    fn worker_material(task: &Value) -> Result<Value> {
        let payload = object(
            field(object(task, "fixture task")?, "payload")?,
            "fixture payload",
        )?;
        let start = "2024-01-01T00:00:00Z";
        let last = "2024-01-31T23:55:00Z";
        let terminal = json!({
            "schemaVersion":"temporal_terminal_valuation_v1",
            "policy":"leave_open_mark_to_market_v1",
            "positionStatus":"no_open_position",
            "lastCompletedBarId":"EURUSD:M5:fixture",
            "lastCompletedBarStart":last,
            "lastCompletedBarClose":last,
            "markPrice":1.101,
            "exitCostPercent":0.0,
            "pendingEffectStatus":"none",
            "pendingEffectCancellationTreatment":"not_applicable",
            "closedTradeCountDelta":0
        });
        let metrics = json!({
            "observationsProcessed":10,
            "tradesClosed":3,
            "totalGrossR":1.2,
            "totalNetR":1.0,
            "totalExecutionCostPercent":0.0,
            "unresolvedPosition":false,
            "unresolvedPendingEffect":false,
            "terminalValuation":terminal,
            "terminalAdjustedTotalGrossR":1.2,
            "terminalAdjustedTotalNetR":1.0,
            "terminalAdjustedTotalExecutionCostPercent":0.0,
            "terminalAdjustedEquityCurveR":[1.0],
            "terminalAdjustedMaxDrawdownR":0.0
        });
        let replay = json!({
            "streamSha256":SHA_A,
            "profileSnapshotSha256":SHA_B,
            "programSha256":SHA_C,
            "graphTraces":[],
            "executionTraces":[],
            "trades":[],
            "metrics":metrics
        });
        let path = canonical_sha256(&json!({
            "schema_version":"temporal_graph_cost_view_path_v3",
            "graph_path":[],"execution_path":[],"trade_path":[],"final_execution_state":null
        }))?;
        let evidence = json!({
            "schema_version":"temporal_graph_candidate_window_evidence_contract_v1",
            "analysis_window_start":start,
            "analysis_window_end":"2024-02-01T00:00:00Z",
            "analysis_window_end_exclusive":true,
            "requested_bar_limit":100,
            "effective_bar_limit":120,
            "observation_count":10,
            "first_admitted_observation_timestamp":start,
            "last_admitted_observation_timestamp":last,
            "warmup_sufficient":true,
            "warmup_sufficiency":{"sufficient":true,"source":"aligned_scoring"},
            "excluded_provisional_count":1,
            "excluded_outside_analysis_window_count":2
        });
        let mut material = json!({
            "schema_version":ADMITTED_SCHEMA,
            "task_kind":TASK_KIND,
            "job_id":field(payload,"job_id")?,
            "authority_id":field(payload,"authority_id")?,
            "candidate_id":field(payload,"candidate_id")?,
            "evidence_plan_id":field(object(field(payload,"evidence_plan")?,"fixture evidence")?,"plan_id")?,
            "lake_window_semantic_sha256":field(payload,"lake_window_semantic_sha256")?,
            "shared_observation_stream_id":field(payload,"shared_observation_stream_id")?,
            "analysis_window_start":start,
            "analysis_window_end":"2024-02-01T00:00:00Z",
            "source_profile_snapshot_sha256":SHA_A,
            "resolved_profile_snapshot_sha256":SHA_B,
            "program_sha256":SHA_C,
            "observation_stream_sha256":SHA_A,
            "observation_summary":{"observation_count":10,"first_bar_start":start,"last_bar_start":last},
            "evidence_contract":evidence,
            "cost_view_results":{
                "research_conservative":{"cost_view":"research_conservative","observation_stream_sha256":SHA_A,"replay_result":replay},
                "none":{"cost_view":"none","observation_stream_sha256":SHA_A,"replay_result":replay}
            },
            "diagnostics":{
                "observation_count":10,"requested_bar_limit":100,"effective_bar_limit":120,
                "warmup_sufficient":true,"warmup_sufficiency":{"sufficient":true,"source":"aligned_scoring"},
                "first_admitted_observation_timestamp":start,"last_admitted_observation_timestamp":last,
                "excluded_provisional_count":1,"excluded_outside_analysis_window_count":2,
                "cost_view_decision_path_sha256":path,"cost_view_path_parity":"matched",
                "cost_view_count":2,"shared_stream_required":true
            }
        });
        material["artifact_sha256"] = Value::String(canonical_sha256(&material)?);
        let mut size = 1_u64;
        for _ in 0..16 {
            material["artifact_size_bytes"] = Value::Number(size.into());
            let next = canonical_json_bytes(&material)?.len() as u64;
            if next == size {
                return Ok(material);
            }
            size = next;
        }
        bail!("fixture worker material size did not stabilize")
    }

    fn successful_completion(task: &Value, lease: &str) -> Result<Value> {
        let task_map = object(task, "fixture task")?;
        Ok(json!({
            "task_id":field(task_map,"task_id")?,
            "lease_id":lease,
            "worker_id":"fixture-worker",
            "lane_id":field(task_map,"lane_id")?,
            "attempt_id":field(task_map,"attempt_id")?,
            "status":"success",
            "result":{"status":"success","job_kind":TASK_KIND,"result":worker_material(task)?}
        }))
    }

    fn warmup_failure_completion(task: &Value, lease: &str) -> Result<Value> {
        let task_map = object(task, "fixture task")?;
        Ok(json!({
            "task_id":field(task_map,"task_id")?,
            "lease_id":lease,
            "worker_id":"fixture-worker",
            "lane_id":field(task_map,"lane_id")?,
            "attempt_id":field(task_map,"attempt_id")?,
            "status":"failed",
            "result":{
                "status":"failed",
                "error_type":"AlignedScoringWarmupInsufficientError",
                "detail":"fixture deterministic exhaustion"
            }
        }))
    }

    fn request(manifest: &Path, root: &Path, mode: DispatchMode) -> GatewayDispatchRequest {
        let mut request = GatewayDispatchRequest::bounded(manifest, root, mode);
        request.timeout = Duration::from_secs(3);
        request.poll_interval = Duration::from_millis(10);
        request.enqueue_batch_size = 2;
        request.result_batch_size = 1;
        request.max_request_bytes = 1024 * 1024;
        request.max_response_bytes = 1024 * 1024;
        request.maintenance_probe_interval = Duration::from_millis(10);
        request.maintenance_timeout = Duration::from_secs(1);
        request
    }

    #[test]
    fn shared_maintenance_gate_collapses_each_outage_and_preserves_durable_completion() -> Result<()>
    {
        let directory = TempDir::new()?;
        let root = directory.path();
        let tasks = vec![task("maintenance-task", "maintenance-candidate")];
        let manifest = write_fixture_manifest(root, &tasks)?;
        let mut gateway = FakeLocalGateway::new(
            root,
            [warmup_failure_completion(&tasks[0], "maintenance-lease")?],
        );
        gateway.maintenance_reads_remaining = 2;
        gateway.maintenance_acks_remaining = 2;
        let result = execute_gateway_dispatch_with_client(
            &request(&manifest, root, DispatchMode::Fresh),
            &mut gateway,
        )?;
        assert_eq!(result["completedTaskCount"], json!(1));
        assert_eq!(result["telemetry"]["maintenanceActivations"], json!(2));
        assert_eq!(result["telemetry"]["maintenanceProbeCount"], json!(2));
        assert_eq!(
            result["telemetry"]["infrastructureAvailabilityEvents"],
            json!(4)
        );
        assert!(
            result["telemetry"]["maintenancePauseMillis"]
                .as_u64()
                .is_some_and(|millis| millis >= 40)
        );
        assert_eq!(result["telemetry"]["enqueuedTaskCount"], json!(0));
        assert!(gateway.saw_durable_journal_before_ack);
        Ok(())
    }

    #[test]
    fn maintenance_pause_has_a_separate_timeout_from_scientific_wait() -> Result<()> {
        let directory = TempDir::new()?;
        let root = directory.path();
        let tasks = vec![task("maintenance-timeout", "maintenance-candidate")];
        let manifest = write_fixture_manifest(root, &tasks)?;
        let mut gateway = FakeLocalGateway::new(root, Vec::<Value>::new());
        gateway.maintenance_reads_remaining = 100;
        let mut dispatch = request(&manifest, root, DispatchMode::Fresh);
        dispatch.timeout = Duration::from_secs(1);
        dispatch.maintenance_timeout = Duration::from_millis(35);
        let error = execute_gateway_dispatch_with_client(&dispatch, &mut gateway)
            .expect_err("continuous maintenance must respect its own bound");
        assert!(
            format!("{error:#}").contains("shared maintenance"),
            "unexpected maintenance error: {error:#}"
        );
        Ok(())
    }

    #[test]
    fn fake_gateway_crash_resume_duplicate_rejection_and_durable_before_ack() -> Result<()> {
        let directory = TempDir::new()?;
        let root = directory.path();
        let tasks = vec![
            task("fixture-task-a", "candidate_a"),
            task("fixture-task-b", "candidate_b"),
            task("fixture-task-c", "candidate_c"),
        ];
        let manifest = write_fixture_manifest(root, &tasks)?;

        let mut crashing =
            FakeLocalGateway::new(root, [successful_completion(&tasks[0], "lease-a")?]);
        crashing.fail_ack_once = true;
        assert!(
            execute_gateway_dispatch_with_client(
                &request(&manifest, root, DispatchMode::Fresh),
                &mut crashing,
            )
            .is_err()
        );
        assert!(crashing.saw_durable_journal_before_ack);
        assert!(
            root.join(SIDECAR_DIR)
                .join(COMPLETION_JOURNAL_NAME)
                .is_file()
        );
        // The task-object/index sidecar is the restart authority after its
        // source manifest was already consumed; Resume fails closed on a
        // present-but-tampered manifest but does not need the source file.
        fs::remove_file(&manifest)?;

        let mut resumed = FakeLocalGateway::new(
            root,
            [
                successful_completion(&tasks[0], "lease-a")?,
                warmup_failure_completion(&tasks[1], "lease-b")?,
                successful_completion(&tasks[2], "lease-c")?,
            ],
        );
        // Resume receives explicit rejected duplicate enqueue receipts, as the
        // real gateway does after an acknowledgement timeout/redelivery.
        resumed.reject_enqueues = true;
        let result = execute_gateway_dispatch_with_client(
            &request(&manifest, root, DispatchMode::Resume),
            &mut resumed,
        )?;
        assert!(resumed.saw_durable_journal_before_ack);
        assert_eq!(resumed.max_enqueue_batch, 2);
        assert_eq!(result["completedTaskCount"], json!(3));
        assert_eq!(result["telemetry"]["duplicateRedeliveryCount"], json!(1));
        assert_eq!(result["telemetry"]["rejectedCompletionCount"], json!(1));
        let checkpoint_raw = fs::read(root.join(CHECKPOINT_NAME))?;
        let checkpoint: Value = serde_json::from_slice(&checkpoint_raw)?;
        assert_eq!(
            checkpoint_raw,
            python_pretty_json_line(&checkpoint, JsonNewline::Lf)?
        );
        assert_eq!(
            checkpoint["completed"]["fixture-task-b"]["outcome"],
            json!("rejected")
        );
        assert_eq!(
            checkpoint["completed"]["fixture-task-b"]["rejectionCode"],
            json!("aligned_scoring_warmup_insufficient")
        );
        assert!(
            root.join(FAILURES_DIR)
                .join("fixture-task-b.json")
                .is_file()
        );
        Ok(())
    }

    #[test]
    fn fake_gateway_bounds_enqueue_and_read_batches_without_result_vectors() -> Result<()> {
        let directory = TempDir::new()?;
        let root = directory.path();
        let tasks: Vec<Value> = (0..5)
            .map(|ordinal| {
                task(
                    &format!("fixture-task-{ordinal}"),
                    &format!("candidate_{ordinal}"),
                )
            })
            .collect();
        let manifest = write_fixture_manifest(root, &tasks)?;
        let completions: Result<Vec<Value>> = tasks
            .iter()
            .enumerate()
            .map(|(ordinal, task)| warmup_failure_completion(task, &format!("lease-{ordinal}")))
            .collect();
        let mut gateway = FakeLocalGateway::new(root, completions?);
        let result = execute_gateway_dispatch_with_client(
            &request(&manifest, root, DispatchMode::Fresh),
            &mut gateway,
        )?;
        assert!(gateway.max_enqueue_batch <= 2);
        assert_eq!(gateway.max_read_limit, 1);
        assert_eq!(result["telemetry"]["peakLiveTaskBatch"], json!(2));
        assert_eq!(result["telemetry"]["peakLiveCompletionBatch"], json!(1));
        assert_eq!(result["telemetry"]["completedTaskCount"], json!(5));
        Ok(())
    }

    #[test]
    fn local_http_gateway_fixture_exercises_all_three_runtime_endpoints() -> Result<()> {
        let directory = TempDir::new()?;
        let root = directory.path();
        let tasks = vec![task("fixture-http-task", "candidate_http")];
        let manifest = write_fixture_manifest(root, &tasks)?;
        let (url, server) = spawn_http_gateway_fixture()?;
        let mut request = request(&manifest, root, DispatchMode::Fresh);
        request.result_batch_size = 1;
        let runtime = GatewayRuntimeOptions {
            base_url: url,
            bearer_token: Some("fixture-token".into()),
            request_timeout: Duration::from_secs(3),
        };
        let result = execute_gateway_dispatch(&request, &runtime)?;
        let state = server
            .join()
            .map_err(|_| anyhow!("fake gateway server panicked"))??;
        assert_eq!(
            state.endpoint_log,
            vec![
                "GET /results",
                "POST /tasks",
                "GET /results",
                "POST /results/ack",
            ]
        );
        assert!(state.results.is_empty());
        assert_eq!(result["completedTaskCount"], json!(1));
        assert_eq!(result["telemetry"]["rejectedCompletionCount"], json!(1));
        Ok(())
    }

    #[test]
    fn committed_receipt_resumes_without_source_reopen_and_rejects_checkpoint_tamper() -> Result<()>
    {
        let directory = TempDir::new()?;
        let root = directory.path();
        let tasks = vec![task("fixture-receipt-task", "candidate_receipt")];
        let manifest = write_fixture_manifest(root, &tasks)?;
        let mut first = FakeLocalGateway::new(
            root,
            [warmup_failure_completion(&tasks[0], "lease-receipt")?],
        );
        let committed = execute_gateway_dispatch_with_client(
            &request(&manifest, root, DispatchMode::Fresh),
            &mut first,
        )?;
        assert!(
            root.join(SIDECAR_DIR)
                .join(EXECUTION_RECEIPT_NAME)
                .is_file()
        );
        assert!(committed.get("executionReceiptSha256").is_some());
        fs::remove_file(&manifest)?;
        let mut resumed = FakeLocalGateway::new(root, Vec::<Value>::new());
        let recovered = execute_gateway_dispatch_with_client(
            &request(&manifest, root, DispatchMode::Resume),
            &mut resumed,
        )?;
        assert_eq!(
            recovered["executionReceiptSha256"],
            committed["executionReceiptSha256"]
        );
        assert_eq!(resumed.max_enqueue_batch, 0);
        let checkpoint = root.join(CHECKPOINT_NAME);
        let mut tampered = fs::read(&checkpoint)?;
        tampered.push(b' ');
        fs::write(&checkpoint, tampered)?;
        assert!(
            execute_gateway_dispatch_with_client(
                &request(&manifest, root, DispatchMode::Resume),
                &mut resumed,
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn v2_inventory_is_receipt_bounded_and_rejects_sidecar_attacks() -> Result<()> {
        let directory = TempDir::new()?;
        let root = directory.path();
        let tasks: Vec<Value> = (0..128)
            .map(|ordinal| task(&format!("inventory-{ordinal:03}"), &format!("c-{ordinal}")))
            .collect();
        let manifest = write_fixture_manifest(root, &tasks)?;
        let completions: Result<Vec<_>> = tasks
            .iter()
            .enumerate()
            .map(|(ordinal, task)| warmup_failure_completion(task, &format!("lease-{ordinal}")))
            .collect();
        let mut gateway = FakeLocalGateway::new(root, completions?);
        let mut dispatch_request = request(&manifest, root, DispatchMode::Fresh);
        // This witness durably fsyncs 128 terminal rows and their compact
        // inventory.  The shared three-second single-task fixture budget is
        // too tight on Windows when antivirus or another workspace test owns
        // the volume; production uses a 900-second completion budget.
        dispatch_request.timeout = Duration::from_secs(30);
        dispatch_request.enqueue_batch_size = 16;
        dispatch_request.result_batch_size = 16;
        let result = execute_gateway_dispatch_with_client(&dispatch_request, &mut gateway)?;
        let sidecar = root.join(SIDECAR_DIR);
        let receipt_path = sidecar.join(EXECUTION_RECEIPT_NAME);
        let receipt = read_canonical_line(&receipt_path, "test receipt")?;
        assert_eq!(receipt["schemaVersion"], EXECUTION_RECEIPT_SCHEMA);
        assert!(receipt.get("resultInventory").is_none());
        assert_eq!(receipt["resultInventoryCount"], json!(128));
        assert!(fs::metadata(&receipt_path)?.len() < 4_096);
        assert!(sidecar.join(RESULT_INVENTORY_NAME).is_file());
        assert_eq!(result["telemetry"]["peakLiveCompletionBatch"], json!(16));
        fs::remove_file(&manifest)?;

        let inventory = sidecar.join(RESULT_INVENTORY_NAME);
        let inventory_bytes = fs::read(&inventory)?;
        fs::remove_file(&inventory)?;
        let mut resumed = FakeLocalGateway::new(root, Vec::<Value>::new());
        assert!(
            execute_gateway_dispatch_with_client(
                &request(&manifest, root, DispatchMode::Resume),
                &mut resumed
            )
            .is_err()
        );
        fs::write(&inventory, &inventory_bytes)?;

        let mut reordered: Vec<&[u8]> = inventory_bytes.split_inclusive(|b| *b == b'\n').collect();
        reordered.swap(0, 1);
        fs::write(&inventory, reordered.concat())?;
        assert!(
            execute_gateway_dispatch_with_client(
                &request(&manifest, root, DispatchMode::Resume),
                &mut resumed
            )
            .is_err()
        );
        fs::write(&inventory, &inventory_bytes)?;

        fs::write(
            &inventory,
            [
                inventory_bytes.clone(),
                inventory_bytes[..inventory_bytes.iter().position(|b| *b == b'\n').unwrap() + 1]
                    .to_vec(),
            ]
            .concat(),
        )?;
        assert!(
            execute_gateway_dispatch_with_client(
                &request(&manifest, root, DispatchMode::Resume),
                &mut resumed
            )
            .is_err()
        );
        fs::write(&inventory, &inventory_bytes)?;

        let receipt_bytes = fs::read(&receipt_path)?;
        let mut foreign: Value = serde_json::from_slice(&receipt_bytes)?;
        foreign["authorityId"] = Value::String(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        );
        foreign["receiptSha256"] = Value::String(canonical_sha256_without_object_field(
            &foreign,
            "receiptSha256",
        )?);
        fs::write(&receipt_path, canonical_json_line(&foreign)?)?;
        assert!(
            execute_gateway_dispatch_with_client(
                &request(&manifest, root, DispatchMode::Resume),
                &mut resumed
            )
            .is_err()
        );
        fs::write(&receipt_path, receipt_bytes)?;
        Ok(())
    }
}
