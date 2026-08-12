//! Bounded native dispatcher for the existing local Lab gateway contract.
//!
//! The campaign-input checkpoint owns one immutable canonical JSONL task pack.
//! This crate indexes that payload without copying it into per-task files, keeps
//! only compact task-index and completion metadata in memory, and writes the
//! legacy Python checkpoint once after every task has a durable terminal record.
//! A result is durably recorded before its gateway lease is acknowledged.

#![recursion_limit = "256"]

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    fmt,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use flate2::{Compression, GzBuilder, read::GzDecoder};
use reqwest::{StatusCode, blocking::Client};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use temporal_qd_campaign_freeze::{V5CampaignInputCheckpoint, open_v5_campaign_input_checkpoint};
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
pub const COMPLETION_JOURNAL_SCHEMA: &str =
    "temporal_qd_native_gateway_completion_journal_entry_v1";
pub const FAILURE_RECEIPT_SCHEMA: &str = "temporal_qd_native_gateway_failure_receipt_v1";
pub const TELEMETRY_SCHEMA: &str = "temporal_qd_native_gateway_dispatch_telemetry_v1";
/// Current-runtime receipt ABI.  Version one deliberately remains historical:
/// v5 callers must never fall back to its embedded O(T) result inventory.
pub const EXECUTION_RECEIPT_SCHEMA: &str = "temporal_qd_native_gateway_execution_receipt_v3";
const TASK_KIND: &str = "temporal_graph_candidate_window";
#[cfg(test)]
const ADMITTED_SCHEMA: &str = "temporal_graph_candidate_window_result_v1";
const REJECTED_SCHEMA: &str = "temporal_graph_candidate_window_rejected_result_v1";
const SIDECAR_DIR: &str = ".native-gateway-dispatch";
const TASK_INDEX_NAME: &str = "task-index.jsonl";
const TASK_INDEX_ROOT_NAME: &str = "task-index.json";
const COMPLETION_JOURNAL_NAME: &str = "completion-journal.jsonl";
pub const RESULT_PACK_NAME: &str = "results.pack";
const FAILURES_DIR: &str = "failures";
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
    pub campaign_input_checkpoint_path: PathBuf,
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
        campaign_input_checkpoint_path: impl Into<PathBuf>,
        output_root: impl Into<PathBuf>,
        mode: DispatchMode,
    ) -> Self {
        Self {
            campaign_input_checkpoint_path: campaign_input_checkpoint_path.into(),
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
            !self.campaign_input_checkpoint_path.as_os_str().is_empty(),
            "campaign-input checkpoint path is required"
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
    pub result_pack_committed: bool,
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
            "resultPackCommitted": self.result_pack_committed,
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
    let campaign_input = open_v5_campaign_input_checkpoint(&request.campaign_input_checkpoint_path)
        .context("open gateway campaign-input checkpoint")?;
    fs::create_dir_all(&request.output_root).with_context(|| {
        format!(
            "create dispatcher output root: {}",
            request.output_root.display()
        )
    })?;
    let paths = DispatchPaths::new(&request.output_root);
    paths.ensure_directories()?;
    let (index, created_index) = open_or_build_task_index(request, &paths, &campaign_input)?;
    let mut journal = load_completion_journal(&paths, &index)?;
    if paths.execution_receipt.exists() {
        ensure!(
            matches!(request.mode, DispatchMode::Resume),
            "fresh dispatcher run cannot reuse a committed execution receipt"
        );
        return load_gateway_execution_receipt(
            &paths,
            &index,
            &journal,
            created_index,
            &request.campaign_input_checkpoint_path,
        );
    }
    validate_dispatch_mode(request, &journal)?;
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
    telemetry.completed_task_count = journal.len() as u64;
    telemetry.result_pack_committed = true;
    let receipt = commit_gateway_execution_receipt(&paths, &index, &journal)?;
    Ok(json!({
        "schemaVersion": DISPATCH_SCHEMA,
        "authorityId": index.authority_id,
        "taskMatrixSha256": index.task_matrix_sha256,
        "campaignInputCheckpointSha256": index.campaign_input_checkpoint_sha256,
        "campaignInputCheckpointPath": request.campaign_input_checkpoint_path,
        "taskCount": index.task_count,
        "completedTaskCount": journal.len(),
        "taskIndexRootSha256": index.root_sha256,
        "resultPackPath": paths.result_pack.to_string_lossy(),
        "sidecarRoot": paths.sidecar_root.to_string_lossy(),
        "createdTaskIndex": created_index,
        "executionReceiptSha256": receipt.receipt_sha256,
        "semanticExecutionReceiptSha256": receipt.semantic_receipt_sha256,
        "executionReceiptPath": paths.execution_receipt.to_string_lossy(),
        "telemetry": telemetry.to_value(),
    }))
}

#[derive(Clone, Debug)]
struct DispatchPaths {
    sidecar_root: PathBuf,
    task_index: PathBuf,
    task_index_root: PathBuf,
    completion_journal: PathBuf,
    result_pack: PathBuf,
    failures: PathBuf,
    execution_receipt: PathBuf,
}

impl DispatchPaths {
    fn new(output_root: &Path) -> Self {
        let sidecar_root = output_root.join(SIDECAR_DIR);
        Self {
            task_index: sidecar_root.join(TASK_INDEX_NAME),
            task_index_root: sidecar_root.join(TASK_INDEX_ROOT_NAME),
            completion_journal: sidecar_root.join(COMPLETION_JOURNAL_NAME),
            result_pack: sidecar_root.join(RESULT_PACK_NAME),
            failures: output_root.join(FAILURES_DIR),
            execution_receipt: sidecar_root.join(EXECUTION_RECEIPT_NAME),
            sidecar_root,
        }
    }

    fn ensure_directories(&self) -> Result<()> {
        for (path, label) in [
            (&self.sidecar_root, "dispatcher sidecar root"),
            (&self.failures, "dispatcher failure root"),
        ] {
            fs::create_dir_all(path)
                .with_context(|| format!("create {label}: {}", path.display()))?;
            ensure_real_directory(path, label)?;
        }
        Ok(())
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

fn journal_entries_in_ordinal_order(
    journal: &CompletionJournal,
) -> Result<Vec<&CompletionJournalEntry>> {
    let mut entries = journal.values().collect::<Vec<_>>();
    entries.sort_by_key(|entry| entry.ordinal);
    for (ordinal, entry) in entries.iter().enumerate() {
        ensure!(
            entry.ordinal == ordinal as u64,
            "completion journal ordinals are not contiguous"
        );
    }
    Ok(entries)
}

fn committed_result_pack_end(journal: &CompletionJournal) -> Result<u64> {
    let mut expected_offset = 0_u64;
    for entry in journal_entries_in_ordinal_order(journal)? {
        let record = object(&entry.record, "completion journal result record")?;
        ensure!(
            unsigned(record, "resultPackOffsetBytes")? == expected_offset,
            "completion journal result-pack offsets are not contiguous"
        );
        expected_offset = expected_offset
            .checked_add(unsigned(record, "resultPackLengthBytes")?)
            .ok_or_else(|| anyhow!("result-pack byte count overflow"))?;
    }
    Ok(expected_offset)
}

/// Reconcile an interrupted append-only result pack with the durable journal.
/// Bytes beyond the last committed journal row are an unacknowledged crash
/// tail and may be truncated. Once the execution receipt exists, every byte is
/// immutable and any length drift fails closed.
fn reconcile_result_pack(
    paths: &DispatchPaths,
    journal: &CompletionJournal,
    committed: bool,
) -> Result<u64> {
    let expected = committed_result_pack_end(journal)?;
    if !paths.result_pack.exists() {
        ensure!(
            expected == 0,
            "completion journal commits a missing result pack"
        );
        return Ok(0);
    }
    ensure_real_file(&paths.result_pack, "gateway result pack")?;
    let actual = fs::metadata(&paths.result_pack)?.len();
    ensure!(
        actual >= expected,
        "gateway result pack is shorter than the durable completion journal"
    );
    if actual > expected {
        ensure!(
            !committed,
            "committed gateway result pack has unbound trailing bytes"
        );
        let file = OpenOptions::new().write(true).open(&paths.result_pack)?;
        file.set_len(expected)
            .context("truncate uncommitted result-pack crash tail")?;
        file.sync_all()
            .context("fsync truncated result-pack crash tail")?;
    }
    Ok(expected)
}

fn read_result_blob(paths: &DispatchPaths, record: &Map<String, Value>) -> Result<Vec<u8>> {
    let offset = unsigned(record, "resultPackOffsetBytes")?;
    let length = usize::try_from(unsigned(record, "resultPackLengthBytes")?)?;
    let mut file = File::open(&paths.result_pack)
        .with_context(|| format!("open result pack: {}", paths.result_pack.display()))?;
    let pack_size = file.metadata()?.len();
    ensure!(
        offset
            .checked_add(length as u64)
            .is_some_and(|end| end <= pack_size),
        "completion record result-pack range is invalid"
    );
    file.seek(SeekFrom::Start(offset))?;
    let mut blob = vec![0_u8; length];
    file.read_exact(&mut blob)?;
    ensure!(
        sha256_prefixed(&blob) == sha_field(record, "resultBlobSha256")?,
        "completion record result-pack blob identity drifted"
    );
    Ok(blob)
}

fn result_set_semantic_sha256(journal: &CompletionJournal) -> Result<String> {
    let rows = journal
        .iter()
        .map(|(task_id, entry)| {
            let record = object(&entry.record, "completion semantic result")?;
            Ok(json!({
                "taskId": task_id,
                "taskSha256": entry.task_sha256,
                "completionSha256": entry.completion_sha256,
                "resultSemanticSha256": sha_field(record, "resultSemanticSha256")?,
                "outcome": record.get("outcome").cloned().unwrap_or(Value::String("admitted".into())),
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(canonical_sha256(&Value::Array(rows))?)
}

fn ensure_result_pack_exists(paths: &DispatchPaths) -> Result<()> {
    if paths.result_pack.exists() {
        ensure_real_file(&paths.result_pack, "gateway result pack")?;
        return Ok(());
    }
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&paths.result_pack)
        .with_context(|| format!("create result pack: {}", paths.result_pack.display()))?;
    file.sync_all().context("fsync empty result pack")?;
    Ok(())
}

fn commit_gateway_execution_receipt(
    paths: &DispatchPaths,
    index: &TaskIndex,
    journal: &CompletionJournal,
) -> Result<GatewayExecutionReceipt> {
    ensure!(
        journal.len() as u64 == index.task_count,
        "gateway execution receipt requires every task completion"
    );
    ensure_result_pack_exists(paths)?;
    let result_pack_size_bytes = reconcile_result_pack(paths, journal, false)?;
    let semantic = json!({
        "schemaVersion": EXECUTION_RECEIPT_SCHEMA,
        "runtimeRoleSha256": gateway_runtime_role_sha256()?,
        "authorityId": index.authority_id,
        "taskMatrixSha256": index.task_matrix_sha256,
        "campaignInputCheckpointSha256": index.campaign_input_checkpoint_sha256,
        "campaignTaskPackRawSha256": index.pack_sha256,
        "campaignTaskPackSizeBytes": index.pack_size_bytes,
        "taskIndexRootSha256": index.root_sha256,
        "taskCount": index.task_count,
        "completedTaskCount": journal.len(),
        "resultSetSemanticSha256": result_set_semantic_sha256(journal)?,
    });
    let semantic_receipt_sha256 = canonical_sha256(&semantic)?;
    let mut value = semantic;
    value["completionJournalSha256"] = Value::String(sha_file(&paths.completion_journal)?);
    value["resultPackSha256"] = Value::String(sha_file(&paths.result_pack)?);
    value["resultPackSizeBytes"] = Value::from(result_pack_size_bytes);
    value["resultCount"] = Value::from(journal.len() as u64);
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
    created_index: bool,
    campaign_input_checkpoint_path: &Path,
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
            "campaignInputCheckpointSha256",
            "campaignTaskPackRawSha256",
            "campaignTaskPackSizeBytes",
            "taskIndexRootSha256",
            "taskCount",
            "completedTaskCount",
            "resultSetSemanticSha256",
            "completionJournalSha256",
            "resultPackSha256",
            "resultPackSizeBytes",
            "resultCount",
            "semanticReceiptSha256",
            "receiptSha256",
        ],
        "gateway execution receipt",
    )?;
    let receipt_sha256 = sha_field(map, "receiptSha256")?;
    let result_pack_size_bytes = reconcile_result_pack(paths, journal, true)?;
    ensure!(
        text(map, "schemaVersion")? == EXECUTION_RECEIPT_SCHEMA
            && canonical_sha256_without_object_field(&value, "receiptSha256")? == receipt_sha256
            && sha_field(map, "runtimeRoleSha256")? == gateway_runtime_role_sha256()?
            && sha_field(map, "authorityId")? == index.authority_id
            && sha_field(map, "taskMatrixSha256")? == index.task_matrix_sha256
            && sha_field(map, "campaignInputCheckpointSha256")?
                == index.campaign_input_checkpoint_sha256
            && sha_field(map, "campaignTaskPackRawSha256")? == index.pack_sha256
            && unsigned(map, "campaignTaskPackSizeBytes")? == index.pack_size_bytes
            && sha_field(map, "taskIndexRootSha256")? == index.root_sha256
            && unsigned(map, "taskCount")? == index.task_count
            && unsigned(map, "completedTaskCount")? == journal.len() as u64
            && sha_field(map, "resultSetSemanticSha256")? == result_set_semantic_sha256(journal)?
            && sha_field(map, "completionJournalSha256")? == sha_file(&paths.completion_journal)?
            && sha_field(map, "resultPackSha256")? == sha_file(&paths.result_pack)?
            && unsigned(map, "resultPackSizeBytes")? == result_pack_size_bytes
            && unsigned(map, "resultCount")? == journal.len() as u64,
        "gateway execution receipt identity/output binding drifted"
    );
    let semantic = json!({
        "schemaVersion": EXECUTION_RECEIPT_SCHEMA,
        "runtimeRoleSha256": gateway_runtime_role_sha256()?,
        "authorityId": index.authority_id,
        "taskMatrixSha256": index.task_matrix_sha256,
        "campaignInputCheckpointSha256": index.campaign_input_checkpoint_sha256,
        "campaignTaskPackRawSha256": index.pack_sha256,
        "campaignTaskPackSizeBytes": index.pack_size_bytes,
        "taskIndexRootSha256": index.root_sha256,
        "taskCount": index.task_count,
        "completedTaskCount": journal.len(),
        "resultSetSemanticSha256": result_set_semantic_sha256(journal)?,
    });
    ensure!(
        canonical_sha256(&semantic)? == sha_field(map, "semanticReceiptSha256")?,
        "gateway execution receipt semantic identity drifted"
    );
    Ok(json!({
        "schemaVersion": DISPATCH_SCHEMA,
        "authorityId": index.authority_id,
        "taskMatrixSha256": index.task_matrix_sha256,
        "campaignInputCheckpointSha256": index.campaign_input_checkpoint_sha256,
        "campaignInputCheckpointPath": campaign_input_checkpoint_path,
        "taskCount": index.task_count,
        "completedTaskCount": journal.len(),
        "taskIndexRootSha256": index.root_sha256,
        "resultPackPath": paths.result_pack.to_string_lossy(),
        "sidecarRoot": paths.sidecar_root.to_string_lossy(),
        "createdTaskIndex": created_index,
        "executionReceiptSha256": receipt_sha256,
        "semanticExecutionReceiptSha256": sha_field(map, "semanticReceiptSha256")?,
        "executionReceiptPath": paths.execution_receipt.to_string_lossy(),
        // Telemetry is intentionally not a durable semantic claim.
        "telemetry": GatewayDispatchTelemetry { task_count: index.task_count, completed_task_count: journal.len() as u64, result_pack_committed: true, ..Default::default() }.to_value(),
    }))
}

#[derive(Clone, Debug)]
struct TaskIndexEntry {
    ordinal: u64,
    task_id: String,
    task_sha256: String,
    offset_bytes: u64,
    length_bytes: u64,
    entry_sha256: String,
}

impl TaskIndexEntry {
    fn new(
        ordinal: u64,
        task_id: String,
        task_sha256: String,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> Result<Self> {
        let mut value = json!({
            "schemaVersion": TASK_INDEX_ENTRY_SCHEMA,
            "ordinal": ordinal,
            "taskId": task_id,
            "taskSha256": task_sha256,
            "offsetBytes": offset_bytes,
            "lengthBytes": length_bytes,
        });
        let entry_sha256 = canonical_sha256(&value)?;
        value["entrySha256"] = Value::String(entry_sha256);
        Self::from_value(&value)
    }

    fn to_value(&self) -> Value {
        json!({
            "schemaVersion": TASK_INDEX_ENTRY_SCHEMA,
            "ordinal": self.ordinal,
            "taskId": self.task_id,
            "taskSha256": self.task_sha256,
            "offsetBytes": self.offset_bytes,
            "lengthBytes": self.length_bytes,
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
                "offsetBytes",
                "lengthBytes",
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
        let length_bytes = unsigned(map, "lengthBytes")?;
        ensure!(length_bytes > 1, "task index entry length is invalid");
        Ok(Self {
            ordinal: unsigned(map, "ordinal")?,
            task_id: safe_identifier(field(map, "taskId")?, "task index task id")?,
            task_sha256: sha_field(map, "taskSha256")?,
            offset_bytes: unsigned(map, "offsetBytes")?,
            length_bytes,
            entry_sha256,
        })
    }
}

#[derive(Clone, Debug)]
struct TaskIndex {
    campaign_input_checkpoint_sha256: String,
    task_pack_path: PathBuf,
    authority_id: String,
    task_matrix_sha256: String,
    task_count: u64,
    pack_sha256: String,
    pack_size_bytes: u64,
    index_sha256: String,
    index_size_bytes: u64,
    root_sha256: String,
    entries: BTreeMap<String, TaskIndexEntry>,
}

impl TaskIndex {
    fn from_root_value(value: &Value, checkpoint: &V5CampaignInputCheckpoint) -> Result<Self> {
        let map = object(value, "task index root")?;
        exact_keys(
            map,
            &[
                "schemaVersion",
                "campaignInputCheckpointSha256",
                "authorityId",
                "taskMatrixSha256",
                "taskCount",
                "campaignTaskPackRawSha256",
                "campaignTaskPackSizeBytes",
                "taskIndexRelativePath",
                "taskIndexSha256",
                "taskIndexSizeBytes",
                "taskIndexRootSha256",
            ],
            "task index root",
        )?;
        ensure!(
            text(map, "schemaVersion")? == TASK_INDEX_SCHEMA
                && text(map, "taskIndexRelativePath")? == TASK_INDEX_NAME,
            "task index root schema/path is incompatible"
        );
        let root_sha256 = sha_field(map, "taskIndexRootSha256")?;
        ensure!(
            canonical_sha256_without_object_field(value, "taskIndexRootSha256")? == root_sha256,
            "task index root self identity mismatch"
        );
        ensure!(
            sha_field(map, "campaignInputCheckpointSha256")? == checkpoint.checkpoint_sha256
                && sha_field(map, "authorityId")? == checkpoint.authority_id
                && sha_field(map, "taskMatrixSha256")? == checkpoint.task_matrix_sha256
                && unsigned(map, "taskCount")? == checkpoint.task_count
                && sha_field(map, "campaignTaskPackRawSha256")? == checkpoint.task_pack_raw_sha256
                && unsigned(map, "campaignTaskPackSizeBytes")? == checkpoint.task_pack_size_bytes,
            "task index root campaign-input binding drifted"
        );
        Ok(Self {
            campaign_input_checkpoint_sha256: sha_field(map, "campaignInputCheckpointSha256")?,
            task_pack_path: checkpoint.task_pack_path.clone(),
            authority_id: sha_field(map, "authorityId")?,
            task_matrix_sha256: sha_field(map, "taskMatrixSha256")?,
            task_count: unsigned(map, "taskCount")?,
            pack_sha256: sha_field(map, "campaignTaskPackRawSha256")?,
            pack_size_bytes: unsigned(map, "campaignTaskPackSizeBytes")?,
            index_sha256: sha_field(map, "taskIndexSha256")?,
            index_size_bytes: unsigned(map, "taskIndexSizeBytes")?,
            root_sha256,
            entries: BTreeMap::new(),
        })
    }
}

fn open_or_build_task_index(
    _request: &GatewayDispatchRequest,
    paths: &DispatchPaths,
    checkpoint: &V5CampaignInputCheckpoint,
) -> Result<(TaskIndex, bool)> {
    if paths.task_index_root.exists() {
        ensure!(
            paths.task_index.is_file(),
            "dispatcher task index root commits a missing task index"
        );
        return load_task_index(paths, checkpoint).map(|index| (index, false));
    }
    if paths.task_index.exists() {
        ensure_real_file(&paths.task_index, "interrupted task index")?;
        fs::remove_file(&paths.task_index).context("discard interrupted task index")?;
    }
    build_task_index(checkpoint, paths).map(|index| (index, true))
}

fn build_task_index(
    checkpoint: &V5CampaignInputCheckpoint,
    paths: &DispatchPaths,
) -> Result<TaskIndex> {
    let task_pack_path = existing_file(
        &checkpoint.task_pack_path,
        "immutable campaign-input task pack",
    )?;
    let index_staging = paths.sidecar_root.join(".task-index.staging");
    if index_staging.exists() {
        ensure_real_file(&index_staging, "stale task-index staging file")?;
        fs::remove_file(&index_staging).context("remove stale task-index staging file")?;
    }
    let mut index_writer = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&index_staging)
            .context("create task-index staging file")?,
    );
    let mut index_digest = Sha256::new();
    let mut task_pack_digest = Sha256::new();
    let mut matrix_digest = Sha256::new();
    matrix_digest.update(b"[");
    let mut index_size = 0_u64;
    let mut pack_size = 0_u64;
    let mut entries = BTreeMap::new();
    let mut reader = BufReader::new(File::open(&task_pack_path).with_context(|| {
        format!(
            "open campaign-input task pack: {}",
            task_pack_path.display()
        )
    })?);
    let mut ordinal = 0_u64;
    loop {
        let mut line = Vec::new();
        let read = reader
            .read_until(b'\n', &mut line)
            .context("stream campaign-input task-pack row")?;
        if read == 0 {
            break;
        }
        ensure!(
            line.len() > 1 && line.last() == Some(&b'\n'),
            "campaign-input task-pack row is not canonical JSONL"
        );
        let task: Value = serde_json::from_slice(&line[..line.len() - 1])
            .context("parse campaign-input task-pack row")?;
        ensure!(
            canonical_json_line(&task)? == line,
            "campaign-input task-pack row is not canonical JSONL"
        );
        let task_map = object(&task, "campaign-input task")?;
        let task_id = safe_identifier(field(task_map, "task_id")?, "campaign-input task id")?;
        let task_sha256 = canonical_sha256(&task)?;
        let length_bytes = line.len() as u64;
        let entry = TaskIndexEntry::new(ordinal, task_id, task_sha256, pack_size, length_bytes)?;
        let entry_bytes = canonical_json_line(&entry.to_value())?;
        index_writer.write_all(&entry_bytes)?;
        index_digest.update(&entry_bytes);
        index_size = index_size
            .checked_add(entry_bytes.len() as u64)
            .ok_or_else(|| anyhow!("task index byte count overflow"))?;
        task_pack_digest.update(&line);
        if ordinal > 0 {
            matrix_digest.update(b",");
        }
        matrix_digest.update(canonical_json_bytes(&task)?);
        pack_size = pack_size
            .checked_add(length_bytes)
            .ok_or_else(|| anyhow!("campaign task-pack byte count overflow"))?;
        ensure!(
            entries.insert(entry.task_id.clone(), entry).is_none(),
            "campaign-input task pack repeats a task id"
        );
        ordinal = ordinal
            .checked_add(1)
            .ok_or_else(|| anyhow!("task index entry count overflow"))?;
    }
    matrix_digest.update(b"]");
    let task_pack_sha256 = digest_sha256_prefixed(task_pack_digest.finalize());
    let task_matrix_sha256 = digest_sha256_prefixed(matrix_digest.finalize());
    ensure!(
        ordinal == checkpoint.task_count
            && pack_size == checkpoint.task_pack_size_bytes
            && task_pack_sha256 == checkpoint.task_pack_raw_sha256
            && task_matrix_sha256 == checkpoint.task_matrix_sha256,
        "campaign-input task-pack/checkpoint binding drifted"
    );
    index_writer.flush()?;
    index_writer
        .get_ref()
        .sync_all()
        .context("fsync task index")?;
    drop(index_writer);
    let index_sha256 = digest_sha256_prefixed(index_digest.finalize());
    let mut root_value = json!({
        "schemaVersion": TASK_INDEX_SCHEMA,
        "campaignInputCheckpointSha256": checkpoint.checkpoint_sha256.clone(),
        "authorityId": checkpoint.authority_id.clone(),
        "taskMatrixSha256": checkpoint.task_matrix_sha256.clone(),
        "campaignTaskPackRawSha256": checkpoint.task_pack_raw_sha256.clone(),
        "campaignTaskPackSizeBytes": checkpoint.task_pack_size_bytes,
        "taskCount": checkpoint.task_count,
        "taskIndexRelativePath": TASK_INDEX_NAME,
        "taskIndexSha256": index_sha256,
        "taskIndexSizeBytes": index_size,
    });
    let root_sha256 = canonical_sha256(&root_value)?;
    root_value["taskIndexRootSha256"] = Value::String(root_sha256);
    publish_staging_once(&index_staging, &paths.task_index)?;
    write_immutable_canonical(&paths.task_index_root, &root_value)?;
    let mut index = TaskIndex::from_root_value(&root_value, checkpoint)?;
    index.entries = entries;
    Ok(index)
}

fn load_task_index(
    paths: &DispatchPaths,
    checkpoint: &V5CampaignInputCheckpoint,
) -> Result<TaskIndex> {
    let root_value = read_canonical_line(&paths.task_index_root, "task index root")?;
    let mut index = TaskIndex::from_root_value(&root_value, checkpoint)?;
    ensure_real_file(&index.task_pack_path, "campaign-input task pack")?;
    ensure_real_file(&paths.task_index, "task index")?;
    ensure!(
        fs::metadata(&index.task_pack_path)?.len() == index.pack_size_bytes
            && sha_file(&index.task_pack_path)? == index.pack_sha256
            && fs::metadata(&paths.task_index)?.len() == index.index_size_bytes
            && sha_file(&paths.task_index)? == index.index_sha256,
        "campaign task-pack/task-index byte identity drifted"
    );
    let mut previous_end = 0_u64;
    let mut count = 0_u64;
    stream_task_index(paths, |entry| {
        ensure!(
            entry.ordinal == count
                && entry.offset_bytes == previous_end
                && entry
                    .offset_bytes
                    .checked_add(entry.length_bytes)
                    .is_some_and(|end| end <= index.pack_size_bytes),
            "task index offsets/ordinals are not contiguous"
        );
        previous_end = previous_end
            .checked_add(entry.length_bytes)
            .ok_or_else(|| anyhow!("task-pack offset overflow"))?;
        ensure!(
            index.entries.insert(entry.task_id.clone(), entry).is_none(),
            "task index contains duplicate task ids"
        );
        count += 1;
        Ok(())
    })?;
    ensure!(
        count == index.task_count && previous_end == index.pack_size_bytes,
        "task index task/pack size drifted"
    );
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

fn load_task_object(index: &TaskIndex, entry: &TaskIndexEntry) -> Result<Value> {
    let mut file = File::open(&index.task_pack_path).with_context(|| {
        format!(
            "open campaign task pack: {}",
            index.task_pack_path.display()
        )
    })?;
    let pack_size = fs::metadata(&index.task_pack_path)?.len();
    ensure!(
        entry
            .offset_bytes
            .checked_add(entry.length_bytes)
            .is_some_and(|end| end <= pack_size),
        "task pack entry range is invalid"
    );
    file.seek(SeekFrom::Start(entry.offset_bytes))?;
    let mut bytes = vec![0_u8; entry.length_bytes as usize];
    file.read_exact(&mut bytes)?;
    ensure!(
        bytes.last() == Some(&b'\n'),
        "task pack entry lacks canonical newline"
    );
    let task: Value = serde_json::from_slice(&bytes[..bytes.len() - 1])?;
    ensure!(
        canonical_json_line(&task)? == bytes && canonical_sha256(&task)? == entry.task_sha256,
        "task pack entry identity drifted"
    );
    let task_id = safe_identifier(
        field(object(&task, "task pack task")?, "task_id")?,
        "task pack task id",
    )?;
    ensure!(task_id == entry.task_id, "task pack task id drifted");
    Ok(task)
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
        reconcile_result_pack(paths, &BTreeMap::new(), false)?;
        return Ok(BTreeMap::new());
    }
    ensure_real_file(&paths.completion_journal, "completion journal")?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&paths.completion_journal)?;
    let mut reader = BufReader::new(file);
    let mut journal = BTreeMap::new();
    let mut ordinal = 0_u64;
    let mut valid_bytes = 0_u64;
    loop {
        let mut line = Vec::new();
        let read = reader.read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        if line.last() != Some(&b'\n') {
            ensure!(
                !paths.execution_receipt.exists(),
                "committed completion journal has a partial final row"
            );
            reader
                .get_ref()
                .set_len(valid_bytes)
                .context("truncate incomplete completion-journal crash tail")?;
            reader
                .get_ref()
                .sync_all()
                .context("fsync truncated completion-journal crash tail")?;
            break;
        }
        let value: Value = serde_json::from_slice(&line[..line.len() - 1])
            .with_context(|| format!("parse completion journal row {}", ordinal + 1))?;
        ensure!(
            canonical_json_line(&value)? == line,
            "completion journal row {} is not canonical JSONL",
            ordinal + 1
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
        ensure!(
            journal.insert(entry.task_id.clone(), entry).is_none(),
            "completion journal contains duplicate task ids"
        );
        ordinal = ordinal
            .checked_add(1)
            .ok_or_else(|| anyhow!("completion journal ordinal overflow"))?;
        valid_bytes = valid_bytes
            .checked_add(read as u64)
            .ok_or_else(|| anyhow!("completion journal byte count overflow"))?;
    }
    ensure!(
        journal.len() as u64 <= index.task_count,
        "completion journal exceeds immutable task count"
    );
    reconcile_result_pack(paths, &journal, paths.execution_receipt.exists())?;
    for entry in journal.values() {
        let index_entry = find_index_entry(paths, index, &entry.task_id)?;
        validate_completion_record(paths, index, &index_entry, &entry.record)?;
    }
    Ok(journal)
}

#[derive(Clone, Debug)]
struct PendingCompletion {
    entry: CompletionJournalEntry,
    blob: Vec<u8>,
}

fn commit_completion_batch(
    paths: &DispatchPaths,
    journal: &mut CompletionJournal,
    pending: Vec<PendingCompletion>,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    let mut expected_offset = reconcile_result_pack(paths, journal, false)?;
    let mut encoded_rows = Vec::with_capacity(pending.len());
    for (index, item) in pending.iter().enumerate() {
        ensure!(
            item.entry.ordinal == journal.len() as u64 + index as u64,
            "completion batch journal ordinals are not contiguous"
        );
        ensure!(
            !journal.contains_key(&item.entry.task_id),
            "completion batch repeats a durable task"
        );
        let record = object(&item.entry.record, "pending completion record")?;
        ensure!(
            unsigned(record, "resultPackOffsetBytes")? == expected_offset
                && unsigned(record, "resultPackLengthBytes")? == item.blob.len() as u64
                && sha256_prefixed(&item.blob) == sha_field(record, "resultBlobSha256")?,
            "completion batch result-pack binding drifted"
        );
        expected_offset = expected_offset
            .checked_add(item.blob.len() as u64)
            .ok_or_else(|| anyhow!("result-pack byte count overflow"))?;
        encoded_rows.push(canonical_json_line(&item.entry.to_value())?);
    }

    let mut pack = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.result_pack)
        .with_context(|| format!("open result pack: {}", paths.result_pack.display()))?;
    ensure!(
        pack.metadata()?.len() == committed_result_pack_end(journal)?,
        "result pack changed before completion batch commit"
    );
    for item in &pending {
        pack.write_all(&item.blob)?;
    }
    pack.flush()?;
    pack.sync_data()
        .context("fsync result-pack completion batch")?;
    drop(pack);

    let mut journal_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.completion_journal)
        .with_context(|| {
            format!(
                "open completion journal: {}",
                paths.completion_journal.display()
            )
        })?;
    for row in &encoded_rows {
        journal_file.write_all(row)?;
    }
    journal_file.flush()?;
    journal_file
        .sync_data()
        .context("fsync completion-journal batch")?;
    drop(journal_file);

    for item in pending {
        journal.insert(item.entry.task_id.clone(), item.entry);
    }
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

fn validate_completion_record_shape(
    index: &TaskIndex,
    entry: &TaskIndexEntry,
    record: &Value,
) -> Result<bool> {
    let record_map = object(record, "completion record")?;
    let mut expected: BTreeSet<&str> = [
        "resultSha256",
        "candidateId",
        "resultCodec",
        "resultSemanticSha256",
        "resultSemanticSizeBytes",
        "resultUncompressedSha256",
        "resultUncompressedSizeBytes",
        "resultBlobSha256",
        "resultBlobSizeBytes",
        "resultPackOffsetBytes",
        "resultPackLengthBytes",
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
        "resultPackOffsetBytes",
        "resultPackLengthBytes",
    ] {
        unsigned(record_map, key)?;
    }
    ensure!(
        unsigned(record_map, "resultPackLengthBytes")?
            == unsigned(record_map, "resultBlobSizeBytes")?,
        "completion record result-pack length drifted from blob length"
    );
    ensure!(
        text(record_map, "resultCodec")? == "gzip-json-v1",
        "completion record codec is incompatible"
    );
    let task = load_task_object(index, entry)?;
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
    Ok(rejected)
}

fn validate_completion_record(
    paths: &DispatchPaths,
    index: &TaskIndex,
    entry: &TaskIndexEntry,
    record: &Value,
) -> Result<()> {
    let rejected = validate_completion_record_shape(index, entry, record)?;
    let record_map = object(record, "completion record")?;
    let blob = read_result_blob(paths, record_map)?;
    let (material, metadata) = decode_gzip_json_blob(&blob, "result-pack blob")?;
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
    let task = load_task_object(index, entry)?;
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

fn decode_gzip_json_blob(blob: &[u8], label: &str) -> Result<(Value, ResultCodecMetadata)> {
    let mut decoder = GzDecoder::new(blob);
    let mut uncompressed = Vec::new();
    decoder
        .read_to_end(&mut uncompressed)
        .with_context(|| format!("inflate {label}"))?;
    let mut inner = decoder.into_inner();
    ensure!(
        inner.read(&mut [0_u8; 1])? == 0,
        "{label} has trailing gzip bytes"
    );
    let value: Value =
        serde_json::from_slice(&uncompressed).with_context(|| format!("parse {label} JSON"))?;
    ensure!(
        python_pretty_json_line(&value, JsonNewline::Lf)? == uncompressed,
        "{label} uncompressed JSON is not deterministic pretty JSON"
    );
    let (expected_blob, metadata) = encode_gzip_json(&value)?;
    ensure!(
        expected_blob == blob,
        "{label} is not canonical deterministic gzip"
    );
    Ok((value, metadata))
}

fn completion_record(
    index: &TaskIndex,
    entry: &TaskIndexEntry,
    material: &Value,
    result_pack_offset_bytes: u64,
) -> Result<(Value, Vec<u8>)> {
    let (blob, metadata) = encode_gzip_json(material)?;
    let task = load_task_object(index, entry)?;
    let payload = object(
        field(object(&task, "completion record task")?, "payload")?,
        "completion record task payload",
    )?;
    let mut record = json!({
        "resultSha256": metadata.semantic_sha256,
        "candidateId": field(payload, "candidate_id")?,
        "resultPackOffsetBytes": result_pack_offset_bytes,
        "resultPackLengthBytes": metadata.blob_size_bytes,
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
    validate_completion_record_shape(index, entry, &record)?;
    Ok((record, blob))
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

fn read_failure_receipt(
    paths: &DispatchPaths,
    index: &TaskIndex,
    entry: &TaskIndexEntry,
) -> Result<FailureReceipt> {
    let value = read_canonical_line(&paths.failure_path(&entry.task_id)?, "failure receipt")?;
    let receipt = FailureReceipt::from_value(&value)?;
    ensure!(
        receipt.task_id == entry.task_id && receipt.task_sha256 == entry.task_sha256,
        "failure receipt task binding drifted"
    );
    require_completion_routing(&load_task_object(index, entry)?, &receipt.completion)?;
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
    let mut pending = Vec::new();
    let mut result_pack_offset = committed_result_pack_end(journal)?;
    for entry in index.entries.values() {
        if journal.contains_key(&entry.task_id) {
            continue;
        }
        let failure_path = paths.failure_path(&entry.task_id)?;
        if !failure_path.exists() {
            continue;
        }
        let receipt = read_failure_receipt(paths, index, entry)?;
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
        let task = load_task_object(index, entry)?;
        let material = rejected_material(&task, &receipt.completion, classification, worker_error)?;
        ensure!(
            admit_candidate_window_task_result(&task, &material)?
                == CandidateWindowResultAdmission::Rejected,
            "recovered deterministic failure did not produce an admitted rejection"
        );
        let (record, blob) = completion_record(index, entry, &material, result_pack_offset)?;
        result_pack_offset = result_pack_offset
            .checked_add(blob.len() as u64)
            .ok_or_else(|| anyhow!("result-pack byte count overflow"))?;
        pending.push(PendingCompletion {
            entry: CompletionJournalEntry::new(
                journal.len() as u64 + pending.len() as u64,
                entry,
                receipt.completion_sha256,
                record,
            )?,
            blob,
        });
        telemetry.recovered_completion_count = telemetry
            .recovered_completion_count
            .checked_add(1)
            .ok_or_else(|| anyhow!("recovered completion count overflow"))?;
        telemetry.rejected_completion_count = telemetry
            .rejected_completion_count
            .checked_add(1)
            .ok_or_else(|| anyhow!("rejected completion count overflow"))?;
    }
    commit_completion_batch(paths, journal, pending)?;
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
    let mut pending = Vec::<PendingCompletion>::new();
    let mut pending_semantics = BTreeMap::<String, String>::new();
    let mut fatal_after_ack = None::<String>;
    let mut completion_batch_bytes = 0_usize;
    let mut result_pack_offset = committed_result_pack_end(journal)?;
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
        match prepare_completion(
            paths,
            index,
            journal,
            completion,
            telemetry,
            journal.len() as u64 + pending.len() as u64,
            result_pack_offset,
        )? {
            PreparedCompletion::Duplicate { lease } => {
                leases.insert(lease);
            }
            PreparedCompletion::Pending { lease, completion } => {
                let task_id = completion.entry.task_id.clone();
                let semantic = sha_field(
                    object(&completion.entry.record, "pending completion record")?,
                    "resultSha256",
                )?;
                if let Some(previous) = pending_semantics.get(&task_id) {
                    ensure!(
                        previous == &semantic,
                        "conflicting duplicate completion material in one gateway batch"
                    );
                    telemetry.duplicate_redelivery_count = telemetry
                        .duplicate_redelivery_count
                        .checked_add(1)
                        .ok_or_else(|| anyhow!("duplicate redelivery count overflow"))?;
                    leases.insert(lease);
                    continue;
                }
                result_pack_offset = result_pack_offset
                    .checked_add(completion.blob.len() as u64)
                    .ok_or_else(|| anyhow!("result-pack byte count overflow"))?;
                pending_semantics.insert(task_id, semantic);
                pending.push(completion);
                leases.insert(lease);
            }
            PreparedCompletion::FatalAfterReceipt { lease, detail } => {
                leases.insert(lease);
                fatal_after_ack = Some(detail);
                break;
            }
        }
    }
    // One result-pack sync and one journal sync durably commit the whole
    // delivery batch before any lease acknowledgement.
    commit_completion_batch(paths, journal, pending)?;
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

enum PreparedCompletion {
    Duplicate {
        lease: String,
    },
    Pending {
        lease: String,
        completion: PendingCompletion,
    },
    /// The exact failure receipt is already fsynced, so it is safe and
    /// necessary to acknowledge before surfacing the terminal controller
    /// error. Resume will fail closed from that receipt rather than enqueue.
    FatalAfterReceipt {
        lease: String,
        detail: String,
    },
}

fn prepare_completion(
    paths: &DispatchPaths,
    index: &TaskIndex,
    journal: &CompletionJournal,
    completion: &Value,
    telemetry: &mut GatewayDispatchTelemetry,
    ordinal: u64,
    result_pack_offset: u64,
) -> Result<PreparedCompletion> {
    let completion_map = object(completion, "gateway completion")?;
    let task_id = safe_identifier(field(completion_map, "task_id")?, "completion task id")?;
    let entry = find_index_entry(paths, index, &task_id)?;
    let task = load_task_object(index, &entry)?;
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
            return Ok(PreparedCompletion::FatalAfterReceipt {
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
        return Ok(PreparedCompletion::Duplicate { lease });
    }
    let (record, blob) = completion_record(index, &entry, &material, result_pack_offset)?;
    Ok(PreparedCompletion::Pending {
        lease,
        completion: PendingCompletion {
            entry: CompletionJournalEntry::new(ordinal, &entry, completion_sha256, record)?,
            blob,
        },
    })
}

fn enqueue_missing_tasks(
    client: &mut dyn GatewayClient,
    request: &GatewayDispatchRequest,
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
        let task = load_task_object(index, entry)?;
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

fn validate_dispatch_mode(
    request: &GatewayDispatchRequest,
    journal: &CompletionJournal,
) -> Result<()> {
    if matches!(request.mode, DispatchMode::Fresh) {
        ensure!(
            journal.is_empty(),
            "fresh dispatcher run already has durable task completions"
        );
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
    use temporal_qd_contract::CONTRACT_VERSION;

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
        let screening_root = root.join("screening-run");
        fs::create_dir_all(&screening_root)?;
        let path = screening_root.join("tasks.jsonl");
        let mut task_pack = Vec::new();
        for task in tasks {
            task_pack.extend(canonical_json_line(task)?);
        }
        fs::write(&path, task_pack)?;
        let candidates = tasks
            .iter()
            .map(|task| {
                let task = object(task, "fixture task")?;
                Ok(json!({"candidateId":text(task,"lane_id")?}))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut cohort = json!({
            "schemaVersion":"temporal_qd_rotating_cohort_population_v1",
            "generationIndex":1,
            "panelId":"panel-a",
            "cohortRole":"proposal_current_panel",
            "rotatingEvidenceSha256":SHA_A,
            "candidateCount":tasks.len(),
            "candidates":candidates,
            "proposalPopulation":true,
        });
        cohort["populationSha256"] = Value::String(canonical_sha256(&cohort)?);
        let cohort_path = root.join("cohort-population.json");
        fs::write(
            &cohort_path,
            python_pretty_json_line(&cohort, JsonNewline::Lf)?,
        )?;
        let manifest_bytes = fs::metadata(&path)?.len();
        let cohort_bytes = fs::metadata(&cohort_path)?.len();
        let mut campaign_input = json!({
            "schemaVersion":"temporal_qd_v5_campaign_input_checkpoint_v1",
            "contractVersion":CONTRACT_VERSION,
            "manifestSha256":SHA_A,
            "nativeRuntimeAuthoritySha256":SHA_B,
            "generationIndex":1,
            "campaignRole":"proposal_current_panel",
            "panelId":"panel-a",
            "authorityId":AUTHORITY,
            "campaignSha256":SHA_C,
            "evaluationIdentitySha256":SHA_A,
            "taskMatrixSha256":matrix,
            "candidateCount":tasks.len(),
            "windowCount":1,
            "taskCount":tasks.len(),
            "tasks":{
                "relativePath":"screening-run/tasks.jsonl",
                "rawSha256":sha_file(&path)?,
                "sizeBytes":manifest_bytes,
                "recordCount":tasks.len(),
                "taskMatrixSha256":matrix,
            },
            "cohortPopulation":{
                "relativePath":"cohort-population.json",
                "rawSha256":sha_file(&cohort_path)?,
                "sizeBytes":cohort_bytes,
                "populationSha256":cohort["populationSha256"],
            },
            "sourceInputs":{
                "evaluationPopulationRawSha256":SHA_A,
                "templatePreparationSha256":SHA_B,
                "constructionCatalogSha256":SHA_C,
                "preparationSha256":AUTHORITY,
            },
            "artifactMetrics":{
                "payloadFileCount":2,
                "payloadBytes":manifest_bytes + cohort_bytes,
                "taskPackBytes":manifest_bytes,
                "cohortPopulationBytes":cohort_bytes,
            },
        });
        campaign_input["checkpointSha256"] = Value::String(canonical_sha256(&campaign_input)?);
        let campaign_input_path = root.join("campaign-input-checkpoint.json");
        fs::write(&campaign_input_path, canonical_json_line(&campaign_input)?)?;
        Ok(campaign_input_path)
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
        // Resume reopens the same committed campaign-input checkpoint.  Its
        // two payloads remain the immutable campaign boundary.

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
        let journal_path = root.join(SIDECAR_DIR).join(COMPLETION_JOURNAL_NAME);
        let rejected = fs::read_to_string(&journal_path)?
            .lines()
            .map(serde_json::from_str::<Value>)
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            .find(|row| row["taskId"] == json!("fixture-task-b"))
            .ok_or_else(|| anyhow!("rejected fixture task is absent from result journal"))?;
        assert_eq!(rejected["record"]["outcome"], json!("rejected"));
        assert_eq!(
            rejected["record"]["rejectionCode"],
            json!("aligned_scoring_warmup_insufficient")
        );
        assert!(root.join(SIDECAR_DIR).join(RESULT_PACK_NAME).is_file());
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
    fn committed_receipt_resumes_without_source_reopen_and_rejects_pack_tamper() -> Result<()> {
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
        let result_pack = root.join(SIDECAR_DIR).join(RESULT_PACK_NAME);
        let mut tampered = fs::read(&result_pack)?;
        tampered.push(b' ');
        fs::write(&result_pack, tampered)?;
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
    fn packed_results_are_receipt_bounded_and_reject_byte_drift() -> Result<()> {
        let directory = TempDir::new()?;
        let root = directory.path();
        let tasks: Vec<Value> = (0..128)
            .map(|ordinal| task(&format!("packed-{ordinal:03}"), &format!("c-{ordinal}")))
            .collect();
        let manifest = write_fixture_manifest(root, &tasks)?;
        let completions: Result<Vec<_>> = tasks
            .iter()
            .enumerate()
            .map(|(ordinal, task)| successful_completion(task, &format!("lease-{ordinal}")))
            .collect();
        let mut gateway = FakeLocalGateway::new(root, completions?);
        let mut dispatch_request = request(&manifest, root, DispatchMode::Fresh);
        dispatch_request.timeout = Duration::from_secs(30);
        dispatch_request.enqueue_batch_size = 16;
        dispatch_request.result_batch_size = 16;
        let result = execute_gateway_dispatch_with_client(&dispatch_request, &mut gateway)?;
        let sidecar = root.join(SIDECAR_DIR);
        let receipt_path = sidecar.join(EXECUTION_RECEIPT_NAME);
        let receipt = read_canonical_line(&receipt_path, "test receipt")?;
        assert_eq!(receipt["schemaVersion"], EXECUTION_RECEIPT_SCHEMA);
        assert_eq!(receipt["resultCount"], json!(128));
        assert!(receipt.get("resultInventoryRootSha256").is_none());
        assert!(receipt["resultPackSizeBytes"].as_u64().unwrap_or(0) > 0);
        assert!(fs::metadata(&receipt_path)?.len() < 4_096);
        assert_eq!(result["telemetry"]["peakLiveCompletionBatch"], json!(16));
        assert_eq!(result["telemetry"]["resultPackCommitted"], json!(true));
        assert_eq!(
            fs::read_dir(&sidecar)?
                .collect::<std::io::Result<Vec<_>>>()?
                .len(),
            5,
            "gateway sidecar should remain constant-size regardless of task count"
        );
        assert!(!root.join("results").exists());

        let pack = sidecar.join(RESULT_PACK_NAME);
        let pack_bytes = fs::read(&pack)?;
        let mut resumed = FakeLocalGateway::new(root, Vec::<Value>::new());

        fs::remove_file(&pack)?;
        assert!(
            execute_gateway_dispatch_with_client(
                &request(&manifest, root, DispatchMode::Resume),
                &mut resumed
            )
            .is_err()
        );
        fs::write(&pack, &pack_bytes)?;

        let mut trailing = pack_bytes.clone();
        trailing.push(0);
        fs::write(&pack, trailing)?;
        assert!(
            execute_gateway_dispatch_with_client(
                &request(&manifest, root, DispatchMode::Resume),
                &mut resumed
            )
            .is_err()
        );
        fs::write(&pack, &pack_bytes)?;

        let mut changed = pack_bytes.clone();
        changed[0] ^= 1;
        fs::write(&pack, changed)?;
        assert!(
            execute_gateway_dispatch_with_client(
                &request(&manifest, root, DispatchMode::Resume),
                &mut resumed
            )
            .is_err()
        );
        fs::write(&pack, &pack_bytes)?;

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
