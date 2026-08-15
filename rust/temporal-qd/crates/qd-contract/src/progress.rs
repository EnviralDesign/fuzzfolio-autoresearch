//! Operational-only progress and stage timing for current Temporal QD v5 binaries.
//!
//! Nothing in this module is an input to scientific decisions, canonical
//! identities, receipts, restart admission, or output artifacts.  Events are
//! emitted only on stderr, are centrally cadence-throttled, and deliberately
//! fail open if the diagnostic stream is unavailable.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    io::{self, Write},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

pub const NATIVE_V5_PROGRESS_SCHEMA: &str = "temporal_qd_v5_native_progress_v1";
pub const NATIVE_V5_STAGE_SUMMARY_SCHEMA: &str = "temporal_qd_v5_native_stage_summary_v1";
pub const NATIVE_V5_STAGE_SUMMARY_TABLE_SCHEMA: &str =
    "temporal_qd_v5_native_stage_summary_table_v1";
pub const NATIVE_V5_COUNTER_SUMMARY_SCHEMA: &str = "temporal_qd_v5_native_counter_summary_v1";
pub const NATIVE_V5_PROGRESS_PREFIX: &str = "TEMPORAL_QD_V5_PROGRESS ";
pub const NATIVE_V5_PROGRESS_ENABLED_ENV: &str = "TEMPORAL_QD_V5_PROGRESS";
pub const NATIVE_V5_PROGRESS_CADENCE_ENV: &str = "TEMPORAL_QD_V5_PROGRESS_CADENCE_SECONDS";
pub const NATIVE_V5_PROGRESS_DEFAULT_CADENCE: Duration = Duration::from_secs(5);
pub const NATIVE_V5_PROGRESS_MINIMUM_CADENCE: Duration = Duration::from_millis(250);
pub const NATIVE_V5_PROGRESS_MAXIMUM_CADENCE: Duration = Duration::from_secs(300);

const UNKNOWN_TOTAL: u64 = u64::MAX;
const MAX_STAGE_SECTIONS: usize = 24;
const MAX_TABLE_BYTES: usize = 8 * 1024;

type LineSink = Arc<dyn Fn(&str) + Send + Sync + 'static>;

#[derive(Clone, Debug)]
pub struct NativeProgressSpec {
    pub family: String,
    pub generation_kind: Option<String>,
    pub generation_index: Option<u64>,
    pub phase: String,
    pub subphase: String,
    pub work_unit: String,
    pub total_work_units: Option<u64>,
    pub report_acceptance_counts: bool,
    pub thread_cap: Option<u64>,
}

impl NativeProgressSpec {
    pub fn new(family: impl Into<String>, phase: impl Into<String>) -> Self {
        Self {
            family: family.into(),
            generation_kind: None,
            generation_index: None,
            phase: phase.into(),
            subphase: "start".to_owned(),
            work_unit: "milestone".to_owned(),
            total_work_units: None,
            report_acceptance_counts: false,
            thread_cap: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct NativeProgressSection {
    pub name: String,
    pub wall: Duration,
    pub cpu: Option<Duration>,
    pub completed_work_units: Option<u64>,
    pub bytes_processed: Option<u64>,
    pub files_processed: Option<u64>,
    pub parallel_workers: Option<u64>,
}

impl NativeProgressSection {
    pub fn wall(name: impl Into<String>, wall: Duration) -> Self {
        Self {
            name: name.into(),
            wall,
            ..Self::default()
        }
    }
}

#[derive(Clone, Debug)]
struct ProgressContext {
    family: String,
    generation_kind: Option<String>,
    generation_index: Option<u64>,
    phase: String,
    subphase: String,
    work_unit: String,
    eta_unavailable_reason: Option<String>,
    report_acceptance_counts: bool,
    thread_cap: Option<u64>,
    phase_started: Instant,
    phase_started_at: OffsetDateTime,
}

struct SharedProgress {
    context: Mutex<ProgressContext>,
    started: Instant,
    started_at: OffsetDateTime,
    cadence: Duration,
    total_work_units: AtomicU64,
    completed_work_units: AtomicU64,
    constructed_count: AtomicU64,
    attempted_count: AtomicU64,
    accepted_count: AtomicU64,
    rejected_count: AtomicU64,
    bytes_processed: AtomicU64,
    files_processed: AtomicU64,
    active_workers: AtomicU64,
    max_active_workers: AtomicU64,
    stopped: AtomicBool,
    wake: Mutex<()>,
    wake_signal: Condvar,
    sections: Mutex<Vec<NativeProgressSection>>,
    sink: LineSink,
    enabled: bool,
}

#[derive(Clone)]
pub struct NativeProgressHandle {
    shared: Arc<SharedProgress>,
}

pub struct NativeProgress {
    handle: NativeProgressHandle,
    reporter: Option<JoinHandle<()>>,
    enabled: bool,
}

#[derive(Clone, Copy)]
struct Sample {
    completed: u64,
    at: Instant,
}

impl NativeProgress {
    pub fn from_environment(spec: NativeProgressSpec) -> Self {
        let enabled = !matches!(
            std::env::var(NATIVE_V5_PROGRESS_ENABLED_ENV),
            Ok(value) if matches!(value.as_str(), "0" | "false" | "off" | "disabled")
        );
        let cadence = std::env::var(NATIVE_V5_PROGRESS_CADENCE_ENV)
            .ok()
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value > 0.0)
            .map(Duration::from_secs_f64)
            .map(|value| {
                value.clamp(
                    NATIVE_V5_PROGRESS_MINIMUM_CADENCE,
                    NATIVE_V5_PROGRESS_MAXIMUM_CADENCE,
                )
            })
            .unwrap_or(NATIVE_V5_PROGRESS_DEFAULT_CADENCE);
        Self::with_sink(spec, cadence, enabled, Arc::new(stderr_sink))
    }

    #[cfg(test)]
    fn for_test(
        spec: NativeProgressSpec,
        cadence: Duration,
        enabled: bool,
        sink: LineSink,
    ) -> Self {
        Self::with_sink(spec, cadence, enabled, sink)
    }

    fn with_sink(
        spec: NativeProgressSpec,
        cadence: Duration,
        enabled: bool,
        sink: LineSink,
    ) -> Self {
        let shared = Arc::new(SharedProgress {
            context: Mutex::new(ProgressContext {
                family: spec.family,
                generation_kind: spec.generation_kind,
                generation_index: spec.generation_index,
                phase: spec.phase,
                subphase: spec.subphase,
                work_unit: spec.work_unit,
                eta_unavailable_reason: None,
                report_acceptance_counts: spec.report_acceptance_counts,
                thread_cap: spec.thread_cap,
                phase_started: Instant::now(),
                phase_started_at: OffsetDateTime::now_utc(),
            }),
            started: Instant::now(),
            started_at: OffsetDateTime::now_utc(),
            cadence,
            total_work_units: AtomicU64::new(spec.total_work_units.unwrap_or(UNKNOWN_TOTAL)),
            completed_work_units: AtomicU64::new(0),
            constructed_count: AtomicU64::new(0),
            attempted_count: AtomicU64::new(0),
            accepted_count: AtomicU64::new(0),
            rejected_count: AtomicU64::new(0),
            bytes_processed: AtomicU64::new(0),
            files_processed: AtomicU64::new(0),
            active_workers: AtomicU64::new(0),
            max_active_workers: AtomicU64::new(0),
            stopped: AtomicBool::new(false),
            wake: Mutex::new(()),
            wake_signal: Condvar::new(),
            sections: Mutex::new(Vec::new()),
            sink,
            enabled,
        });
        let handle = NativeProgressHandle {
            shared: Arc::clone(&shared),
        };
        if !enabled {
            return Self {
                handle,
                reporter: None,
                enabled: false,
            };
        }
        emit_progress(&shared, "started", None);
        let reporter_shared = Arc::clone(&shared);
        let reporter = thread::Builder::new()
            .name("temporal-qd-v5-progress".to_owned())
            .spawn(move || progress_loop(reporter_shared))
            .ok();
        Self {
            handle,
            reporter,
            enabled: true,
        }
    }

    pub fn handle(&self) -> NativeProgressHandle {
        self.handle.clone()
    }

    pub fn finish(mut self, process_cpu: Option<Duration>) {
        self.stop_reporter();
        if !self.enabled {
            return;
        }
        emit_progress(&self.handle.shared, "completed", None);
        emit_summary(&self.handle.shared, process_cpu);
        self.enabled = false;
    }

    fn stop_reporter(&mut self) {
        self.handle.shared.stopped.store(true, Ordering::Release);
        self.handle.shared.wake_signal.notify_all();
        if let Some(reporter) = self.reporter.take() {
            let _ = reporter.join();
        }
    }
}

impl Drop for NativeProgress {
    fn drop(&mut self) {
        self.stop_reporter();
    }
}

impl NativeProgressHandle {
    pub fn is_enabled(&self) -> bool {
        self.shared.enabled
    }

    pub fn set_generation(&self, generation_kind: Option<&str>, generation_index: Option<u64>) {
        if let Ok(mut context) = self.shared.context.lock() {
            context.generation_kind = generation_kind.map(str::to_owned);
            context.generation_index = generation_index;
        }
        self.shared.wake_signal.notify_one();
    }

    #[allow(clippy::too_many_arguments)]
    pub fn begin_phase(
        &self,
        phase: impl Into<String>,
        subphase: impl Into<String>,
        work_unit: impl Into<String>,
        total_work_units: Option<u64>,
        report_acceptance_counts: bool,
        thread_cap: Option<u64>,
        eta_unavailable_reason: Option<&str>,
    ) {
        if let Ok(mut context) = self.shared.context.lock() {
            context.phase = phase.into();
            context.subphase = subphase.into();
            context.work_unit = work_unit.into();
            context.eta_unavailable_reason = eta_unavailable_reason.map(str::to_owned);
            context.report_acceptance_counts = report_acceptance_counts;
            context.thread_cap = thread_cap;
            context.phase_started = Instant::now();
            context.phase_started_at = OffsetDateTime::now_utc();
        }
        self.shared
            .total_work_units
            .store(total_work_units.unwrap_or(UNKNOWN_TOTAL), Ordering::Relaxed);
        self.shared.completed_work_units.store(0, Ordering::Relaxed);
        self.shared.constructed_count.store(0, Ordering::Relaxed);
        self.shared.attempted_count.store(0, Ordering::Relaxed);
        self.shared.accepted_count.store(0, Ordering::Relaxed);
        self.shared.rejected_count.store(0, Ordering::Relaxed);
        self.shared.bytes_processed.store(0, Ordering::Relaxed);
        self.shared.files_processed.store(0, Ordering::Relaxed);
        self.shared.active_workers.store(0, Ordering::Relaxed);
        self.shared.wake_signal.notify_one();
    }

    pub fn set_subphase(&self, subphase: impl Into<String>, reason: Option<&str>) {
        if let Ok(mut context) = self.shared.context.lock() {
            context.subphase = subphase.into();
            context.eta_unavailable_reason = reason.map(str::to_owned);
        }
        self.shared.wake_signal.notify_one();
    }

    pub fn set_total_work_units(&self, total: Option<u64>) {
        self.shared
            .total_work_units
            .store(total.unwrap_or(UNKNOWN_TOTAL), Ordering::Relaxed);
        self.shared.wake_signal.notify_one();
    }

    pub fn set_completed_work_units(&self, completed: u64) {
        self.shared
            .completed_work_units
            .store(completed, Ordering::Relaxed);
    }

    pub fn advance_completed(&self, count: u64) {
        saturating_add(&self.shared.completed_work_units, count);
    }

    pub fn advance_constructed(&self, count: u64) {
        saturating_add(&self.shared.constructed_count, count);
    }

    pub fn advance_attempted(&self, count: u64) {
        saturating_add(&self.shared.attempted_count, count);
    }

    pub fn advance_accepted(&self, count: u64) {
        saturating_add(&self.shared.accepted_count, count);
    }

    pub fn advance_rejected(&self, count: u64) {
        saturating_add(&self.shared.rejected_count, count);
    }

    pub fn add_bytes(&self, bytes: u64) {
        saturating_add(&self.shared.bytes_processed, bytes);
    }

    pub fn add_files(&self, files: u64) {
        saturating_add(&self.shared.files_processed, files);
    }

    pub fn worker_started(&self) {
        let active = self
            .shared
            .active_workers
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let mut observed = self.shared.max_active_workers.load(Ordering::Relaxed);
        while active > observed {
            match self.shared.max_active_workers.compare_exchange_weak(
                observed,
                active,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => observed = current,
            }
        }
    }

    pub fn worker_finished(&self) {
        let _ = self.shared.active_workers.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |value| Some(value.saturating_sub(1)),
        );
    }

    pub fn record_section(&self, section: NativeProgressSection) {
        if !self.shared.enabled || section.name.trim().is_empty() {
            return;
        }
        if let Ok(mut sections) = self.shared.sections.lock() {
            if sections.len() < MAX_STAGE_SECTIONS {
                sections.push(section);
            }
        }
    }

    /// Emit one bounded operational counter set without mislabeling counts as
    /// byte/file units or consuming the fixed stage-section budget.
    pub fn emit_counters(&self, name: &str, counters: &BTreeMap<String, u64>) {
        if !self.shared.enabled || name.trim().is_empty() {
            return;
        }
        let counters = counters
            .iter()
            .take(32)
            .map(|(key, value)| (bounded_name(key), Value::Number((*value).into())))
            .collect::<serde_json::Map<_, _>>();
        let context = match self.shared.context.lock() {
            Ok(context) => context.clone(),
            Err(_) => return,
        };
        emit_json(
            &self.shared,
            &json!({
                "schemaVersion": NATIVE_V5_COUNTER_SUMMARY_SCHEMA,
                "event": "native_v5_counter_summary",
                "family": context.family,
                "generationKind": context.generation_kind,
                "generationIndex": context.generation_index,
                "name": bounded_name(name),
                "counters": counters,
            }),
        );
    }
}

fn saturating_add(target: &AtomicU64, value: u64) {
    let _ = target.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

fn progress_loop(shared: Arc<SharedProgress>) {
    let mut previous = Sample {
        completed: 0,
        at: shared.started,
    };
    loop {
        let guard = match shared.wake.lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        let _ = shared.wake_signal.wait_timeout(guard, shared.cadence);
        if shared.stopped.load(Ordering::Acquire) {
            return;
        }
        let now = Instant::now();
        emit_progress(&shared, "progress", Some(previous));
        previous = Sample {
            completed: shared.completed_work_units.load(Ordering::Relaxed),
            at: now,
        };
    }
}

fn emit_progress(shared: &SharedProgress, status: &str, previous: Option<Sample>) {
    if !shared.enabled {
        return;
    }
    let now = Instant::now();
    let process_elapsed = now.saturating_duration_since(shared.started);
    let completed = shared.completed_work_units.load(Ordering::Relaxed);
    let raw_total = shared.total_work_units.load(Ordering::Relaxed);
    let total = (raw_total != UNKNOWN_TOTAL).then_some(raw_total);
    let remaining = total.map(|value| value.saturating_sub(completed));
    let context = match shared.context.lock() {
        Ok(context) => context.clone(),
        Err(_) => return,
    };
    let elapsed = now.saturating_duration_since(context.phase_started);
    let cumulative_rate = rate(completed, elapsed);
    let recent_rate = previous.and_then(|sample| {
        if sample.at < context.phase_started || completed < sample.completed {
            return None;
        }
        let delta_elapsed = now.saturating_duration_since(sample.at);
        if delta_elapsed < Duration::from_millis(200) {
            None
        } else {
            rate(completed.saturating_sub(sample.completed), delta_elapsed)
        }
    });
    let (estimated_remaining, estimated_completion_at, eta_reason) = estimate(
        context.phase_started_at,
        elapsed,
        completed,
        total,
        cumulative_rate,
        context.eta_unavailable_reason.clone(),
    );
    let at = (shared.started_at + time_duration(process_elapsed))
        .format(&Rfc3339)
        .ok();
    let value = json!({
        "schemaVersion": NATIVE_V5_PROGRESS_SCHEMA,
        "event": "native_v5_progress",
        "status": status,
        "at": at,
        "family": context.family,
        "generationKind": context.generation_kind,
        "generationIndex": context.generation_index,
        "phase": context.phase,
        "subphase": context.subphase,
        "workUnit": context.work_unit,
        "completedWorkUnits": completed,
        "totalWorkUnits": total,
        "remainingWorkUnits": remaining,
        "acceptedCount": context.report_acceptance_counts.then(|| shared.accepted_count.load(Ordering::Relaxed)),
        "rejectedCount": context.report_acceptance_counts.then(|| shared.rejected_count.load(Ordering::Relaxed)),
        "attemptedCount": shared.attempted_count.load(Ordering::Relaxed),
        "constructedCount": shared.constructed_count.load(Ordering::Relaxed),
        "elapsedSeconds": seconds(elapsed),
        "processElapsedSeconds": seconds(process_elapsed),
        "cumulativeRatePerSecond": cumulative_rate,
        "recentWindowRatePerSecond": recent_rate,
        "estimatedRemainingSeconds": estimated_remaining,
        "estimatedCompletionAt": estimated_completion_at,
        "etaUnavailableReason": eta_reason,
        "bytesProcessed": shared.bytes_processed.load(Ordering::Relaxed),
        "filesProcessed": shared.files_processed.load(Ordering::Relaxed),
        "activeWorkers": shared.active_workers.load(Ordering::Relaxed),
        "maxActiveWorkers": shared.max_active_workers.load(Ordering::Relaxed),
        "threadCap": context.thread_cap,
        "cadenceSeconds": seconds(shared.cadence),
    });
    emit_json(shared, &value);
}

fn estimate(
    started_at: OffsetDateTime,
    elapsed: Duration,
    completed: u64,
    total: Option<u64>,
    cumulative_rate: Option<f64>,
    override_reason: Option<String>,
) -> (Option<f64>, Option<String>, Option<String>) {
    if let Some(reason) = override_reason {
        return (None, None, Some(reason));
    }
    let Some(total) = total else {
        return (None, None, Some("total_work_units_unknown".to_owned()));
    };
    if completed >= total {
        let completion = started_at + time_duration(elapsed);
        return (Some(0.0), completion.format(&Rfc3339).ok(), None);
    }
    if completed == 0 {
        return (None, None, Some("insufficient_completed_work".to_owned()));
    }
    let Some(rate) = cumulative_rate.filter(|value| *value > 0.0) else {
        return (None, None, Some("nonpositive_cumulative_rate".to_owned()));
    };
    let remaining = total.saturating_sub(completed) as f64;
    let estimate = remaining / rate;
    if !estimate.is_finite() || estimate < 0.0 {
        return (None, None, Some("eta_overflow".to_owned()));
    }
    let completion = started_at + time_duration(elapsed) + time_seconds(estimate);
    (
        Some(round3(estimate)),
        completion.format(&Rfc3339).ok(),
        None,
    )
}

fn emit_summary(shared: &SharedProgress, process_cpu: Option<Duration>) {
    let total_wall = shared.started.elapsed();
    let sections = shared
        .sections
        .lock()
        .map(|sections| sections.clone())
        .unwrap_or_default();
    let accounted = sections.iter().fold(Duration::ZERO, |total, section| {
        total.saturating_add(section.wall)
    });
    let residual = total_wall.saturating_sub(accounted);
    let overlap = accounted.saturating_sub(total_wall);
    let context = match shared.context.lock() {
        Ok(context) => context.clone(),
        Err(_) => return,
    };
    let rows = sections
        .iter()
        .map(|section| {
            json!({
                "name": section.name,
                "wallSeconds": seconds(section.wall),
                "cpuSeconds": section.cpu.map(seconds),
                "completedWorkUnits": section.completed_work_units,
                "bytesProcessed": section.bytes_processed,
                "filesProcessed": section.files_processed,
                "parallelWorkers": section.parallel_workers,
            })
        })
        .collect::<Vec<_>>();
    let value = json!({
        "schemaVersion": NATIVE_V5_STAGE_SUMMARY_SCHEMA,
        "event": "native_v5_stage_summary",
        "family": context.family,
        "generationKind": context.generation_kind,
        "generationIndex": context.generation_index,
        "totalWallSeconds": seconds(total_wall),
        "totalCpuSeconds": process_cpu.map(seconds),
        "accountedWallSeconds": seconds(accounted),
        "residualWallSeconds": seconds(residual),
        "overlapWallSeconds": seconds(overlap),
        "maxActiveWorkers": shared.max_active_workers.load(Ordering::Relaxed),
        "threadCap": context.thread_cap,
        "sections": rows,
    });
    emit_json(shared, &value);

    let mut table = String::from("section|wall_s|cpu_s|work|bytes|files|workers");
    for section in &sections {
        let _ = write!(
            table,
            ";{}|{:.3}|{}|{}|{}|{}|{}",
            bounded_name(&section.name),
            section.wall.as_secs_f64(),
            optional_duration(section.cpu),
            optional_u64(section.completed_work_units),
            optional_u64(section.bytes_processed),
            optional_u64(section.files_processed),
            optional_u64(section.parallel_workers),
        );
        if table.len() >= MAX_TABLE_BYTES {
            table.truncate(MAX_TABLE_BYTES);
            break;
        }
    }
    let _ = write!(
        table,
        ";residual_unattributed|{:.3}|-|-|-|-|-;overlap|{:.3}|-|-|-|-|-",
        residual.as_secs_f64(),
        overlap.as_secs_f64(),
    );
    let table_value = json!({
        "schemaVersion": NATIVE_V5_STAGE_SUMMARY_TABLE_SCHEMA,
        "event": "native_v5_stage_summary_table",
        "family": context.family,
        "generationKind": context.generation_kind,
        "generationIndex": context.generation_index,
        "table": table,
    });
    emit_json(shared, &table_value);
}

fn emit_json(shared: &SharedProgress, value: &Value) {
    if let Ok(encoded) = serde_json::to_string(value) {
        (shared.sink)(&format!("{NATIVE_V5_PROGRESS_PREFIX}{encoded}"));
    }
}

fn stderr_sink(line: &str) {
    let mut stderr = io::stderr().lock();
    let _ = writeln!(stderr, "{line}");
    let _ = stderr.flush();
}

fn rate(count: u64, elapsed: Duration) -> Option<f64> {
    let seconds = elapsed.as_secs_f64();
    (seconds > 0.0).then(|| round3(count as f64 / seconds))
}

fn seconds(duration: Duration) -> f64 {
    round3(duration.as_secs_f64())
}

fn round3(value: f64) -> f64 {
    (value * 1_000.0).round() / 1_000.0
}

fn time_duration(duration: Duration) -> time::Duration {
    let millis = i64::try_from(duration.as_millis()).unwrap_or(i64::MAX);
    time::Duration::milliseconds(millis)
}

fn time_seconds(seconds: f64) -> time::Duration {
    let millis = (seconds * 1_000.0).round();
    let millis = if millis >= i64::MAX as f64 {
        i64::MAX
    } else {
        millis as i64
    };
    time::Duration::milliseconds(millis)
}

fn bounded_name(value: &str) -> String {
    value
        .chars()
        .filter(|character| !matches!(character, ';' | '|' | '\n' | '\r'))
        .take(48)
        .collect()
}

fn optional_duration(value: Option<Duration>) -> String {
    value
        .map(|duration| format!("{:.3}", duration.as_secs_f64()))
        .unwrap_or_else(|| "-".to_owned())
}

fn optional_u64(value: Option<u64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_owned())
}

#[cfg(test)]
mod tests {
    use std::{
        hint::black_box,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::*;

    fn sink() -> (LineSink, Arc<Mutex<Vec<String>>>) {
        let lines = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&lines);
        let sink: LineSink = Arc::new(move |line| {
            captured
                .lock()
                .expect("capture progress line")
                .push(line.to_owned());
        });
        (sink, lines)
    }

    fn payloads(lines: &Arc<Mutex<Vec<String>>>) -> Vec<Value> {
        lines
            .lock()
            .expect("read progress lines")
            .iter()
            .map(|line| {
                let payload = line
                    .strip_prefix(NATIVE_V5_PROGRESS_PREFIX)
                    .expect("progress prefix");
                serde_json::from_str(payload).expect("progress JSON")
            })
            .collect()
    }

    #[test]
    fn progress_is_cadence_bounded_monotonic_and_eta_is_explicit() {
        let (sink, lines) = sink();
        let mut spec = NativeProgressSpec::new("proposal", "construction");
        spec.total_work_units = Some(10);
        spec.work_unit = "accepted_candidate".to_owned();
        let progress = NativeProgress::for_test(spec, Duration::from_millis(20), true, sink);
        let handle = progress.handle();
        std::thread::sleep(Duration::from_millis(25));
        handle.set_completed_work_units(3);
        std::thread::sleep(Duration::from_millis(45));
        handle.set_completed_work_units(7);
        std::thread::sleep(Duration::from_millis(25));
        progress.finish(None);

        let events = payloads(&lines);
        assert!(events.len() <= 10, "cadence output must stay bounded");
        let progress_events = events
            .iter()
            .filter(|value| {
                value.get("event").and_then(Value::as_str) == Some("native_v5_progress")
            })
            .collect::<Vec<_>>();
        assert!(progress_events.len() >= 3);
        let completed = progress_events
            .iter()
            .map(|value| {
                value
                    .get("completedWorkUnits")
                    .and_then(Value::as_u64)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert!(completed.windows(2).all(|pair| pair[0] <= pair[1]));
        assert_eq!(
            progress_events[0]
                .get("etaUnavailableReason")
                .and_then(Value::as_str),
            Some("insufficient_completed_work")
        );
        assert!(progress_events.iter().any(|value| {
            value
                .get("estimatedRemainingSeconds")
                .and_then(Value::as_f64)
                .is_some()
        }));
    }

    #[test]
    fn unknown_total_emits_a_bounded_heartbeat_with_null_eta() {
        let (sink, lines) = sink();
        let progress = NativeProgress::for_test(
            NativeProgressSpec::new("gateway", "maintenance_wait"),
            Duration::from_millis(20),
            true,
            sink,
        );
        std::thread::sleep(Duration::from_millis(45));
        progress.finish(None);
        for event in payloads(&lines).into_iter().filter(|value| {
            value.get("event").and_then(Value::as_str) == Some("native_v5_progress")
        }) {
            assert!(event.get("totalWorkUnits").is_some_and(Value::is_null));
            assert!(
                event
                    .get("estimatedRemainingSeconds")
                    .is_some_and(Value::is_null)
            );
            assert_eq!(
                event.get("etaUnavailableReason").and_then(Value::as_str),
                Some("total_work_units_unknown")
            );
        }
    }

    #[test]
    fn summary_reports_residual_and_has_one_bounded_human_table() {
        let (sink, lines) = sink();
        let progress = NativeProgress::for_test(
            NativeProgressSpec::new("campaign_output", "execute"),
            Duration::from_secs(60),
            true,
            sink,
        );
        let handle = progress.handle();
        std::thread::sleep(Duration::from_millis(10));
        handle.record_section(NativeProgressSection {
            name: "authentication".to_owned(),
            wall: Duration::from_millis(3),
            completed_work_units: Some(2),
            bytes_processed: Some(4096),
            files_processed: Some(2),
            parallel_workers: Some(1),
            ..NativeProgressSection::default()
        });
        handle.emit_counters(
            "evolved_admission",
            &BTreeMap::from([
                ("changedSideProbes".to_owned(), 128),
                ("fallbackSweeps".to_owned(), 0),
            ]),
        );
        progress.finish(Some(Duration::from_millis(2)));
        let events = payloads(&lines);
        let counters = events
            .iter()
            .find(|value| {
                value.get("event").and_then(Value::as_str) == Some("native_v5_counter_summary")
            })
            .expect("counter summary");
        assert_eq!(
            counters
                .get("counters")
                .and_then(|value| value.get("changedSideProbes"))
                .and_then(Value::as_u64),
            Some(128),
        );
        let summary = events
            .iter()
            .find(|value| {
                value.get("event").and_then(Value::as_str) == Some("native_v5_stage_summary")
            })
            .expect("stage summary");
        assert!(
            summary
                .get("residualWallSeconds")
                .and_then(Value::as_f64)
                .is_some_and(|value| value >= 0.0)
        );
        let tables = events
            .iter()
            .filter(|value| {
                value.get("event").and_then(Value::as_str) == Some("native_v5_stage_summary_table")
            })
            .collect::<Vec<_>>();
        assert_eq!(tables.len(), 1);
        assert!(
            tables[0]
                .get("table")
                .and_then(Value::as_str)
                .is_some_and(|value| value.len() < MAX_TABLE_BYTES)
        );
    }

    #[test]
    fn four_thousand_atomic_updates_are_bounded_and_negligible() {
        let (sink, lines) = sink();
        let mut spec = NativeProgressSpec::new("proposal", "construction");
        spec.total_work_units = Some(4_000);
        let progress = NativeProgress::for_test(spec, Duration::from_secs(3_600), true, sink);
        let handle = progress.handle();
        let started = Instant::now();
        for _ in 0..4_000 {
            handle.advance_constructed(black_box(1));
            handle.advance_attempted(black_box(1));
            handle.advance_accepted(black_box(1));
            handle.advance_completed(black_box(1));
        }
        let elapsed = started.elapsed();
        progress.finish(None);
        let output_bytes = lines
            .lock()
            .expect("read progress output")
            .iter()
            .map(String::len)
            .sum::<usize>();
        assert!(elapsed < Duration::from_millis(100));
        assert!(output_bytes < 32 * 1024);
        assert!(lines.lock().expect("read progress output").len() <= 4);
        eprintln!(
            "v5 progress 4k witness: updates=16000 elapsedMicros={} outputBytes={output_bytes}",
            elapsed.as_micros()
        );
    }

    #[test]
    fn disabled_progress_emits_nothing() {
        let (sink, lines) = sink();
        let progress = NativeProgress::for_test(
            NativeProgressSpec::new("proposal", "construction"),
            Duration::from_millis(20),
            false,
            sink,
        );
        let handle = progress.handle();
        assert!(!handle.is_enabled());
        handle.advance_completed(1);
        handle.record_section(NativeProgressSection::wall(
            "disabled_section",
            Duration::from_secs(1),
        ));
        handle.emit_counters(
            "disabled_counters",
            &BTreeMap::from([("changedSideProbes".to_owned(), 4_000)]),
        );
        assert!(
            handle
                .shared
                .sections
                .lock()
                .expect("read disabled sections")
                .is_empty()
        );
        progress.finish(None);
        assert!(lines.lock().expect("read disabled output").is_empty());
    }
}
