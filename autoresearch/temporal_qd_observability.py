"""Out-of-band performance and resource observability for temporal QD.

These artifacts are deliberately excluded from candidate, proposal, journal,
population, and run-config identities.  They measure an execution of a frozen
semantic authority; they never define that authority.
"""

from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import threading
import time
from typing import Any, Iterator, Mapping
import uuid

import psutil


PERFORMANCE_POLICY_SCHEMA = "temporal_qd_performance_observability_policy_v2"
PERFORMANCE_EVENT_SCHEMA = "temporal_qd_performance_span_v2"
RESOURCE_SAMPLE_SCHEMA = "temporal_qd_process_tree_resource_sample_v2"
PERFORMANCE_SUMMARY_SCHEMA = "temporal_qd_performance_session_summary_v2"

# Detailed samples include recursive process inspection, CPU/IO collection,
# active-span attribution, JSON serialization, and a durable JSONL append.
# The inexpensive watchdog below retains the 500 ms RAM/headroom response.
DEFAULT_RESOURCE_SAMPLE_INTERVAL_SECONDS = 2.0
DEFAULT_RESOURCE_WATCHDOG_INTERVAL_SECONDS = 0.5
DEFAULT_MAX_TREE_RSS_BYTES = 8 * 1024**3
DEFAULT_MINIMUM_HOST_AVAILABLE_BYTES = 12 * 1024**3


class PerformanceResourcePressureError(RuntimeError):
    """The observational run crossed a frozen workstation safety boundary."""


def performance_observability_policy() -> dict[str, Any]:
    return {
        "schemaVersion": PERFORMANCE_POLICY_SCHEMA,
        "sampleIntervalSeconds": DEFAULT_RESOURCE_SAMPLE_INTERVAL_SECONDS,
        "watchdogIntervalSeconds": DEFAULT_RESOURCE_WATCHDOG_INTERVAL_SECONDS,
        "spanClock": "monotonic_perf_counter_ns",
        "spanCpuClock": "coordinator_main_thread_time_ns",
        "samplerCpuClock": "sampler_thread_time_ns",
        "samplingDesign": {
            "detailedSamples": "periodic_recursive_process_tree_jsonl",
            "watchdog": "frequent_rss_and_host_headroom_guard_without_jsonl",
            "proposalBoundaryCheck": "fresh_watchdog_when_cadence_is_stale",
        },
        "resourceScope": "coordinator_and_recursive_children",
        "resourceGuard": {
            "maximumTreeRssBytes": DEFAULT_MAX_TREE_RSS_BYTES,
            "minimumHostAvailableBytes": DEFAULT_MINIMUM_HOST_AVAILABLE_BYTES,
            "action": "stop_after_current_proposal_and_preserve_evidence",
        },
        "resourceMetrics": [
            "rssBytes",
            "privateBytes",
            "virtualBytes",
            "pagefileBytes",
            "cpuSeconds",
            "ioReadBytes",
            "ioWriteBytes",
            "hostAvailableBytes",
            "hostMemoryPercent",
            "hostSwapUsedBytes",
        ],
        "childClassification": "executable_and_script_basename_only_no_command_lines",
        "semanticIdentityParticipation": "excluded_observational_artifacts",
    }


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _sha256(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _thread_cpu_time_ns() -> int:
    """Return CPU consumed by the current thread, never sibling sampler work."""

    clock = getattr(time, "thread_time_ns", None)
    return int(clock()) if callable(clock) else time.process_time_ns()


def _percentile(values: list[int], fraction: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return int(round(ordered[lower] * (1.0 - weight) + ordered[upper] * weight))


def _safe_json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Mapping):
        return {str(key): _safe_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_safe_json_value(item) for item in value]
    return str(value)


class _SpanAnnotation:
    def __init__(self, attributes: dict[str, Any]) -> None:
        self._attributes = attributes

    def annotate(self, **attributes: Any) -> None:
        self._attributes.update(
            {str(key): _safe_json_value(value) for key, value in attributes.items()}
        )


class _NullSpan:
    def annotate(self, **attributes: Any) -> None:
        del attributes


_ACTIVE_TRACE: ContextVar[PerformanceTrace | None] = ContextVar(
    "temporal_qd_active_performance_trace", default=None
)


def current_performance_trace() -> PerformanceTrace | None:
    return _ACTIVE_TRACE.get()


@contextmanager
def activate_performance_trace(trace: PerformanceTrace) -> Iterator[None]:
    token = _ACTIVE_TRACE.set(trace)
    try:
        yield
    finally:
        _ACTIVE_TRACE.reset(token)


@contextmanager
def timing_scope(**attributes: Any) -> Iterator[None]:
    trace = current_performance_trace()
    if trace is None:
        yield
        return
    with trace.scope(**attributes):
        yield


@contextmanager
def timed_span(name: str, **attributes: Any) -> Iterator[_SpanAnnotation | _NullSpan]:
    trace = current_performance_trace()
    if trace is None:
        yield _NullSpan()
        return
    with trace.span(name, **attributes) as annotation:
        yield annotation


def flush_performance_events() -> None:
    trace = current_performance_trace()
    if trace is not None:
        trace.flush_events()


def assert_performance_resource_guard() -> None:
    trace = current_performance_trace()
    if trace is not None:
        trace.assert_resource_guard()


def start_performance_interval() -> tuple[int, int, str]:
    return time.perf_counter_ns(), _thread_cpu_time_ns(), _utc_now()


def record_performance_interval(
    name: str,
    started: tuple[int, int, str],
    **attributes: Any,
) -> None:
    trace = current_performance_trace()
    if trace is not None:
        trace.record_interval(name, started=started, **attributes)


class PerformanceTrace:
    """One execution session with nested spans and process-tree sampling."""

    def __init__(
        self,
        *,
        output_root: Path | str,
        generation_index: int,
        sample_interval_seconds: float = DEFAULT_RESOURCE_SAMPLE_INTERVAL_SECONDS,
        watchdog_interval_seconds: float = DEFAULT_RESOURCE_WATCHDOG_INTERVAL_SECONDS,
        maximum_tree_rss_bytes: int = DEFAULT_MAX_TREE_RSS_BYTES,
        minimum_host_available_bytes: int = DEFAULT_MINIMUM_HOST_AVAILABLE_BYTES,
    ) -> None:
        interval = float(sample_interval_seconds)
        if not 0.1 <= interval <= 60.0:
            raise ValueError("resource sample interval must be within 0.1..60 seconds")
        watchdog_interval = float(watchdog_interval_seconds)
        if not 0.05 <= watchdog_interval <= 60.0:
            raise ValueError(
                "resource watchdog interval must be within 0.05..60 seconds"
            )
        self.output_root = Path(output_root)
        self.performance_root = self.output_root / "performance"
        self.sessions_root = self.performance_root / "sessions"
        self.performance_root.mkdir(parents=True, exist_ok=True)
        self.sessions_root.mkdir(parents=True, exist_ok=True)
        self.generation_index = int(generation_index)
        self.sample_interval_seconds = interval
        self.watchdog_interval_seconds = watchdog_interval
        self.maximum_tree_rss_bytes = int(maximum_tree_rss_bytes)
        self.minimum_host_available_bytes = int(minimum_host_available_bytes)
        if self.maximum_tree_rss_bytes < 1 or self.minimum_host_available_bytes < 1:
            raise ValueError("resource guard byte thresholds must be positive")
        self.session_id = (
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
            + f"-pid{os.getpid()}-{uuid.uuid4().hex[:12]}"
        )
        self.events_path = self.performance_root / "performance-events.jsonl"
        self.resources_path = self.performance_root / "resource-samples.jsonl"
        self.summary_path = self.sessions_root / f"{self.session_id}.json"
        self.latest_summary_path = self.performance_root / "latest-summary.json"
        self.policy_path = self.performance_root / "observability-policy.json"
        self._write_policy()

        self._lock = threading.RLock()
        # A foreground proposal-boundary check and the background sampler may
        # coincide.  Serialize collection so their peak/guard accounting stays
        # coherent and a boundary check never races a detailed process walk.
        self._resource_sampling_lock = threading.Lock()
        self._events_file = self.events_path.open("a", encoding="utf-8", newline="\n")
        self._resources_file = self.resources_path.open(
            "a", encoding="utf-8", newline="\n"
        )
        self._pending_event_lines: list[str] = []
        self._span_stack: ContextVar[tuple[dict[str, Any], ...]] = ContextVar(
            f"temporal_qd_span_stack_{self.session_id}", default=()
        )
        self._scope_stack: ContextVar[tuple[dict[str, Any], ...]] = ContextVar(
            f"temporal_qd_scope_stack_{self.session_id}", default=()
        )
        self._next_span_id = 0
        self._active_main_spans: list[dict[str, Any]] = []
        self._span_stats: dict[str, dict[str, list[int]]] = defaultdict(
            lambda: {
                "inclusive": [],
                "exclusive": [],
                "cpuInclusive": [],
                "cpuExclusive": [],
            }
        )
        self._overlapping_total_phases: set[str] = set()
        self._started_at_utc = _utc_now()
        self._started_perf_ns = time.perf_counter_ns()
        self._started_cpu_ns = _thread_cpu_time_ns()
        self._closed = False
        self._result: dict[str, Any] = {}
        self._outcome = "unknown"
        self._error_type: str | None = None

        self._event_write_count = 0
        self._event_write_bytes = 0
        self._event_write_wall_ns = 0
        self._resource_write_count = 0
        self._resource_write_bytes = 0
        self._resource_write_wall_ns = 0
        self._resource_sample_collection_wall_ns = 0
        self._resource_sample_thread_cpu_ns = 0
        self._resource_watchdog_count = 0
        self._resource_watchdog_error_count = 0
        self._resource_watchdog_wall_ns = 0
        self._resource_watchdog_thread_cpu_ns = 0
        self._last_watchdog_perf_ns: int | None = None

        self._resource_sample_count = 0
        self._resource_error_count = 0
        self._resource_guard_breach: dict[str, Any] | None = None
        self._tree_rss: list[int] = []
        self._tree_private: list[int] = []
        self._tree_virtual: list[int] = []
        self._tree_pagefile: list[int] = []
        self._coordinator_rss: list[int] = []
        self._child_rss: list[int] = []
        self._host_available: list[int] = []
        self._host_memory_percent: list[float] = []
        self._host_swap_used: list[int] = []
        self._tree_cpu_cores: list[float] = []
        self._tree_cpu_normalized_percent: list[float] = []
        self._process_counts: list[int] = []
        self._peak_tree_rss_sample: dict[str, Any] | None = None
        self._peak_tree_private_sample: dict[str, Any] | None = None
        self._minimum_host_available_sample: dict[str, Any] | None = None
        self._peak_host_swap_sample: dict[str, Any] | None = None
        self._peak_tree_cpu_sample: dict[str, Any] | None = None
        self._first_io_read_bytes: int | None = None
        self._first_io_write_bytes: int | None = None
        self._last_io_read_bytes: int | None = None
        self._last_io_write_bytes: int | None = None
        self._last_sample_perf_ns: int | None = None
        self._last_cpu_by_pid: dict[int, float] = {}
        self._stop = threading.Event()
        psutil.cpu_percent(interval=None)
        self._sample_resources(origin="startup")
        self._sampler = threading.Thread(
            target=self._resource_loop,
            name=f"temporal-qd-resource-sampler-{self.session_id}",
            daemon=True,
        )
        self._sampler.start()

    def _write_policy(self) -> None:
        policy = performance_observability_policy()
        policy["sampleIntervalSeconds"] = self.sample_interval_seconds
        policy["watchdogIntervalSeconds"] = self.watchdog_interval_seconds
        policy["resourceGuard"] = {
            "maximumTreeRssBytes": self.maximum_tree_rss_bytes,
            "minimumHostAvailableBytes": self.minimum_host_available_bytes,
            "action": "stop_after_current_proposal_and_preserve_evidence",
        }
        policy["policySha256"] = _sha256(policy)
        encoded = json.dumps(policy, indent=2, sort_keys=True) + "\n"
        if self.policy_path.exists():
            if self.policy_path.read_text(encoding="utf-8") != encoded:
                # Performance policy is deliberately non-semantic.  Do not
                # prevent a restart of an otherwise valid historical
                # generation merely because its observation schema evolved.
                # Preserve the old immutable policy and bind this session to a
                # versioned sibling instead.
                versioned = self.performance_root / "observability-policy-v2.json"
                if versioned.exists():
                    if versioned.read_text(encoding="utf-8") != encoded:
                        raise ValueError(
                            "temporal QD observability policy changed within one output root"
                        )
                else:
                    versioned.write_text(encoded, encoding="utf-8", newline="\n")
                self.policy_path = versioned
            return
        self.policy_path.write_text(encoded, encoding="utf-8", newline="\n")

    @contextmanager
    def scope(self, **attributes: Any) -> Iterator[None]:
        current = self._scope_stack.get()
        normalized = {
            str(key): _safe_json_value(value) for key, value in attributes.items()
        }
        token = self._scope_stack.set((*current, normalized))
        try:
            yield
        finally:
            self._scope_stack.reset(token)

    def _scope_attributes(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for scope in self._scope_stack.get():
            result.update(scope)
        return result

    @contextmanager
    def span(self, name: str, **attributes: Any) -> Iterator[_SpanAnnotation]:
        phase = str(name).strip()
        if not phase:
            raise ValueError("performance span name cannot be empty")
        stack = self._span_stack.get()
        self._next_span_id += 1
        frame = {
            "spanId": self._next_span_id,
            "parentSpanId": stack[-1]["spanId"] if stack else None,
            "phase": phase,
            "startedPerfNs": time.perf_counter_ns(),
            "startedCpuNs": _thread_cpu_time_ns(),
            "startedAtUtc": _utc_now(),
            "childWallNs": 0,
            "childCpuNs": 0,
            "attributes": {
                **self._scope_attributes(),
                **{
                    str(key): _safe_json_value(value)
                    for key, value in attributes.items()
                },
            },
        }
        token = self._span_stack.set((*stack, frame))
        with self._lock:
            self._active_main_spans.append(frame)
        status = "ok"
        error_type = None
        try:
            yield _SpanAnnotation(frame["attributes"])
        except BaseException as exc:
            status = "error"
            error_type = type(exc).__name__
            raise
        finally:
            ended_perf_ns = time.perf_counter_ns()
            ended_cpu_ns = _thread_cpu_time_ns()
            self._span_stack.reset(token)
            with self._lock:
                if (
                    not self._active_main_spans
                    or self._active_main_spans[-1]["spanId"] != frame["spanId"]
                ):
                    raise RuntimeError("performance span stack became unbalanced")
                self._active_main_spans.pop()
            inclusive_ns = max(0, ended_perf_ns - int(frame["startedPerfNs"]))
            exclusive_ns = max(0, inclusive_ns - int(frame["childWallNs"]))
            cpu_inclusive_ns = max(0, ended_cpu_ns - int(frame["startedCpuNs"]))
            cpu_exclusive_ns = max(
                0,
                cpu_inclusive_ns - int(frame["childCpuNs"]),
            )
            if stack:
                stack[-1]["childWallNs"] += inclusive_ns
                stack[-1]["childCpuNs"] += cpu_inclusive_ns
            event = {
                "schemaVersion": PERFORMANCE_EVENT_SCHEMA,
                "sessionId": self.session_id,
                "generationIndex": self.generation_index,
                "spanId": frame["spanId"],
                "parentSpanId": frame["parentSpanId"],
                "phase": phase,
                "startedAtUtc": frame["startedAtUtc"],
                "startedOffsetNs": int(frame["startedPerfNs"])
                - self._started_perf_ns,
                "wallInclusiveNs": inclusive_ns,
                "wallExclusiveNs": exclusive_ns,
                "coordinatorCpuInclusiveNs": cpu_inclusive_ns,
                "coordinatorCpuExclusiveNs": cpu_exclusive_ns,
                "status": status,
                "attributes": frame["attributes"],
                **({"errorType": error_type} if error_type is not None else {}),
            }
            self._record_span(event)

    def _record_span(self, event: Mapping[str, Any]) -> None:
        encoded = _canonical_json(event) + "\n"
        phase = str(event["phase"])
        with self._lock:
            self._pending_event_lines.append(encoded)
            stats = self._span_stats[phase]
            stats["inclusive"].append(int(event["wallInclusiveNs"]))
            stats["exclusive"].append(int(event["wallExclusiveNs"]))
            stats["cpuInclusive"].append(
                int(event["coordinatorCpuInclusiveNs"])
            )
            stats["cpuExclusive"].append(
                int(event["coordinatorCpuExclusiveNs"])
            )
            attributes = event.get("attributes")
            if (
                isinstance(attributes, Mapping)
                and attributes.get("aggregationRole") == "overlapping_total"
            ):
                self._overlapping_total_phases.add(phase)

    def record_interval(
        self,
        name: str,
        *,
        started: tuple[int, int, str],
        **attributes: Any,
    ) -> None:
        """Record an overlapping total without changing nested-span accounting."""

        started_perf_ns, started_cpu_ns, started_at_utc = started
        ended_perf_ns = time.perf_counter_ns()
        ended_cpu_ns = _thread_cpu_time_ns()
        stack = self._span_stack.get()
        self._next_span_id += 1
        merged_attributes = {
            **self._scope_attributes(),
            **{
                str(key): _safe_json_value(value)
                for key, value in attributes.items()
            },
            "aggregationRole": "overlapping_total",
        }
        inclusive_ns = max(0, ended_perf_ns - int(started_perf_ns))
        cpu_ns = max(0, ended_cpu_ns - int(started_cpu_ns))
        event = {
            "schemaVersion": PERFORMANCE_EVENT_SCHEMA,
            "sessionId": self.session_id,
            "generationIndex": self.generation_index,
            "spanId": self._next_span_id,
            "parentSpanId": stack[-1]["spanId"] if stack else None,
            "phase": str(name),
            "startedAtUtc": started_at_utc,
            "startedOffsetNs": int(started_perf_ns) - self._started_perf_ns,
            "wallInclusiveNs": inclusive_ns,
            "wallExclusiveNs": inclusive_ns,
            "coordinatorCpuInclusiveNs": cpu_ns,
            "coordinatorCpuExclusiveNs": cpu_ns,
            "status": "ok",
            "attributes": merged_attributes,
        }
        self._record_span(event)

    def flush_events(self) -> None:
        started = time.perf_counter_ns()
        with self._lock:
            if not self._pending_event_lines:
                return
            encoded = "".join(self._pending_event_lines)
            self._pending_event_lines.clear()
            self._events_file.write(encoded)
            self._events_file.flush()
            self._event_write_count += encoded.count("\n")
            self._event_write_bytes += len(encoded.encode("utf-8"))
        self._event_write_wall_ns += time.perf_counter_ns() - started

    @staticmethod
    def _process_role(process: psutil.Process, *, root_pid: int) -> str:
        if process.pid == root_pid:
            return "coordinator"
        try:
            arguments = process.cmdline()
        except (psutil.Error, OSError):
            arguments = []
        if any(Path(item).name == "temporal_search_validate_candidate.py" for item in arguments):
            return "native_validator"
        return "child"

    def _active_resource_context(self) -> dict[str, Any]:
        """Return the narrow attribution surface safe for resource artifacts."""

        with self._lock:
            active_spans = list(self._active_main_spans)
        allowed_context = {
            "workClass",
            "proposalOrdinal",
            "scheduledOrigin",
            "originKind",
            "side",
            "grammarStep",
            "indicatorStep",
            "operationFamily",
            "operatorId",
            "artifactKind",
        }
        active_context: dict[str, Any] = {}
        for active_span in active_spans:
            for key, value in active_span["attributes"].items():
                if key in allowed_context:
                    active_context[key] = value
        return {
            "phase": active_spans[-1]["phase"] if active_spans else None,
            "phasePath": [item["phase"] for item in active_spans],
            "spanId": active_spans[-1]["spanId"] if active_spans else None,
            **active_context,
        }

    def _record_resource_guard_breach(
        self,
        *,
        tree_rss_bytes: int | None,
        host_available_bytes: int | None,
        sample_context: Mapping[str, Any],
        source: str,
        failure_reason: str | None = None,
    ) -> None:
        """Seal the first safety breach with its narrowest useful evidence."""

        reasons: list[str] = []
        if failure_reason is not None:
            reasons.append(str(failure_reason))
        else:
            if (
                tree_rss_bytes is not None
                and tree_rss_bytes > self.maximum_tree_rss_bytes
            ):
                reasons.append("maximum_tree_rss_exceeded")
            if (
                host_available_bytes is not None
                and host_available_bytes < self.minimum_host_available_bytes
            ):
                reasons.append("minimum_host_available_breached")
        if not reasons:
            return
        with self._lock:
            if self._resource_guard_breach is not None:
                return
            self._resource_guard_breach = {
                **_safe_json_value(sample_context),
                "sampleSource": source,
                "reasonCodes": reasons,
                "treeRssBytes": tree_rss_bytes,
                "hostAvailableBytes": host_available_bytes,
                "maximumTreeRssBytes": self.maximum_tree_rss_bytes,
                "minimumHostAvailableBytes": self.minimum_host_available_bytes,
            }

    def _sample_resources(self, *, origin: str) -> None:
        """Capture one detailed JSONL sample and account for telemetry cost."""

        sample_started = time.perf_counter_ns()
        sample_started_cpu_ns = _thread_cpu_time_ns()
        try:
            with self._resource_sampling_lock:
                self._sample_resources_locked(
                    origin=origin,
                    sample_started=sample_started,
                )
        finally:
            collection_wall_ns = max(0, time.perf_counter_ns() - sample_started)
            collection_cpu_ns = max(0, _thread_cpu_time_ns() - sample_started_cpu_ns)
            with self._lock:
                self._resource_sample_collection_wall_ns += collection_wall_ns
                self._resource_sample_thread_cpu_ns += collection_cpu_ns

    def _sample_resources_locked(self, *, origin: str, sample_started: int) -> None:
        try:
            # Capture phase attribution before the more expensive process walk;
            # the work being measured can finish while that walk is in flight.
            active_context = self._active_resource_context()
            root = psutil.Process(os.getpid())
            processes = [root, *root.children(recursive=True)]
            unique = {process.pid: process for process in processes}
            rows: list[dict[str, Any]] = []
            current_cpu_by_pid: dict[int, float] = {}
            tree_rss = 0
            tree_private = 0
            tree_virtual = 0
            tree_pagefile = 0
            coordinator_rss = 0
            child_rss = 0
            io_read = 0
            io_write = 0
            for pid in sorted(unique):
                process = unique[pid]
                try:
                    memory = process.memory_info()
                    cpu = process.cpu_times()
                    io = process.io_counters()
                    parent_pid = process.ppid()
                    executable = Path(process.exe()).name
                except (psutil.Error, OSError):
                    continue
                role = self._process_role(process, root_pid=root.pid)
                rss = int(getattr(memory, "rss", 0))
                private = int(getattr(memory, "private", getattr(memory, "uss", 0)))
                virtual = int(getattr(memory, "vms", 0))
                pagefile = int(getattr(memory, "pagefile", private))
                cpu_seconds = float(cpu.user + cpu.system)
                read_bytes = int(getattr(io, "read_bytes", 0))
                write_bytes = int(getattr(io, "write_bytes", 0))
                current_cpu_by_pid[pid] = cpu_seconds
                tree_rss += rss
                tree_private += private
                tree_virtual += virtual
                tree_pagefile += pagefile
                io_read += read_bytes
                io_write += write_bytes
                if role == "coordinator":
                    coordinator_rss += rss
                else:
                    child_rss += rss
                rows.append(
                    {
                        "pid": pid,
                        "parentPid": parent_pid,
                        "role": role,
                        "executable": executable,
                        "rssBytes": rss,
                        "privateBytes": private,
                        "virtualBytes": virtual,
                        "pagefileBytes": pagefile,
                        "cpuSeconds": cpu_seconds,
                        "ioReadBytes": read_bytes,
                        "ioWriteBytes": write_bytes,
                    }
                )

            now_perf_ns = time.perf_counter_ns()
            cpu_core_equivalent = None
            cpu_normalized_percent = None
            if self._last_sample_perf_ns is not None:
                elapsed_seconds = max(
                    1e-9, (now_perf_ns - self._last_sample_perf_ns) / 1_000_000_000
                )
                cpu_delta = sum(
                    max(0.0, value - self._last_cpu_by_pid.get(pid, 0.0))
                    for pid, value in current_cpu_by_pid.items()
                )
                cpu_core_equivalent = cpu_delta / elapsed_seconds
                cpu_normalized_percent = (
                    100.0
                    * cpu_core_equivalent
                    / max(1, int(psutil.cpu_count(logical=True) or 1))
                )
            self._last_sample_perf_ns = now_perf_ns
            self._last_cpu_by_pid = current_cpu_by_pid
            host = psutil.virtual_memory()
            swap = psutil.swap_memory()
            with self._lock:
                # A successful detailed sample is also a successful guard
                # observation, so proposal-boundary checks need not repeat it.
                self._last_watchdog_perf_ns = now_perf_ns
            sample = {
                "schemaVersion": RESOURCE_SAMPLE_SCHEMA,
                "sessionId": self.session_id,
                "generationIndex": self.generation_index,
                "sampleOrdinal": self._resource_sample_count,
                "sampledAtUtc": _utc_now(),
                "sampleOffsetNs": now_perf_ns - self._started_perf_ns,
                "sampleCollectionWallNs": now_perf_ns - sample_started,
                "sampleSource": "detailed_sample",
                "sampleOrigin": origin,
                "activeContext": active_context,
                "processCount": len(rows),
                "tree": {
                    "rssBytes": tree_rss,
                    "privateBytes": tree_private,
                    "virtualBytes": tree_virtual,
                    "pagefileBytes": tree_pagefile,
                    "ioReadBytes": io_read,
                    "ioWriteBytes": io_write,
                    "cpuCoreEquivalent": cpu_core_equivalent,
                    "cpuNormalizedPercent": cpu_normalized_percent,
                },
                "host": {
                    "logicalCpuCount": int(psutil.cpu_count(logical=True) or 1),
                    "cpuPercent": float(psutil.cpu_percent(interval=None)),
                    "memoryTotalBytes": int(host.total),
                    "memoryAvailableBytes": int(host.available),
                    "memoryUsedBytes": int(host.used),
                    "memoryPercent": float(host.percent),
                    "swapTotalBytes": int(swap.total),
                    "swapUsedBytes": int(swap.used),
                    "swapPercent": float(swap.percent),
                },
                "processes": rows,
            }
            self._record_resource_sample(sample, coordinator_rss, child_rss)
        except BaseException as exc:
            self._resource_error_count += 1
            error = {
                "schemaVersion": RESOURCE_SAMPLE_SCHEMA,
                "sessionId": self.session_id,
                "generationIndex": self.generation_index,
                "sampleOrdinal": self._resource_sample_count,
                "sampledAtUtc": _utc_now(),
                "sampleOffsetNs": time.perf_counter_ns() - self._started_perf_ns,
                "sampleSource": "detailed_sample",
                "sampleOrigin": origin,
                "status": "error",
                "errorType": type(exc).__name__,
            }
            self._write_resource_line(error)

    def _record_resource_sample(
        self,
        sample: Mapping[str, Any],
        coordinator_rss: int,
        child_rss: int,
    ) -> None:
        tree = sample["tree"]
        host = sample["host"]
        self._resource_sample_count += 1
        self._tree_rss.append(int(tree["rssBytes"]))
        self._tree_private.append(int(tree["privateBytes"]))
        self._tree_virtual.append(int(tree["virtualBytes"]))
        self._tree_pagefile.append(int(tree["pagefileBytes"]))
        self._coordinator_rss.append(int(coordinator_rss))
        self._child_rss.append(int(child_rss))
        self._host_available.append(int(host["memoryAvailableBytes"]))
        self._host_memory_percent.append(float(host["memoryPercent"]))
        self._host_swap_used.append(int(host["swapUsedBytes"]))
        if tree["cpuCoreEquivalent"] is not None:
            self._tree_cpu_cores.append(float(tree["cpuCoreEquivalent"]))
        if tree["cpuNormalizedPercent"] is not None:
            self._tree_cpu_normalized_percent.append(
                float(tree["cpuNormalizedPercent"])
            )
        self._process_counts.append(int(sample["processCount"]))
        sample_context = {
            "sampleOrdinal": int(sample["sampleOrdinal"]),
            "sampledAtUtc": sample["sampledAtUtc"],
            "sampleOffsetNs": int(sample["sampleOffsetNs"]),
            "activeContext": sample["activeContext"],
        }
        if (
            self._peak_tree_rss_sample is None
            or int(tree["rssBytes"]) > self._peak_tree_rss_sample["valueBytes"]
        ):
            self._peak_tree_rss_sample = {
                **sample_context,
                "valueBytes": int(tree["rssBytes"]),
            }
        if (
            self._peak_tree_private_sample is None
            or int(tree["privateBytes"])
            > self._peak_tree_private_sample["valueBytes"]
        ):
            self._peak_tree_private_sample = {
                **sample_context,
                "valueBytes": int(tree["privateBytes"]),
            }
        if (
            self._minimum_host_available_sample is None
            or int(host["memoryAvailableBytes"])
            < self._minimum_host_available_sample["valueBytes"]
        ):
            self._minimum_host_available_sample = {
                **sample_context,
                "valueBytes": int(host["memoryAvailableBytes"]),
            }
        if (
            self._peak_host_swap_sample is None
            or int(host["swapUsedBytes"])
            > self._peak_host_swap_sample["valueBytes"]
        ):
            self._peak_host_swap_sample = {
                **sample_context,
                "valueBytes": int(host["swapUsedBytes"]),
            }
        if tree["cpuCoreEquivalent"] is not None and (
            self._peak_tree_cpu_sample is None
            or float(tree["cpuCoreEquivalent"])
            > self._peak_tree_cpu_sample["valueCoreEquivalent"]
        ):
            self._peak_tree_cpu_sample = {
                **sample_context,
                "valueCoreEquivalent": float(tree["cpuCoreEquivalent"]),
                "normalizedPercent": float(tree["cpuNormalizedPercent"]),
            }
        self._record_resource_guard_breach(
            tree_rss_bytes=int(tree["rssBytes"]),
            host_available_bytes=int(host["memoryAvailableBytes"]),
            sample_context=sample_context,
            source=str(sample.get("sampleSource") or "detailed_sample"),
        )
        io_read = int(tree["ioReadBytes"])
        io_write = int(tree["ioWriteBytes"])
        if self._first_io_read_bytes is None:
            self._first_io_read_bytes = io_read
            self._first_io_write_bytes = io_write
        self._last_io_read_bytes = io_read
        self._last_io_write_bytes = io_write
        self._write_resource_line(sample)

    def _write_resource_line(self, value: Mapping[str, Any]) -> None:
        encoded = _canonical_json(value) + "\n"
        started = time.perf_counter_ns()
        with self._lock:
            self._resources_file.write(encoded)
            self._resources_file.flush()
            self._resource_write_count += 1
            self._resource_write_bytes += len(encoded.encode("utf-8"))
        self._resource_write_wall_ns += time.perf_counter_ns() - started

    def _watchdog_resources(self, *, origin: str) -> None:
        """Perform a cheap, no-artifact RSS/headroom safety check.

        This intentionally avoids command-line, CPU, IO, swap, and JSONL work.
        Its sole purpose is to keep the memory guard responsive between detailed
        samples and when a proposal reaches its cooperative boundary.
        """

        started_perf_ns = time.perf_counter_ns()
        started_cpu_ns = _thread_cpu_time_ns()
        try:
            with self._resource_sampling_lock:
                root = psutil.Process(os.getpid())
                processes = [root, *root.children(recursive=True)]
                unique = {process.pid: process for process in processes}
                tree_rss = 0
                for process in unique.values():
                    try:
                        tree_rss += int(process.memory_info().rss)
                    except (psutil.Error, OSError):
                        # A transient child exit is not a telemetry failure.
                        continue
                host_available = int(psutil.virtual_memory().available)
                now_perf_ns = time.perf_counter_ns()
                if (
                    tree_rss > self.maximum_tree_rss_bytes
                    or host_available < self.minimum_host_available_bytes
                ):
                    self._record_resource_guard_breach(
                        tree_rss_bytes=tree_rss,
                        host_available_bytes=host_available,
                        sample_context={
                            "watchdogOrdinal": self._resource_watchdog_count,
                            "sampledAtUtc": _utc_now(),
                            "sampleOffsetNs": now_perf_ns - self._started_perf_ns,
                            "activeContext": self._active_resource_context(),
                            "watchdogOrigin": origin,
                        },
                        source="watchdog",
                    )
                with self._lock:
                    self._last_watchdog_perf_ns = now_perf_ns
        except BaseException as exc:
            now_perf_ns = time.perf_counter_ns()
            try:
                active_context = self._active_resource_context()
            except BaseException:
                active_context = {"phase": None, "phasePath": [], "spanId": None}
            self._record_resource_guard_breach(
                tree_rss_bytes=None,
                host_available_bytes=None,
                sample_context={
                    "watchdogOrdinal": self._resource_watchdog_count,
                    "sampledAtUtc": _utc_now(),
                    "sampleOffsetNs": now_perf_ns - self._started_perf_ns,
                    "activeContext": active_context,
                    "watchdogOrigin": origin,
                    "errorType": type(exc).__name__,
                },
                source="watchdog",
                failure_reason="resource_watchdog_failed",
            )
            with self._lock:
                self._resource_watchdog_error_count += 1
                self._last_watchdog_perf_ns = now_perf_ns
        finally:
            elapsed_wall_ns = max(0, time.perf_counter_ns() - started_perf_ns)
            elapsed_cpu_ns = max(0, _thread_cpu_time_ns() - started_cpu_ns)
            with self._lock:
                self._resource_watchdog_count += 1
                self._resource_watchdog_wall_ns += elapsed_wall_ns
                self._resource_watchdog_thread_cpu_ns += elapsed_cpu_ns

    def _resource_loop(self) -> None:
        detailed_interval_ns = int(self.sample_interval_seconds * 1_000_000_000)
        next_detailed_perf_ns = time.perf_counter_ns() + detailed_interval_ns
        while not self._stop.is_set():
            now_perf_ns = time.perf_counter_ns()
            until_detailed_seconds = max(
                0.01,
                (next_detailed_perf_ns - now_perf_ns) / 1_000_000_000,
            )
            wait_seconds = min(
                self.watchdog_interval_seconds,
                until_detailed_seconds,
            )
            if self._stop.wait(wait_seconds):
                return
            now_perf_ns = time.perf_counter_ns()
            if now_perf_ns >= next_detailed_perf_ns:
                self._sample_resources(origin="sampler")
                missed_intervals = max(
                    1,
                    (now_perf_ns - next_detailed_perf_ns) // detailed_interval_ns + 1,
                )
                next_detailed_perf_ns += missed_intervals * detailed_interval_ns
            else:
                self._watchdog_resources(origin="sampler")

    def set_result(self, result: Mapping[str, Any]) -> None:
        allowed = (
            "completed",
            "proposalCount",
            "candidateCount",
            "terminationReason",
            "populationSha256",
            "journalSha256",
        )
        self._result = {
            key: _safe_json_value(result[key]) for key in allowed if key in result
        }

    def assert_resource_guard(self) -> None:
        now_perf_ns = time.perf_counter_ns()
        with self._lock:
            last_watchdog_perf_ns = self._last_watchdog_perf_ns
        if (
            last_watchdog_perf_ns is None
            or now_perf_ns - last_watchdog_perf_ns
            >= int(self.watchdog_interval_seconds * 1_000_000_000)
        ):
            self._watchdog_resources(origin="proposal_boundary")
        with self._lock:
            breach = _safe_json_value(self._resource_guard_breach)
        if breach is not None:
            reasons = ",".join(breach["reasonCodes"])
            raise PerformanceResourcePressureError(
                f"temporal QD resource guard stopped the run: {reasons}"
            )

    @staticmethod
    def _duration_summary(values: list[int]) -> dict[str, Any]:
        if not values:
            return {
                "count": 0,
                "totalNs": 0,
                "meanNs": 0,
                "minNs": 0,
                "p50Ns": 0,
                "p90Ns": 0,
                "p95Ns": 0,
                "p99Ns": 0,
                "maxNs": 0,
            }
        return {
            "count": len(values),
            "totalNs": sum(values),
            "meanNs": int(round(sum(values) / len(values))),
            "minNs": min(values),
            "p50Ns": _percentile(values, 0.50),
            "p90Ns": _percentile(values, 0.90),
            "p95Ns": _percentile(values, 0.95),
            "p99Ns": _percentile(values, 0.99),
            "maxNs": max(values),
        }

    def _span_summary(self) -> dict[str, Any]:
        return {
            phase: {
                "inclusiveWall": self._duration_summary(stats["inclusive"]),
                "exclusiveWall": self._duration_summary(stats["exclusive"]),
                "coordinatorCpuInclusive": self._duration_summary(
                    stats["cpuInclusive"]
                ),
                "coordinatorCpuExclusive": self._duration_summary(
                    stats["cpuExclusive"]
                ),
                **(
                    {"aggregationRole": "overlapping_total"}
                    if phase in self._overlapping_total_phases
                    else {}
                ),
            }
            for phase, stats in sorted(self._span_stats.items())
        }

    def _resource_summary(self) -> dict[str, Any]:
        def maximum(values: list[int] | list[float]) -> int | float:
            return max(values) if values else 0

        def minimum(values: list[int] | list[float]) -> int | float:
            return min(values) if values else 0

        return {
            "sampleCount": self._resource_sample_count,
            "sampleErrorCount": self._resource_error_count,
            "watchdogCheckCount": self._resource_watchdog_count,
            "watchdogErrorCount": self._resource_watchdog_error_count,
            "peakTreeRssBytes": maximum(self._tree_rss),
            "peakTreePrivateBytes": maximum(self._tree_private),
            "peakTreeVirtualBytes": maximum(self._tree_virtual),
            "peakTreePagefileBytes": maximum(self._tree_pagefile),
            "peakCoordinatorRssBytes": maximum(self._coordinator_rss),
            "peakChildrenRssBytes": maximum(self._child_rss),
            "peakProcessCount": maximum(self._process_counts),
            "minimumHostAvailableBytes": minimum(self._host_available),
            "peakHostMemoryPercent": maximum(self._host_memory_percent),
            "peakHostSwapUsedBytes": maximum(self._host_swap_used),
            "peakTreeCpuCoreEquivalent": maximum(self._tree_cpu_cores),
            "peakTreeCpuNormalizedPercent": maximum(
                self._tree_cpu_normalized_percent
            ),
            "treeIoReadBytesDelta": max(
                0,
                int(self._last_io_read_bytes or 0)
                - int(self._first_io_read_bytes or 0),
            ),
            "treeIoWriteBytesDelta": max(
                0,
                int(self._last_io_write_bytes or 0)
                - int(self._first_io_write_bytes or 0),
            ),
            "peakContexts": {
                "treeRss": self._peak_tree_rss_sample,
                "treePrivate": self._peak_tree_private_sample,
                "minimumHostAvailable": self._minimum_host_available_sample,
                "hostSwapUsed": self._peak_host_swap_sample,
                "treeCpu": self._peak_tree_cpu_sample,
            },
            "resourceGuard": {
                "maximumTreeRssBytes": self.maximum_tree_rss_bytes,
                "minimumHostAvailableBytes": self.minimum_host_available_bytes,
                "status": "breached" if self._resource_guard_breach else "within_limits",
                "breach": self._resource_guard_breach,
            },
        }

    @staticmethod
    def _atomic_write(path: Path, value: Mapping[str, Any]) -> None:
        encoded = json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
        temporary = path.with_name(path.name + f".{uuid.uuid4().hex}.tmp")
        temporary.write_text(encoded, encoding="utf-8", newline="\n")
        os.replace(temporary, path)

    def close(self, *, outcome: str, error_type: str | None = None) -> None:
        if self._closed:
            return
        self._outcome = str(outcome)
        self._error_type = str(error_type) if error_type is not None else None
        self._stop.set()
        self._sampler.join(timeout=max(2.0, self.sample_interval_seconds * 4.0))
        self._sample_resources(origin="shutdown")
        self.flush_events()
        ended_perf_ns = time.perf_counter_ns()
        ended_cpu_ns = _thread_cpu_time_ns()
        summary = {
            "schemaVersion": PERFORMANCE_SUMMARY_SCHEMA,
            "sessionId": self.session_id,
            "generationIndex": self.generation_index,
            "startedAtUtc": self._started_at_utc,
            "completedAtUtc": _utc_now(),
            "outcome": self._outcome,
            **({"errorType": self._error_type} if self._error_type else {}),
            "wallDurationNs": ended_perf_ns - self._started_perf_ns,
            "coordinatorCpuNs": ended_cpu_ns - self._started_cpu_ns,
            "platform": {
                "pythonVersion": platform.python_version(),
                "system": platform.system(),
                "release": platform.release(),
                "machine": platform.machine(),
                "logicalCpuCount": int(psutil.cpu_count(logical=True) or 1),
                "physicalCpuCount": int(psutil.cpu_count(logical=False) or 0),
            },
            "policy": {
                **performance_observability_policy(),
                "sampleIntervalSeconds": self.sample_interval_seconds,
                "watchdogIntervalSeconds": self.watchdog_interval_seconds,
                "resourceGuard": {
                    "maximumTreeRssBytes": self.maximum_tree_rss_bytes,
                    "minimumHostAvailableBytes": self.minimum_host_available_bytes,
                    "action": "stop_after_current_proposal_and_preserve_evidence",
                },
            },
            "result": self._result,
            "phaseBreakdown": self._span_summary(),
            "resources": self._resource_summary(),
            "instrumentation": {
                "performanceEventCount": self._event_write_count,
                "performanceEventBytes": self._event_write_bytes,
                "performanceEventWriteWallNs": self._event_write_wall_ns,
                "resourceSampleLineCount": self._resource_write_count,
                "resourceSampleBytes": self._resource_write_bytes,
                "resourceSampleWriteWallNs": self._resource_write_wall_ns,
                "resourceDetailedSampleTelemetryWallNs": self._resource_sample_collection_wall_ns,
                "resourceDetailedSampleTelemetryCpuNs": self._resource_sample_thread_cpu_ns,
                "resourceWatchdogTelemetryCount": self._resource_watchdog_count,
                "resourceWatchdogTelemetryErrorCount": self._resource_watchdog_error_count,
                "resourceWatchdogTelemetryWallNs": self._resource_watchdog_wall_ns,
                "resourceWatchdogTelemetryCpuNs": self._resource_watchdog_thread_cpu_ns,
                "resourceTelemetryCpuNs": (
                    self._resource_sample_thread_cpu_ns
                    + self._resource_watchdog_thread_cpu_ns
                ),
            },
            "artifacts": {
                "events": str(self.events_path),
                "resourceSamples": str(self.resources_path),
                "policy": str(self.policy_path),
            },
            "semanticIdentityParticipation": "excluded_observational_artifacts",
        }
        summary["summarySha256"] = _sha256(summary)
        self._atomic_write(self.summary_path, summary)
        self._atomic_write(self.latest_summary_path, summary)
        self._events_file.close()
        self._resources_file.close()
        self._closed = True


__all__ = [
    "DEFAULT_RESOURCE_SAMPLE_INTERVAL_SECONDS",
    "DEFAULT_RESOURCE_WATCHDOG_INTERVAL_SECONDS",
    "DEFAULT_MAX_TREE_RSS_BYTES",
    "DEFAULT_MINIMUM_HOST_AVAILABLE_BYTES",
    "PERFORMANCE_EVENT_SCHEMA",
    "PERFORMANCE_POLICY_SCHEMA",
    "PERFORMANCE_SUMMARY_SCHEMA",
    "RESOURCE_SAMPLE_SCHEMA",
    "PerformanceTrace",
    "PerformanceResourcePressureError",
    "activate_performance_trace",
    "assert_performance_resource_guard",
    "current_performance_trace",
    "flush_performance_events",
    "performance_observability_policy",
    "record_performance_interval",
    "start_performance_interval",
    "timed_span",
    "timing_scope",
]
