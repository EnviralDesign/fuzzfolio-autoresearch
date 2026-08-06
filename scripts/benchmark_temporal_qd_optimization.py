"""Fresh-process A/B measurements for the temporal-QD optimization detour.

This is deliberately a boundary harness rather than another implementation of
proposal construction or archive reduction.  A benchmark plan pins one fixture
per population shape and phase, then maps each implementation label to a
callable or command runner.  The runner receives the exact fixture and an
empty artifact directory; the harness owns process isolation and all resource
measurement.

Plan shape (the fixture payload is intentionally implementation-specific)::

    {
      "schemaVersion": "temporal_qd_optimization_benchmark_plan_v1",
      "fixtures": {
        "64": {
          "proposal_construction": {
            "frozenInput": "...",
            "inputClone": {
              "sourcePath": "frozen/qd-64-proposal",
              "sourceSha256": "sha256:..."
            }
          },
          "consolidation": {"frozenInput": "..."}
        },
        "128": {"...": "..."},
        "1024": {"...": "..."}
      },
      "implementations": {
        "python_legacy": {
          "proposal_construction": {"callable": "package.module:run"},
          "consolidation": {"callable": "package.module:run"}
        },
        "compact_candidate": {
          "proposal_construction": {"callable": "package.module:run"},
          "consolidation": {"callable": "package.module:run"}
        }
      },
      "nonRegressionThresholds": {
        "proposal_construction": {
          "maxWallTimeRegressionRatio": 0.05,
          "maxMainThreadCpuRegressionRatio": 0.05,
          "maxPeakRecursiveRssRegressionRatio": 0.0
        }
      }
    }

Callable runners are invoked as ``run(context, **arguments)`` and return a
JSON-object summary.  Command runners receive these environment variables:
``TEMPORAL_QD_BENCHMARK_CONTEXT_FILE``,
``TEMPORAL_QD_BENCHMARK_ARTIFACT_ROOT``, and
``TEMPORAL_QD_BENCHMARK_RESULT_FILE``.  They must write a JSON object to the
result file.  A runner can expose its own telemetry CPU with
``telemetryCpuNs`` in that object, or a plan can point at an artifact JSON
field with ``telemetryCpuArtifactPath`` and
``telemetryCpuArtifactPointer``.

Use an output root outside this repository.  The normal mode starts one fresh
Python worker/process tree for every shape x phase x implementation, so Python
allocator state cannot leak from the old path into the new-path measurement.
It alternates old/new then new/old across repetitions (two repetitions by
default); each worker receives a unique clone of the frozen fixture input.
Process-tree CPU and I/O are observability only, not hard admission gates,
because short-lived descendants can still escape OS-level observation.
The hidden worker mode exists only for that parent/child protocol.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
import hashlib
import importlib
import json
import math
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import threading
import time
from typing import Any

import psutil


PLAN_SCHEMA = "temporal_qd_optimization_benchmark_plan_v1"
WORKER_SCHEMA = "temporal_qd_optimization_benchmark_worker_v1"
REPORT_SCHEMA = "temporal_qd_optimization_benchmark_report_v1"
FAILURE_SCHEMA = "temporal_qd_optimization_benchmark_failure_v1"
PHASES = ("proposal_construction", "consolidation")
DEFAULT_SHAPES = (64, 128, 1024)
DEFAULT_REPETITIONS = 2
OBSERVATIONAL_METRICS = (
    "processTreeCpuNs",
    "processTreeReadBytes",
    "processTreeWrittenBytes",
)


class BenchmarkContractError(RuntimeError):
    """The plan or a runner violated the benchmark contract."""


class BenchmarkExecutionError(RuntimeError):
    """An isolated worker or its runner did not finish successfully."""


class BenchmarkThresholdError(RuntimeError):
    """A requested non-regression gate failed after measurements completed."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _canonical_sha256(value: Any) -> str:
    return "sha256:" + hashlib.sha256(
        _canonical_json(value).encode("utf-8")
    ).hexdigest()


def _atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(
        value,
        indent=2,
        sort_keys=True,
        ensure_ascii=True,
        allow_nan=False,
    ) + "\n"
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(encoded, encoding="utf-8", newline="\n")
    os.replace(temporary, path)


def _read_json_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BenchmarkContractError(f"could not read {label}: {path}") from exc
    if not isinstance(value, dict):
        raise BenchmarkContractError(f"{label} must be a JSON object: {path}")
    return value


def _parse_shapes(value: str) -> tuple[int, ...]:
    try:
        shapes = tuple(int(part.strip()) for part in str(value).split(","))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "--shapes must be comma-separated positive population shapes"
        ) from exc
    if not shapes or any(shape < 1 for shape in shapes):
        raise argparse.ArgumentTypeError(
            "--shapes must be comma-separated positive population shapes"
        )
    if len(set(shapes)) != len(shapes):
        raise argparse.ArgumentTypeError("--shapes cannot contain duplicates")
    return shapes


def _require_mapping(value: Any, *, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise BenchmarkContractError(f"{label} must be an object")
    return value


def _require_nonempty_string(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise BenchmarkContractError(f"{label} must be a non-empty string")
    return value


def _coerce_nonnegative_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise BenchmarkContractError(f"{label} must be a non-negative integer")
    return int(value)


def _coerce_nonnegative_number(value: Any, *, label: str) -> int | float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) < 0
    ):
        raise BenchmarkContractError(f"{label} must be a finite non-negative number")
    return value


def _validate_runner_spec(value: Any, *, label: str) -> Mapping[str, Any]:
    spec = _require_mapping(value, label=label)
    has_callable = "callable" in spec
    has_command = "command" in spec
    if has_callable == has_command:
        raise BenchmarkContractError(
            f"{label} must contain exactly one of callable or command"
        )
    if has_callable:
        _require_nonempty_string(spec["callable"], label=f"{label}.callable")
    else:
        command = spec["command"]
        if (
            isinstance(command, (str, bytes))
            or not isinstance(command, Sequence)
            or not command
            or any(not isinstance(token, str) or not token for token in command)
        ):
            raise BenchmarkContractError(
                f"{label}.command must be a non-empty list of strings"
            )
    if "arguments" in spec:
        _require_mapping(spec["arguments"], label=f"{label}.arguments")
    for field in (
        "telemetryCpuResultPointer",
        "telemetryCpuArtifactPath",
        "telemetryCpuArtifactPointer",
        "workingDirectory",
    ):
        if field in spec:
            _require_nonempty_string(spec[field], label=f"{label}.{field}")
    return spec


def validate_benchmark_plan(plan: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the immutable plan without interpreting fixture contents."""

    value = _require_mapping(plan, label="benchmark plan")
    if value.get("schemaVersion") != PLAN_SCHEMA:
        raise BenchmarkContractError(
            f"benchmark plan schemaVersion must be {PLAN_SCHEMA!r}"
        )
    fixtures = _require_mapping(value.get("fixtures"), label="plan.fixtures")
    implementations = _require_mapping(
        value.get("implementations"), label="plan.implementations"
    )
    if not implementations:
        raise BenchmarkContractError("plan.implementations must not be empty")
    for implementation, implementation_spec in implementations.items():
        _require_nonempty_string(implementation, label="implementation label")
        phases = _require_mapping(
            implementation_spec,
            label=f"plan.implementations.{implementation}",
        )
        for phase in PHASES:
            if phase not in phases:
                raise BenchmarkContractError(
                    f"plan.implementations.{implementation} misses {phase}"
                )
            _validate_runner_spec(
                phases[phase],
                label=f"plan.implementations.{implementation}.{phase}",
            )
    for shape, shape_fixtures in fixtures.items():
        try:
            normalized_shape = int(str(shape))
        except ValueError as exc:
            raise BenchmarkContractError(
                "plan.fixtures keys must be positive integer population shapes"
            ) from exc
        if normalized_shape < 1 or str(normalized_shape) != str(shape):
            raise BenchmarkContractError(
                "plan.fixtures keys must be canonical positive integer population shapes"
            )
        phase_fixtures = _require_mapping(
            shape_fixtures, label=f"plan.fixtures.{shape}"
        )
        for phase in PHASES:
            if phase not in phase_fixtures:
                raise BenchmarkContractError(f"plan.fixtures.{shape} misses {phase}")
            fixture = _require_mapping(
                phase_fixtures[phase],
                label=f"plan.fixtures.{shape}.{phase}",
            )
            _validate_input_clone_spec(fixture)
    thresholds = value.get("nonRegressionThresholds")
    if thresholds is not None:
        threshold_groups = _require_mapping(
            thresholds, label="plan.nonRegressionThresholds"
        )
        allowed_groups = {"default", *PHASES}
        unknown_groups = sorted(set(threshold_groups) - allowed_groups)
        if unknown_groups:
            raise BenchmarkContractError(
                "plan.nonRegressionThresholds contains unknown groups: "
                + ", ".join(unknown_groups)
            )
        for group, rules in threshold_groups.items():
            _validate_thresholds(rules, label=f"plan.nonRegressionThresholds.{group}")
    return dict(value)


_THRESHOLD_METRICS = {
    "maxWallTimeRegressionRatio": "endToEndWallNs",
    "maxMainThreadCpuRegressionRatio": "mainThreadCpuNs",
    "maxPeakRecursiveRssRegressionRatio": "peakRecursiveRssBytes",
    "maxArtifactBytesRegressionRatio": "artifactTotalBytes",
    "maxHostHeadroomRegressionRatio": "minimumHostAvailableBytes",
}


def _validate_thresholds(value: Any, *, label: str) -> Mapping[str, Any]:
    rules = _require_mapping(value, label=label)
    unknown = sorted(set(rules) - set(_THRESHOLD_METRICS))
    if unknown:
        raise BenchmarkContractError(
            f"{label} contains unknown threshold fields: {', '.join(unknown)}"
        )
    if not rules:
        raise BenchmarkContractError(f"{label} must not be empty")
    for field, threshold in rules.items():
        if (
            isinstance(threshold, bool)
            or not isinstance(threshold, (int, float))
            or not math.isfinite(float(threshold))
            or float(threshold) < 0
        ):
            raise BenchmarkContractError(
                f"{label}.{field} must be a non-negative number"
            )
    return rules


def _thresholds_for_phase(plan: Mapping[str, Any], phase: str) -> dict[str, float]:
    configured = plan.get("nonRegressionThresholds")
    if configured is None:
        return {}
    threshold_groups = _require_mapping(
        configured, label="plan.nonRegressionThresholds"
    )
    merged: dict[str, float] = {}
    for group in ("default", phase):
        rules = threshold_groups.get(group)
        if rules is None:
            continue
        for key, value in _validate_thresholds(
            rules, label=f"plan.nonRegressionThresholds.{group}"
        ).items():
            merged[key] = float(value)
    return merged


def _fresh_external_root(output_root: Path) -> Path:
    root = output_root.resolve()
    repository = Path(__file__).resolve().parents[1]
    try:
        inside_repository = os.path.commonpath((str(root), str(repository))) == str(
            repository
        )
    except ValueError:
        inside_repository = False
    if inside_repository:
        raise BenchmarkContractError(
            "--output-root must be external to the autoresearch repository"
        )
    if root.exists() and any(root.iterdir()):
        raise BenchmarkContractError("--output-root must be absent or empty")
    root.mkdir(parents=True, exist_ok=True)
    return root


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _input_fingerprint(path: Path) -> dict[str, Any]:
    """Return a portable, symlink-free content fingerprint for a frozen input."""

    if path.is_symlink():
        raise BenchmarkContractError("frozen input source cannot be a symlink")
    if path.is_file():
        size = int(path.stat().st_size)
        return {
            "kind": "file",
            "contentSha256": _sha256_file(path),
            "fileCount": 1,
            "totalBytes": size,
        }
    if not path.is_dir():
        raise BenchmarkContractError(f"frozen input source does not exist: {path}")
    files: list[dict[str, Any]] = []
    for child in sorted(path.rglob("*"), key=lambda item: item.as_posix()):
        if child.is_symlink():
            raise BenchmarkContractError("frozen input directory cannot contain symlinks")
        if not child.is_file():
            continue
        files.append(
            {
                "path": child.relative_to(path).as_posix(),
                "bytes": int(child.stat().st_size),
                "sha256": _sha256_file(child),
            }
        )
    manifest = {
        "schemaVersion": "temporal_qd_optimization_input_tree_v1",
        "files": files,
    }
    return {
        "kind": "directory",
        "contentSha256": _canonical_sha256(manifest),
        "fileCount": len(files),
        "totalBytes": sum(int(item["bytes"]) for item in files),
    }


def _validate_input_clone_spec(fixture: Mapping[str, Any]) -> Mapping[str, Any] | None:
    value = fixture.get("inputClone")
    if value is None:
        return None
    spec = _require_mapping(value, label="fixture.inputClone")
    _require_nonempty_string(spec.get("sourcePath"), label="fixture.inputClone.sourcePath")
    source_sha = _require_nonempty_string(
        spec.get("sourceSha256"), label="fixture.inputClone.sourceSha256"
    )
    if not source_sha.startswith("sha256:"):
        raise BenchmarkContractError(
            "fixture.inputClone.sourceSha256 must use the sha256:<hex> form"
        )
    if "sourceKind" in spec and spec["sourceKind"] not in {"file", "directory"}:
        raise BenchmarkContractError(
            "fixture.inputClone.sourceKind must be file or directory when supplied"
        )
    return spec


def _materialize_input_clone(
    *,
    fixture: Mapping[str, Any],
    plan_path: Path,
    worker_root: Path,
) -> tuple[Path, dict[str, Any]]:
    """Stage a unique verified input clone before the worker starts measuring."""

    worker_root.mkdir(parents=True, exist_ok=True)
    input_root = worker_root / "input"
    if input_root.exists():
        raise BenchmarkContractError("input clone destination already exists")
    clone_spec = _validate_input_clone_spec(fixture)
    fixture_sha256 = _canonical_sha256(fixture)
    if clone_spec is None:
        input_root.mkdir()
        _atomic_write_json(input_root / "fixture.json", dict(fixture))
        fingerprint = _input_fingerprint(input_root)
        manifest = {
            "schemaVersion": "temporal_qd_optimization_input_clone_v1",
            "mode": "inline_fixture_copy",
            "fixtureSha256": fixture_sha256,
            "inputContentSha256": fingerprint["contentSha256"],
            "fileCount": fingerprint["fileCount"],
            "totalBytes": fingerprint["totalBytes"],
        }
    else:
        configured = Path(str(clone_spec["sourcePath"]))
        source = configured if configured.is_absolute() else plan_path.parent / configured
        source = source.resolve()
        source_fingerprint = _input_fingerprint(source)
        declared_kind = clone_spec.get("sourceKind")
        if declared_kind is not None and source_fingerprint["kind"] != declared_kind:
            raise BenchmarkContractError("frozen input source kind differs from its plan")
        if source_fingerprint["contentSha256"] != clone_spec["sourceSha256"]:
            raise BenchmarkContractError("frozen input source content hash differs from its plan")
        if source_fingerprint["kind"] == "file":
            input_root.mkdir()
            shutil.copy2(source, input_root / source.name)
        else:
            shutil.copytree(source, input_root)
        fingerprint = _input_fingerprint(input_root)
        if source_fingerprint["kind"] == "file":
            # A file source is intentionally wrapped in a one-file input root;
            # compare its payload through the copied file rather than the root.
            copied_source = input_root / source.name
            copied_fingerprint = _input_fingerprint(copied_source)
            copied_sha256 = copied_fingerprint["contentSha256"]
        else:
            copied_sha256 = fingerprint["contentSha256"]
        if copied_sha256 != source_fingerprint["contentSha256"]:
            raise BenchmarkContractError("materialized input clone content hash mismatch")
        manifest = {
            "schemaVersion": "temporal_qd_optimization_input_clone_v1",
            "mode": "verified_source_copy",
            "fixtureSha256": fixture_sha256,
            "sourceSha256": source_fingerprint["contentSha256"],
            "sourceKind": source_fingerprint["kind"],
            "inputContentSha256": fingerprint["contentSha256"],
            "fileCount": fingerprint["fileCount"],
            "totalBytes": fingerprint["totalBytes"],
        }
    _atomic_write_json(worker_root / "input-clone.json", manifest)
    return input_root, manifest


def _safe_relative_path(value: str, *, label: str) -> Path:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise BenchmarkContractError(f"{label} must be a relative path below artifact root")
    return path


def _json_pointer(value: Any, pointer: str, *, label: str) -> Any:
    if pointer == "":
        return value
    if not pointer.startswith("/"):
        raise BenchmarkContractError(f"{label} must be an RFC 6901 JSON pointer")
    current = value
    for escaped_token in pointer[1:].split("/"):
        token = escaped_token.replace("~1", "/").replace("~0", "~")
        if isinstance(current, Mapping):
            if token not in current:
                raise BenchmarkContractError(f"{label} did not resolve at {token!r}")
            current = current[token]
        elif isinstance(current, list):
            try:
                index = int(token)
            except ValueError as exc:
                raise BenchmarkContractError(
                    f"{label} list token must be an integer: {token!r}"
                ) from exc
            try:
                current = current[index]
            except IndexError as exc:
                raise BenchmarkContractError(f"{label} index is out of range") from exc
        else:
            raise BenchmarkContractError(f"{label} did not resolve through a container")
    return current


def _telemetry_cpu_from_runner(
    *,
    runner_result: Mapping[str, Any],
    runner_spec: Mapping[str, Any],
    artifact_root: Path,
) -> tuple[int | None, str | None]:
    if "telemetryCpuResultPointer" in runner_spec:
        pointer = str(runner_spec["telemetryCpuResultPointer"])
        value = _json_pointer(
            runner_result,
            pointer,
            label="runner telemetryCpuResultPointer",
        )
        return (
            _coerce_nonnegative_int(value, label="runner exposed telemetry CPU"),
            f"runner-result:{pointer}",
        )
    if "telemetryCpuArtifactPath" in runner_spec:
        relative = _safe_relative_path(
            str(runner_spec["telemetryCpuArtifactPath"]),
            label="telemetryCpuArtifactPath",
        )
        artifact = _read_json_object(
            artifact_root / relative,
            label="telemetry artifact",
        )
        pointer = str(
            runner_spec.get("telemetryCpuArtifactPointer")
            or "/instrumentation/resourceTelemetryCpuNs"
        )
        value = _json_pointer(
            artifact,
            pointer,
            label="runner telemetryCpuArtifactPointer",
        )
        return (
            _coerce_nonnegative_int(value, label="runner exposed telemetry CPU"),
            f"artifact:{relative.as_posix()}#{pointer}",
        )
    for pointer in (
        "/telemetryCpuNs",
        "/instrumentation/resourceTelemetryCpuNs",
        "/performanceSummary/instrumentation/resourceTelemetryCpuNs",
    ):
        try:
            value = _json_pointer(runner_result, pointer, label="automatic telemetry")
        except BenchmarkContractError:
            continue
        return (
            _coerce_nonnegative_int(value, label="runner exposed telemetry CPU"),
            f"runner-result:{pointer}",
        )
    return None, None


def _thread_cpu_clock() -> tuple[Callable[[], int], str]:
    clock = getattr(time, "thread_time_ns", None)
    if callable(clock):
        return clock, "time.thread_time_ns"
    return time.process_time_ns, "time.process_time_ns_fallback"


class _ProcessTreeMonitor:
    """Sample only observational process-tree metrics around one phase run."""

    def __init__(self, *, interval_seconds: float) -> None:
        if interval_seconds <= 0:
            raise BenchmarkContractError("monitor interval must be positive")
        self.interval_seconds = float(interval_seconds)
        self._root = psutil.Process(os.getpid())
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._sample_count = 0
        self._peak_recursive_rss_bytes = 0
        self._peak_worker_rss_bytes = 0
        self._minimum_host_available_bytes: int | None = None
        self._host_available_start_bytes: int | None = None
        self._host_available_end_bytes: int | None = None
        self._processes: dict[tuple[int, float], dict[str, int | None]] = {}
        self._registered_spawned_pids: set[int] = set()
        self._observed_spawned_pids: set[int] = set()
        self._telemetry_cpu_ns = 0
        self._telemetry_cpu_clock = ""

    def _tree_processes(self) -> list[psutil.Process]:
        try:
            descendants = self._root.children(recursive=True)
        except (psutil.Error, OSError):
            descendants = []
        unique: dict[int, psutil.Process] = {self._root.pid: self._root}
        for process in descendants:
            unique[process.pid] = process
        return list(unique.values())

    @staticmethod
    def _io_snapshot(process: psutil.Process) -> tuple[int, int] | None:
        try:
            counters = process.io_counters()
            return int(counters.read_bytes), int(counters.write_bytes)
        except (psutil.Error, OSError, AttributeError):
            return None

    @staticmethod
    def _cpu_snapshot(process: psutil.Process) -> int | None:
        try:
            counters = process.cpu_times()
            return int((float(counters.user) + float(counters.system)) * 1_000_000_000)
        except (psutil.Error, OSError, AttributeError):
            return None

    def _sample(self) -> None:
        try:
            available = int(psutil.virtual_memory().available)
        except (psutil.Error, OSError):
            available = 0
        recursive_rss = 0
        worker_rss = 0
        samples: list[tuple[tuple[int, float], int, tuple[int, int] | None, int | None]] = []
        for process in self._tree_processes():
            try:
                key = (int(process.pid), float(process.create_time()))
                rss = int(process.memory_info().rss)
            except (psutil.Error, OSError):
                continue
            recursive_rss += rss
            if process.pid == self._root.pid:
                worker_rss = rss
            samples.append((key, rss, self._io_snapshot(process), self._cpu_snapshot(process)))
        with self._lock:
            self._sample_count += 1
            if self._host_available_start_bytes is None:
                self._host_available_start_bytes = available
            self._host_available_end_bytes = available
            if (
                self._minimum_host_available_bytes is None
                or available < self._minimum_host_available_bytes
            ):
                self._minimum_host_available_bytes = available
            self._peak_recursive_rss_bytes = max(
                self._peak_recursive_rss_bytes, recursive_rss
            )
            self._peak_worker_rss_bytes = max(self._peak_worker_rss_bytes, worker_rss)
            for key, _rss, io_counters, cpu_ns in samples:
                is_lifecycle_child = key[0] != self._root.pid
                if key[0] in self._registered_spawned_pids:
                    self._observed_spawned_pids.add(key[0])
                state = self._processes.setdefault(
                    key,
                    {
                        "initialReadBytes": None,
                        "latestReadBytes": None,
                        "initialWrittenBytes": None,
                        "latestWrittenBytes": None,
                        "initialCpuNs": None,
                        "latestCpuNs": None,
                        "lifecycleChild": int(is_lifecycle_child),
                    },
                )
                if io_counters is not None:
                    read_bytes, written_bytes = io_counters
                    if state["initialReadBytes"] is None:
                        # A child first observed after it has already done
                        # work must contribute its lifetime counters.  The
                        # worker itself predates the timed region and still
                        # uses a start/end delta.
                        state["initialReadBytes"] = (
                            0 if state["lifecycleChild"] else read_bytes
                        )
                        state["initialWrittenBytes"] = (
                            0 if state["lifecycleChild"] else written_bytes
                        )
                    state["latestReadBytes"] = read_bytes
                    state["latestWrittenBytes"] = written_bytes
                if cpu_ns is not None:
                    if state["initialCpuNs"] is None:
                        state["initialCpuNs"] = (
                            0 if state["lifecycleChild"] else cpu_ns
                        )
                    state["latestCpuNs"] = cpu_ns

    def _run(self) -> None:
        clock, clock_name = _thread_cpu_clock()
        started = int(clock())
        try:
            while not self._stop.wait(self.interval_seconds):
                self._sample()
        finally:
            self._sample()
            with self._lock:
                self._telemetry_cpu_ns += max(0, int(clock()) - started)
                self._telemetry_cpu_clock = clock_name

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("process-tree monitor was already started")
        self._sample()
        self._thread = threading.Thread(
            target=self._run,
            name="temporal-qd-optimization-benchmark-monitor",
            daemon=True,
        )
        self._thread.start()

    def sample_now(self) -> None:
        """Capture a lifecycle boundary such as command child start/exit."""

        self._sample()

    def register_spawned_process(self, pid: int) -> None:
        """Mark a command child as born within the timed lifecycle."""

        with self._lock:
            self._registered_spawned_pids.add(int(pid))

    def stop(self) -> dict[str, Any]:
        if self._thread is None:
            raise RuntimeError("process-tree monitor was not started")
        self._stop.set()
        self._thread.join(timeout=max(2.0, self.interval_seconds * 4.0))
        self._sample()
        with self._lock:
            read_bytes = 0
            written_bytes = 0
            cpu_ns = 0
            io_available = False
            cpu_available = False
            for state in self._processes.values():
                initial_read = state["initialReadBytes"]
                latest_read = state["latestReadBytes"]
                initial_written = state["initialWrittenBytes"]
                latest_written = state["latestWrittenBytes"]
                if initial_read is not None and latest_read is not None:
                    read_bytes += max(0, int(latest_read) - int(initial_read))
                    io_available = True
                if initial_written is not None and latest_written is not None:
                    written_bytes += max(0, int(latest_written) - int(initial_written))
                    io_available = True
                initial_cpu = state["initialCpuNs"]
                latest_cpu = state["latestCpuNs"]
                if initial_cpu is not None and latest_cpu is not None:
                    cpu_ns += max(0, int(latest_cpu) - int(initial_cpu))
                    cpu_available = True
            return {
                "monitorIntervalSeconds": self.interval_seconds,
                "sampleCount": self._sample_count,
                "observedProcessCount": len(self._processes),
                "peakRecursiveRssBytes": self._peak_recursive_rss_bytes,
                "peakWorkerRssBytes": self._peak_worker_rss_bytes,
                "minimumHostAvailableBytes": self._minimum_host_available_bytes or 0,
                "hostAvailableAtStartBytes": self._host_available_start_bytes or 0,
                "hostAvailableAtEndBytes": self._host_available_end_bytes or 0,
                "processTreeReadBytes": read_bytes if io_available else None,
                "processTreeWrittenBytes": written_bytes if io_available else None,
                "processTreeCpuNs": cpu_ns if cpu_available else None,
                "harnessTelemetryCpuNs": self._telemetry_cpu_ns,
                "harnessTelemetryCpuClock": self._telemetry_cpu_clock,
                "processTreeCounterAccounting": {
                    "mode": "worker_delta_plus_observed_child_lifetime",
                    "registeredDirectChildCount": len(self._registered_spawned_pids),
                    "observedRegisteredChildCount": len(
                        self._registered_spawned_pids
                        & self._observed_spawned_pids
                    ),
                    "unobservedRegisteredChildCount": len(
                        self._registered_spawned_pids
                        - self._observed_spawned_pids
                    ),
                    "admissionUse": "observational_only",
                },
                "measurementScope": "benchmark worker and recursive children",
            }


def _artifact_sizes(root: Path) -> dict[str, Any]:
    files: list[dict[str, Any]] = []
    extensions: dict[str, int] = {}
    if not root.exists():
        raise BenchmarkContractError("runner artifact root was not created")
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        if not path.is_file() or path.is_symlink():
            continue
        relative = path.relative_to(root).as_posix()
        size = int(path.stat().st_size)
        files.append({"path": relative, "bytes": size})
        extension = path.suffix.lower() or "<none>"
        extensions[extension] = extensions.get(extension, 0) + size
    return {
        "fileCount": len(files),
        "totalBytes": sum(int(item["bytes"]) for item in files),
        "bytesByExtension": dict(sorted(extensions.items())),
        "files": files,
    }


def _resolve_callable(reference: str) -> Callable[..., Any]:
    module_name, separator, attribute = reference.partition(":")
    if not separator or not module_name or not attribute:
        raise BenchmarkContractError(
            "callable runner must use the exact module:function form"
        )
    try:
        module = importlib.import_module(module_name)
        runner = getattr(module, attribute)
    except (ImportError, AttributeError) as exc:
        raise BenchmarkContractError(f"could not resolve callable runner {reference!r}") from exc
    if not callable(runner):
        raise BenchmarkContractError(f"callable runner is not callable: {reference!r}")
    return runner


def _expanded_command(command: Sequence[str], context: Mapping[str, Any]) -> list[str]:
    replacements = {
        "{context_file}": str(context["contextFile"]),
        "{artifact_root}": str(context["artifactRoot"]),
        "{runner_result_file}": str(context["runnerResultFile"]),
        "{shape}": str(context["shape"]),
        "{phase}": str(context["phase"]),
        "{implementation}": str(context["implementation"]),
    }
    expanded: list[str] = []
    for token in command:
        value = token
        for source, replacement in replacements.items():
            value = value.replace(source, replacement)
        expanded.append(value)
    return expanded


def _run_command_runner(
    *,
    runner_spec: Mapping[str, Any],
    context: Mapping[str, Any],
    plan_path: Path,
    monitor: _ProcessTreeMonitor,
) -> Mapping[str, Any]:
    command = _expanded_command(runner_spec["command"], context)
    environment = os.environ.copy()
    environment.update(
        {
            "TEMPORAL_QD_BENCHMARK_CONTEXT_FILE": str(context["contextFile"]),
            "TEMPORAL_QD_BENCHMARK_ARTIFACT_ROOT": str(context["artifactRoot"]),
            "TEMPORAL_QD_BENCHMARK_RESULT_FILE": str(context["runnerResultFile"]),
        }
    )
    working_directory = plan_path.parent
    if "workingDirectory" in runner_spec:
        configured = Path(str(runner_spec["workingDirectory"]))
        working_directory = (
            configured if configured.is_absolute() else plan_path.parent / configured
        )
    if not working_directory.is_dir():
        raise BenchmarkContractError(
            f"command runner working directory does not exist: {working_directory}"
        )
    try:
        process = subprocess.Popen(
            command,
            cwd=working_directory,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            errors="replace",
        )
    except OSError as exc:
        raise BenchmarkExecutionError("could not start command runner") from exc
    monitor.register_spawned_process(process.pid)
    monitor.sample_now()
    while True:
        try:
            stdout, stderr = process.communicate(
                timeout=min(0.05, monitor.interval_seconds)
            )
            break
        except subprocess.TimeoutExpired:
            monitor.sample_now()
    monitor.sample_now()
    if process.returncode != 0:
        detail = (stderr or stdout or "").strip()[-4000:]
        raise BenchmarkExecutionError(
            f"command runner exited {process.returncode}: {detail}"
        )
    result_path = Path(str(context["runnerResultFile"]))
    return _read_json_object(result_path, label="command runner result")


def _run_callable_runner(
    *,
    runner_spec: Mapping[str, Any],
    context: Mapping[str, Any],
) -> Mapping[str, Any]:
    runner = _resolve_callable(str(runner_spec["callable"]))
    arguments = dict(
        _require_mapping(runner_spec.get("arguments", {}), label="runner arguments")
    )
    try:
        result = runner(context, **arguments)
    except (BenchmarkContractError, BenchmarkExecutionError):
        raise
    except Exception as exc:
        raise BenchmarkExecutionError("callable runner raised an exception") from exc
    if not isinstance(result, Mapping):
        raise BenchmarkContractError("callable runner must return a JSON object")
    copied = dict(result)
    try:
        _atomic_write_json(Path(str(context["runnerResultFile"])), copied)
    except (TypeError, ValueError) as exc:
        raise BenchmarkContractError(
            "callable runner result must be JSON-serializable without NaN"
        ) from exc
    return copied


def _runner_kind(runner_spec: Mapping[str, Any]) -> str:
    return "callable" if "callable" in runner_spec else "command"


def _run_phase_worker(
    *,
    plan_path: Path,
    expected_plan_sha256: str,
    worker_root: Path,
    shape: int,
    phase: str,
    implementation: str,
    monitor_interval_seconds: float,
    measurement_context: str,
    input_root: Path,
    input_clone_manifest_path: Path,
) -> dict[str, Any]:
    plan = validate_benchmark_plan(_read_json_object(plan_path, label="benchmark plan"))
    actual_plan_sha256 = _canonical_sha256(plan)
    if actual_plan_sha256 != expected_plan_sha256:
        raise BenchmarkContractError("benchmark plan changed after parent dispatch")
    if phase not in PHASES:
        raise BenchmarkContractError(f"unknown benchmark phase: {phase}")
    implementations = _require_mapping(plan["implementations"], label="plan.implementations")
    if implementation not in implementations:
        raise BenchmarkContractError(
            f"selected implementation is not in the plan: {implementation}"
        )
    fixtures = _require_mapping(plan["fixtures"], label="plan.fixtures")
    shape_key = str(shape)
    if shape_key not in fixtures:
        raise BenchmarkContractError(f"plan has no fixture for requested shape {shape}")
    phase_fixtures = _require_mapping(
        fixtures[shape_key], label=f"plan.fixtures.{shape_key}"
    )
    fixture = _require_mapping(
        phase_fixtures[phase], label=f"plan.fixtures.{shape_key}.{phase}"
    )
    implementation_phases = _require_mapping(
        implementations[implementation], label=f"implementation {implementation}"
    )
    runner_spec = _validate_runner_spec(
        implementation_phases[phase],
        label=f"implementation {implementation}.{phase}",
    )
    expected_staged_names = {"input", "input-clone.json"}
    if not worker_root.is_dir() or {
        child.name for child in worker_root.iterdir()
    } != expected_staged_names:
        raise BenchmarkContractError(
            "worker root must contain only the parent-staged input clone"
        )
    if input_root.resolve() != (worker_root / "input").resolve():
        raise BenchmarkContractError("worker input root does not match its staged location")
    if input_clone_manifest_path.resolve() != (
        worker_root / "input-clone.json"
    ).resolve():
        raise BenchmarkContractError(
            "worker input clone manifest does not match its staged location"
        )
    input_clone = _read_json_object(
        input_clone_manifest_path, label="input clone manifest"
    )
    input_fingerprint = _input_fingerprint(input_root)
    if input_clone.get("inputContentSha256") != input_fingerprint["contentSha256"]:
        raise BenchmarkContractError("staged input clone content hash changed before measurement")
    worker_root.mkdir(parents=True, exist_ok=True)
    artifact_root = worker_root / "artifacts"
    artifact_root.mkdir()
    runner_result_file = worker_root / "runner-result.json"
    context = {
        "schemaVersion": "temporal_qd_optimization_benchmark_context_v1",
        "planSha256": actual_plan_sha256,
        "fixtureSha256": _canonical_sha256(fixture),
        "shape": shape,
        "phase": phase,
        "implementation": implementation,
        "fixture": dict(fixture),
        "inputRoot": str(input_root.resolve()),
        "inputClone": input_clone,
        "artifactRoot": str(artifact_root.resolve()),
        "runnerResultFile": str(runner_result_file.resolve()),
        "contextFile": str((worker_root / "context.json").resolve()),
    }
    _atomic_write_json(Path(str(context["contextFile"])), context)
    main_cpu_clock, main_cpu_clock_name = _thread_cpu_clock()
    monitor = _ProcessTreeMonitor(interval_seconds=monitor_interval_seconds)
    monitor.start()
    started_wall_ns = time.perf_counter_ns()
    started_main_cpu_ns = int(main_cpu_clock())
    try:
        if "callable" in runner_spec:
            runner_result = _run_callable_runner(
                runner_spec=runner_spec,
                context=context,
            )
        else:
            runner_result = _run_command_runner(
                runner_spec=runner_spec,
                context=context,
                plan_path=plan_path,
                monitor=monitor,
            )
    finally:
        ended_main_cpu_ns = int(main_cpu_clock())
        ended_wall_ns = time.perf_counter_ns()
        monitor_result = monitor.stop()
    exposed_telemetry_cpu_ns, exposed_telemetry_cpu_source = _telemetry_cpu_from_runner(
        runner_result=runner_result,
        runner_spec=runner_spec,
        artifact_root=artifact_root,
    )
    measurement = {
        "endToEndWallNs": max(0, ended_wall_ns - started_wall_ns),
        "mainThreadCpuNs": max(0, ended_main_cpu_ns - started_main_cpu_ns),
        "mainThreadCpuClock": main_cpu_clock_name,
        **monitor_result,
        "runnerTelemetryCpuNs": exposed_telemetry_cpu_ns,
        "runnerTelemetryCpuSource": exposed_telemetry_cpu_source,
    }
    result = {
        "schemaVersion": WORKER_SCHEMA,
        "status": "completed",
        "planSha256": actual_plan_sha256,
        "fixtureSha256": context["fixtureSha256"],
        "shape": shape,
        "phase": phase,
        "implementation": implementation,
        "measurementContext": measurement_context,
        "inputClone": input_clone,
        "isolation": {
            "mode": "fresh_python_worker_process_tree",
            "workerProcessId": os.getpid(),
            "workerParentProcessId": os.getppid(),
        },
        "measurement": measurement,
        "artifactSizes": _artifact_sizes(artifact_root),
        "runner": {
            "kind": _runner_kind(runner_spec),
            "resultSha256": _canonical_sha256(runner_result),
            "result": runner_result,
        },
    }
    _atomic_write_json(worker_root / "worker-report.json", result)
    return result


def _worker_failure(worker_root: Path, exc: BaseException) -> None:
    try:
        worker_root.mkdir(parents=True, exist_ok=True)
        _atomic_write_json(
            worker_root / "worker-failure.json",
            {
                "schemaVersion": FAILURE_SCHEMA,
                "status": "failed",
                "errorType": type(exc).__name__,
                "message": str(exc),
            },
        )
    except OSError:
        # Preserve the original error; the worker path is only diagnostic.
        return


def _path_label(value: str) -> str:
    readable = "".join(
        character if character.isalnum() or character in {"-", "_"} else "-"
        for character in value
    ).strip("-")
    readable = readable[:40] or "implementation"
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:10]
    return f"{readable}-{digest}"


def _run_isolated(
    *,
    plan_path: Path,
    plan_sha256: str,
    worker_root: Path,
    shape: int,
    phase: str,
    implementation: str,
    monitor_interval_seconds: float,
    measurement_context: str,
    fixture: Mapping[str, Any],
) -> dict[str, Any]:
    """Run exactly one phase/implementation in a fresh interpreter tree."""

    input_root, _input_manifest = _materialize_input_clone(
        fixture=fixture,
        plan_path=plan_path,
        worker_root=worker_root,
    )
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--plan",
        str(plan_path.resolve()),
        "--expected-plan-sha256",
        plan_sha256,
        "--worker-root",
        str(worker_root.resolve()),
        "--shape",
        str(shape),
        "--phase",
        phase,
        "--implementation",
        implementation,
        "--monitor-interval-seconds",
        str(monitor_interval_seconds),
        "--measurement-context",
        measurement_context,
        "--input-root",
        str(input_root.resolve()),
        "--input-clone-manifest",
        str((worker_root / "input-clone.json").resolve()),
    ]
    completed = subprocess.run(
        command,
        cwd=Path(__file__).resolve().parents[1],
        check=False,
        capture_output=True,
        text=True,
        errors="replace",
    )
    if completed.returncode != 0:
        failure_path = worker_root / "worker-failure.json"
        if failure_path.is_file():
            try:
                failure = _read_json_object(failure_path, label="worker failure")
                detail = str(failure.get("message") or "")
            except BenchmarkContractError:
                detail = ""
        else:
            detail = ""
        detail = detail or (completed.stderr or completed.stdout or "").strip()[-4000:]
        raise BenchmarkExecutionError(
            f"fresh worker failed for shape={shape}, phase={phase}, "
            f"implementation={implementation}: {detail}"
        )
    report_path = worker_root / "worker-report.json"
    report = _read_json_object(report_path, label="fresh worker report")
    if (
        report.get("schemaVersion") != WORKER_SCHEMA
        or report.get("status") != "completed"
        or report.get("shape") != shape
        or report.get("phase") != phase
        or report.get("implementation") != implementation
        or report.get("planSha256") != plan_sha256
    ):
        raise BenchmarkContractError("fresh worker report does not match its dispatch")
    return report


def _metric_values(run: Mapping[str, Any]) -> dict[str, int | float | None]:
    measurement = _require_mapping(run.get("measurement"), label="worker measurement")
    artifacts = _require_mapping(run.get("artifactSizes"), label="worker artifact sizes")
    names = (
        "endToEndWallNs",
        "mainThreadCpuNs",
        "processTreeCpuNs",
        "peakRecursiveRssBytes",
        "minimumHostAvailableBytes",
        "processTreeReadBytes",
        "processTreeWrittenBytes",
    )
    values: dict[str, int | float | None] = {}
    for name in names:
        value = measurement.get(name)
        if value is None:
            values[name] = None
        else:
            values[name] = _coerce_nonnegative_number(
                value, label=f"measurement.{name}"
            )
    values["artifactTotalBytes"] = _coerce_nonnegative_number(
        artifacts.get("totalBytes"), label="artifactSizes.totalBytes"
    )
    return values


def _metric_delta(
    *,
    old_value: int | float | None,
    new_value: int | float | None,
    lower_is_better: bool,
) -> dict[str, Any]:
    if old_value is None or new_value is None:
        return {
            "old": old_value,
            "new": new_value,
            "delta": None,
            "ratio": None,
            "regressionRatio": None,
            "comparable": False,
        }
    delta = new_value - old_value
    if old_value == 0:
        ratio = None
        if new_value == 0:
            regression_ratio = 0.0
        else:
            regression_ratio = float("inf")
    else:
        ratio = new_value / old_value
        regression_ratio = (
            (new_value - old_value) / old_value
            if lower_is_better
            else (old_value - new_value) / old_value
        )
    return {
        "old": old_value,
        "new": new_value,
        "delta": delta,
        "ratio": ratio,
        "regressionRatio": (
            None if regression_ratio == float("inf") else regression_ratio
        ),
        "comparable": True,
    }


def evaluate_non_regression(
    *,
    metric_deltas: Mapping[str, Mapping[str, Any]],
    thresholds: Mapping[str, float],
) -> dict[str, Any]:
    """Evaluate explicit speed/resource gates without hiding missing metrics."""

    if not thresholds:
        return {"enabled": False, "passed": None, "checks": {}}
    checks: dict[str, Any] = {}
    for field, limit in sorted(thresholds.items()):
        metric = _THRESHOLD_METRICS[field]
        delta = metric_deltas[metric]
        regression_ratio = delta["regressionRatio"]
        if not delta["comparable"]:
            passed = False
            reason = "metric_unavailable"
        elif regression_ratio is None:
            passed = False
            reason = "zero_baseline_with_positive_regression"
        else:
            passed = float(regression_ratio) <= float(limit)
            reason = "within_limit" if passed else "regression_exceeds_limit"
        checks[field] = {
            "metric": metric,
            "maximumRegressionRatio": float(limit),
            "actualRegressionRatio": regression_ratio,
            "passed": passed,
            "reason": reason,
        }
    return {
        "enabled": True,
        "passed": all(bool(check["passed"]) for check in checks.values()),
        "checks": checks,
    }


def compare_runs(
    *,
    old_run: Mapping[str, Any],
    new_run: Mapping[str, Any],
    thresholds: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    """Produce directional deltas for one old/new phase comparison."""

    old_values = _metric_values(old_run)
    new_values = _metric_values(new_run)
    deltas: dict[str, Any] = {}
    for metric in sorted(old_values):
        deltas[metric] = _metric_delta(
            old_value=old_values[metric],
            new_value=new_values[metric],
            lower_is_better=metric != "minimumHostAvailableBytes",
        )
    selected_thresholds = dict(thresholds or {})
    return {
        "oldImplementation": old_run.get("implementation"),
        "newImplementation": new_run.get("implementation"),
        "deltas": deltas,
        "nonRegression": evaluate_non_regression(
            metric_deltas=deltas,
            thresholds=selected_thresholds,
        ),
    }


def _counterbalanced_execution_order(
    *, old_implementation: str, new_implementation: str, repetition_index: int
) -> tuple[str, str]:
    if repetition_index < 1:
        raise BenchmarkContractError("repetition index must begin at one")
    return (
        (old_implementation, new_implementation)
        if repetition_index % 2
        else (new_implementation, old_implementation)
    )


def _median_statistics(values: Sequence[int | float]) -> dict[str, Any]:
    if not values:
        return {
            "availableSampleCount": 0,
            "minimum": None,
            "median": None,
            "maximum": None,
        }
    return {
        "availableSampleCount": len(values),
        "minimum": min(values),
        "median": statistics.median(values),
        "maximum": max(values),
    }


def _aggregate_repetitions(
    *, implementation: str, runs: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    if not runs:
        raise BenchmarkContractError("cannot aggregate an empty repetition set")
    if any(run.get("implementation") != implementation for run in runs):
        raise BenchmarkContractError("repetition aggregate mixes implementation labels")
    metric_rows = [_metric_values(run) for run in runs]
    metric_statistics: dict[str, Any] = {}
    measurement: dict[str, Any] = {}
    for metric in sorted(metric_rows[0]):
        values = [row[metric] for row in metric_rows if row[metric] is not None]
        stats = _median_statistics(values)
        stats["missingSampleCount"] = len(metric_rows) - len(values)
        metric_statistics[metric] = stats
        if metric == "artifactTotalBytes":
            continue
        measurement[metric] = stats["median"]
    return {
        "implementation": implementation,
        "measurement": measurement,
        "artifactSizes": {"totalBytes": metric_statistics["artifactTotalBytes"]["median"]},
        "aggregation": {
            "method": "median",
            "sampleCount": len(runs),
            "metricStatistics": metric_statistics,
        },
    }


def run_benchmark(
    *,
    plan_path: Path | str,
    output_root: Path | str,
    shapes: Sequence[int] = DEFAULT_SHAPES,
    old_implementation: str = "python_legacy",
    new_implementation: str = "candidate",
    monitor_interval_seconds: float = 0.1,
    enforce_non_regression: bool = False,
    measurement_context: str = "concurrent_load_provisional",
    repetitions: int = DEFAULT_REPETITIONS,
) -> dict[str, Any]:
    """Run counterbalanced fresh-process A/B phase measurements."""

    if old_implementation == new_implementation:
        raise BenchmarkContractError("old and new implementations must differ")
    if monitor_interval_seconds <= 0:
        raise BenchmarkContractError("monitor interval must be positive")
    _require_nonempty_string(measurement_context, label="measurement context")
    if isinstance(repetitions, bool) or not isinstance(repetitions, int) or repetitions < 1:
        raise BenchmarkContractError("repetitions must be a positive integer")
    normalized_shapes = tuple(int(shape) for shape in shapes)
    if not normalized_shapes or any(shape < 1 for shape in normalized_shapes):
        raise BenchmarkContractError("benchmark shapes must be positive")
    if len(set(normalized_shapes)) != len(normalized_shapes):
        raise BenchmarkContractError("benchmark shapes cannot contain duplicates")
    source_path = Path(plan_path).resolve()
    plan = validate_benchmark_plan(_read_json_object(source_path, label="benchmark plan"))
    plan_sha256 = _canonical_sha256(plan)
    implementations = _require_mapping(plan["implementations"], label="plan.implementations")
    for label in (old_implementation, new_implementation):
        if label not in implementations:
            raise BenchmarkContractError(f"selected implementation is not in the plan: {label}")
    fixtures = _require_mapping(plan["fixtures"], label="plan.fixtures")
    for shape in normalized_shapes:
        if str(shape) not in fixtures:
            raise BenchmarkContractError(f"plan has no fixture for requested shape {shape}")
    if enforce_non_regression and not plan.get("nonRegressionThresholds"):
        raise BenchmarkContractError(
            "--enforce-non-regression requires plan.nonRegressionThresholds"
        )
    root = _fresh_external_root(Path(output_root))
    runs: list[dict[str, Any]] = []
    completed_measurement_count = 0
    try:
        for shape in normalized_shapes:
            for phase in PHASES:
                phase_fixtures = _require_mapping(
                    fixtures[str(shape)], label=f"plan.fixtures.{shape}"
                )
                fixture = _require_mapping(
                    phase_fixtures[phase], label=f"plan.fixtures.{shape}.{phase}"
                )
                repetition_records: list[dict[str, Any]] = []
                old_repetitions: list[dict[str, Any]] = []
                new_repetitions: list[dict[str, Any]] = []
                for repetition_index in range(1, repetitions + 1):
                    execution_order = _counterbalanced_execution_order(
                        old_implementation=old_implementation,
                        new_implementation=new_implementation,
                        repetition_index=repetition_index,
                    )
                    results_by_implementation: dict[str, dict[str, Any]] = {}
                    for execution_ordinal, implementation in enumerate(
                        execution_order, start=1
                    ):
                        role = (
                            "old"
                            if implementation == old_implementation
                            else "new"
                        )
                        worker_root = (
                            root
                            / "workers"
                            / f"shape-{shape}"
                            / phase
                            / f"repetition-{repetition_index:04d}"
                            / (
                                f"{execution_ordinal:02d}-{role}-"
                                f"{_path_label(implementation)}"
                            )
                        )
                        results_by_implementation[implementation] = _run_isolated(
                            plan_path=source_path,
                            plan_sha256=plan_sha256,
                            worker_root=worker_root,
                            shape=shape,
                            phase=phase,
                            implementation=implementation,
                            monitor_interval_seconds=monitor_interval_seconds,
                            measurement_context=measurement_context,
                            fixture=fixture,
                        )
                        completed_measurement_count += 1
                    old_run = results_by_implementation[old_implementation]
                    new_run = results_by_implementation[new_implementation]
                    old_repetitions.append(old_run)
                    new_repetitions.append(new_run)
                    repetition_records.append(
                        {
                            "repetitionIndex": repetition_index,
                            "executionOrder": list(execution_order),
                            "old": old_run,
                            "new": new_run,
                            "comparison": compare_runs(
                                old_run=old_run,
                                new_run=new_run,
                            ),
                        }
                    )
                old_aggregate = _aggregate_repetitions(
                    implementation=old_implementation,
                    runs=old_repetitions,
                )
                new_aggregate = _aggregate_repetitions(
                    implementation=new_implementation,
                    runs=new_repetitions,
                )
                thresholds = _thresholds_for_phase(plan, phase)
                runs.append(
                    {
                        "shape": shape,
                        "phase": phase,
                        "repetitionCount": repetitions,
                        "repetitions": repetition_records,
                        "aggregate": {
                            "old": old_aggregate,
                            "new": new_aggregate,
                            "comparison": compare_runs(
                                old_run=old_aggregate,
                                new_run=new_aggregate,
                                thresholds=thresholds,
                            ),
                        },
                    }
                )
    except (BenchmarkContractError, BenchmarkExecutionError) as exc:
        _atomic_write_json(
            root / "benchmark-failure.json",
            {
                "schemaVersion": FAILURE_SCHEMA,
                "status": "failed",
                "planSha256": plan_sha256,
                "oldImplementation": old_implementation,
                "newImplementation": new_implementation,
                "measurementContext": measurement_context,
                "requestedShapes": list(normalized_shapes),
                "completedRunCount": len(runs),
                "completedMeasurementCount": completed_measurement_count,
                "errorType": type(exc).__name__,
                "message": str(exc),
            },
        )
        raise
    threshold_outcomes = [
        item["aggregate"]["comparison"]["nonRegression"] for item in runs
    ]
    enabled_outcomes = [outcome for outcome in threshold_outcomes if outcome["enabled"]]
    thresholds_passed = (
        all(bool(outcome["passed"]) for outcome in enabled_outcomes)
        if enabled_outcomes
        else None
    )
    fully_counterbalanced = repetitions >= 2 and repetitions % 2 == 0
    hard_gate_passed = (
        bool(thresholds_passed) and fully_counterbalanced
        if enabled_outcomes
        else None
    )
    report: dict[str, Any] = {
        "schemaVersion": REPORT_SCHEMA,
        "status": "completed",
        "planSha256": plan_sha256,
        "selectedShapes": list(normalized_shapes),
        "oldImplementation": old_implementation,
        "newImplementation": new_implementation,
        "measurementContext": measurement_context,
        "measurementIsolation": "one fresh Python worker/process tree per shape, phase, and implementation",
        "inputIsolation": "one verified input clone per worker before measurement",
        "comparisonOrder": "alternating old-new/new-old by deterministic repetition index",
        "repetitionCount": repetitions,
        "phaseOrder": list(PHASES),
        "runs": runs,
        "nonRegression": {
            "enforced": enforce_non_regression,
            "configured": bool(enabled_outcomes),
            "thresholdsPassed": thresholds_passed,
            "counterbalance": {
                "fullyCounterbalanced": fully_counterbalanced,
                "requiredRepetitions": 2,
                "reason": (
                    "even_repetition_count_with_both_execution_orders"
                    if fully_counterbalanced
                    else "hard_gates_require_an_even_repetition_count_of_at_least_two"
                ),
            },
            "observationalMetricsExcludedFromHardGates": list(
                OBSERVATIONAL_METRICS
            ),
            "passed": hard_gate_passed,
        },
    }
    report["benchmarkReportSha256"] = _canonical_sha256(report)
    _atomic_write_json(root / "benchmark-report.json", report)
    if enforce_non_regression and report["nonRegression"]["passed"] is not True:
        raise BenchmarkThresholdError(
            "non-regression thresholds failed; see benchmark-report.json"
        )
    return report


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--shapes", type=_parse_shapes, default=DEFAULT_SHAPES)
    parser.add_argument("--old-implementation", default="python_legacy")
    parser.add_argument("--new-implementation", default="candidate")
    parser.add_argument("--monitor-interval-seconds", type=float, default=0.1)
    parser.add_argument("--repetitions", type=int, default=DEFAULT_REPETITIONS)
    parser.add_argument("--enforce-non-regression", action="store_true")
    parser.add_argument(
        "--measurement-context",
        default="concurrent_load_provisional",
        help=(
            "provenance label such as concurrent_load_provisional or "
            "clean_admission; this is recorded in every report"
        ),
    )
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--expected-plan-sha256", help=argparse.SUPPRESS)
    parser.add_argument("--worker-root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--shape", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--phase", choices=PHASES, help=argparse.SUPPRESS)
    parser.add_argument("--implementation", help=argparse.SUPPRESS)
    parser.add_argument("--input-root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--input-clone-manifest", type=Path, help=argparse.SUPPRESS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.worker:
        required = {
            "expected plan SHA-256": args.expected_plan_sha256,
            "worker root": args.worker_root,
            "shape": args.shape,
            "phase": args.phase,
            "implementation": args.implementation,
            "input root": args.input_root,
            "input clone manifest": args.input_clone_manifest,
        }
        missing = [label for label, value in required.items() if value is None]
        if missing:
            raise SystemExit("--worker requires " + ", ".join(missing))
        worker_root = Path(args.worker_root)
        try:
            report = _run_phase_worker(
                plan_path=Path(args.plan),
                expected_plan_sha256=str(args.expected_plan_sha256),
                worker_root=worker_root,
                shape=int(args.shape),
                phase=str(args.phase),
                implementation=str(args.implementation),
                monitor_interval_seconds=float(args.monitor_interval_seconds),
                measurement_context=str(args.measurement_context),
                input_root=Path(args.input_root),
                input_clone_manifest_path=Path(args.input_clone_manifest),
            )
        except (BenchmarkContractError, BenchmarkExecutionError) as exc:
            _worker_failure(worker_root, exc)
            print(
                json.dumps(
                    {
                        "schemaVersion": FAILURE_SCHEMA,
                        "status": "failed",
                        "errorType": type(exc).__name__,
                        "message": str(exc),
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
            return 2
        print(
            json.dumps(
                {
                    "schemaVersion": WORKER_SCHEMA,
                    "status": "completed",
                    "shape": report["shape"],
                    "phase": report["phase"],
                    "implementation": report["implementation"],
                },
                sort_keys=True,
            )
        )
        return 0
    if args.output_root is None:
        raise SystemExit("--output-root is required outside --worker mode")
    try:
        report = run_benchmark(
            plan_path=args.plan,
            output_root=args.output_root,
            shapes=args.shapes,
            old_implementation=str(args.old_implementation),
            new_implementation=str(args.new_implementation),
            monitor_interval_seconds=float(args.monitor_interval_seconds),
            enforce_non_regression=bool(args.enforce_non_regression),
            measurement_context=str(args.measurement_context),
            repetitions=int(args.repetitions),
        )
    except (BenchmarkContractError, BenchmarkExecutionError, BenchmarkThresholdError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(report, indent=2, sort_keys=True, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
