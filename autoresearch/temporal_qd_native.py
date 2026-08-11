"""One-shot native-foundation bridge for the Temporal QD coordinator.

This module intentionally does not select, mutate, materialize, validate, or
evaluate a candidate.  It resolves one exact standalone executable, writes a
small identity-bound manifest, and checks the strict result from its isolated
foundation probe.  The bridge has no Python fallback when native execution is
requested.
"""

from __future__ import annotations

import hashlib
import json
import os
import signal
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import psutil

from .result_codec import canonical_json_bytes, fsync_directory, sha256
from .temporal_qd_observability import DEFAULT_MINIMUM_HOST_AVAILABLE_BYTES

NATIVE_CONTRACT_VERSION = "temporal_qd_native_foundation_v1"
NATIVE_VERSION_SCHEMA = "temporal_qd_native_version_v1"
NATIVE_MANIFEST_SCHEMA = "temporal_qd_native_manifest_v1"
NATIVE_RESULT_SCHEMA = "temporal_qd_native_result_v1"
NATIVE_AUTHORITY_SCHEMA = "temporal_qd_native_authority_v1"
NATIVE_OPERATION = "foundation_probe"
NATIVE_RESULT_FILENAME = "result.json"
PAIR_GENERATION_RUNTIME_SCHEMA = "temporal_qd_pair_generation_runtime_v1"
PAIR_GENERATION_RUNTIME_PYTHON = "python_optimized_v1"
PAIR_GENERATION_RUNTIME_RUST = "rust_native_v1"
PAIR_GENERATION_RUNTIME_DEFAULT = PAIR_GENERATION_RUNTIME_RUST
PAIR_GENERATION_RUNTIME_FALLBACK_POLICY = "forbidden"
G0_FINALIZATION_RUNTIME_SCHEMA = "temporal_qd_g0_finalization_runtime_v1"
G0_FINALIZATION_RUNTIME_RUST = "rust_g0_funnel_v1"
G0_FINALIZATION_RUNTIME_PYTHON_ORACLE = "python_g0_oracle_v1"
G0_FINALIZATION_RUNTIME_DEFAULT = G0_FINALIZATION_RUNTIME_RUST
G0_FINALIZATION_RUNTIME_FALLBACK_POLICY = "forbidden"
G0_FUNNEL_MANIFEST_SCHEMA = "temporal_qd_native_g0_funnel_manifest_v2"
G0_FUNNEL_RESULT_SCHEMA = "temporal_qd_native_g0_funnel_result_v2"
G0_FUNNEL_RECEIPT_SCHEMA = "temporal_qd_native_g0_funnel_receipt_v2"
G0_EXECUTION_AUTHORITY_SCHEMA = "temporal_qd_native_g0_execution_authority_v1"
G0_IDENTITY_LEDGER_BINDING_SCHEMA = "temporal_qd_native_g0_identity_ledger_binding_v1"
G0_FUNNEL_OPERATION = "finalize_g0"
G0_FUNNEL_RESULT_FILENAME = "g0-funnel-result.json"
G0_ADMISSION_THREAD_CAP_ENV = "TEMPORAL_QD_G0_ADMISSION_THREAD_CAP"
G0_ADMISSION_THREAD_CAP_DEFAULT = 8
G0_ADMISSION_THREAD_CAP_MAXIMUM = 8
G0_DIAGNOSTICS_SCHEMA = "temporal_qd_native_g0_diagnostics_v1"
_G0_DIAGNOSTICS_PREFIX = "TEMPORAL_QD_G0_DIAGNOSTICS "
G0_LEGACY_V5_RUNTIME_MIGRATION_SCHEMA = (
    "temporal_qd_legacy_v5_g0_runtime_migration_v1"
)
G0_LEGACY_V5_RUNTIME_MIGRATION_PATH = (
    Path("internal") / "g0-funnel" / "legacy-v5-runtime-migration.json"
)
# The preserved 4,000 -> 1,024 v5 checkpoint predates the separately frozen
# G0 finalization runtime.  This is deliberately a singleton migration, not a
# compatibility mode for arbitrary configurations that omit that authority.
G0_LEGACY_V5_PRE_CUTOVER_CONFIG_SHA256 = (
    "sha256:ebe9055522e8e1399c3df0d0fa296a4842a4b4c6ea7a88e8b296ba34479d093d"
)
NATIVE_GENERATION_MANIFEST_SCHEMA = (
    "temporal_qd_native_generate_generation_manifest_v1"
)
NATIVE_GENERATION_RESULT_SCHEMA = "temporal_qd_native_generate_generation_result_v1"
NATIVE_GENERATION_OPERATION = "generate_generation"
NATIVE_GENERATION_RESULT_FILENAME = "generation-result.json"
RUNTIME_MANIFEST_SCHEMA = "temporal_qd_runtime_manifest_v1"
RUNTIME_AUTHORITY_SCHEMA = "temporal_qd_runtime_authority_v1"
PAIR_GENERATION_SCHEMA = "temporal_qd_pair_generation_v2"
PAIR_GENERATION_RESULT_SCHEMA = "temporal_qd_pair_generation_result_v1"
PAIR_GENERATION_PROGRESS_SCHEMA = "temporal_qd_front_generation_progress_v1"
NATIVE_BINARY_ENV = "FUZZFOLIO_TEMPORAL_QD_NATIVE_BINARY"
NATIVE_MINIMUM_HOST_AVAILABLE_BYTES_ENV = (
    "FUZZFOLIO_TEMPORAL_QD_MINIMUM_HOST_AVAILABLE_BYTES"
)
NATIVE_BINARY_NAME = "temporal-qd-batch"
_SHARING_RETRY_DELAYS_SECONDS = (0.005, 0.01, 0.02, 0.04, 0.08)
_NATIVE_CAPTURE_LIMIT_BYTES = 1024 * 1024
_NATIVE_RESOURCE_GUARD_POLL_SECONDS = 0.5
_NATIVE_OUTPUT_BASE_HEADROOM_BYTES = 4 * 1024**3
_NATIVE_OUTPUT_BYTES_PER_TARGET_CANDIDATE = 8 * 1024**2


class TemporalQDNativeError(RuntimeError):
    """The exact native foundation could not be resolved or verified."""


def _native_minimum_host_available_bytes() -> int:
    raw = os.environ.get(NATIVE_MINIMUM_HOST_AVAILABLE_BYTES_ENV)
    if raw is None:
        return DEFAULT_MINIMUM_HOST_AVAILABLE_BYTES
    if not raw or not raw.isascii() or not raw.isdecimal():
        raise TemporalQDNativeError(
            f"{NATIVE_MINIMUM_HOST_AVAILABLE_BYTES_ENV} must be a positive "
            "base-10 byte count"
        )
    value = int(raw)
    if value < 1:
        raise TemporalQDNativeError(
            f"{NATIVE_MINIMUM_HOST_AVAILABLE_BYTES_ENV} must be a positive "
            "base-10 byte count"
        )
    return value


def _assert_native_prelaunch_resources(
    *, output_root: Path, target_unique_candidates: int
) -> dict[str, int]:
    """Fail closed before native generation can pressure a shared workstation."""

    if (
        isinstance(target_unique_candidates, bool)
        or not isinstance(target_unique_candidates, int)
        or target_unique_candidates < 1
    ):
        raise TemporalQDNativeError(
            "native Temporal QD prelaunch target width is invalid"
        )
    minimum_host_available_bytes = _native_minimum_host_available_bytes()
    host_available_bytes = int(psutil.virtual_memory().available)
    output_free_bytes = int(shutil.disk_usage(output_root).free)
    required_output_free_bytes = (
        _NATIVE_OUTPUT_BASE_HEADROOM_BYTES
        + target_unique_candidates * _NATIVE_OUTPUT_BYTES_PER_TARGET_CANDIDATE
    )
    reasons: list[str] = []
    if host_available_bytes < minimum_host_available_bytes:
        reasons.append("minimum_host_available_breached")
    if output_free_bytes < required_output_free_bytes:
        reasons.append("minimum_output_volume_free_space_breached")
    if reasons:
        raise TemporalQDNativeError(
            "native Temporal QD prelaunch resource guard stopped the run: "
            f"{','.join(reasons)}; "
            f"hostAvailableBytes={host_available_bytes}; "
            f"minimumHostAvailableBytes={minimum_host_available_bytes}; "
            f"outputFreeBytes={output_free_bytes}; "
            f"requiredOutputFreeBytes={required_output_free_bytes}; "
            f"targetUniqueCandidates={target_unique_candidates}"
        )
    return {
        "hostAvailableBytes": host_available_bytes,
        "minimumHostAvailableBytes": minimum_host_available_bytes,
        "outputFreeBytes": output_free_bytes,
        "requiredOutputFreeBytes": required_output_free_bytes,
        "targetUniqueCandidates": target_unique_candidates,
    }


def build_pair_generation_runtime_config(
    *, engine: str, execution_timeout_seconds: int = 3600
) -> dict[str, Any]:
    value = {
        "schemaVersion": PAIR_GENERATION_RUNTIME_SCHEMA,
        "engine": engine,
        "fallbackPolicy": PAIR_GENERATION_RUNTIME_FALLBACK_POLICY,
        "executionTimeoutSeconds": execution_timeout_seconds,
    }
    value["runtimeSha256"] = sha256(canonical_json_bytes(value))
    return validate_pair_generation_runtime_config(value)


def validate_pair_generation_runtime_config(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "schemaVersion",
        "engine",
        "fallbackPolicy",
        "executionTimeoutSeconds",
        "runtimeSha256",
    }:
        raise TemporalQDNativeError(
            "pair generation runtime config fields are not exact"
        )
    if (
        value.get("schemaVersion") != PAIR_GENERATION_RUNTIME_SCHEMA
        or value.get("engine")
        not in {PAIR_GENERATION_RUNTIME_PYTHON, PAIR_GENERATION_RUNTIME_RUST}
        or value.get("fallbackPolicy")
        != PAIR_GENERATION_RUNTIME_FALLBACK_POLICY
        or isinstance(value.get("executionTimeoutSeconds"), bool)
        or not isinstance(value.get("executionTimeoutSeconds"), int)
        or value["executionTimeoutSeconds"] < 60
    ):
        raise TemporalQDNativeError("pair generation runtime config is incompatible")
    supplied = _validate_exact_sha256(
        value.get("runtimeSha256"), name="pair generation runtimeSha256"
    )
    material = {key: item for key, item in value.items() if key != "runtimeSha256"}
    if supplied != sha256(canonical_json_bytes(material)):
        raise TemporalQDNativeError("pair generation runtime config identity mismatch")
    return dict(value)


def build_g0_finalization_runtime_config(
    *,
    engine: str = G0_FINALIZATION_RUNTIME_DEFAULT,
    execution_timeout_seconds: int = 3600,
) -> dict[str, Any]:
    """Freeze the independent post-construction G0 authority.

    This is deliberately distinct from ``pairGenerationRuntime``.  v5
    proposal construction still needs the live Python evolvable factory and
    compiler, while a completed proposal journal is finalized by one native
    G0 transaction.
    """

    value = {
        "schemaVersion": G0_FINALIZATION_RUNTIME_SCHEMA,
        "engine": engine,
        "fallbackPolicy": G0_FINALIZATION_RUNTIME_FALLBACK_POLICY,
        "executionTimeoutSeconds": execution_timeout_seconds,
    }
    value["runtimeSha256"] = sha256(canonical_json_bytes(value))
    return validate_g0_finalization_runtime_config(value)


def validate_g0_finalization_runtime_config(value: object) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != {
        "schemaVersion",
        "engine",
        "fallbackPolicy",
        "executionTimeoutSeconds",
        "runtimeSha256",
    }:
        raise TemporalQDNativeError(
            "G0 finalization runtime config fields are not exact"
        )
    if (
        value.get("schemaVersion") != G0_FINALIZATION_RUNTIME_SCHEMA
        or value.get("engine")
        not in {
            G0_FINALIZATION_RUNTIME_RUST,
            G0_FINALIZATION_RUNTIME_PYTHON_ORACLE,
        }
        or value.get("fallbackPolicy")
        != G0_FINALIZATION_RUNTIME_FALLBACK_POLICY
        or isinstance(value.get("executionTimeoutSeconds"), bool)
        or not isinstance(value.get("executionTimeoutSeconds"), int)
        or value["executionTimeoutSeconds"] < 60
    ):
        raise TemporalQDNativeError("G0 finalization runtime config is incompatible")
    supplied = _validate_exact_sha256(
        value.get("runtimeSha256"), name="G0 finalization runtimeSha256"
    )
    material = {key: item for key, item in value.items() if key != "runtimeSha256"}
    if supplied != sha256(canonical_json_bytes(material)):
        raise TemporalQDNativeError("G0 finalization runtime config identity mismatch")
    return dict(value)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def native_workspace_root() -> Path:
    return _repo_root() / "rust" / "temporal-qd"


def native_workspace_manifest() -> Path:
    return native_workspace_root() / "Cargo.toml"


def native_target_root() -> Path:
    return native_workspace_root() / "target"


def native_batch_binary_path() -> Path:
    suffix = ".exe" if sys.platform.startswith("win") else ""
    return native_target_root() / "release" / f"{NATIVE_BINARY_NAME}{suffix}"


def _source_paths() -> tuple[Path, ...]:
    root = native_workspace_root()
    paths = [
        _repo_root() / "rust-toolchain.toml",
        root / "Cargo.toml",
        root / "Cargo.lock",
    ]
    for crate_manifest in sorted(root.glob("crates/*/Cargo.toml")):
        crate_root = crate_manifest.parent
        paths.append(crate_manifest)
        paths.extend(sorted((crate_root / "src").rglob("*.rs")))
    if any(not path.is_file() for path in paths):
        missing = next(path for path in paths if not path.is_file())
        raise TemporalQDNativeError(f"native Temporal QD source is incomplete: {missing}")
    return tuple(paths)


def native_source_sha256() -> str:
    root = _repo_root()
    digest = hashlib.sha256()
    for path in _source_paths():
        relative = path.relative_to(root).as_posix().encode("utf-8")
        payload = path.read_bytes()
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return "sha256:" + digest.hexdigest()


def _sha256_file(path: Path) -> str:
    _require_regular_file(path, name="native executable")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(4 * 1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


class _BoundedPipeCapture:
    """Drain one child pipe without trusting a failed child to stay small."""

    def __init__(self, stream: Any, *, limit_bytes: int) -> None:
        self._stream = stream
        self._limit_bytes = limit_bytes
        self._buffer = bytearray()
        self._overflowed = False
        self._error: BaseException | None = None
        self._thread = threading.Thread(target=self._drain, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def _drain(self) -> None:
        try:
            while chunk := self._stream.read(64 * 1024):
                remaining = self._limit_bytes - len(self._buffer)
                if remaining > 0:
                    self._buffer.extend(chunk[:remaining])
                if len(chunk) > remaining:
                    self._overflowed = True
        except BaseException as exc:
            self._error = exc
        finally:
            self._stream.close()

    def finish(self) -> bytes:
        self._thread.join()
        if self._error is not None:
            raise TemporalQDNativeError("could not drain native Temporal QD command output") from self._error
        return bytes(self._buffer)

    @property
    def overflowed(self) -> bool:
        """Whether the child wrote beyond the bounded retained capture."""

        return self._overflowed


class _WindowsKillOnCloseJob:
    """Own a Windows process tree and kill it when this handle is closed.

    A Rust batch maintains a long-lived Dashboard JSONL child. ``Popen.kill``
    only addresses the batch process, so a per-invocation job object is the
    ownership boundary on Windows. Children created after assignment join the
    job automatically; closing it is therefore also the normal leak check.
    """

    _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9
    _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000

    def __init__(self, process: subprocess.Popen[bytes]) -> None:
        import ctypes
        from ctypes import wintypes

        class _IoCounters(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        class _BasicLimitInformation(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class _ExtendedLimitInformation(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", _BasicLimitInformation),
                ("IoInfo", _IoCounters),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        create = kernel32.CreateJobObjectW
        create.argtypes = (ctypes.c_void_p, wintypes.LPCWSTR)
        create.restype = wintypes.HANDLE
        configure = kernel32.SetInformationJobObject
        configure.argtypes = (
            wintypes.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            wintypes.DWORD,
        )
        configure.restype = wintypes.BOOL
        assign = kernel32.AssignProcessToJobObject
        assign.argtypes = (wintypes.HANDLE, wintypes.HANDLE)
        assign.restype = wintypes.BOOL
        self._close = kernel32.CloseHandle
        self._close.argtypes = (wintypes.HANDLE,)
        self._close.restype = wintypes.BOOL
        self._handle = create(None, None)
        if not self._handle:
            raise OSError(ctypes.get_last_error(), "CreateJobObjectW failed")
        try:
            limits = _ExtendedLimitInformation()
            limits.BasicLimitInformation.LimitFlags = (
                self._JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            )
            if not configure(
                self._handle,
                self._JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
                ctypes.byref(limits),
                ctypes.sizeof(limits),
            ):
                raise OSError(ctypes.get_last_error(), "SetInformationJobObject failed")
            if not assign(self._handle, wintypes.HANDLE(process._handle)):
                raise OSError(ctypes.get_last_error(), "AssignProcessToJobObject failed")
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        if self._handle:
            self._close(self._handle)
            self._handle = None


_WINDOWS_CREATE_SUSPENDED = 0x00000004
_WINDOWS_THREAD_SUSPEND_RESUME = 0x0002
_WINDOWS_TH32CS_SNAPTHREAD = 0x00000004
_WINDOWS_ERROR_NO_MORE_FILES = 18
_WINDOWS_INVALID_DWORD = 0xFFFFFFFF


def _resume_windows_suspended_process(process: subprocess.Popen[bytes]) -> None:
    """Resume exactly the primary thread we deliberately created suspended.

    ``subprocess.Popen`` retains the process handle but closes the initial
    thread handle before returning.  A ToolHelp snapshot lets us reopen that
    one suspended primary thread after the process tree has been assigned to
    its kill-on-close Job Object.  Requiring exactly one owned thread keeps an
    unexpected process state fail-closed rather than resuming an unbound tree.
    """

    if os.name != "nt":  # pragma: no cover - defensive call-site guard
        raise TemporalQDNativeError("Windows suspended-process resume was requested off Windows")

    import ctypes
    from ctypes import wintypes

    class _ThreadEntry32(ctypes.Structure):
        _fields_ = [
            ("dwSize", wintypes.DWORD),
            ("cntUsage", wintypes.DWORD),
            ("th32ThreadID", wintypes.DWORD),
            ("th32OwnerProcessID", wintypes.DWORD),
            ("tpBasePri", wintypes.LONG),
            ("tpDeltaPri", wintypes.LONG),
            ("dwFlags", wintypes.DWORD),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    snapshot_threads = kernel32.CreateToolhelp32Snapshot
    snapshot_threads.argtypes = (wintypes.DWORD, wintypes.DWORD)
    snapshot_threads.restype = wintypes.HANDLE
    first_thread = kernel32.Thread32First
    first_thread.argtypes = (wintypes.HANDLE, ctypes.POINTER(_ThreadEntry32))
    first_thread.restype = wintypes.BOOL
    next_thread = kernel32.Thread32Next
    next_thread.argtypes = (wintypes.HANDLE, ctypes.POINTER(_ThreadEntry32))
    next_thread.restype = wintypes.BOOL
    open_thread = kernel32.OpenThread
    open_thread.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    open_thread.restype = wintypes.HANDLE
    resume_thread = kernel32.ResumeThread
    resume_thread.argtypes = (wintypes.HANDLE,)
    resume_thread.restype = wintypes.DWORD
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = (wintypes.HANDLE,)
    close_handle.restype = wintypes.BOOL

    invalid_handle = ctypes.c_void_p(-1).value
    snapshot = snapshot_threads(_WINDOWS_TH32CS_SNAPTHREAD, 0)
    if snapshot in (None, invalid_handle):
        raise OSError(ctypes.get_last_error(), "CreateToolhelp32Snapshot failed")
    try:
        entry = _ThreadEntry32()
        entry.dwSize = ctypes.sizeof(entry)
        if not first_thread(snapshot, ctypes.byref(entry)):
            raise OSError(ctypes.get_last_error(), "Thread32First failed")
        owned_threads: list[int] = []
        while True:
            if entry.th32OwnerProcessID == process.pid:
                owned_threads.append(int(entry.th32ThreadID))
            entry.dwSize = ctypes.sizeof(entry)
            if next_thread(snapshot, ctypes.byref(entry)):
                continue
            error = ctypes.get_last_error()
            if error != _WINDOWS_ERROR_NO_MORE_FILES:
                raise OSError(error, "Thread32Next failed")
            break
        if len(owned_threads) != 1:
            raise OSError(
                0,
                "suspended native Temporal QD child did not expose exactly one primary thread",
            )
        thread = open_thread(_WINDOWS_THREAD_SUSPEND_RESUME, False, owned_threads[0])
        if not thread:
            raise OSError(ctypes.get_last_error(), "OpenThread failed")
        try:
            previous_suspend_count = resume_thread(thread)
            if previous_suspend_count != 1:
                error = ctypes.get_last_error() if previous_suspend_count == _WINDOWS_INVALID_DWORD else 0
                raise OSError(
                    error,
                    "ResumeThread did not release the expected primary-thread suspension",
                )
        finally:
            close_handle(thread)
    finally:
        close_handle(snapshot)


def _terminate_process_group(process: subprocess.Popen[bytes]) -> None:
    """Best-effort terminate of the process tree which this invocation owns."""

    if os.name == "nt":
        # The job object handles a living parent and descendants. ``taskkill``
        # is a bounded second defense for a process created in the tiny window
        # before Windows accepted the job assignment.
        try:
            subprocess.run(
                ("taskkill", "/PID", str(process.pid), "/T", "/F"),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=10,
            )
        except (OSError, subprocess.TimeoutExpired):
            pass
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except OSError:
        # Do not mask the original native error/interruption with a cleanup
        # race after the process group has naturally exited.
        pass


def _run_checked(
    command: Sequence[str],
    *,
    cwd: Path,
    timeout: float = 300.0,
    env: Mapping[str, str] | None = None,
    raise_on_nonzero: bool = True,
    on_process_started: Callable[[subprocess.Popen[bytes]], None] | None = None,
    stdout_limit_bytes: int = _NATIVE_CAPTURE_LIMIT_BYTES,
    stderr_limit_bytes: int = _NATIVE_CAPTURE_LIMIT_BYTES,
    minimum_host_available_bytes: int | None = None,
) -> subprocess.CompletedProcess[bytes]:
    """Run one native command with complete child-tree ownership.

    The successful command/result protocol remains byte-for-byte unchanged.
    On every exceptional path, including caller interruption, all descendants
    are terminated before the exception escapes.
    """

    if (
        isinstance(stdout_limit_bytes, bool)
        or not isinstance(stdout_limit_bytes, int)
        or stdout_limit_bytes < 1
        or isinstance(stderr_limit_bytes, bool)
        or not isinstance(stderr_limit_bytes, int)
        or stderr_limit_bytes < 1
    ):
        raise TemporalQDNativeError("native command capture limits must be positive integers")
    if minimum_host_available_bytes is None:
        minimum_host_available_bytes = _native_minimum_host_available_bytes()
    if (
        isinstance(minimum_host_available_bytes, bool)
        or not isinstance(minimum_host_available_bytes, int)
        or minimum_host_available_bytes < 1
    ):
        raise TemporalQDNativeError(
            "native command minimum host-available byte threshold must be a positive integer"
        )
    command_strings = [str(part) for part in command]
    popen_options: dict[str, Any] = {
        "cwd": cwd,
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.PIPE,
    }
    if env is not None:
        popen_options["env"] = dict(env)
    if os.name == "nt":
        # A fast `--version-json` child previously could exit in the small
        # gap between Popen and AssignProcessToJobObject.  Keep its primary
        # thread suspended until the Job Object owns the complete process
        # tree, then explicitly resume it after both output drainers exist.
        popen_options["creationflags"] = (
            subprocess.CREATE_NEW_PROCESS_GROUP | _WINDOWS_CREATE_SUSPENDED
        )
    else:
        popen_options["start_new_session"] = True
    process = subprocess.Popen(command_strings, **popen_options)
    job: _WindowsKillOnCloseJob | None = None
    stdout_capture: _BoundedPipeCapture | None = None
    stderr_capture: _BoundedPipeCapture | None = None
    try:
        if os.name == "nt":
            try:
                job = _WindowsKillOnCloseJob(process)
            except (AttributeError, OSError) as exc:
                raise TemporalQDNativeError(
                    "could not bind native Temporal QD command to a Windows job object"
                ) from exc
        assert process.stdout is not None and process.stderr is not None
        stdout_capture = _BoundedPipeCapture(
            process.stdout, limit_bytes=stdout_limit_bytes
        )
        stderr_capture = _BoundedPipeCapture(
            process.stderr, limit_bytes=stderr_limit_bytes
        )
        stdout_capture.start()
        stderr_capture.start()
        if on_process_started is not None:
            try:
                # Start any external observer only once Windows has assigned
                # the process to its kill-on-close Job Object.  The callback
                # therefore never sees an uncontained long-lived child, and
                # on Windows can sample the process while it is still safely
                # suspended before its primary thread resumes.
                on_process_started(process)
            except BaseException as exc:
                raise TemporalQDNativeError(
                    "native Temporal QD process observer could not start"
                ) from exc
        if os.name == "nt":
            try:
                _resume_windows_suspended_process(process)
            except (AttributeError, OSError, TemporalQDNativeError) as exc:
                raise TemporalQDNativeError(
                    "could not resume native Temporal QD command after Windows job assignment"
                ) from exc
        try:
            deadline = time.monotonic() + timeout
            while True:
                host_available_bytes = int(psutil.virtual_memory().available)
                if host_available_bytes < minimum_host_available_bytes:
                    _terminate_process_group(process)
                    if job is not None:
                        job.close()
                    process.wait()
                    raise TemporalQDNativeError(
                        "native Temporal QD host resource guard stopped the run: "
                        "minimum_host_available_breached; "
                        f"hostAvailableBytes={host_available_bytes}; "
                        f"minimumHostAvailableBytes={minimum_host_available_bytes}"
                    )
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    _terminate_process_group(process)
                    if job is not None:
                        job.close()
                    process.wait()
                    raise TemporalQDNativeError(
                        f"native Temporal QD command exceeded its frozen {timeout:g}s timeout"
                    )
                try:
                    returncode = process.wait(
                        timeout=min(_NATIVE_RESOURCE_GUARD_POLL_SECONDS, remaining)
                    )
                    break
                except subprocess.TimeoutExpired:
                    continue
        finally:
            # The main process can exit while a retained descendant still has
            # either output pipe open. Close/kill the owned tree before joining
            # drainers, otherwise a corrupt child can make this call hang.
            if job is not None:
                job.close()
            _terminate_process_group(process)
        stdout = stdout_capture.finish()
        stderr = stderr_capture.finish()
        if stdout_capture.overflowed:
            raise TemporalQDNativeError(
                "native Temporal QD command stdout exceeded its "
                f"{stdout_limit_bytes} byte capture limit"
            )
        if stderr_capture.overflowed:
            raise TemporalQDNativeError(
                "native Temporal QD command stderr exceeded its "
                f"{stderr_limit_bytes} byte capture limit"
            )
        completed = subprocess.CompletedProcess(
            command_strings, returncode, stdout=stdout, stderr=stderr
        )
    except BaseException:
        if job is not None:
            job.close()
        _terminate_process_group(process)
        try:
            process.wait(timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            pass
        for capture in (stdout_capture, stderr_capture):
            if capture is not None:
                capture.finish()
        raise
    if raise_on_nonzero and completed.returncode != 0:
        detail_bytes = completed.stderr.strip() or completed.stdout.strip()
        detail = detail_bytes.decode("utf-8", errors="replace")
        raise TemporalQDNativeError(
            f"native Temporal QD command failed ({completed.returncode}): {detail}"
        )
    return completed


def resolve_native_batch_binary() -> Path | None:
    """Return a configured prebuilt binary without building or falling back."""

    override = os.environ.get(NATIVE_BINARY_ENV)
    if not override:
        return None
    binary = Path(override).expanduser().resolve()
    _require_regular_file(binary, name=NATIVE_BINARY_ENV)
    return binary


def _validate_exact_sha256(value: object, *, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 71
        or not value.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise TemporalQDNativeError(f"{name} must be a lowercase sha256 identity")
    return value


def validate_native_version(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise TemporalQDNativeError("native version response must be an object")
    expected = {"schemaVersion", "contractVersion", "crateVersion", "binaryName"}
    if set(value) != expected:
        raise TemporalQDNativeError("native version response fields are not exact")
    result = {key: value[key] for key in expected}
    if (
        result["schemaVersion"] != NATIVE_VERSION_SCHEMA
        or result["contractVersion"] != NATIVE_CONTRACT_VERSION
        or result["binaryName"] != NATIVE_BINARY_NAME
        or not isinstance(result["crateVersion"], str)
        or not result["crateVersion"].strip()
    ):
        raise TemporalQDNativeError("native version response is incompatible")
    return {key: str(item) for key, item in result.items()}


def _native_version(binary: Path) -> dict[str, str]:
    completed = _run_checked((str(binary), "--version-json"), cwd=_repo_root(), timeout=30)
    return validate_native_version(
        _parse_canonical_json_line(completed.stdout, name="native version response")
    )


def build_native_authority(
    *,
    binary: Path,
    version: Mapping[str, str],
    source_sha256: str | None = None,
    executable_sha256: str | None = None,
) -> dict[str, str]:
    """Bind the exact source and executable used by one output root."""

    checked_version = validate_native_version(version)
    executable_identity = (
        _sha256_file(binary) if executable_sha256 is None else executable_sha256
    )
    source_identity = native_source_sha256() if source_sha256 is None else source_sha256
    authority: dict[str, str] = {
        "schemaVersion": NATIVE_AUTHORITY_SCHEMA,
        "contractVersion": NATIVE_CONTRACT_VERSION,
        "crateVersion": checked_version["crateVersion"],
        "binaryName": checked_version["binaryName"],
        "buildProfile": "release",
        "executableSha256": executable_identity,
        "sourceSha256": source_identity,
    }
    _validate_exact_sha256(authority["executableSha256"], name="executableSha256")
    _validate_exact_sha256(authority["sourceSha256"], name="sourceSha256")
    authority["authoritySha256"] = sha256(canonical_json_bytes(authority))
    return authority


def validate_native_authority(value: object) -> dict[str, str]:
    """Validate the closed authority of the batch executable itself.

    G0 finalization is intentionally a different runtime from Python proposal
    construction, but it is still executed by ``temporal-qd-batch``.  Keeping
    the complete executable/source authority here prevents a receipt from
    being adopted after a different batch binary has been built in place.
    """

    expected = {
        "schemaVersion",
        "contractVersion",
        "crateVersion",
        "binaryName",
        "buildProfile",
        "executableSha256",
        "sourceSha256",
        "authoritySha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise TemporalQDNativeError("native authority fields are not exact")
    result = {key: value[key] for key in expected}
    if (
        result["schemaVersion"] != NATIVE_AUTHORITY_SCHEMA
        or result["contractVersion"] != NATIVE_CONTRACT_VERSION
        or result["binaryName"] != NATIVE_BINARY_NAME
        or result["buildProfile"] != "release"
        or not isinstance(result["crateVersion"], str)
        or not result["crateVersion"].strip()
    ):
        raise TemporalQDNativeError("native authority is incompatible")
    for key in ("executableSha256", "sourceSha256", "authoritySha256"):
        _validate_exact_sha256(result[key], name=f"native authority {key}")
    material = {key: item for key, item in result.items() if key != "authoritySha256"}
    if sha256(canonical_json_bytes(material)) != result["authoritySha256"]:
        raise TemporalQDNativeError("native authority identity mismatch")
    return {key: str(item) for key, item in result.items()}


def _build_g0_execution_authority(
    *,
    g0_finalization_runtime: Mapping[str, Any],
    native_batch_authority: Mapping[str, Any],
) -> dict[str, Any]:
    """Bind the distinct G0 runtime to the concrete batch executable/source."""

    runtime = validate_g0_finalization_runtime_config(g0_finalization_runtime)
    batch = validate_native_authority(native_batch_authority)
    value: dict[str, Any] = {
        "schemaVersion": G0_EXECUTION_AUTHORITY_SCHEMA,
        "g0FinalizationRuntimeSha256": runtime["runtimeSha256"],
        "nativeBatchAuthority": batch,
        "nativeBatchAuthoritySha256": batch["authoritySha256"],
    }
    value["authoritySha256"] = sha256(canonical_json_bytes(value))
    return _validate_g0_execution_authority(value)


def _validate_g0_execution_authority(value: object) -> dict[str, Any]:
    expected = {
        "schemaVersion",
        "g0FinalizationRuntimeSha256",
        "nativeBatchAuthority",
        "nativeBatchAuthoritySha256",
        "authoritySha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise TemporalQDNativeError("native G0 execution authority fields are not exact")
    result = dict(value)
    if result["schemaVersion"] != G0_EXECUTION_AUTHORITY_SCHEMA:
        raise TemporalQDNativeError("native G0 execution authority is incompatible")
    for key in (
        "g0FinalizationRuntimeSha256",
        "nativeBatchAuthoritySha256",
        "authoritySha256",
    ):
        _validate_exact_sha256(result[key], name=f"native G0 execution authority {key}")
    batch = validate_native_authority(result["nativeBatchAuthority"])
    if result["nativeBatchAuthoritySha256"] != batch["authoritySha256"]:
        raise TemporalQDNativeError("native G0 execution authority batch binding drifted")
    material = {key: item for key, item in result.items() if key != "authoritySha256"}
    if sha256(canonical_json_bytes(material)) != result["authoritySha256"]:
        raise TemporalQDNativeError("native G0 execution authority identity mismatch")
    result["nativeBatchAuthority"] = batch
    return result


def ensure_native_batch() -> tuple[Path, dict[str, str]]:
    """Build or resolve the exact release binary; never select Python fallback."""

    source_before = native_source_sha256()
    binary = resolve_native_batch_binary()
    if binary is None:
        manifest = native_workspace_manifest()
        if not manifest.is_file():
            raise TemporalQDNativeError(f"native Temporal QD manifest is absent: {manifest}")
        environment = dict(os.environ)
        environment["CARGO_BUILD_JOBS"] = "2"
        command = (
            "cargo",
            "build",
            "--quiet",
            "--locked",
            "--release",
            "--jobs",
            "2",
            "--manifest-path",
            str(manifest),
            "--target-dir",
            str(native_target_root()),
            "-p",
            "temporal-qd-batch",
        )
        completed = _run_checked(
            command,
            cwd=_repo_root(),
            timeout=600,
            env=environment,
            raise_on_nonzero=False,
        )
        if completed.returncode != 0:
            detail = (
                completed.stderr.strip() or completed.stdout.strip()
            ).decode("utf-8", errors="replace")
            raise TemporalQDNativeError(
                f"failed to build native Temporal QD batch binary: {detail}"
            )
        binary = native_batch_binary_path().resolve()
        if not binary.is_file():
            raise TemporalQDNativeError(
                f"native Temporal QD build did not produce {binary}"
            )
        source_after = native_source_sha256()
        if source_after != source_before:
            raise TemporalQDNativeError(
                "native Temporal QD source changed during the locked binary build"
            )
    else:
        source_after = source_before
    executable_before = _sha256_file(binary)
    version = _native_version(binary)
    executable_after = _sha256_file(binary)
    if executable_after != executable_before:
        raise TemporalQDNativeError(
            "native Temporal QD executable changed during the version handshake"
        )
    return binary, build_native_authority(
        binary=binary,
        version=version,
        source_sha256=source_after,
        executable_sha256=executable_after,
    )


def require_native_batch() -> tuple[Path, dict[str, str]]:
    """Resolve an already-built batch binary without compiling one.

    Fresh v5 production transactions are not allowed to turn a missing Rust
    authority into a hidden Cargo build.  That would make the executable used
    for a receipt depend on ambient source state at launch time and would
    defeat the caller's fail-closed runtime-authority gate.  Historical v4
    callers retain :func:`ensure_native_batch`; the v5 bridge uses this
    resolver exclusively.
    """

    source_before = native_source_sha256()
    binary = resolve_native_batch_binary()
    if binary is None:
        binary = native_batch_binary_path()
        _require_regular_file(binary, name="native Temporal QD batch binary")
    binary = binary.absolute()
    executable_before = _sha256_file(binary)
    version = _native_version(binary)
    executable_after = _sha256_file(binary)
    if executable_after != executable_before:
        raise TemporalQDNativeError(
            "native Temporal QD executable changed during the version handshake"
        )
    source_after = native_source_sha256()
    if source_after != source_before:
        raise TemporalQDNativeError(
            "native Temporal QD source changed during the version handshake"
        )
    return binary, build_native_authority(
        binary=binary,
        version=version,
        source_sha256=source_after,
        executable_sha256=executable_after,
    )


def build_foundation_manifest(*, authority_sha256: str) -> dict[str, str]:
    _validate_exact_sha256(authority_sha256, name="authoritySha256")
    manifest: dict[str, str] = {
        "schemaVersion": NATIVE_MANIFEST_SCHEMA,
        "contractVersion": NATIVE_CONTRACT_VERSION,
        "operation": NATIVE_OPERATION,
        "authoritySha256": authority_sha256,
        "resultPath": NATIVE_RESULT_FILENAME,
    }
    manifest["manifestSha256"] = sha256(canonical_json_bytes(manifest))
    return manifest


def validate_foundation_manifest(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise TemporalQDNativeError("native foundation manifest must be an object")
    expected = {
        "schemaVersion",
        "contractVersion",
        "operation",
        "authoritySha256",
        "resultPath",
        "manifestSha256",
    }
    if set(value) != expected:
        raise TemporalQDNativeError("native foundation manifest fields are not exact")
    result = {key: value[key] for key in expected}
    if (
        result["schemaVersion"] != NATIVE_MANIFEST_SCHEMA
        or result["contractVersion"] != NATIVE_CONTRACT_VERSION
        or result["operation"] != NATIVE_OPERATION
        or result["resultPath"] != NATIVE_RESULT_FILENAME
    ):
        raise TemporalQDNativeError("native foundation manifest is incompatible")
    authority_sha = _validate_exact_sha256(result["authoritySha256"], name="manifest authoritySha256")
    manifest_sha = _validate_exact_sha256(result["manifestSha256"], name="manifest manifestSha256")
    body = {key: result[key] for key in expected - {"manifestSha256"}}
    expected_sha = sha256(canonical_json_bytes(body))
    if manifest_sha != expected_sha:
        raise TemporalQDNativeError("native foundation manifest identity mismatch")
    return {
        "schemaVersion": NATIVE_MANIFEST_SCHEMA,
        "contractVersion": NATIVE_CONTRACT_VERSION,
        "operation": NATIVE_OPERATION,
        "authoritySha256": authority_sha,
        "resultPath": NATIVE_RESULT_FILENAME,
        "manifestSha256": manifest_sha,
    }


def validate_foundation_result(
    value: object,
    *,
    manifest: Mapping[str, str],
) -> dict[str, str]:
    checked_manifest = validate_foundation_manifest(manifest)
    if not isinstance(value, Mapping):
        raise TemporalQDNativeError("native foundation result must be an object")
    expected = {
        "schemaVersion",
        "contractVersion",
        "operation",
        "authoritySha256",
        "manifestSha256",
        "status",
    }
    if set(value) != expected:
        raise TemporalQDNativeError("native foundation result fields are not exact")
    result = {key: value[key] for key in expected}
    if (
        result["schemaVersion"] != NATIVE_RESULT_SCHEMA
        or result["contractVersion"] != NATIVE_CONTRACT_VERSION
        or result["operation"] != NATIVE_OPERATION
        or result["status"] != "completed"
        or result["authoritySha256"] != checked_manifest["authoritySha256"]
        or result["manifestSha256"] != checked_manifest["manifestSha256"]
    ):
        raise TemporalQDNativeError("native foundation result is incompatible")
    _validate_exact_sha256(result["authoritySha256"], name="result authoritySha256")
    _validate_exact_sha256(result["manifestSha256"], name="result manifestSha256")
    return {key: str(item) for key, item in result.items()}


def build_generation_runtime_authority(
    *,
    pair_run_config: Mapping[str, Any],
    pair_policy: Mapping[str, Any],
    evidence_identity_context: Mapping[str, Any] | None,
    generation_config: Mapping[str, Any],
) -> dict[str, Any]:
    config_sha = _validate_exact_sha256(
        generation_config.get("configSha256"), name="generation configSha256"
    )
    if generation_config.get("schemaVersion") != PAIR_GENERATION_SCHEMA:
        raise TemporalQDNativeError("pair generation config schema is incompatible")
    pair_run_config_sha = _validate_exact_sha256(
        pair_run_config.get("pairRunConfigSha256"),
        name="pair run config pairRunConfigSha256",
    )
    pair_run_material = {
        key: item
        for key, item in pair_run_config.items()
        if key != "pairRunConfigSha256"
    }
    if sha256(canonical_json_bytes(pair_run_material)) != pair_run_config_sha:
        raise TemporalQDNativeError("pair run config self-hash mismatch")
    evidence_sha = None
    if evidence_identity_context is not None:
        evidence_sha = _validate_exact_sha256(
            evidence_identity_context.get("predeclaredEvidenceContextSha256"),
            name="evidence context predeclaredEvidenceContextSha256",
        )
        evidence_material = {
            key: item
            for key, item in evidence_identity_context.items()
            if key != "predeclaredEvidenceContextSha256"
        }
        if sha256(canonical_json_bytes(evidence_material)) != evidence_sha:
            raise TemporalQDNativeError("evidence identity context self-hash mismatch")
    runtime = {
        "schemaVersion": RUNTIME_AUTHORITY_SCHEMA,
        "pairRunConfig": dict(pair_run_config),
        "pairRunConfigSha256": pair_run_config_sha,
        "bidirectionalPairPolicy": dict(pair_policy),
        "bidirectionalPairPolicySha256": sha256(canonical_json_bytes(pair_policy)),
        "evidenceIdentityContext": (
            dict(evidence_identity_context)
            if evidence_identity_context is not None
            else None
        ),
        "evidenceIdentityContextSha256": evidence_sha,
        "generationIndex": generation_config.get("generationIndex"),
        "pairGenerationConfigSha256": config_sha,
    }
    runtime["runtimeAuthoritySha256"] = sha256(canonical_json_bytes(runtime))
    return runtime


def build_generation_manifest(
    *,
    authority_sha256: str,
    runtime_authority: Mapping[str, Any],
    parent_archive_path: Path | str,
    parent_archive_sha256: str,
    identity_ledger_path: Path | str,
    identity_ledger_sha256: str,
    output_root: Path | str,
    generation_config: Mapping[str, Any],
    max_new_proposals: int | None,
    native_execution_timeout_seconds: int,
    allow_empty_quality_bootstrap: bool,
    g0_evaluation_width: int | None,
    frozen_construction_catalog: Mapping[str, Any] | None,
    qd_version: str,
    policy_name: str,
    policy_sha256: str,
    frozen_policy: Mapping[str, Any],
) -> dict[str, Any]:
    config = dict(generation_config)
    config_sha = _validate_exact_sha256(
        config.get("configSha256"), name="generation configSha256"
    )
    config_material = {key: item for key, item in config.items() if key != "configSha256"}
    if sha256(canonical_json_bytes(config_material)) != config_sha:
        raise TemporalQDNativeError("generation config self-hash mismatch")
    native_authority = (
        runtime_authority.get("pairRunConfig", {}).get("nativeAuthority")
        if isinstance(runtime_authority.get("pairRunConfig"), Mapping)
        else None
    )
    if not isinstance(native_authority, Mapping):
        raise TemporalQDNativeError("runtime pair config lacks native authority")
    evidence_context = runtime_authority.get("evidenceIdentityContext")
    evidence_sha = None
    if g0_evaluation_width is None and isinstance(evidence_context, Mapping):
        evidence_sha = evidence_context.get("predeclaredEvidenceContextSha256")
        _validate_exact_sha256(
            evidence_sha, name="publication predeclaredEvidenceContextSha256"
        )
    publication_policy = {
        "qdVersion": qd_version,
        "policyName": policy_name,
        "policySha256": _validate_exact_sha256(
            policy_sha256, name="publication policySha256"
        ),
        "frozenPolicy": dict(frozen_policy),
        "pairPolicy": config.get("pairPolicy"),
        "operatorImplementationIdentity": config.get("operatorImplementation"),
        "predeclaredEvidenceContextSha256": evidence_sha,
    }
    if sha256(canonical_json_bytes(frozen_policy)) != publication_policy["policySha256"]:
        raise TemporalQDNativeError("frozen publication policy identity mismatch")
    publication_policy["publicationAuthoritySha256"] = sha256(
        canonical_json_bytes(publication_policy)
    )
    catalog = (
        dict(frozen_construction_catalog)
        if frozen_construction_catalog is not None
        else None
    )
    value = {
        "schemaVersion": NATIVE_GENERATION_MANIFEST_SCHEMA,
        "contractVersion": NATIVE_CONTRACT_VERSION,
        "operation": NATIVE_GENERATION_OPERATION,
        "authoritySha256": _validate_exact_sha256(
            authority_sha256, name="generation authoritySha256"
        ),
        "runtimeAuthority": dict(runtime_authority),
        "runtimeAuthoritySha256": runtime_authority.get(
            "runtimeAuthoritySha256"
        ),
        "parentArchivePath": str(Path(parent_archive_path).resolve()),
        "parentArchiveSha256": _validate_exact_sha256(
            parent_archive_sha256, name="generation parentArchiveSha256"
        ),
        "identityLedgerPath": str(Path(identity_ledger_path).resolve()),
        "identityLedgerSha256": _validate_exact_sha256(
            identity_ledger_sha256, name="generation identityLedgerSha256"
        ),
        "outputRoot": str(Path(output_root).resolve()),
        "finalNewline": "crlf" if os.linesep == "\r\n" else "lf",
        "generationConfig": config,
        "generationConfigSha256": config_sha,
        "targetUniqueCandidates": config.get("targetUniqueCandidates"),
        "maxProposalAttempts": config.get("maxProposalAttempts"),
        "maxNewProposals": max_new_proposals,
        "nativeExecutionTimeoutSeconds": native_execution_timeout_seconds,
        "allowEmptyQualityBootstrap": allow_empty_quality_bootstrap,
        "parentSchedule": config.get("parentSchedule"),
        "g0EvaluationWidth": g0_evaluation_width,
        "frozenConstructionCatalog": catalog,
        "frozenConstructionCatalogSha256": (
            sha256(canonical_json_bytes(catalog)) if catalog is not None else None
        ),
        "publicationPolicy": publication_policy,
        "nativeProposalAuthoritySha256": sha256(
            canonical_json_bytes(native_authority)
        ),
        "resultPath": NATIVE_GENERATION_RESULT_FILENAME,
    }
    value["manifestSha256"] = sha256(canonical_json_bytes(value))
    return validate_generation_manifest(value)


def validate_generation_manifest(value: object) -> dict[str, Any]:
    expected = {
        "schemaVersion",
        "contractVersion",
        "operation",
        "authoritySha256",
        "runtimeAuthority",
        "runtimeAuthoritySha256",
        "parentArchivePath",
        "parentArchiveSha256",
        "identityLedgerPath",
        "identityLedgerSha256",
        "outputRoot",
        "finalNewline",
        "generationConfig",
        "generationConfigSha256",
        "targetUniqueCandidates",
        "maxProposalAttempts",
        "maxNewProposals",
        "nativeExecutionTimeoutSeconds",
        "allowEmptyQualityBootstrap",
        "parentSchedule",
        "g0EvaluationWidth",
        "frozenConstructionCatalog",
        "frozenConstructionCatalogSha256",
        "publicationPolicy",
        "nativeProposalAuthoritySha256",
        "resultPath",
        "manifestSha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise TemporalQDNativeError("native generation manifest fields are not exact")
    result = dict(value)
    if (
        result["schemaVersion"] != NATIVE_GENERATION_MANIFEST_SCHEMA
        or result["contractVersion"] != NATIVE_CONTRACT_VERSION
        or result["operation"] != NATIVE_GENERATION_OPERATION
        or result["resultPath"] != NATIVE_GENERATION_RESULT_FILENAME
    ):
        raise TemporalQDNativeError("native generation manifest is incompatible")
    expected_final_newline = "crlf" if os.linesep == "\r\n" else "lf"
    if result["finalNewline"] != expected_final_newline:
        raise TemporalQDNativeError(
            "native generation manifest finalNewline is incompatible with this platform"
        )
    for key in (
        "authoritySha256",
        "runtimeAuthoritySha256",
        "parentArchiveSha256",
        "identityLedgerSha256",
        "generationConfigSha256",
        "nativeProposalAuthoritySha256",
        "manifestSha256",
    ):
        _validate_exact_sha256(result[key], name=f"generation manifest {key}")
    runtime = result["runtimeAuthority"]
    config = result["generationConfig"]
    if not isinstance(runtime, Mapping) or not isinstance(config, Mapping):
        raise TemporalQDNativeError("native generation manifest nested values are invalid")
    if set(runtime) != {
        "schemaVersion",
        "pairRunConfig",
        "pairRunConfigSha256",
        "bidirectionalPairPolicy",
        "bidirectionalPairPolicySha256",
        "evidenceIdentityContext",
        "evidenceIdentityContextSha256",
        "generationIndex",
        "pairGenerationConfigSha256",
        "runtimeAuthoritySha256",
    }:
        raise TemporalQDNativeError("runtime authority fields are not exact")
    if runtime.get("schemaVersion") != RUNTIME_AUTHORITY_SCHEMA:
        raise TemporalQDNativeError("runtime authority schema is incompatible")
    pair_run_config = runtime.get("pairRunConfig")
    pair_run_config_sha = _validate_exact_sha256(
        runtime.get("pairRunConfigSha256"),
        name="runtime authority pairRunConfigSha256",
    )
    if not isinstance(pair_run_config, Mapping):
        raise TemporalQDNativeError("runtime authority pair run config is invalid")
    if pair_run_config.get("pairRunConfigSha256") != pair_run_config_sha:
        raise TemporalQDNativeError(
            "runtime authority pair run config embedded identity mismatch"
        )
    pair_run_material = {
        key: item
        for key, item in pair_run_config.items()
        if key != "pairRunConfigSha256"
    }
    if sha256(canonical_json_bytes(pair_run_material)) != pair_run_config_sha:
        raise TemporalQDNativeError(
            "runtime authority pair run config self-hash mismatch"
        )
    pair_policy = runtime.get("bidirectionalPairPolicy")
    pair_policy_sha = _validate_exact_sha256(
        runtime.get("bidirectionalPairPolicySha256"),
        name="runtime authority bidirectionalPairPolicySha256",
    )
    if (
        not isinstance(pair_policy, Mapping)
        or sha256(canonical_json_bytes(pair_policy)) != pair_policy_sha
    ):
        raise TemporalQDNativeError("runtime authority pair policy identity mismatch")
    evidence_context = runtime.get("evidenceIdentityContext")
    evidence_sha = runtime.get("evidenceIdentityContextSha256")
    if evidence_context is None and evidence_sha is None:
        pass
    elif isinstance(evidence_context, Mapping):
        checked_evidence_sha = _validate_exact_sha256(
            evidence_sha,
            name="runtime authority evidenceIdentityContextSha256",
        )
        if (
            evidence_context.get("predeclaredEvidenceContextSha256")
            != checked_evidence_sha
        ):
            raise TemporalQDNativeError(
                "runtime authority evidence context embedded identity mismatch"
            )
        evidence_material = {
            key: item
            for key, item in evidence_context.items()
            if key != "predeclaredEvidenceContextSha256"
        }
        if sha256(canonical_json_bytes(evidence_material)) != checked_evidence_sha:
            raise TemporalQDNativeError(
                "runtime authority evidence context self-hash mismatch"
            )
    else:
        raise TemporalQDNativeError(
            "runtime authority evidence context and identity must be paired"
        )
    supplied_runtime_sha = runtime.get("runtimeAuthoritySha256")
    runtime_material = {
        key: item for key, item in runtime.items() if key != "runtimeAuthoritySha256"
    }
    if (
        supplied_runtime_sha != result["runtimeAuthoritySha256"]
        or sha256(canonical_json_bytes(runtime_material)) != supplied_runtime_sha
    ):
        raise TemporalQDNativeError("runtime authority identity mismatch")
    if (
        config.get("schemaVersion") != PAIR_GENERATION_SCHEMA
        or config.get("configSha256") != result["generationConfigSha256"]
        or runtime.get("pairGenerationConfigSha256")
        != result["generationConfigSha256"]
    ):
        raise TemporalQDNativeError("generation config/runtime identity mismatch")
    config_material = {key: item for key, item in config.items() if key != "configSha256"}
    if sha256(canonical_json_bytes(config_material)) != result["generationConfigSha256"]:
        raise TemporalQDNativeError("generation config self-hash mismatch")
    if (
        config.get("targetUniqueCandidates") != result["targetUniqueCandidates"]
        or config.get("maxProposalAttempts") != result["maxProposalAttempts"]
        or config.get("parentSchedule") != result["parentSchedule"]
        or not isinstance(result["allowEmptyQualityBootstrap"], bool)
        or isinstance(result["nativeExecutionTimeoutSeconds"], bool)
        or not isinstance(result["nativeExecutionTimeoutSeconds"], int)
        or result["nativeExecutionTimeoutSeconds"] < 60
    ):
        raise TemporalQDNativeError("generation manifest fields diverge from config")
    if result["frozenConstructionCatalog"] is None:
        if result["frozenConstructionCatalogSha256"] is not None:
            raise TemporalQDNativeError("frozen construction catalog identity mismatch")
    elif (
        not isinstance(result["frozenConstructionCatalog"], Mapping)
        or sha256(canonical_json_bytes(result["frozenConstructionCatalog"]))
        != result["frozenConstructionCatalogSha256"]
    ):
        raise TemporalQDNativeError("frozen construction catalog identity mismatch")
    publication = result["publicationPolicy"]
    if not isinstance(publication, Mapping) or set(publication) != {
        "qdVersion",
        "policyName",
        "policySha256",
        "frozenPolicy",
        "pairPolicy",
        "operatorImplementationIdentity",
        "predeclaredEvidenceContextSha256",
        "publicationAuthoritySha256",
    }:
        raise TemporalQDNativeError("native publication policy fields are not exact")
    frozen_policy = publication["frozenPolicy"]
    if (
        not isinstance(frozen_policy, Mapping)
        or sha256(canonical_json_bytes(frozen_policy)) != publication["policySha256"]
        or frozen_policy.get("policyName") != publication["policyName"]
        or publication["pairPolicy"] != config.get("pairPolicy")
        or publication["operatorImplementationIdentity"]
        != config.get("operatorImplementation")
    ):
        raise TemporalQDNativeError("native publication policy authority mismatch")
    publication_material = {
        key: item
        for key, item in publication.items()
        if key != "publicationAuthoritySha256"
    }
    if (
        sha256(canonical_json_bytes(publication_material))
        != publication["publicationAuthoritySha256"]
    ):
        raise TemporalQDNativeError("native publication policy identity mismatch")
    supplied = result["manifestSha256"]
    material = {key: item for key, item in result.items() if key != "manifestSha256"}
    if sha256(canonical_json_bytes(material)) != supplied:
        raise TemporalQDNativeError("native generation manifest identity mismatch")
    return result


def validate_generation_result(
    value: object, *, manifest: Mapping[str, Any]
) -> dict[str, Any]:
    checked = validate_generation_manifest(manifest)
    expected = {
        "schemaVersion",
        "contractVersion",
        "operation",
        "status",
        "authoritySha256",
        "manifestSha256",
        "runtimeAuthoritySha256",
        "parentArchiveSha256",
        "inputIdentityLedgerSha256",
        "outputIdentityLedgerSha256",
        "generationConfigSha256",
        "pairGenerationResult",
        "resultSha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise TemporalQDNativeError("native generation result fields are not exact")
    result = dict(value)
    inner = result["pairGenerationResult"]
    if not isinstance(inner, dict):
        raise TemporalQDNativeError("native pair generation result must be an object")
    completed = inner.get("completed")
    if (
        inner.get("schemaVersion")
        not in {PAIR_GENERATION_RESULT_SCHEMA, PAIR_GENERATION_PROGRESS_SCHEMA}
        or completed not in {True, False}
        or inner.get("configSha256") != checked["generationConfigSha256"]
    ):
        raise TemporalQDNativeError("native inner pair generation result is incompatible")
    if completed:
        required_inner = {
            "schemaVersion",
            "configSha256",
            "populationSha256",
            "evaluationPopulationSha256",
            "journalSha256",
            "proposalCount",
            "candidateCount",
            "originProposalCounts",
            "originAcceptedCounts",
            "proposalSlots",
            "uniqueIdentityCounts",
            "duplicateCounters",
            "proposalSlotCounters",
            "nextImmigrantContinuationOrdinal",
            "completed",
        }
        optional_inner = {
            "constructionPoolSize",
            "constructedAcceptedCount",
            "g0Bootstrap",
            "immigrantConstructionDistribution",
        }
        if not required_inner.issubset(inner) or set(inner) - required_inner - optional_inner:
            raise TemporalQDNativeError(
                "completed native pair generation result fields are not exact"
            )
    elif set(inner) != {
        "schemaVersion",
        "configSha256",
        "proposalCount",
        "acceptedCount",
        "maxProposalAttempts",
        "terminationReason",
        "completed",
    }:
        raise TemporalQDNativeError(
            "native pair generation progress fields are not exact"
        )
    if (
        result["schemaVersion"] != NATIVE_GENERATION_RESULT_SCHEMA
        or result["contractVersion"] != NATIVE_CONTRACT_VERSION
        or result["operation"] != NATIVE_GENERATION_OPERATION
        or result["status"] != ("completed" if completed else "progress")
        or result["authoritySha256"] != checked["authoritySha256"]
        or result["manifestSha256"] != checked["manifestSha256"]
        or result["runtimeAuthoritySha256"] != checked["runtimeAuthoritySha256"]
        or result["parentArchiveSha256"] != checked["parentArchiveSha256"]
        or result["inputIdentityLedgerSha256"]
        != checked["identityLedgerSha256"]
        or result["generationConfigSha256"] != checked["generationConfigSha256"]
    ):
        raise TemporalQDNativeError("native generation result is incompatible")
    _validate_exact_sha256(
        result["outputIdentityLedgerSha256"],
        name="native generation outputIdentityLedgerSha256",
    )
    supplied = _validate_exact_sha256(
        result["resultSha256"], name="native generation resultSha256"
    )
    material = {key: item for key, item in result.items() if key != "resultSha256"}
    if sha256(canonical_json_bytes(material)) != supplied:
        raise TemporalQDNativeError("native generation result identity mismatch")
    return result


def _validate_g0_publication_policy(value: object) -> dict[str, Any]:
    expected = {
        "qdVersion",
        "policyName",
        "policySha256",
        "pairPolicy",
        "operatorImplementationIdentity",
        "predeclaredEvidenceContextSha256",
        "archivePolicyAuthority",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise TemporalQDNativeError("native G0 publication policy fields are not exact")
    result = dict(value)
    if (
        not isinstance(result["qdVersion"], str)
        or not result["qdVersion"]
        or not isinstance(result["policyName"], str)
        or not result["policyName"]
        or not isinstance(result["pairPolicy"], Mapping)
        or not isinstance(result["operatorImplementationIdentity"], Mapping)
    ):
        raise TemporalQDNativeError("native G0 publication policy is invalid")
    _validate_exact_sha256(result["policySha256"], name="G0 publication policySha256")
    evidence = result["predeclaredEvidenceContextSha256"]
    if evidence is not None:
        _validate_exact_sha256(
            evidence, name="G0 publication predeclaredEvidenceContextSha256"
        )
    authority = result["archivePolicyAuthority"]
    if authority is not None:
        if not isinstance(authority, Mapping) or set(authority) != {
            "qdVersion",
            "policyName",
            "policySha256",
            "frozenPolicy",
        }:
            raise TemporalQDNativeError(
                "native G0 archive policy authority fields are not exact"
            )
        if (
            authority.get("qdVersion") != result["qdVersion"]
            or authority.get("policyName") != result["policyName"]
            or authority.get("policySha256") != result["policySha256"]
            or not isinstance(authority.get("frozenPolicy"), Mapping)
            or sha256(canonical_json_bytes(authority["frozenPolicy"]))
            != result["policySha256"]
        ):
            raise TemporalQDNativeError(
                "native G0 archive policy authority binding is invalid"
            )
    return result


def _validate_g0_global_identity_ledger(value: object) -> dict[str, Any] | None:
    if value is None:
        return None
    expected = {
        "pairExecutableSemanticCount",
        "pairExecutableSemanticDuplicateRejections",
        "identityLedgerSha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise TemporalQDNativeError("native G0 global identity ledger fields are not exact")
    result = dict(value)
    for key in (
        "pairExecutableSemanticCount",
        "pairExecutableSemanticDuplicateRejections",
    ):
        if isinstance(result[key], bool) or not isinstance(result[key], int) or result[key] < 0:
            raise TemporalQDNativeError(f"native G0 global identity ledger {key} is invalid")
    _validate_exact_sha256(
        result["identityLedgerSha256"], name="G0 identity ledgerSha256"
    )
    return result


def _validate_g0_identity_ledger_binding(value: object) -> dict[str, Any] | None:
    """Validate the compact pointer/authority passed to Rust, never its rows."""

    if value is None:
        return None
    expected = {
        "schemaVersion",
        "ledgerPath",
        "policyName",
        "policySha256",
        "identityPolicy",
        "identityPolicySha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise TemporalQDNativeError("native G0 identity ledger binding fields are not exact")
    result = dict(value)
    if (
        result["schemaVersion"] != G0_IDENTITY_LEDGER_BINDING_SCHEMA
        or not isinstance(result["ledgerPath"], str)
        or not Path(result["ledgerPath"]).is_absolute()
        or not isinstance(result["policyName"], str)
        or not result["policyName"]
        or not isinstance(result["identityPolicy"], Mapping)
    ):
        raise TemporalQDNativeError("native G0 identity ledger binding is invalid")
    for key in ("policySha256", "identityPolicySha256"):
        _validate_exact_sha256(result[key], name=f"native G0 identity ledger {key}")
    if sha256(canonical_json_bytes(result["identityPolicy"])) != result[
        "identityPolicySha256"
    ]:
        raise TemporalQDNativeError("native G0 identity ledger policy identity drifted")
    return result


def build_g0_funnel_manifest(
    *,
    g0_finalization_runtime: Mapping[str, Any],
    output_root: Path | str,
    generation_config: Mapping[str, Any],
    evaluation_population_size: int,
    publication_policy: Mapping[str, Any],
    native_batch_authority: Mapping[str, Any] | None = None,
    identity_ledger_binding: Mapping[str, Any] | None = None,
    global_identity_ledger: Mapping[str, Any] | None = None,
    audit: bool = False,
) -> dict[str, Any]:
    """Build the compact native G0 request; rich rows stay on disk."""

    runtime = validate_g0_finalization_runtime_config(g0_finalization_runtime)
    if runtime["engine"] != G0_FINALIZATION_RUNTIME_RUST:
        raise TemporalQDNativeError("native G0 manifest requires the Rust G0 runtime")
    if native_batch_authority is None:
        raise TemporalQDNativeError("native G0 manifest requires batch executable authority")
    if global_identity_ledger is not None:
        raise TemporalQDNativeError(
            "native G0 manifest accepts only a compact identity ledger binding"
        )
    config = dict(generation_config)
    config_sha = _validate_exact_sha256(
        config.get("configSha256"), name="G0 generation configSha256"
    )
    config_material = {key: item for key, item in config.items() if key != "configSha256"}
    if (
        config.get("schemaVersion") != PAIR_GENERATION_SCHEMA
        or sha256(canonical_json_bytes(config_material)) != config_sha
        or isinstance(config.get("generationIndex"), bool)
        or config.get("generationIndex") != 1
        or isinstance(config.get("targetUniqueCandidates"), bool)
        or not isinstance(config.get("targetUniqueCandidates"), int)
        or config["targetUniqueCandidates"] < 1
        or isinstance(config.get("maxProposalAttempts"), bool)
        or not isinstance(config.get("maxProposalAttempts"), int)
        or config["maxProposalAttempts"] < config["targetUniqueCandidates"]
    ):
        raise TemporalQDNativeError("G0 generation config is incompatible")
    if (
        isinstance(evaluation_population_size, bool)
        or not isinstance(evaluation_population_size, int)
        or not 1 <= evaluation_population_size <= config["targetUniqueCandidates"]
        or not isinstance(audit, bool)
    ):
        raise TemporalQDNativeError("G0 funnel dimensions or audit flag are invalid")
    policy = _validate_g0_publication_policy(publication_policy)
    execution_authority = _build_g0_execution_authority(
        g0_finalization_runtime=runtime,
        native_batch_authority=native_batch_authority,
    )
    ledger = _validate_g0_identity_ledger_binding(identity_ledger_binding)
    value = {
        "schemaVersion": G0_FUNNEL_MANIFEST_SCHEMA,
        "contractVersion": NATIVE_CONTRACT_VERSION,
        "operation": G0_FUNNEL_OPERATION,
        "authoritySha256": execution_authority["authoritySha256"],
        "executionAuthority": execution_authority,
        "outputRoot": str(Path(output_root).resolve()),
        "finalNewline": "crlf" if os.linesep == "\r\n" else "lf",
        "generationConfig": config,
        "generationConfigSha256": config_sha,
        "generationIndex": config["generationIndex"],
        "constructionPoolSize": config["targetUniqueCandidates"],
        "evaluationPopulationSize": evaluation_population_size,
        "maxProposalAttempts": config["maxProposalAttempts"],
        "publicationPolicy": policy,
        "identityLedger": ledger,
        "audit": audit,
        "resultPath": G0_FUNNEL_RESULT_FILENAME,
    }
    value["manifestSha256"] = sha256(canonical_json_bytes(value))
    return validate_g0_funnel_manifest(value)


def validate_g0_funnel_manifest(value: object) -> dict[str, Any]:
    expected = {
        "schemaVersion",
        "contractVersion",
        "operation",
        "authoritySha256",
        "executionAuthority",
        "outputRoot",
        "finalNewline",
        "generationConfig",
        "generationConfigSha256",
        "generationIndex",
        "constructionPoolSize",
        "evaluationPopulationSize",
        "maxProposalAttempts",
        "publicationPolicy",
        "identityLedger",
        "audit",
        "resultPath",
        "manifestSha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise TemporalQDNativeError("native G0 funnel manifest fields are not exact")
    result = dict(value)
    if (
        result["schemaVersion"] != G0_FUNNEL_MANIFEST_SCHEMA
        or result["contractVersion"] != NATIVE_CONTRACT_VERSION
        or result["operation"] != G0_FUNNEL_OPERATION
        or result["resultPath"] != G0_FUNNEL_RESULT_FILENAME
        or result["finalNewline"] != ("crlf" if os.linesep == "\r\n" else "lf")
        or not isinstance(result["outputRoot"], str)
        or not Path(result["outputRoot"]).is_absolute()
        or not isinstance(result["audit"], bool)
    ):
        raise TemporalQDNativeError("native G0 funnel manifest is incompatible")
    for key in ("authoritySha256", "generationConfigSha256", "manifestSha256"):
        _validate_exact_sha256(result[key], name=f"native G0 funnel {key}")
    execution_authority = _validate_g0_execution_authority(result["executionAuthority"])
    if result["authoritySha256"] != execution_authority["authoritySha256"]:
        raise TemporalQDNativeError("native G0 funnel execution authority drifted")
    config = result["generationConfig"]
    if not isinstance(config, Mapping):
        raise TemporalQDNativeError("native G0 funnel generation config is invalid")
    material = {key: item for key, item in config.items() if key != "configSha256"}
    if (
        config.get("schemaVersion") != PAIR_GENERATION_SCHEMA
        or config.get("configSha256") != result["generationConfigSha256"]
        or sha256(canonical_json_bytes(material)) != result["generationConfigSha256"]
        or result["generationIndex"] != config.get("generationIndex")
        or result["constructionPoolSize"] != config.get("targetUniqueCandidates")
        or result["maxProposalAttempts"] != config.get("maxProposalAttempts")
        or isinstance(result["generationIndex"], bool)
        or result["generationIndex"] != 1
        or isinstance(result["constructionPoolSize"], bool)
        or not isinstance(result["constructionPoolSize"], int)
        or result["constructionPoolSize"] < 1
        or isinstance(result["evaluationPopulationSize"], bool)
        or not isinstance(result["evaluationPopulationSize"], int)
        or not 1 <= result["evaluationPopulationSize"] <= result["constructionPoolSize"]
        or isinstance(result["maxProposalAttempts"], bool)
        or not isinstance(result["maxProposalAttempts"], int)
        or result["maxProposalAttempts"] < result["constructionPoolSize"]
    ):
        raise TemporalQDNativeError("native G0 funnel manifest/config dimensions drifted")
    _validate_g0_publication_policy(result["publicationPolicy"])
    _validate_g0_identity_ledger_binding(result["identityLedger"])
    supplied = result["manifestSha256"]
    material = {key: item for key, item in result.items() if key != "manifestSha256"}
    if sha256(canonical_json_bytes(material)) != supplied:
        raise TemporalQDNativeError("native G0 funnel manifest identity mismatch")
    return result


def _validate_g0_pair_generation_result(
    value: object, *, manifest: Mapping[str, Any]
) -> dict[str, Any]:
    required = {
        "schemaVersion",
        "configSha256",
        "populationSha256",
        "evaluationPopulationSha256",
        "journalSha256",
        "proposalCount",
        "candidateCount",
        "constructionPoolSize",
        "constructedAcceptedCount",
        "g0Bootstrap",
        "originProposalCounts",
        "originAcceptedCounts",
        "reproductionAllocation",
        "reproductionAllocationAccounting",
        "proposalSlots",
        "uniqueIdentityCounts",
        "duplicateCounters",
        "proposalSlotCounters",
        "nextImmigrantContinuationOrdinal",
        "completed",
    }
    optional = {"immigrantConstructionDistribution"}
    if not isinstance(value, Mapping) or set(value) - optional != required:
        raise TemporalQDNativeError("native G0 pair generation result fields are not exact")
    result = dict(value)
    if (
        result["schemaVersion"] != PAIR_GENERATION_RESULT_SCHEMA
        or result["configSha256"] != manifest["generationConfigSha256"]
        or result["completed"] is not True
        or result["constructionPoolSize"] != manifest["constructionPoolSize"]
        or result["constructedAcceptedCount"] != manifest["constructionPoolSize"]
        or result["proposalCount"] != manifest["evaluationPopulationSize"]
        or result["candidateCount"] != manifest["evaluationPopulationSize"]
        or result["nextImmigrantContinuationOrdinal"] != 0
        or result["reproductionAllocation"]
        != manifest["generationConfig"].get("reproductionAllocation")
        or not isinstance(result["g0Bootstrap"], Mapping)
    ):
        raise TemporalQDNativeError("native G0 pair generation result is incompatible")
    for key in ("populationSha256", "evaluationPopulationSha256", "journalSha256"):
        _validate_exact_sha256(result[key], name=f"native G0 result {key}")
    for key in (
        "proposalCount",
        "candidateCount",
        "constructionPoolSize",
        "constructedAcceptedCount",
        "nextImmigrantContinuationOrdinal",
    ):
        if isinstance(result[key], bool) or not isinstance(result[key], int) or result[key] < 0:
            raise TemporalQDNativeError(f"native G0 result {key} is invalid")
    for key in (
        "originProposalCounts",
        "originAcceptedCounts",
        "reproductionAllocationAccounting",
        "proposalSlots",
        "uniqueIdentityCounts",
        "duplicateCounters",
        "proposalSlotCounters",
    ):
        if not isinstance(result[key], Mapping):
            raise TemporalQDNativeError(f"native G0 result {key} is invalid")
    return result


def _validate_g0_receipt(
    value: object,
    *,
    manifest: Mapping[str, Any],
    pair_generation_result: Mapping[str, Any],
) -> dict[str, Any]:
    expected = {
        "schemaVersion",
        "requestSha256",
        "authoritySha256",
        "executionAuthority",
        "configSha256",
        "generationIndex",
        "constructionPoolSize",
        "evaluationPopulationSize",
        "operatorImplementationSha256",
        "archivePolicyAuthoritySha256",
        "journalInventorySha256",
        "sourceHandoffSha256",
        "globalIdentityLedger",
        "identityLedgerBinding",
        "g0Bootstrap",
        "population",
        "evaluationPopulation",
        "generationJournal",
        "pairGenerationResult",
        "constructionPoolIdentitySha256",
        "acceptedPoolSha256",
        "selectionSha256",
        "ledgerSha256",
        "receiptSha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise TemporalQDNativeError("native G0 receipt fields are not exact")
    result = dict(value)
    if (
        result["schemaVersion"] != G0_FUNNEL_RECEIPT_SCHEMA
        or result["requestSha256"] != manifest["manifestSha256"]
        or result["authoritySha256"] != manifest["authoritySha256"]
        or result["executionAuthority"] != manifest["executionAuthority"]
        or result["configSha256"] != manifest["generationConfigSha256"]
        or result["generationIndex"] != manifest["generationIndex"]
        or result["constructionPoolSize"] != manifest["constructionPoolSize"]
        or result["evaluationPopulationSize"] != manifest["evaluationPopulationSize"]
        or result["identityLedgerBinding"] != manifest["identityLedger"]
        or result["g0Bootstrap"] != pair_generation_result["g0Bootstrap"]
        or result["pairGenerationResult"] != pair_generation_result
    ):
        raise TemporalQDNativeError("native G0 receipt manifest binding drifted")
    _validate_g0_execution_authority(result["executionAuthority"])
    _validate_g0_identity_ledger_binding(result["identityLedgerBinding"])
    _validate_g0_global_identity_ledger(result["globalIdentityLedger"])
    operator_sha = sha256(
        canonical_json_bytes(
            manifest["publicationPolicy"]["operatorImplementationIdentity"]
        )
    )
    if result["operatorImplementationSha256"] != operator_sha:
        raise TemporalQDNativeError("native G0 receipt operator binding drifted")
    archive = manifest["publicationPolicy"]["archivePolicyAuthority"]
    expected_archive_sha = sha256(canonical_json_bytes(archive)) if archive is not None else None
    if result["archivePolicyAuthoritySha256"] != expected_archive_sha:
        raise TemporalQDNativeError("native G0 receipt archive authority binding drifted")
    for key in (
        "requestSha256",
        "authoritySha256",
        "configSha256",
        "operatorImplementationSha256",
        "journalInventorySha256",
        "constructionPoolIdentitySha256",
        "acceptedPoolSha256",
        "selectionSha256",
        "ledgerSha256",
        "receiptSha256",
    ):
        _validate_exact_sha256(result[key], name=f"native G0 receipt {key}")
    for key in ("archivePolicyAuthoritySha256", "sourceHandoffSha256"):
        if result[key] is not None:
            _validate_exact_sha256(result[key], name=f"native G0 receipt {key}")
    for key, expected_path, expected_sha in (
        ("population", "population.json", pair_generation_result["populationSha256"]),
        (
            "evaluationPopulation",
            "evaluation-population.json",
            pair_generation_result["evaluationPopulationSha256"],
        ),
        ("generationJournal", "generation-journal.json", pair_generation_result["journalSha256"]),
    ):
        artifact = result[key]
        if (
            not isinstance(artifact, Mapping)
            or set(artifact) != {"relativePath", "semanticSha256", "fileSha256", "encodedBytes"}
            or artifact.get("relativePath") != expected_path
            or artifact.get("semanticSha256") != expected_sha
            or isinstance(artifact.get("encodedBytes"), bool)
            or not isinstance(artifact.get("encodedBytes"), int)
            or artifact["encodedBytes"] < 1
        ):
            raise TemporalQDNativeError(f"native G0 receipt {key} binding is invalid")
        _validate_exact_sha256(artifact.get("fileSha256"), name=f"native G0 receipt {key} fileSha256")
    supplied = result["receiptSha256"]
    material = {key: item for key, item in result.items() if key != "receiptSha256"}
    if sha256(canonical_json_bytes(material)) != supplied:
        raise TemporalQDNativeError("native G0 receipt identity mismatch")
    return result


def validate_g0_funnel_result(
    value: object, *, manifest: Mapping[str, Any]
) -> dict[str, Any]:
    checked = validate_g0_funnel_manifest(manifest)
    if not isinstance(value, Mapping):
        raise TemporalQDNativeError("native G0 funnel result must be an object")
    result = dict(value)
    status = result.get("status")
    if status == "construction_incomplete":
        if set(result) != {"schemaVersion", "status", "proposalCount", "acceptedCount"}:
            raise TemporalQDNativeError("incomplete native G0 result fields are not exact")
        for key in ("proposalCount", "acceptedCount"):
            if isinstance(result[key], bool) or not isinstance(result[key], int) or result[key] < 0:
                raise TemporalQDNativeError(f"incomplete native G0 result {key} is invalid")
        if result["acceptedCount"] >= checked["constructionPoolSize"]:
            raise TemporalQDNativeError("incomplete native G0 result has a completed pool")
    elif status in {"completed", "adopted"}:
        expected = {"schemaVersion", "status", "pairGenerationResult", "receipt"}
        if status == "adopted":
            expected.add("adoptionVerification")
        if set(result) != expected:
            raise TemporalQDNativeError("sealed native G0 result fields are not exact")
        pair = _validate_g0_pair_generation_result(
            result["pairGenerationResult"], manifest=checked
        )
        _validate_g0_receipt(result["receipt"], manifest=checked, pair_generation_result=pair)
        if status == "adopted":
            verification = result["adoptionVerification"]
            if not isinstance(verification, Mapping) or set(verification) != {
                "schemaVersion",
                "outputBytesHashed",
                "outputHashElapsedMilliseconds",
                "proposalJournalBytesRead",
            } or verification.get("schemaVersion") != "temporal_qd_native_g0_adoption_verification_v1":
                raise TemporalQDNativeError("native G0 adoption verification fields are invalid")
            for key in (
                "outputBytesHashed",
                "outputHashElapsedMilliseconds",
                "proposalJournalBytesRead",
            ):
                if isinstance(verification.get(key), bool) or not isinstance(verification.get(key), int) or verification[key] < 0:
                    raise TemporalQDNativeError(f"native G0 adoption verification {key} is invalid")
            if verification["proposalJournalBytesRead"] != 0:
                raise TemporalQDNativeError("native G0 adoption must not read proposal journal bytes")
    else:
        raise TemporalQDNativeError("native G0 funnel result status is invalid")
    if result.get("schemaVersion") != G0_FUNNEL_RESULT_SCHEMA:
        raise TemporalQDNativeError("native G0 funnel result schema is incompatible")
    return result


def _is_link_or_reparse(status: os.stat_result) -> bool:
    reparse_point = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
    attributes = getattr(status, "st_file_attributes", 0)
    return stat.S_ISLNK(status.st_mode) or bool(attributes & reparse_point)


def _require_real_directory(path: Path, *, name: str) -> None:
    try:
        status = os.lstat(path)
    except OSError as exc:
        raise TemporalQDNativeError(f"could not inspect {name}: {path}") from exc
    if _is_link_or_reparse(status) or not stat.S_ISDIR(status.st_mode):
        raise TemporalQDNativeError(f"{name} is not a real directory: {path}")


def _require_regular_file(path: Path, *, name: str) -> None:
    try:
        status = os.lstat(path)
    except FileNotFoundError as exc:
        raise TemporalQDNativeError(f"{name} is not a regular file: {path}") from exc
    except OSError as exc:
        raise TemporalQDNativeError(f"could not inspect {name}: {path}") from exc
    if _is_link_or_reparse(status) or not stat.S_ISREG(status.st_mode):
        raise TemporalQDNativeError(f"{name} is not a regular file: {path}")


def _file_identity(status: os.stat_result) -> tuple[int, int] | None:
    device = int(status.st_dev)
    inode = int(status.st_ino)
    if inode == 0:
        return None
    return device, inode


def _require_same_identity(
    current: os.stat_result,
    expected: os.stat_result,
    *,
    name: str,
    path: Path,
) -> None:
    current_identity = _file_identity(current)
    expected_identity = _file_identity(expected)
    if current_identity is None or current_identity != expected_identity:
        raise TemporalQDNativeError(f"{name} changed identity: {path}")


def _require_directory_identity(
    path: Path, expected: os.stat_result, *, name: str
) -> None:
    _require_existing_real_directory_tree(path, name=name)
    try:
        current = os.lstat(path)
    except OSError as exc:
        raise TemporalQDNativeError(f"could not inspect {name}: {path}") from exc
    _require_same_identity(current, expected, name=name, path=path)


def _existing_regular_file(
    path: Path,
    *,
    name: str,
    parent_status: os.stat_result | None = None,
    maximum_bytes: int | None = None,
) -> tuple[bytes, os.stat_result] | None:
    if parent_status is not None:
        _require_directory_identity(path.parent, parent_status, name=f"{name} parent")
    try:
        before = os.lstat(path)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise TemporalQDNativeError(f"could not inspect {name}: {path}") from exc
    if _is_link_or_reparse(before) or not stat.S_ISREG(before.st_mode):
        raise TemporalQDNativeError(f"{name} is not a regular file: {path}")
    if maximum_bytes is not None and (
        isinstance(maximum_bytes, bool)
        or not isinstance(maximum_bytes, int)
        or maximum_bytes < 1
    ):
        raise TemporalQDNativeError(f"{name} maximum byte limit is invalid")
    if maximum_bytes is not None and before.st_size > maximum_bytes:
        raise TemporalQDNativeError(
            f"{name} exceeded its {maximum_bytes} byte compact-document limit"
        )
    try:
        with path.open("rb") as handle:
            opened = os.fstat(handle.fileno())
            _require_same_identity(opened, before, name=name, path=path)
            payload = handle.read(
                maximum_bytes + 1 if maximum_bytes is not None else -1
            )
    except OSError as exc:
        raise TemporalQDNativeError(f"could not read {name}: {path}") from exc
    if parent_status is not None:
        _require_directory_identity(path.parent, parent_status, name=f"{name} parent")
    try:
        after = os.lstat(path)
    except OSError as exc:
        raise TemporalQDNativeError(f"could not re-inspect {name}: {path}") from exc
    if _is_link_or_reparse(after) or not stat.S_ISREG(after.st_mode):
        raise TemporalQDNativeError(f"{name} is not a regular file: {path}")
    _require_same_identity(after, opened, name=name, path=path)
    if maximum_bytes is not None and len(payload) > maximum_bytes:
        raise TemporalQDNativeError(
            f"{name} exceeded its {maximum_bytes} byte compact-document limit"
        )
    return payload, opened


def _ensure_real_directory_tree(path: Path | str, *, name: str) -> Path:
    absolute = Path(os.path.abspath(os.fspath(path)))
    anchor = Path(absolute.anchor)
    if not absolute.anchor:
        raise TemporalQDNativeError(f"{name} must be absolute")
    _require_real_directory(anchor, name=f"{name} filesystem anchor")
    current = anchor
    for component in absolute.parts[1:]:
        if not component or component in {".", ".."}:
            raise TemporalQDNativeError(f"{name} has an unsafe path component")
        current = current / component
        try:
            status = os.lstat(current)
        except FileNotFoundError:
            try:
                os.mkdir(current)
            except FileExistsError:
                pass
            except OSError as exc:
                raise TemporalQDNativeError(
                    f"could not create {name} component: {current}"
                ) from exc
        _require_real_directory(current, name=name)
    return absolute


def _require_existing_real_directory_tree(path: Path | str, *, name: str) -> Path:
    absolute = Path(os.path.abspath(os.fspath(path)))
    anchor = Path(absolute.anchor)
    if not absolute.anchor:
        raise TemporalQDNativeError(f"{name} must be absolute")
    _require_real_directory(anchor, name=f"{name} filesystem anchor")
    current = anchor
    for component in absolute.parts[1:]:
        if not component or component in {".", ".."}:
            raise TemporalQDNativeError(f"{name} has an unsafe path component")
        current = current / component
        _require_real_directory(current, name=name)
    return absolute


def _validate_existing_payload(
    record: tuple[bytes, os.stat_result], *, payload: bytes, target: Path
) -> os.stat_result:
    existing, status = record
    if existing != payload:
        raise TemporalQDNativeError(
            f"refusing to overwrite divergent native foundation artifact: {target}"
        )
    return status


def _is_windows_sharing_violation(error: OSError) -> bool:
    return os.name == "nt" and getattr(error, "winerror", None) in {32, 33}


def _allows_unsupported_directory_sync() -> bool:
    return os.name == "nt"


def _sync_publication_directory(parent: Path) -> None:
    """Durably publish a directory entry, with the documented Windows fallback.

    On Windows, some filesystems reject directory ``FlushFileBuffers`` even
    though the payload itself was flushed. ``result_codec.fsync_directory``
    reports that documented limitation as ``False``. POSIX directory-sync
    unavailability is fail-closed because its durability primitive is defined.
    """

    try:
        synced = fsync_directory(parent)
    except OSError as exc:
        raise TemporalQDNativeError(
            f"could not synchronize native foundation publication directory: {parent}"
        ) from exc
    if not synced and not _allows_unsupported_directory_sync():
        raise TemporalQDNativeError(
            f"native foundation publication directory could not be synchronized: {parent}"
        )


def _owned_temporary_present(
    temporary: Path,
    *,
    temporary_status: os.stat_result,
    parent_status: os.stat_result,
) -> bool:
    _require_directory_identity(
        temporary.parent, parent_status, name="native foundation temporary parent"
    )
    try:
        current = os.lstat(temporary)
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise TemporalQDNativeError(
            f"could not inspect owned native foundation temporary: {temporary}"
        ) from exc
    if _is_link_or_reparse(current) or not stat.S_ISREG(current.st_mode):
        raise TemporalQDNativeError(
            f"owned native foundation temporary is not a regular file: {temporary}"
        )
    _require_same_identity(
        current,
        temporary_status,
        name="owned native foundation temporary",
        path=temporary,
    )
    return True


def _require_owned_temporary(
    temporary: Path,
    *,
    temporary_status: os.stat_result,
    parent_status: os.stat_result,
) -> None:
    if not _owned_temporary_present(
        temporary,
        temporary_status=temporary_status,
        parent_status=parent_status,
    ):
        raise TemporalQDNativeError(
            f"owned native foundation temporary vanished: {temporary}"
        )


def _remove_owned_temporary(
    temporary: Path,
    *,
    temporary_status: os.stat_result,
    parent_status: os.stat_result,
) -> None:
    for attempt in range(len(_SHARING_RETRY_DELAYS_SECONDS) + 1):
        if not _owned_temporary_present(
            temporary,
            temporary_status=temporary_status,
            parent_status=parent_status,
        ):
            return
        try:
            temporary.unlink()
            return
        except FileNotFoundError:
            return
        except OSError as exc:
            if (
                not _is_windows_sharing_violation(exc)
                or attempt == len(_SHARING_RETRY_DELAYS_SECONDS)
            ):
                raise TemporalQDNativeError(
                    f"could not remove owned native foundation temporary: {temporary}"
                ) from exc
            time.sleep(_SHARING_RETRY_DELAYS_SECONDS[attempt])


def _link_immutable(
    temporary: Path,
    target: Path,
    *,
    payload: bytes,
    temporary_status: os.stat_result,
    parent_status: os.stat_result,
) -> bool:
    """Return whether this caller installed the target's immutable name."""

    for attempt in range(len(_SHARING_RETRY_DELAYS_SECONDS) + 1):
        _require_owned_temporary(
            temporary,
            temporary_status=temporary_status,
            parent_status=parent_status,
        )
        try:
            os.link(temporary, target)
        except FileExistsError:
            record = _existing_regular_file(
                target,
                name="existing native foundation artifact",
                parent_status=parent_status,
            )
            if record is None:
                raise TemporalQDNativeError(
                    f"native foundation artifact vanished during immutable publication: {target}"
                )
            _validate_existing_payload(record, payload=payload, target=target)
            return False
        except OSError as exc:
            record = _existing_regular_file(
                target,
                name="existing native foundation artifact",
                parent_status=parent_status,
            )
            if record is not None:
                _validate_existing_payload(record, payload=payload, target=target)
                return False
            if (
                not _is_windows_sharing_violation(exc)
                or attempt == len(_SHARING_RETRY_DELAYS_SECONDS)
            ):
                raise TemporalQDNativeError(
                    f"could not publish immutable native foundation artifact: {target}"
                ) from exc
            time.sleep(_SHARING_RETRY_DELAYS_SECONDS[attempt])
        else:
            record = _existing_regular_file(
                target,
                name="published native foundation artifact",
                parent_status=parent_status,
            )
            if record is None:
                raise TemporalQDNativeError(
                    f"native foundation artifact vanished after immutable publication: {target}"
                )
            target_status = _validate_existing_payload(
                record, payload=payload, target=target
            )
            _require_same_identity(
                target_status,
                temporary_status,
                name="published native foundation artifact",
                path=target,
            )
            return True
    raise AssertionError("bounded immutable-link loop did not return")


def _write_payload(handle: Any, payload: bytes) -> None:
    handle.write(payload)


def _sync_payload(handle: Any) -> None:
    handle.flush()
    os.fsync(handle.fileno())


def _write_bytes_once(path: Path, payload: bytes) -> None:
    parent = _ensure_real_directory_tree(path.parent, name="native foundation parent")
    parent_status = os.lstat(parent)
    target = parent / path.name
    existing = _existing_regular_file(
        target,
        name="existing native foundation artifact",
        parent_status=parent_status,
    )
    if existing is not None:
        _validate_existing_payload(existing, payload=payload, target=target)
        _sync_publication_directory(parent)
        record = _existing_regular_file(
            target,
            name="existing native foundation artifact",
            parent_status=parent_status,
        )
        if record is None:
            raise TemporalQDNativeError(
                f"native foundation artifact vanished after directory sync: {target}"
            )
        _validate_existing_payload(record, payload=payload, target=target)
        return
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=target.name + ".", suffix=".tmp", dir=parent
    )
    temporary = Path(temporary_name)
    temporary_status = os.fstat(descriptor)
    descriptor_owned = True
    try:
        try:
            handle = os.fdopen(descriptor, "wb")
            descriptor_owned = False
            with handle:
                _write_payload(handle, payload)
                _sync_payload(handle)
            published = _link_immutable(
                temporary,
                target,
                payload=payload,
                temporary_status=temporary_status,
                parent_status=parent_status,
            )
            _sync_publication_directory(parent)
            record = _existing_regular_file(
                target,
                name="published native foundation artifact",
                parent_status=parent_status,
            )
            if record is None:
                raise TemporalQDNativeError(
                    f"native foundation artifact vanished after directory sync: {target}"
                )
            target_status = _validate_existing_payload(
                record, payload=payload, target=target
            )
            if published:
                _require_same_identity(
                    target_status,
                    temporary_status,
                    name="published native foundation artifact",
                    path=target,
                )
        except Exception as operation_error:
            try:
                _remove_owned_temporary(
                    temporary,
                    temporary_status=temporary_status,
                    parent_status=parent_status,
                )
            except Exception as cleanup_error:
                raise TemporalQDNativeError(
                    f"native foundation publication failed and its owned temporary "
                    f"could not be safely removed: {cleanup_error}"
                ) from operation_error
            raise
        _remove_owned_temporary(
            temporary,
            temporary_status=temporary_status,
            parent_status=parent_status,
        )
    except Exception:
        # ``os.fdopen`` takes ownership only after it succeeds. Ensure a
        # descriptor is not leaked by a rare wrapper-construction failure.
        if descriptor_owned:
            try:
                os.close(descriptor)
            except OSError:
                pass
        raise


def _parse_canonical_json_line(raw: bytes, *, name: str) -> dict[str, Any]:
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TemporalQDNativeError(f"{name} is invalid JSON") from exc
    if not isinstance(value, dict) or raw != canonical_json_bytes(value) + b"\n":
        raise TemporalQDNativeError(f"{name} is not exactly canonical JSON with LF")
    return value


def _load_canonical_object(path: Path, *, name: str) -> dict[str, Any]:
    try:
        _require_regular_file(path, name=name)
        return _parse_canonical_json_line(path.read_bytes(), name=name)
    except OSError as exc:
        raise TemporalQDNativeError(f"could not read {name}: {path}") from exc


def _load_immutable_canonical_object(path: Path, *, name: str) -> dict[str, Any]:
    """Read a canonical object without following an aliased parent or leaf."""

    target = Path(os.path.abspath(os.fspath(path)))
    parent = _require_existing_real_directory_tree(
        target.parent, name=f"{name} parent"
    )
    try:
        parent_status = os.lstat(parent)
    except OSError as exc:
        raise TemporalQDNativeError(f"could not inspect {name} parent: {parent}") from exc
    record = _existing_regular_file(
        target, name=name, parent_status=parent_status
    )
    if record is None:
        raise TemporalQDNativeError(f"{name} is absent: {target}")
    return _parse_canonical_json_line(record[0], name=name)


def _load_immutable_json_object(path: Path, *, name: str) -> dict[str, Any]:
    """Read an immutable JSON object while preserving the caller's newline contract.

    Frozen supervisor files are self-hashed semantic JSON and predate the
    bridge's LF-only internal-manifest convention.  Keep their no-alias file
    checks without incorrectly rejecting their legitimate CRLF/pretty layout.
    """

    target = Path(os.path.abspath(os.fspath(path)))
    parent = _require_existing_real_directory_tree(
        target.parent, name=f"{name} parent"
    )
    try:
        parent_status = os.lstat(parent)
    except OSError as exc:
        raise TemporalQDNativeError(f"could not inspect {name} parent: {parent}") from exc
    record = _existing_regular_file(
        target, name=name, parent_status=parent_status
    )
    if record is None:
        raise TemporalQDNativeError(f"{name} is absent: {target}")
    try:
        value = json.loads(record[0].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TemporalQDNativeError(f"{name} is invalid JSON") from exc
    if not isinstance(value, dict):
        raise TemporalQDNativeError(f"{name} must be a JSON object")
    return value


def _legacy_v5_identity_ledger_binding(
    *, supervisor_config: Mapping[str, Any], ledger_path: Path
) -> dict[str, Any]:
    """Reconstruct only the compact binding that production passes to Rust."""

    frozen_policy = supervisor_config.get("frozenPolicy")
    identity_policy = (
        frozen_policy.get("identity") if isinstance(frozen_policy, Mapping) else None
    )
    policy_name = supervisor_config.get("policyName")
    policy_sha256 = supervisor_config.get("policySha256")
    if (
        not isinstance(identity_policy, Mapping)
        or not isinstance(policy_name, str)
        or not policy_name
    ):
        raise TemporalQDNativeError(
            "legacy v5 G0 migration lacks frozen identity-policy authority"
        )
    _validate_exact_sha256(policy_sha256, name="legacy v5 G0 policySha256")
    return {
        "schemaVersion": G0_IDENTITY_LEDGER_BINDING_SCHEMA,
        "ledgerPath": str(ledger_path.resolve()),
        "policyName": policy_name,
        "policySha256": policy_sha256,
        "identityPolicy": dict(identity_policy),
        "identityPolicySha256": sha256(canonical_json_bytes(identity_policy)),
    }


def derive_legacy_v5_g0_finalization_runtime(
    *,
    supervisor_config_sha256: object,
    pair_generation_runtime: object,
) -> dict[str, Any]:
    """Derive the singleton pre-cutover Rust G0 runtime, or fail closed.

    The preserved v5 checkpoint froze Python-owned proposal construction before
    the separate G0 runtime authority existed.  Its exact config identity is
    intentionally the only one eligible for this deterministic projection.
    """

    config_sha256 = _validate_exact_sha256(
        supervisor_config_sha256, name="legacy v5 supervisor configSha256"
    )
    if config_sha256 != G0_LEGACY_V5_PRE_CUTOVER_CONFIG_SHA256:
        raise TemporalQDNativeError(
            "legacy v5 G0 runtime migration is not authorized for this frozen config"
        )
    pair_runtime = validate_pair_generation_runtime_config(pair_generation_runtime)
    if pair_runtime["engine"] != PAIR_GENERATION_RUNTIME_PYTHON:
        raise TemporalQDNativeError(
            "legacy v5 G0 runtime migration requires the frozen Python construction runtime"
        )
    return build_g0_finalization_runtime_config(
        engine=G0_FINALIZATION_RUNTIME_RUST,
        execution_timeout_seconds=pair_runtime["executionTimeoutSeconds"],
    )


def _legacy_v5_expected_paths(run_root: Path | str) -> tuple[Path, Path, Path, Path, Path]:
    root = _require_existing_real_directory_tree(
        run_root, name="legacy v5 G0 migration run root"
    )
    proposal_root = root / "generations" / "generation-0001" / "proposal"
    proposal_root = _require_existing_real_directory_tree(
        proposal_root, name="legacy v5 G0 migration proposal root"
    )
    config_path = root / "config.json"
    pair_config_path = proposal_root / "pair-config.json"
    ledger_path = root / "identity-ledger.json"
    receipt_path = proposal_root / "internal" / "g0-funnel" / "receipt.json"
    return config_path, proposal_root, pair_config_path, ledger_path, receipt_path


def _legacy_v5_read_pair_config(path: Path) -> dict[str, Any]:
    value = _load_immutable_json_object(path, name="legacy v5 pair config")
    config_sha = _validate_exact_sha256(
        value.get("configSha256"), name="legacy v5 pair configSha256"
    )
    material = {key: item for key, item in value.items() if key != "configSha256"}
    if (
        value.get("schemaVersion") != PAIR_GENERATION_SCHEMA
        or value.get("generationIndex") != 1
        or sha256(canonical_json_bytes(material)) != config_sha
    ):
        raise TemporalQDNativeError("legacy v5 pair config is incompatible")
    return value


def _legacy_v5_validate_receipt(
    receipt: object,
    *,
    runtime: Mapping[str, Any],
    pair_config_sha256: str,
    identity_ledger_binding: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate the compact receipt surface before granting migration authority.

    The regular native G0 transaction re-verifies this receipt and every
    public output before use.  This narrow preflight deliberately checks only
    the immutable bindings necessary to re-open the legacy supervisor config.
    """

    expected = {
        "schemaVersion",
        "requestSha256",
        "authoritySha256",
        "executionAuthority",
        "configSha256",
        "generationIndex",
        "constructionPoolSize",
        "evaluationPopulationSize",
        "operatorImplementationSha256",
        "archivePolicyAuthoritySha256",
        "journalInventorySha256",
        "sourceHandoffSha256",
        "globalIdentityLedger",
        "identityLedgerBinding",
        "g0Bootstrap",
        "population",
        "evaluationPopulation",
        "generationJournal",
        "pairGenerationResult",
        "constructionPoolIdentitySha256",
        "acceptedPoolSha256",
        "selectionSha256",
        "ledgerSha256",
        "receiptSha256",
    }
    if not isinstance(receipt, Mapping) or set(receipt) != expected:
        raise TemporalQDNativeError("legacy v5 native G0 receipt fields are not exact")
    result = dict(receipt)
    if result.get("schemaVersion") != G0_FUNNEL_RECEIPT_SCHEMA:
        raise TemporalQDNativeError("legacy v5 native G0 receipt schema is incompatible")
    receipt_sha = _validate_exact_sha256(
        result.get("receiptSha256"), name="legacy v5 native G0 receiptSha256"
    )
    material = {key: item for key, item in result.items() if key != "receiptSha256"}
    if sha256(canonical_json_bytes(material)) != receipt_sha:
        raise TemporalQDNativeError("legacy v5 native G0 receipt identity mismatch")
    execution_authority = _validate_g0_execution_authority(
        result.get("executionAuthority")
    )
    if (
        result.get("authoritySha256") != execution_authority["authoritySha256"]
        or execution_authority["g0FinalizationRuntimeSha256"]
        != runtime["runtimeSha256"]
        or result.get("configSha256") != pair_config_sha256
        or result.get("generationIndex") != 1
        or result.get("identityLedgerBinding") != identity_ledger_binding
    ):
        raise TemporalQDNativeError("legacy v5 native G0 receipt binding drifted")
    return result


def _validate_legacy_v5_g0_runtime_migration(
    value: object,
    *,
    supervisor_config: Mapping[str, Any],
    run_root: Path,
    pair_runtime: Mapping[str, Any],
) -> dict[str, Any]:
    expected = {
        "schemaVersion",
        "supervisorConfigPath",
        "supervisorConfigSha256",
        "pairGenerationRuntimeSha256",
        "derivedG0FinalizationRuntime",
        "derivedG0FinalizationRuntimeSha256",
        "proposalRoot",
        "proposalConfigPath",
        "proposalConfigSha256",
        "identityLedgerPath",
        "identityLedgerBinding",
        "nativeReceiptPath",
        "nativeReceiptSha256",
        "nativeExecutionAuthority",
        "nativeExecutionAuthoritySha256",
        "migrationSha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise TemporalQDNativeError("legacy v5 G0 migration fields are not exact")
    result = dict(value)
    if result.get("schemaVersion") != G0_LEGACY_V5_RUNTIME_MIGRATION_SCHEMA:
        raise TemporalQDNativeError("legacy v5 G0 migration schema is incompatible")
    migration_sha = _validate_exact_sha256(
        result.get("migrationSha256"), name="legacy v5 G0 migrationSha256"
    )
    material = {key: item for key, item in result.items() if key != "migrationSha256"}
    if sha256(canonical_json_bytes(material)) != migration_sha:
        raise TemporalQDNativeError("legacy v5 G0 migration identity mismatch")
    (
        config_path,
        proposal_root,
        pair_config_path,
        ledger_path,
        receipt_path,
    ) = _legacy_v5_expected_paths(run_root)
    config_sha = _validate_exact_sha256(
        supervisor_config.get("configSha256"), name="legacy v5 supervisor configSha256"
    )
    runtime = derive_legacy_v5_g0_finalization_runtime(
        supervisor_config_sha256=config_sha,
        pair_generation_runtime=pair_runtime,
    )
    pair_config = _legacy_v5_read_pair_config(pair_config_path)
    _require_regular_file(ledger_path, name="legacy v5 identity ledger")
    identity_binding = _legacy_v5_identity_ledger_binding(
        supervisor_config=supervisor_config, ledger_path=ledger_path
    )
    expected_strings = {
        "supervisorConfigPath": str(config_path.resolve()),
        "supervisorConfigSha256": config_sha,
        "pairGenerationRuntimeSha256": pair_runtime["runtimeSha256"],
        "derivedG0FinalizationRuntimeSha256": runtime["runtimeSha256"],
        "proposalRoot": str(proposal_root.resolve()),
        "proposalConfigPath": str(pair_config_path.resolve()),
        "proposalConfigSha256": pair_config["configSha256"],
        "identityLedgerPath": str(ledger_path.resolve()),
        "nativeReceiptPath": str(receipt_path.resolve()),
    }
    if (
        any(result.get(key) != expected_value for key, expected_value in expected_strings.items())
        or result.get("derivedG0FinalizationRuntime") != runtime
        or result.get("identityLedgerBinding") != identity_binding
    ):
        raise TemporalQDNativeError("legacy v5 G0 migration authority drifted")
    for key in (
        "nativeReceiptSha256",
        "nativeExecutionAuthoritySha256",
    ):
        _validate_exact_sha256(result.get(key), name=f"legacy v5 G0 migration {key}")
    execution_authority = _validate_g0_execution_authority(
        result.get("nativeExecutionAuthority")
    )
    if result["nativeExecutionAuthoritySha256"] != execution_authority["authoritySha256"]:
        raise TemporalQDNativeError("legacy v5 G0 migration execution authority drifted")
    receipt = _load_immutable_json_object(
        receipt_path, name="legacy v5 native G0 receipt"
    )
    checked_receipt = _legacy_v5_validate_receipt(
        receipt,
        runtime=runtime,
        pair_config_sha256=pair_config["configSha256"],
        identity_ledger_binding=identity_binding,
    )
    if (
        result["nativeReceiptSha256"] != checked_receipt["receiptSha256"]
        or result["nativeExecutionAuthority"] != checked_receipt["executionAuthority"]
        or result["nativeExecutionAuthoritySha256"]
        != checked_receipt["authoritySha256"]
    ):
        raise TemporalQDNativeError("legacy v5 G0 migration receipt drifted")
    return result


def seal_legacy_v5_g0_runtime_migration(
    *,
    supervisor_config: Mapping[str, Any],
    run_root: Path | str,
) -> dict[str, Any]:
    """Publish the singleton legacy reopen authority after a native seal.

    The immutable migration receipt does not replace the native G0 receipt. It
    binds that receipt to the exact old supervisor config, derived Rust runtime,
    proposal config, and global ledger path so a later supervisor restart has
    a narrow, auditable exception rather than a Python fallback.
    """

    root = _require_existing_real_directory_tree(
        run_root, name="legacy v5 G0 migration run root"
    )
    config_path, proposal_root, pair_config_path, ledger_path, receipt_path = (
        _legacy_v5_expected_paths(root)
    )
    persisted_config = _load_immutable_json_object(
        config_path, name="legacy v5 supervisor config"
    )
    if persisted_config != dict(supervisor_config):
        raise TemporalQDNativeError("legacy v5 supervisor config changed before migration")
    config_sha = _validate_exact_sha256(
        persisted_config.get("configSha256"), name="legacy v5 supervisor configSha256"
    )
    config_material = {
        key: item for key, item in persisted_config.items() if key != "configSha256"
    }
    if sha256(canonical_json_bytes(config_material)) != config_sha:
        raise TemporalQDNativeError("legacy v5 supervisor config identity mismatch")
    if "g0FinalizationRuntime" in persisted_config:
        raise TemporalQDNativeError(
            "legacy v5 G0 migration is forbidden for a config with a frozen G0 runtime"
        )
    pair_runtime = validate_pair_generation_runtime_config(
        persisted_config.get("pairGenerationRuntime")
    )
    runtime = derive_legacy_v5_g0_finalization_runtime(
        supervisor_config_sha256=config_sha,
        pair_generation_runtime=pair_runtime,
    )
    pair_config = _legacy_v5_read_pair_config(pair_config_path)
    # The G0 receipt performs its own durable ledger reduction.  We only prove
    # the frozen path is a real file here; no Python row scan is permitted.
    _require_regular_file(ledger_path, name="legacy v5 identity ledger")
    identity_binding = _legacy_v5_identity_ledger_binding(
        supervisor_config=persisted_config, ledger_path=ledger_path
    )
    receipt = _load_immutable_json_object(
        receipt_path, name="legacy v5 native G0 receipt"
    )
    receipt = _legacy_v5_validate_receipt(
        receipt,
        runtime=runtime,
        pair_config_sha256=pair_config["configSha256"],
        identity_ledger_binding=identity_binding,
    )
    migration: dict[str, Any] = {
        "schemaVersion": G0_LEGACY_V5_RUNTIME_MIGRATION_SCHEMA,
        "supervisorConfigPath": str(config_path.resolve()),
        "supervisorConfigSha256": config_sha,
        "pairGenerationRuntimeSha256": pair_runtime["runtimeSha256"],
        "derivedG0FinalizationRuntime": runtime,
        "derivedG0FinalizationRuntimeSha256": runtime["runtimeSha256"],
        "proposalRoot": str(proposal_root.resolve()),
        "proposalConfigPath": str(pair_config_path.resolve()),
        "proposalConfigSha256": pair_config["configSha256"],
        "identityLedgerPath": str(ledger_path.resolve()),
        "identityLedgerBinding": identity_binding,
        "nativeReceiptPath": str(receipt_path.resolve()),
        "nativeReceiptSha256": receipt["receiptSha256"],
        "nativeExecutionAuthority": receipt["executionAuthority"],
        "nativeExecutionAuthoritySha256": receipt["authoritySha256"],
    }
    migration["migrationSha256"] = sha256(canonical_json_bytes(migration))
    migration_path = proposal_root / G0_LEGACY_V5_RUNTIME_MIGRATION_PATH
    _write_bytes_once(migration_path, canonical_json_bytes(migration) + b"\n")
    persisted_migration = _load_immutable_json_object(
        migration_path, name="legacy v5 G0 migration"
    )
    return _validate_legacy_v5_g0_runtime_migration(
        persisted_migration,
        supervisor_config=persisted_config,
        run_root=root,
        pair_runtime=pair_runtime,
    )


def load_legacy_v5_g0_finalization_runtime(
    *, supervisor_config: Mapping[str, Any], run_root: Path | str
) -> dict[str, Any]:
    """Return the sealed legacy G0 runtime only after every compact binding.

    This is intentionally unavailable to all new configs.  Its caller still
    invokes the native G0 transaction on restart, which re-hashes public
    output and performs the receipt's complete fail-closed adoption check.
    """

    root = _require_existing_real_directory_tree(
        run_root, name="legacy v5 G0 migration run root"
    )
    config_sha = _validate_exact_sha256(
        supervisor_config.get("configSha256"), name="legacy v5 supervisor configSha256"
    )
    if config_sha != G0_LEGACY_V5_PRE_CUTOVER_CONFIG_SHA256:
        raise TemporalQDNativeError(
            "legacy v5 G0 migration is not authorized for this frozen config"
        )
    if "g0FinalizationRuntime" in supervisor_config:
        raise TemporalQDNativeError(
            "legacy v5 G0 migration cannot shadow a frozen G0 runtime"
        )
    pair_runtime = validate_pair_generation_runtime_config(
        supervisor_config.get("pairGenerationRuntime")
    )
    runtime = derive_legacy_v5_g0_finalization_runtime(
        supervisor_config_sha256=config_sha,
        pair_generation_runtime=pair_runtime,
    )
    _, proposal_root, _, _, _ = _legacy_v5_expected_paths(root)
    migration_path = proposal_root / G0_LEGACY_V5_RUNTIME_MIGRATION_PATH
    migration = _load_immutable_json_object(
        migration_path, name="legacy v5 G0 migration"
    )
    checked = _validate_legacy_v5_g0_runtime_migration(
        migration,
        supervisor_config=supervisor_config,
        run_root=root,
        pair_runtime=pair_runtime,
    )
    if checked["derivedG0FinalizationRuntime"] != runtime:
        raise TemporalQDNativeError("legacy v5 G0 migration runtime drifted")
    return runtime


def _load_json_object(path: Path, *, name: str) -> dict[str, Any]:
    try:
        _require_regular_file(path, name=name)
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TemporalQDNativeError(f"could not read {name}: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalQDNativeError(f"{name} must be an object")
    return value


def run_native_foundation(*, output_root: Path | str) -> dict[str, str]:
    """Run one isolated native probe with immutable authority and result files."""

    root = _ensure_real_directory_tree(output_root, name="native foundation output root")
    native_root = _ensure_real_directory_tree(
        root / "performance" / "temporal-qd-native", name="native foundation root"
    )
    binary, authority = ensure_native_batch()
    if _sha256_file(binary) != authority["executableSha256"]:
        raise TemporalQDNativeError(
            "native Temporal QD executable changed after authority verification"
        )
    authority_path = native_root / "authority.json"
    manifest_path = native_root / "manifest.json"
    _write_bytes_once(authority_path, canonical_json_bytes(authority) + b"\n")
    manifest = build_foundation_manifest(authority_sha256=authority["authoritySha256"])
    _write_bytes_once(manifest_path, canonical_json_bytes(manifest) + b"\n")
    completed = _run_checked(
        (str(binary), "--manifest", str(manifest_path)), cwd=_repo_root(), timeout=300
    )
    stdout_result = validate_foundation_result(
        _parse_canonical_json_line(
            completed.stdout, name="native foundation stdout result"
        ),
        manifest=manifest,
    )
    file_result = validate_foundation_result(
        _load_canonical_object(native_root / NATIVE_RESULT_FILENAME, name="native foundation result"),
        manifest=manifest,
    )
    if file_result != stdout_result:
        raise TemporalQDNativeError("native foundation stdout/result artifact disagreement")
    return file_result


def _is_directional_v5_policy_material(value: object) -> bool:
    """Recognize a v5 policy marker without accepting a generic bridge route."""

    if not isinstance(value, Mapping):
        return False
    # Kept local and lazy so this legacy bridge does not gain an import-time
    # dependency on the evolution implementation it must no longer serve.
    from .temporal_qd_evolution import (
        DIRECTIONAL_QD_POLICY_NAME,
        DIRECTIONAL_QD_POLICY_SHA256,
    )

    frozen = value.get("frozenPolicy")
    return (
        value.get("policyName") == DIRECTIONAL_QD_POLICY_NAME
        or value.get("policySha256") == DIRECTIONAL_QD_POLICY_SHA256
        or (
            isinstance(frozen, Mapping)
            and (
                frozen.get("policyName") == DIRECTIONAL_QD_POLICY_NAME
                or "directionSelection" in frozen
            )
        )
    )


def _reject_directional_v5_legacy_native_generation(
    *,
    runtime_authority: Mapping[str, Any],
    generation_config: Mapping[str, Any],
    qd_version: str,
    policy_name: str,
    policy_sha256: str,
    frozen_policy: Mapping[str, Any],
) -> None:
    """Keep the generic v4 native bridge from becoming a v5 side entrance."""

    bindings: list[object] = [
        {
            "qdVersion": qd_version,
            "policyName": policy_name,
            "policySha256": policy_sha256,
            "frozenPolicy": frozen_policy,
        },
        generation_config,
        runtime_authority,
    ]
    binding_index = 0
    seen_binding_ids: set[int] = set()
    while binding_index < len(bindings):
        binding = bindings[binding_index]
        binding_index += 1
        if isinstance(binding, Mapping):
            binding_id = id(binding)
            if binding_id in seen_binding_ids:
                continue
            seen_binding_ids.add(binding_id)
            bindings.extend(
                binding.get(key)
                for key in (
                    "archivePolicyAuthority",
                    "evolvableModuleAuthority",
                    "runConfig",
                    "pairRunConfig",
                )
            )
    if any(_is_directional_v5_policy_material(binding) for binding in bindings):
        raise TemporalQDNativeError(
            "directional/v5 construction is unavailable through "
            "run_native_generation; use run_native_v5_generation_construction"
        )


def run_native_generation(
    *,
    output_root: Path | str,
    parent_archive_path: Path | str,
    parent_archive_sha256: str | None,
    runtime_authority: Mapping[str, Any],
    generation_config: Mapping[str, Any],
    identity_ledger_path: Path | str,
    max_new_proposals: int | None,
    native_execution_timeout_seconds: int,
    allow_empty_quality_bootstrap: bool,
    g0_evaluation_width: int | None,
    frozen_construction_catalog: Mapping[str, Any] | None,
    qd_version: str,
    policy_name: str,
    policy_sha256: str,
    frozen_policy: Mapping[str, Any],
) -> dict[str, Any]:
    """Execute the selected native generation once; errors never fall back."""

    _reject_directional_v5_legacy_native_generation(
        runtime_authority=runtime_authority,
        generation_config=generation_config,
        qd_version=qd_version,
        policy_name=policy_name,
        policy_sha256=policy_sha256,
        frozen_policy=frozen_policy,
    )
    root = _ensure_real_directory_tree(output_root, name="native generation output root")
    native_base = _ensure_real_directory_tree(
        root / "native-batch", name="native generation invocation base"
    )
    archive_path = Path(parent_archive_path).resolve()
    _require_regular_file(archive_path, name="native generation parent archive")
    checked_parent_archive_sha256 = _validate_exact_sha256(
        parent_archive_sha256,
        name="native generation parent archiveSha256",
    )
    ledger_path = Path(identity_ledger_path).resolve()
    ledger = _load_json_object(ledger_path, name="native generation identity ledger")
    identity_ledger_sha256 = _validate_exact_sha256(
        ledger.get("ledgerSha256"), name="identity ledger ledgerSha256"
    )
    if sha256(
        canonical_json_bytes(
            {key: item for key, item in ledger.items() if key != "ledgerSha256"}
        )
    ) != identity_ledger_sha256:
        raise TemporalQDNativeError("identity ledger self-hash mismatch")
    binary, authority = ensure_native_batch()
    if _sha256_file(binary) != authority["executableSha256"]:
        raise TemporalQDNativeError(
            "native Temporal QD executable changed after authority verification"
        )
    manifest = build_generation_manifest(
        authority_sha256=authority["authoritySha256"],
        runtime_authority=runtime_authority,
        parent_archive_path=archive_path,
        parent_archive_sha256=checked_parent_archive_sha256,
        identity_ledger_path=ledger_path,
        identity_ledger_sha256=identity_ledger_sha256,
        output_root=root,
        generation_config=generation_config,
        max_new_proposals=max_new_proposals,
        native_execution_timeout_seconds=native_execution_timeout_seconds,
        allow_empty_quality_bootstrap=allow_empty_quality_bootstrap,
        g0_evaluation_width=g0_evaluation_width,
        frozen_construction_catalog=frozen_construction_catalog,
        qd_version=qd_version,
        policy_name=policy_name,
        policy_sha256=policy_sha256,
        frozen_policy=frozen_policy,
    )
    native_root = _ensure_real_directory_tree(
        native_base / manifest["manifestSha256"].removeprefix("sha256:"),
        name="native generation invocation root",
    )
    authority_path = native_root / "authority.json"
    manifest_path = native_root / "manifest.json"
    _write_bytes_once(authority_path, canonical_json_bytes(authority) + b"\n")
    _write_bytes_once(manifest_path, canonical_json_bytes(manifest) + b"\n")
    result_path = native_root / NATIVE_GENERATION_RESULT_FILENAME
    existing_result = _existing_regular_file(
        result_path, name="native generation result"
    )
    if existing_result is not None:
        cached_result = validate_generation_result(
            _parse_canonical_json_line(
                existing_result[0], name="native generation result"
            ),
            manifest=manifest,
        )
        # An immutable result is already bound to this exact authority and
        # manifest, so a restart must not re-run native finalization or apply
        # fresh-run resource admission checks.
        return cached_result["pairGenerationResult"]
    target_unique_candidates = generation_config.get("targetUniqueCandidates")
    if (
        isinstance(target_unique_candidates, int)
        and not isinstance(target_unique_candidates, bool)
        and target_unique_candidates > 0
    ):
        _assert_native_prelaunch_resources(
            output_root=root,
            target_unique_candidates=target_unique_candidates,
        )
    completed = _run_checked(
        (str(binary), "--manifest", str(manifest_path)),
        cwd=_repo_root(),
        timeout=float(native_execution_timeout_seconds),
    )
    stdout_result = validate_generation_result(
        _parse_canonical_json_line(
            completed.stdout, name="native generation stdout result"
        ),
        manifest=manifest,
    )
    file_result = validate_generation_result(
        _load_canonical_object(
            result_path,
            name="native generation result",
        ),
        manifest=manifest,
    )
    if file_result != stdout_result:
        raise TemporalQDNativeError(
            "native generation stdout/result artifact disagreement"
        )
    # Return the already-validated existing Python compatibility shape without
    # adding, removing, or renaming any inner field.
    return file_result["pairGenerationResult"]


def validate_g0_admission_thread_cap(value: object) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 1 <= value <= G0_ADMISSION_THREAD_CAP_MAXIMUM
    ):
        raise TemporalQDNativeError(
            "native G0 admission thread cap must be an integer within "
            f"1..={G0_ADMISSION_THREAD_CAP_MAXIMUM}"
        )
    return value


def _parse_native_g0_diagnostics(raw: bytes) -> dict[str, Any] | None:
    """Decode the optional untrusted Rust timing line without affecting results.

    The compact stdout result and sealed receipt remain the only semantic
    authority.  This line exists solely for external release evidence, so a
    malformed line fails the bridge rather than being silently misreported.
    """

    try:
        lines = raw.decode("utf-8", errors="strict").splitlines()
    except UnicodeDecodeError as exc:
        raise TemporalQDNativeError("native G0 diagnostics are not UTF-8") from exc
    payloads = [line.removeprefix(_G0_DIAGNOSTICS_PREFIX) for line in lines if line.startswith(_G0_DIAGNOSTICS_PREFIX)]
    if not payloads:
        return None
    if len(payloads) != 1:
        raise TemporalQDNativeError("native G0 emitted multiple diagnostics records")
    try:
        value = json.loads(payloads[0])
    except json.JSONDecodeError as exc:
        raise TemporalQDNativeError("native G0 diagnostics are not JSON") from exc
    expected = {
        "schemaVersion",
        "mode",
        "threadCap",
        "totalElapsedMilliseconds",
        "configurationBindingMilliseconds",
        "journalAdmission",
        "populationStreamingMilliseconds",
        "reductionMilliseconds",
        "publicationMilliseconds",
        "sealingMilliseconds",
        "outputArtifactBytes",
        "outputBytesHashed",
    }
    if not isinstance(value, Mapping) or set(value) != expected:
        raise TemporalQDNativeError("native G0 diagnostics fields are not exact")
    result = dict(value)
    if result["schemaVersion"] != G0_DIAGNOSTICS_SCHEMA or result["mode"] not in {
        "adopted",
        "completed",
        "construction_incomplete",
    }:
        raise TemporalQDNativeError("native G0 diagnostics are incompatible")
    result["threadCap"] = validate_g0_admission_thread_cap(result["threadCap"])
    for field in (
        "totalElapsedMilliseconds",
        "configurationBindingMilliseconds",
        "populationStreamingMilliseconds",
        "reductionMilliseconds",
        "publicationMilliseconds",
        "sealingMilliseconds",
        "outputArtifactBytes",
        "outputBytesHashed",
    ):
        if isinstance(result[field], bool) or not isinstance(result[field], int) or result[field] < 0:
            raise TemporalQDNativeError(f"native G0 diagnostics {field} is invalid")
    journal = result["journalAdmission"]
    if journal is None:
        return result
    journal_expected = {
        "entryCount",
        "acceptedCount",
        "sourceBytesRead",
        "admissionSourceBytesRead",
        "populationSourceBytesRead",
        "workerCount",
        "enumerationMilliseconds",
        "admissionMilliseconds",
        "mergeMilliseconds",
    }
    if not isinstance(journal, Mapping) or set(journal) != journal_expected:
        raise TemporalQDNativeError("native G0 journal diagnostics fields are not exact")
    normalized_journal = dict(journal)
    for field, item in normalized_journal.items():
        if isinstance(item, bool) or not isinstance(item, int) or item < 0:
            raise TemporalQDNativeError(f"native G0 journal diagnostics {field} is invalid")
    if normalized_journal["workerCount"] > G0_ADMISSION_THREAD_CAP_MAXIMUM:
        raise TemporalQDNativeError("native G0 journal diagnostics worker count is invalid")
    result["journalAdmission"] = normalized_journal
    return result


def run_native_g0_funnel(
    *,
    output_root: Path | str,
    g0_finalization_runtime: Mapping[str, Any],
    generation_config: Mapping[str, Any],
    evaluation_population_size: int,
    publication_policy: Mapping[str, Any],
    identity_ledger_binding: Mapping[str, Any] | None,
    audit: bool = False,
    admission_thread_cap: int = G0_ADMISSION_THREAD_CAP_DEFAULT,
    on_process_started: Callable[[subprocess.Popen[bytes]], None] | None = None,
    on_diagnostics: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Run the single Rust-owned post-construction G0 transaction.

    Unlike the older front-half bridge, this deliberately invokes native code
    on every restart.  A cheap native adoption verifies the sealed receipt and
    output binding without reading proposal-journal bytes; returning a cached
    Python bridge result here would skip that fail-closed check.
    """

    root = _require_existing_real_directory_tree(
        output_root, name="native G0 funnel output root"
    )
    runtime = validate_g0_finalization_runtime_config(g0_finalization_runtime)
    if runtime["engine"] != G0_FINALIZATION_RUNTIME_RUST:
        raise TemporalQDNativeError("native G0 funnel selected a non-Rust runtime")
    admission_thread_cap = validate_g0_admission_thread_cap(admission_thread_cap)
    # Resolve the actual executable authority *before* constructing the
    # manifest.  A receipt is not reusable across an executable/source drift.
    binary, authority = ensure_native_batch()
    authority = validate_native_authority(authority)
    if _sha256_file(binary) != authority["executableSha256"]:
        raise TemporalQDNativeError(
            "native Temporal QD executable changed after authority verification"
        )
    if native_source_sha256() != authority["sourceSha256"]:
        raise TemporalQDNativeError(
            "native Temporal QD source changed after authority verification"
        )
    manifest = build_g0_funnel_manifest(
        g0_finalization_runtime=runtime,
        output_root=root,
        generation_config=generation_config,
        evaluation_population_size=evaluation_population_size,
        publication_policy=publication_policy,
        native_batch_authority=authority,
        identity_ledger_binding=identity_ledger_binding,
        audit=audit,
    )
    native_base = _ensure_real_directory_tree(
        root / "native-batch" / "g0-funnel", name="native G0 funnel invocation base"
    )
    if _sha256_file(binary) != authority["executableSha256"]:
        raise TemporalQDNativeError(
            "native Temporal QD executable changed after authority verification"
        )
    if native_source_sha256() != authority["sourceSha256"]:
        raise TemporalQDNativeError(
            "native Temporal QD source changed after authority verification"
        )
    native_root = _ensure_real_directory_tree(
        native_base / manifest["manifestSha256"].removeprefix("sha256:"),
        name="native G0 funnel invocation root",
    )
    authority_path = native_root / "authority.json"
    manifest_path = native_root / "manifest.json"
    _write_bytes_once(authority_path, canonical_json_bytes(authority) + b"\n")
    _write_bytes_once(manifest_path, canonical_json_bytes(manifest) + b"\n")
    completed = _run_checked(
        (str(binary), "--manifest", str(manifest_path)),
        cwd=_repo_root(),
        timeout=float(runtime["executionTimeoutSeconds"]),
        env={**os.environ, G0_ADMISSION_THREAD_CAP_ENV: str(admission_thread_cap)},
        on_process_started=on_process_started,
    )
    diagnostics = _parse_native_g0_diagnostics(completed.stderr)
    if diagnostics is not None:
        if diagnostics["threadCap"] != admission_thread_cap:
            raise TemporalQDNativeError("native G0 diagnostics thread cap drifted")
        if on_diagnostics is not None:
            on_diagnostics(diagnostics)
    stdout_result = validate_g0_funnel_result(
        _parse_canonical_json_line(
            completed.stdout, name="native G0 funnel stdout result"
        ),
        manifest=manifest,
    )
    result_path = native_root / G0_FUNNEL_RESULT_FILENAME
    existing_result = _existing_regular_file(
        result_path, name="native G0 funnel result"
    )
    if stdout_result["status"] == "construction_incomplete":
        if existing_result is not None:
            raise TemporalQDNativeError(
                "incomplete native G0 invocation found a sealed bridge result"
            )
        return stdout_result
    if existing_result is None:
        if stdout_result["status"] == "completed":
            raise TemporalQDNativeError(
                "completed native G0 transaction did not publish its immutable bridge result"
            )
        # The receipt, not the optional original bridge result, is the
        # authoritative adoption checkpoint.  A preserved receipt can exist
        # without this newer bridge-side convenience artifact.
        return stdout_result
    file_result = validate_g0_funnel_result(
        _parse_canonical_json_line(
            existing_result[0], name="native G0 funnel result"
        ),
        manifest=manifest,
    )
    if (
        file_result.get("pairGenerationResult")
        != stdout_result.get("pairGenerationResult")
        or file_result.get("receipt") != stdout_result.get("receipt")
    ):
        raise TemporalQDNativeError(
            "native G0 stdout/result artifact disagreement"
        )
    return stdout_result


__all__ = [
    "NATIVE_AUTHORITY_SCHEMA",
    "NATIVE_BINARY_ENV",
    "NATIVE_CONTRACT_VERSION",
    "NATIVE_MANIFEST_SCHEMA",
    "NATIVE_MINIMUM_HOST_AVAILABLE_BYTES_ENV",
    "NATIVE_RESULT_SCHEMA",
    "G0_FINALIZATION_RUNTIME_DEFAULT",
    "G0_FINALIZATION_RUNTIME_PYTHON_ORACLE",
    "G0_FINALIZATION_RUNTIME_RUST",
    "G0_FINALIZATION_RUNTIME_SCHEMA",
    "G0_LEGACY_V5_PRE_CUTOVER_CONFIG_SHA256",
    "G0_LEGACY_V5_RUNTIME_MIGRATION_PATH",
    "G0_LEGACY_V5_RUNTIME_MIGRATION_SCHEMA",
    "G0_FUNNEL_MANIFEST_SCHEMA",
    "G0_FUNNEL_RECEIPT_SCHEMA",
    "G0_FUNNEL_RESULT_SCHEMA",
    "G0_ADMISSION_THREAD_CAP_DEFAULT",
    "G0_ADMISSION_THREAD_CAP_MAXIMUM",
    "PAIR_GENERATION_RUNTIME_PYTHON",
    "PAIR_GENERATION_RUNTIME_RUST",
    "PAIR_GENERATION_RUNTIME_DEFAULT",
    "PAIR_GENERATION_RUNTIME_SCHEMA",
    "TemporalQDNativeError",
    "build_generation_manifest",
    "build_generation_runtime_authority",
    "build_g0_finalization_runtime_config",
    "build_g0_funnel_manifest",
    "build_pair_generation_runtime_config",
    "derive_legacy_v5_g0_finalization_runtime",
    "build_foundation_manifest",
    "build_native_authority",
    "ensure_native_batch",
    "require_native_batch",
    "native_batch_binary_path",
    "native_source_sha256",
    "native_workspace_manifest",
    "resolve_native_batch_binary",
    "run_native_foundation",
    "run_native_g0_funnel",
    "run_native_generation",
    "seal_legacy_v5_g0_runtime_migration",
    "validate_foundation_manifest",
    "validate_foundation_result",
    "validate_generation_manifest",
    "validate_generation_result",
    "validate_g0_finalization_runtime_config",
    "validate_g0_admission_thread_cap",
    "validate_g0_funnel_manifest",
    "validate_g0_funnel_result",
    "validate_native_version",
    "validate_pair_generation_runtime_config",
    "load_legacy_v5_g0_finalization_runtime",
]
