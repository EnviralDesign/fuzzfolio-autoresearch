from __future__ import annotations

from collections.abc import Mapping, Sequence
import copy
from datetime import datetime
import json
import math
import os
from pathlib import Path
import random
import re
import queue
import subprocess
import tempfile
import threading
from typing import Any, Protocol
import uuid

from .temporal_search import (
    TEMPORAL_SEARCH_PREPARATION_SCHEMA,
    TemporalSearchContractError,
    build_authority,
    canonical_sha256,
    validate_authority,
)
from .lake_window import (
    LakeWindowBinding,
    lake_window_request_contains,
    resolve_replay_lake_window_request,
)

from .temporal_discovery_base import *
from .temporal_discovery_mutation import *
from .temporal_qd_observability import timed_span

AUTHORED_VALIDATION_BINDING_SCHEMA = "temporal_authored_validation_binding_v1"
AUTHORED_VALIDATOR_PROVENANCE_SCHEMA = "temporal_authored_validator_provenance_v1"
_AUTHORED_VALIDATION_BINDING_FIELDS = {
    "schemaVersion",
    "rawSourceProfileSha256",
    "profileSnapshotSha256",
    "programSha256",
    "validationReportSha256",
    "validatorProvenance",
}
_AUTHORED_VALIDATOR_PROVENANCE_FIELDS = {
    "schemaVersion",
    "validationContractSha256",
    "validatorSchema",
    "fuzzfolioCommit",
    "validatorCommandSha256",
    "commandProvenance",
}
_VALIDATOR_COMMAND_PROVENANCE = {
    "declared_subprocess_command",
    "protocol_command_unavailable",
}

_JSONL_VALIDATOR_PROTOCOL_VERSION = "temporal_search_candidate_validation_jsonl_v1"
_JSONL_REQUEST_SCHEMA = "temporal_search_candidate_validation_jsonl_request_v1"
_JSONL_RESPONSE_SCHEMA = "temporal_search_candidate_validation_jsonl_response_v1"
_JSONL_VALIDATE_CANDIDATE_OPERATION = "validate_candidate"
_JSONL_COMPILE_BIDIRECTIONAL_OPERATION = "compile_bidirectional"
_JSONL_COMPILE_BIDIRECTIONAL_REQUEST_SCHEMA = (
    "temporal_search_bidirectional_compile_jsonl_request_v1"
)
_JSONL_COMPILE_BIDIRECTIONAL_RESPONSE_SCHEMA = (
    "temporal_search_bidirectional_compile_jsonl_response_v1"
)
_JSONL_COMPILE_BIDIRECTIONAL_RESULT_SCHEMA = (
    "temporal_search_bidirectional_compile_result_v1"
)
_JSONL_REQUEST_FIELDS = {
    "schemaVersion",
    "operation",
    "requestId",
    "candidateId",
    "expectedRawSourceProfileSha256",
    "sourceProfile",
}
_JSONL_RESPONSE_FIELDS = {
    "schemaVersion",
    "requestId",
    "operation",
    "semanticExitCode",
    "report",
}
_JSONL_ERROR_RESPONSE_FIELDS = {
    "schemaVersion",
    "requestId",
    "operation",
    "semanticExitCode",
    "error",
}
_JSONL_COMPILE_RESPONSE_FIELDS = {"schemaVersion", "requestId", "operation", "semanticExitCode", "result"}
_JSONL_COMPILE_REQUEST_FIELDS = {"schemaVersion", "operation", "requestId", "candidateId", "longProfile", "shortProfile", "expectedLongRawSourceProfileSha256", "expectedShortRawSourceProfileSha256"}


def _bounded_bytes_append(target: bytearray, data: bytes, *, maximum: int) -> None:
    target.extend(data)
    if len(target) > maximum:
        del target[: len(target) - maximum]


def _reject_non_finite_json(value: str) -> None:
    raise ValueError(f"non-finite JSON value is not permitted: {value}")


class _PersistentJsonlValidator:
    """One ordered, fail-closed JSONL validator process.

    Requests are never retried after bytes are written: the server might have
    already evaluated them.  A later public ``validate`` call may create a new
    server after this transport has been discarded.
    """

    def __init__(
        self,
        command: Sequence[str],
        *,
        timeout_seconds: float,
        max_line_bytes: int,
        stderr_limit_bytes: int,
        environment: Mapping[str, str] | None = None,
    ) -> None:
        self._max_line_bytes = max_line_bytes
        self._timeout_seconds = timeout_seconds
        self._stderr_limit_bytes = stderr_limit_bytes
        self._stderr = bytearray()
        self._responses: queue.Queue[bytes | None] = queue.Queue()
        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        with timed_span("native.transport.spawn_process"):
            self._process = subprocess.Popen(
                [*command, "--jsonl-server", "--jsonl-max-line-bytes", str(max_line_bytes)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
                env={**dict(os.environ), **dict(environment or {})},
                creationflags=creationflags,
            )
        with timed_span("native.transport.start_reader_threads"):
            self._stdout_thread = threading.Thread(
                target=self._drain_stdout,
                name="temporal-validator-jsonl-stdout",
                daemon=True,
            )
            self._stderr_thread = threading.Thread(
                target=self._drain_stderr,
                name="temporal-validator-jsonl-stderr",
                daemon=True,
            )
            self._stdout_thread.start()
            self._stderr_thread.start()

    def _drain_stdout(self) -> None:
        assert self._process.stdout is not None
        while True:
            try:
                line = self._process.stdout.readline(self._max_line_bytes + 1)
            except (OSError, ValueError):
                return
            if not line:
                self._responses.put(None)
                return
            self._responses.put(line)

    def _drain_stderr(self) -> None:
        assert self._process.stderr is not None
        while True:
            try:
                chunk = self._process.stderr.read(4096)
            except (OSError, ValueError):
                return
            if not chunk:
                return
            _bounded_bytes_append(
                self._stderr, chunk, maximum=self._stderr_limit_bytes
            )

    def _stderr_summary(self) -> str:
        return bytes(self._stderr).decode("utf-8", errors="replace").strip()

    def close(self) -> None:
        process = self._process
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2.0)
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None:
                try:
                    stream.close()
                except OSError:
                    pass

    def _fail(self, message: str) -> TemporalDiscoveryInfrastructureError:
        stderr = self._stderr_summary()
        self.close()
        suffix = f"; stderr={stderr!r}" if stderr else ""
        return TemporalDiscoveryInfrastructureError(message + suffix)

    def validate(
        self,
        *,
        candidate_id: str,
        source_profile: Mapping[str, Any],
        expected_raw_source_profile_sha256: str,
    ) -> dict[str, Any]:
        if self._process.poll() is not None:
            raise self._fail(
                "persistent candidate validator exited before request dispatch"
            )
        with timed_span("native.validate.encode_request") as span:
            request_id = uuid.uuid4().hex
            request = {
                "schemaVersion": _JSONL_REQUEST_SCHEMA,
                "operation": _JSONL_VALIDATE_CANDIDATE_OPERATION,
                "requestId": request_id,
                "candidateId": candidate_id,
                "expectedRawSourceProfileSha256": expected_raw_source_profile_sha256,
                "sourceProfile": dict(source_profile),
            }
            try:
                encoded = (
                    json.dumps(
                        request,
                        sort_keys=True,
                        ensure_ascii=True,
                        allow_nan=False,
                        separators=(",", ":"),
                    ).encode("utf-8")
                    + b"\n"
                )
            except (TypeError, ValueError) as exc:
                raise TemporalDiscoveryContractError(
                    "candidate validator request cannot be represented as finite JSON"
                ) from exc
            span.annotate(requestBytes=len(encoded))
        if len(encoded) > self._max_line_bytes:
            raise TemporalDiscoveryContractError(
                "candidate validator request exceeds persistent JSONL line limit"
            )
        with timed_span("native.validate.dispatch_request", requestBytes=len(encoded)):
            try:
                assert self._process.stdin is not None
                self._process.stdin.write(encoded)
                self._process.stdin.flush()
            except (BrokenPipeError, OSError, ValueError) as exc:
                raise self._fail(
                    "persistent candidate validator failed while dispatching request"
                ) from exc
        with timed_span("native.validate.wait_response") as span:
            try:
                line = self._responses.get(timeout=self._timeout_seconds)
            except queue.Empty as exc:
                raise self._fail(
                    "persistent candidate validator timed out; request outcome is ambiguous"
                ) from exc
            span.annotate(responseBytes=len(line) if line is not None else 0)
        if line is None:
            raise self._fail(
                "persistent candidate validator exited before responding"
            )
        if len(line) > self._max_line_bytes or not line.endswith(b"\n"):
            raise self._fail(
                "persistent candidate validator response exceeds JSONL line limit"
            )
        with timed_span("native.validate.decode_response", responseBytes=len(line)):
            try:
                response = json.loads(
                    line.decode("utf-8"), parse_constant=_reject_non_finite_json
                )
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                raise self._fail(
                    "persistent candidate validator returned malformed JSONL response"
                ) from exc
        if not isinstance(response, Mapping):
            raise self._fail(
                "persistent candidate validator response must be an object"
            )
        if response.get("requestId") != request_id:
            raise self._fail(
                "persistent candidate validator response request ID mismatch"
            )
        if response.get("schemaVersion") != _JSONL_RESPONSE_SCHEMA:
            raise self._fail(
                "persistent candidate validator returned an unknown protocol schema"
            )
        if response.get("operation") != _JSONL_VALIDATE_CANDIDATE_OPERATION:
            raise self._fail(
                "persistent candidate validator response operation mismatch"
            )
        semantic_exit = response.get("semanticExitCode")
        if semantic_exit == 1:
            if set(response) != _JSONL_ERROR_RESPONSE_FIELDS:
                raise self._fail(
                    "persistent candidate validator error response fields are not exact"
                )
            raise self._fail("persistent candidate validator rejected its request")
        if set(response) != _JSONL_RESPONSE_FIELDS:
            raise self._fail(
                "persistent candidate validator response fields are not exact"
            )
        report = response.get("report")
        if not isinstance(report, Mapping):
            raise self._fail("persistent candidate validator response lacks report")
        report = dict(report)
        if report.get("schemaVersion") != TEMPORAL_SEARCH_VALIDATION_SCHEMA:
            raise self._fail("candidate validator returned an unknown schema")
        if (
            isinstance(semantic_exit, bool)
            or not isinstance(semantic_exit, int)
            or semantic_exit not in {0, 2}
        ):
            raise self._fail(
                "persistent candidate validator returned invalid semantic exit code"
            )
        if semantic_exit == 0 and report.get("candidateAcceptable") is not True:
            raise self._fail("validator exit code and acceptance disagree")
        if semantic_exit == 2 and report.get("candidateAcceptable") is not False:
            raise self._fail("validator rejection exit code and report disagree")
        return report

    def compile_pair(self, *, candidate_id: str, long_profile: Mapping[str, Any], short_profile: Mapping[str, Any], expected_long_raw_source_profile_sha256: str, expected_short_raw_source_profile_sha256: str) -> dict[str, Any]:
        """Dispatch one non-retriable Dashboard v2+v2 -> v3/both compilation."""
        if self._process.poll() is not None:
            raise self._fail("persistent bidirectional compiler exited before request dispatch")
        candidate = str(candidate_id).strip()
        long_sha = _sha(expected_long_raw_source_profile_sha256, name="expected long raw source profile SHA-256")
        short_sha = _sha(expected_short_raw_source_profile_sha256, name="expected short raw source profile SHA-256")
        if canonical_sha256(long_profile) != long_sha or canonical_sha256(short_profile) != short_sha:
            raise TemporalDiscoveryContractError("bidirectional compiler request profile identity mismatch")
        with timed_span("native.compile_pair.encode_request") as span:
            request_id = uuid.uuid4().hex
            request = {"schemaVersion": _JSONL_COMPILE_BIDIRECTIONAL_REQUEST_SCHEMA, "operation": _JSONL_COMPILE_BIDIRECTIONAL_OPERATION, "requestId": request_id, "candidateId": candidate, "longProfile": dict(long_profile), "shortProfile": dict(short_profile), "expectedLongRawSourceProfileSha256": long_sha, "expectedShortRawSourceProfileSha256": short_sha}
            try:
                encoded = json.dumps(request, sort_keys=True, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("utf-8") + b"\n"
            except (TypeError, ValueError) as exc:
                raise TemporalDiscoveryContractError("bidirectional compiler request cannot be represented as finite JSON") from exc
            span.annotate(requestBytes=len(encoded))
        if len(encoded) > self._max_line_bytes:
            raise TemporalDiscoveryContractError("bidirectional compiler request exceeds persistent JSONL line limit")
        with timed_span("native.compile_pair.dispatch_request", requestBytes=len(encoded)):
            try:
                assert self._process.stdin is not None
                self._process.stdin.write(encoded); self._process.stdin.flush()
            except (BrokenPipeError, OSError, ValueError) as exc:
                raise self._fail("persistent bidirectional compiler failed while dispatching request") from exc
        with timed_span("native.compile_pair.wait_response") as span:
            try:
                line = self._responses.get(timeout=self._timeout_seconds)
            except queue.Empty as exc:
                raise self._fail("persistent bidirectional compiler timed out; request outcome is ambiguous") from exc
            span.annotate(responseBytes=len(line) if line is not None else 0)
        if line is None:
            raise self._fail("persistent bidirectional compiler exited before responding")
        if len(line) > self._max_line_bytes or not line.endswith(b"\n"):
            raise self._fail("persistent bidirectional compiler response exceeds JSONL line limit")
        with timed_span("native.compile_pair.decode_response", responseBytes=len(line)):
            try:
                response = json.loads(line.decode("utf-8"), parse_constant=_reject_non_finite_json)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                raise self._fail("persistent bidirectional compiler returned malformed JSONL response") from exc
        if not isinstance(response, Mapping) or response.get("requestId") != request_id:
            raise self._fail("persistent bidirectional compiler response request ID mismatch")
        if response.get("schemaVersion") != _JSONL_COMPILE_BIDIRECTIONAL_RESPONSE_SCHEMA or response.get("operation") != _JSONL_COMPILE_BIDIRECTIONAL_OPERATION:
            raise self._fail("persistent bidirectional compiler returned an unknown protocol response")
        if response.get("semanticExitCode") == 1:
            if set(response) != _JSONL_ERROR_RESPONSE_FIELDS:
                raise self._fail("persistent bidirectional compiler error response fields are not exact")
            raise self._fail("persistent bidirectional compiler rejected its request")
        if set(response) != _JSONL_COMPILE_RESPONSE_FIELDS or response.get("semanticExitCode") != 0:
            raise self._fail("persistent bidirectional compiler returned invalid semantic exit code")
        result = response.get("result")
        required = {"schemaVersion", "candidateId", "longRawSourceProfileSha256", "shortRawSourceProfileSha256", "rawSourceProfileSha256", "profileSnapshotSha256", "programSha256", "validationReportSha256", "evaluatorId", "profile", "report"}
        if not isinstance(result, Mapping) or set(result) != required or result.get("schemaVersion") != _JSONL_COMPILE_BIDIRECTIONAL_RESULT_SCHEMA:
            raise self._fail("persistent bidirectional compiler result fields are not exact")
        if result.get("candidateId") != candidate or result.get("longRawSourceProfileSha256") != long_sha or result.get("shortRawSourceProfileSha256") != short_sha:
            raise self._fail("persistent bidirectional compiler result identity mismatch")
        profile, report = result.get("profile"), result.get("report")
        if not isinstance(profile, Mapping) or profile.get("version") != "v3" or profile.get("directionMode") != "both" or not isinstance(report, Mapping):
            raise self._fail("persistent bidirectional compiler did not return a v3/both profile")
        raw_sha = canonical_sha256(profile)
        for field in ("rawSourceProfileSha256", "profileSnapshotSha256", "programSha256", "validationReportSha256"):
            _sha(result.get(field), name=f"bidirectional compiler {field}")
        if result.get("rawSourceProfileSha256") != raw_sha or report.get("schemaVersion") != TEMPORAL_SEARCH_VALIDATION_SCHEMA or report.get("candidateId") != candidate:
            raise self._fail("persistent bidirectional compiler result report mismatch")
        for field in ("rawSourceProfileSha256", "profileSnapshotSha256", "programSha256", "validationReportSha256", "evaluatorId"):
            if report.get(field) != result.get(field):
                raise self._fail("persistent bidirectional compiler report identities mismatch")
        if report.get("candidateAcceptable") is not True or report.get("status") != "valid_evaluable":
            raise self._fail("persistent bidirectional compiler did not admit its pair")
        # ``result`` was just decoded from one response line and has passed the
        # complete closed-schema/identity validation above.  It is request
        # owned (never cached or shared with another caller), so returning the
        # outer mapping directly avoids a full JSON clone before the immutable
        # genome boundary performs its required freeze.
        return dict(result)

class SubprocessCandidateValidator:
    """Call the FuzzFolio-owned validator without duplicating its grammar."""

    def __init__(
        self,
        command: Sequence[str],
        *,
        timeout_seconds: float = 30.0,
        persistent_jsonl: bool = False,
        persistent_max_line_bytes: int = 8 * 1024 * 1024,
        persistent_stderr_limit_bytes: int = 64 * 1024,
        persistent_environment: Mapping[str, str] | None = None,
    ) -> None:
        if not command or any(not str(part).strip() for part in command):
            raise TemporalDiscoveryContractError(
                "validator command must be a non-empty argument array"
            )
        self.command = tuple(str(part) for part in command)
        self.timeout_seconds = _number(
            timeout_seconds,
            name="validator timeout",
            minimum=1.0,
            maximum=300.0,
        )
        if not isinstance(persistent_jsonl, bool):
            raise TemporalDiscoveryContractError(
                "persistent JSONL validator mode must be a boolean"
            )
        self.persistent_jsonl = persistent_jsonl
        self.persistent_protocol_version = (
            _JSONL_VALIDATOR_PROTOCOL_VERSION if persistent_jsonl else None
        )
        self.persistent_max_line_bytes = _integer(
            persistent_max_line_bytes,
            name="persistent validator maximum JSONL line bytes",
            minimum=1024,
            maximum=64 * 1024 * 1024,
        )
        self.persistent_stderr_limit_bytes = _integer(
            persistent_stderr_limit_bytes,
            name="persistent validator stderr limit bytes",
            minimum=1024,
            maximum=4 * 1024 * 1024,
        )
        if persistent_environment is not None and (
            not isinstance(persistent_environment, Mapping)
            or not all(isinstance(key, str) and key and isinstance(value, str) for key, value in persistent_environment.items())
        ):
            raise TemporalDiscoveryContractError("persistent validator environment must be a string mapping")
        self.persistent_environment = dict(persistent_environment or {})
        self._persistent_transport: _PersistentJsonlValidator | None = None

    def close(self) -> None:
        """Close the optional persistent process without changing one-shot use."""
        transport, self._persistent_transport = self._persistent_transport, None
        if transport is not None:
            transport.close()

    def __enter__(self) -> SubprocessCandidateValidator:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _persistent_validate(
        self,
        *,
        candidate_id: str,
        source_profile: Mapping[str, Any],
        expected_raw_source_profile_sha256: str,
    ) -> dict[str, Any]:
        if self._persistent_transport is None:
            with timed_span("native.transport.initialize_persistent_validator"):
                self._persistent_transport = _PersistentJsonlValidator(
                    self.command,
                    timeout_seconds=self.timeout_seconds,
                    max_line_bytes=self.persistent_max_line_bytes,
                    stderr_limit_bytes=self.persistent_stderr_limit_bytes,
                    environment=self.persistent_environment,
                )
        transport = self._persistent_transport
        try:
            with timed_span("native.transport.validate_round_trip"):
                return transport.validate(
                    candidate_id=candidate_id,
                    source_profile=source_profile,
                    expected_raw_source_profile_sha256=expected_raw_source_profile_sha256,
                )
        except Exception:
            # A dispatch that timed out/crashed/protocol-mismatched is never
            # retried.  Discard it so a distinct later request can start clean.
            self._persistent_transport = None
            transport.close()
            raise

    def compile_pair(self, *, candidate_id: str, long_profile: Mapping[str, Any], short_profile: Mapping[str, Any], expected_long_raw_source_profile_sha256: str, expected_short_raw_source_profile_sha256: str) -> dict[str, Any]:
        """Compile through the persistent native authority; never retry a dispatch."""
        if not self.persistent_jsonl:
            raise TemporalDiscoveryContractError("bidirectional compilation requires persistent JSONL validator mode")
        if self._persistent_transport is None:
            with timed_span("native.transport.initialize_persistent_validator"):
                self._persistent_transport = _PersistentJsonlValidator(self.command, timeout_seconds=self.timeout_seconds, max_line_bytes=self.persistent_max_line_bytes, stderr_limit_bytes=self.persistent_stderr_limit_bytes, environment=self.persistent_environment)
        transport = self._persistent_transport
        try:
            with timed_span("native.transport.compile_pair_round_trip"):
                return transport.compile_pair(candidate_id=candidate_id, long_profile=long_profile, short_profile=short_profile, expected_long_raw_source_profile_sha256=expected_long_raw_source_profile_sha256, expected_short_raw_source_profile_sha256=expected_short_raw_source_profile_sha256)
        except Exception:
            self._persistent_transport = None
            transport.close()
            raise

    def validate(
        self,
        *,
        candidate_id: str,
        source_profile: Mapping[str, Any],
        expected_raw_source_profile_sha256: str,
    ) -> dict[str, Any]:
        if self.persistent_jsonl:
            return self._persistent_validate(
                candidate_id=candidate_id,
                source_profile=source_profile,
                expected_raw_source_profile_sha256=expected_raw_source_profile_sha256,
            )
        with tempfile.TemporaryDirectory(
            prefix="temporal-discovery-validation-"
        ) as temporary:
            profile_path = Path(temporary) / "profile.json"
            profile_path.write_text(
                json.dumps(
                    dict(source_profile),
                    indent=2,
                    sort_keys=True,
                    ensure_ascii=True,
                    allow_nan=False,
                )
                + "\n",
                encoding="utf-8",
            )
            command = [
                *self.command,
                "--profile",
                str(profile_path),
                "--candidate-id",
                candidate_id,
                "--expected-raw-sha256",
                expected_raw_source_profile_sha256,
            ]
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
                env=dict(os.environ),
            )
        try:
            report = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise TemporalDiscoveryContractError(
                "candidate validator did not return one JSON document; "
                f"exit={completed.returncode}; stderr={completed.stderr.strip()!r}"
            ) from exc
        report = _mapping(report, name="candidate validation report")
        if report.get("schemaVersion") != TEMPORAL_SEARCH_VALIDATION_SCHEMA:
            raise TemporalDiscoveryContractError(
                "candidate validator returned an unknown schema"
            )
        if completed.returncode not in {0, 2}:
            raise TemporalDiscoveryContractError(
                "candidate validator failed operationally; "
                f"exit={completed.returncode}; stderr={completed.stderr.strip()!r}"
            )
        if completed.returncode == 0 and report.get("candidateAcceptable") is not True:
            raise TemporalDiscoveryContractError(
                "validator exit code and acceptance disagree"
            )
        if completed.returncode == 2 and report.get("candidateAcceptable") is not False:
            raise TemporalDiscoveryContractError(
                "validator rejection exit code and report disagree"
            )
        return report


class DashboardBidirectionalPairCompiler:
    """Typed adapter from the native JSONL client to ``FrozenPair`` authority.

    The detailed transport result remains fully checked by
    :meth:`SubprocessCandidateValidator.compile_pair`; the genome boundary
    intentionally receives only its closed ``profile``/``validation`` shape.
    """

    def __init__(self, client: SubprocessCandidateValidator) -> None:
        self._client = client

    def compile_pair(
        self,
        *,
        long_profile: Mapping[str, Any],
        short_profile: Mapping[str, Any],
        candidate_id: str,
    ) -> dict[str, Any]:
        with timed_span("native.compile_pair.compute_source_identities"):
            long_sha = canonical_sha256(long_profile)
            short_sha = canonical_sha256(short_profile)
        with timed_span("native.compile_pair.client_round_trip"):
            result = self._client.compile_pair(
                candidate_id=candidate_id,
                long_profile=long_profile,
                short_profile=short_profile,
                expected_long_raw_source_profile_sha256=long_sha,
                expected_short_raw_source_profile_sha256=short_sha,
            )
        with timed_span("native.compile_pair.prepare_result"):
            return {
                # FrozenPair.compile owns the next boundary and clones/freezes
                # these request-scoped decoded objects.  Do not serialize them
                # once here only to serialize them again there.
                "profile": result["profile"],
                "validation": result["report"],
            }


class DashboardV2ModuleValidator:
    """Typed persistent-native adapter used by frozen typed-module operators."""

    def __init__(self, client: SubprocessCandidateValidator) -> None:
        self._client = client

    def validate_v2(self, *, profile: Mapping[str, Any], candidate_id: str) -> dict[str, Any]:
        if profile.get("version") != "v2" or profile.get("directionMode") not in {"long", "short"}:
            raise TemporalDiscoveryContractError("native module validator was asked to validate a non-v2 side module")
        with timed_span("native.validate_v2.compute_source_identity"):
            source_sha = canonical_sha256(profile)
        with timed_span("native.validate_v2.client_round_trip"):
            return self._client.validate(
                candidate_id=candidate_id,
                source_profile=profile,
                expected_raw_source_profile_sha256=source_sha,
            )


def validator_provenance(
    validator: CandidateValidatorProtocol,
    *,
    validation_contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Describe validator inputs for content binding, not authenticity claims."""
    command = getattr(validator, "command", None)
    has_command = isinstance(command, (tuple, list)) and bool(command) and all(
        isinstance(item, str) and item.strip() for item in command
    )
    contract = _clone(
        validation_contract or {}, name="validator contract provenance"
    )
    protocol_version = getattr(validator, "persistent_protocol_version", None)
    if protocol_version is not None:
        if protocol_version != _JSONL_VALIDATOR_PROTOCOL_VERSION:
            raise TemporalDiscoveryContractError(
                "validator persistent protocol version is unknown"
            )
        # Preserve the closed persisted provenance schema while binding the
        # otherwise operational transport choice into its contract identity.
        contract = {
            **contract,
            "validatorTransportProtocol": protocol_version,
        }
    return {
        "schemaVersion": AUTHORED_VALIDATOR_PROVENANCE_SCHEMA,
        "validationContractSha256": canonical_sha256(contract),
        "validatorSchema": contract.get("validatorSchema"),
        "fuzzfolioCommit": contract.get("fuzzfolioCommit"),
        "validatorCommandSha256": (
            canonical_sha256(list(command)) if has_command else None
        ),
        "commandProvenance": (
            "declared_subprocess_command" if has_command else "protocol_command_unavailable"
        ),
    }


def build_authored_validation_binding(
    *,
    raw_source_profile_sha256: str,
    validation: Mapping[str, Any],
    provenance: Mapping[str, Any],
) -> dict[str, Any]:
    """Content-bind a candidate's authored lineage to its validator output."""
    material = {
        "schemaVersion": AUTHORED_VALIDATION_BINDING_SCHEMA,
        "rawSourceProfileSha256": _sha(
            raw_source_profile_sha256,
            name="authored validation raw source profile SHA-256",
        ),
        "profileSnapshotSha256": _sha(
            validation.get("profileSnapshotSha256"),
            name="authored validation profile snapshot SHA-256",
        ),
        "programSha256": _sha(
            validation.get("programSha256"),
            name="authored validation program SHA-256",
        ),
        "validationReportSha256": _sha(
            validation.get("validationReportSha256"),
            name="authored validation report SHA-256",
        ),
        "validatorProvenance": _clone(
            provenance, name="authored validation validator provenance"
        ),
    }
    return {
        **material,
        "authoredValidationBindingSha256": canonical_sha256(material),
    }


def validate_authored_validation_binding(candidate: Mapping[str, Any]) -> None:
    """Verify the candidate has not drifted from its authored validation."""
    binding = _mapping(
        candidate.get("authoredValidationBinding"),
        name="candidate authored validation binding",
    )
    supplied = _sha(
        candidate.get("authoredValidationBindingSha256"),
        name="candidate authored validation binding SHA-256",
    )
    material = _clone(binding, name="candidate authored validation binding")
    observed = material.pop("authoredValidationBindingSha256", None)
    if observed is not None:
        raise TemporalDiscoveryContractError(
            "candidate authored validation binding material must not embed its hash"
        )
    if canonical_sha256(material) != supplied:
        raise TemporalDiscoveryContractError(
            "candidate authored validation binding identity mismatch"
        )
    if set(material) != _AUTHORED_VALIDATION_BINDING_FIELDS:
        raise TemporalDiscoveryContractError(
            "candidate authored validation binding schema fields are not exact"
        )
    if material.get("schemaVersion") != AUTHORED_VALIDATION_BINDING_SCHEMA:
        raise TemporalDiscoveryContractError(
            "candidate authored validation binding has an unknown schema"
        )
    expected = {
        "rawSourceProfileSha256": candidate.get("sourceProfileSha256"),
        "profileSnapshotSha256": candidate.get("profileSnapshotSha256"),
        "programSha256": candidate.get("programSha256"),
        "validationReportSha256": candidate.get("validationReportSha256"),
    }
    for key, value in expected.items():
        if material.get(key) != value:
            raise TemporalDiscoveryContractError(
                "candidate authored validation binding diverges from candidate " + key
            )
    provenance = material.get("validatorProvenance")
    if not isinstance(provenance, Mapping):
        raise TemporalDiscoveryContractError(
            "candidate authored validation binding lacks validator provenance"
        )
    if set(provenance) != _AUTHORED_VALIDATOR_PROVENANCE_FIELDS:
        raise TemporalDiscoveryContractError(
            "candidate authored validation validator provenance fields are not exact"
        )
    if provenance.get("schemaVersion") != AUTHORED_VALIDATOR_PROVENANCE_SCHEMA:
        raise TemporalDiscoveryContractError(
            "candidate authored validation validator provenance has an unknown schema"
        )
    validator_schema = provenance.get("validatorSchema")
    if (
        not isinstance(validator_schema, str)
        or validator_schema != validator_schema.strip()
        or not _SAFE.fullmatch(validator_schema)
    ):
        raise TemporalDiscoveryContractError(
            "candidate authored validation validator provenance validator schema must be a nonempty canonical string"
        )
    fuzzfolio_commit = provenance.get("fuzzfolioCommit")
    if fuzzfolio_commit is not None and (
        not isinstance(fuzzfolio_commit, str)
        or not re.fullmatch(r"[0-9a-f]{40}", fuzzfolio_commit)
    ):
        raise TemporalDiscoveryContractError(
            "candidate authored validation validator provenance fuzzfolio commit must be None or an exact lowercase commit SHA"
        )
    _sha(
        provenance.get("validationContractSha256"),
        name="candidate authored validation validator provenance validation contract SHA-256",
    )
    command_sha = provenance.get("validatorCommandSha256")
    if command_sha is not None:
        _sha(
            command_sha,
            name="candidate authored validation validator provenance command SHA-256",
        )
    command_provenance = provenance.get("commandProvenance")
    if command_provenance not in _VALIDATOR_COMMAND_PROVENANCE:
        raise TemporalDiscoveryContractError(
            "candidate authored validation validator provenance command provenance is unknown"
        )
    if (
        command_provenance == "declared_subprocess_command"
        and command_sha is None
    ) or (
        command_provenance == "protocol_command_unavailable"
        and command_sha is not None
    ):
        raise TemporalDiscoveryContractError(
            "candidate authored validation validator provenance command presence is inconsistent"
        )


def build_legacy_reference_admission_binding(
    *,
    candidate: Mapping[str, Any],
    execution_binding: Mapping[str, Any],
    source_reference_population_sha256: str,
    authority_id: str,
    worker_contract_sha256: str,
    corrected_result_set_sha256: str,
) -> dict[str, Any]:
    """Bind an evaluated legacy reference to the exact admission evidence.

    A frozen reference predates fresh-generation authored validator bindings.
    It is therefore deliberately labelled as a reference admission binding,
    rather than being represented as newly-authored validator provenance.
    """
    material = {
        "schemaVersion": "stage5e7_v3_legacy_reference_admission_binding_v1",
        "admissionKind": "legacy_reference_result_attested",
        "rawSourceProfileSha256": _sha(
            candidate.get("sourceProfileSha256"),
            name="legacy reference source profile SHA-256",
        ),
        "profileSnapshotSha256": _sha(
            candidate.get("profileSnapshotSha256"),
            name="legacy reference profile snapshot SHA-256",
        ),
        "programSha256": _sha(
            candidate.get("programSha256"),
            name="legacy reference authored program SHA-256",
        ),
        "resolvedProfileSnapshotSha256": _sha(
            execution_binding.get("resolvedProfileSnapshotSha256"),
            name="legacy reference resolved profile snapshot SHA-256",
        ),
        "resolvedProgramSha256": _sha(
            execution_binding.get("resolvedProgramSha256"),
            name="legacy reference resolved program SHA-256",
        ),
        "sourceReferencePopulationSha256": _sha(
            source_reference_population_sha256,
            name="legacy reference population SHA-256",
        ),
        "authorityId": _sha(authority_id, name="legacy reference authority ID"),
        "workerContractSha256": _sha(
            worker_contract_sha256,
            name="legacy reference worker contract SHA-256",
        ),
        "correctedResultSetSha256": _sha(
            corrected_result_set_sha256,
            name="legacy reference corrected result set SHA-256",
        ),
    }
    return {
        **material,
        "legacyReferenceAdmissionBindingSha256": canonical_sha256(material),
    }


def validate_legacy_reference_admission_binding(
    candidate: Mapping[str, Any]
) -> None:
    """Verify a labelled legacy admission did not drift after result attestation."""
    binding = _mapping(
        candidate.get("legacyReferenceAdmissionBinding"),
        name="candidate legacy reference admission binding",
    )
    supplied = _sha(
        candidate.get("legacyReferenceAdmissionBindingSha256"),
        name="candidate legacy reference admission binding SHA-256",
    )
    material = _clone(binding, name="candidate legacy reference admission binding")
    if material.pop("legacyReferenceAdmissionBindingSha256", None) is not None:
        raise TemporalDiscoveryContractError(
            "candidate legacy reference admission binding material must not embed its hash"
        )
    if canonical_sha256(material) != supplied:
        raise TemporalDiscoveryContractError(
            "candidate legacy reference admission binding identity mismatch"
        )
    expected = {
        "rawSourceProfileSha256": candidate.get("sourceProfileSha256"),
        "profileSnapshotSha256": candidate.get("profileSnapshotSha256"),
        "programSha256": candidate.get("programSha256"),
    }
    for key, value in expected.items():
        if material.get(key) != value:
            raise TemporalDiscoveryContractError(
                "candidate legacy reference admission binding diverges from candidate "
                + key
            )
    if (
        material.get("schemaVersion")
        != "stage5e7_v3_legacy_reference_admission_binding_v1"
        or material.get("admissionKind") != "legacy_reference_result_attested"
    ):
        raise TemporalDiscoveryContractError(
            "candidate legacy reference admission binding has an unknown schema"
        )


def _normalize_preparation(raw: Mapping[str, Any]) -> dict[str, Any]:
    payload = _mapping(raw, name="discovery preparation")
    required = {
        "schemaVersion",
        "authorityLabel",
        "generator",
        "validation",
        "workerContract",
        "instrument",
        "timeframe",
        "barLimit",
        "seeds",
        "developmentWindows",
        "evidencePlanTemplates",
        "prohibitedEvidence",
        "screening",
        "bounds",
    }
    if set(payload) != required:
        raise TemporalDiscoveryContractError(
            f"discovery preparation must contain exactly {sorted(required)!r}"
        )
    if payload["schemaVersion"] != TEMPORAL_DISCOVERY_PREPARATION_SCHEMA:
        raise TemporalDiscoveryContractError(
            "unknown discovery preparation schema"
        )

    generator = _mapping(payload["generator"], name="generator")
    generator_keys = {
        "seed",
        "targetUniquePrograms",
        "deNovoFraction",
        "maxProposalAttempts",
        "deNovoMutationCount",
        "seedMutationCount",
    }
    if set(generator) != generator_keys:
        raise TemporalDiscoveryContractError(
            "generator has a closed schema"
        )
    de_novo_count = _mapping(
        generator["deNovoMutationCount"],
        name="generator.deNovoMutationCount",
    )
    seed_count = _mapping(
        generator["seedMutationCount"],
        name="generator.seedMutationCount",
    )
    if set(de_novo_count) != {"min", "max"} or set(seed_count) != {"min", "max"}:
        raise TemporalDiscoveryContractError(
            "mutation-count ranges require min and max"
        )
    normalized_generator = {
        "version": TEMPORAL_DISCOVERY_GENERATOR_VERSION,
        "seed": _integer(
            generator["seed"],
            name="generator.seed",
            minimum=0,
            maximum=2**63 - 1,
        ),
        "targetUniquePrograms": _integer(
            generator["targetUniquePrograms"],
            name="generator.targetUniquePrograms",
            minimum=1,
            maximum=100_000,
        ),
        "deNovoFraction": _number(
            generator["deNovoFraction"],
            name="generator.deNovoFraction",
            minimum=0.0,
            maximum=1.0,
        ),
        "maxProposalAttempts": _integer(
            generator["maxProposalAttempts"],
            name="generator.maxProposalAttempts",
            minimum=1,
            maximum=1_000_000,
        ),
        "deNovoMutationCount": {
            "min": _integer(
                de_novo_count["min"],
                name="generator.deNovoMutationCount.min",
                minimum=1,
                maximum=32,
            ),
            "max": _integer(
                de_novo_count["max"],
                name="generator.deNovoMutationCount.max",
                minimum=1,
                maximum=32,
            ),
        },
        "seedMutationCount": {
            "min": _integer(
                seed_count["min"],
                name="generator.seedMutationCount.min",
                minimum=1,
                maximum=16,
            ),
            "max": _integer(
                seed_count["max"],
                name="generator.seedMutationCount.max",
                minimum=1,
                maximum=16,
            ),
        },
    }
    for key in ("deNovoMutationCount", "seedMutationCount"):
        if normalized_generator[key]["min"] > normalized_generator[key]["max"]:
            raise TemporalDiscoveryContractError(
                f"generator.{key}.min must not exceed max"
            )

    validation = _mapping(payload["validation"], name="validation")
    if set(validation) != {"validatorSchema", "fuzzfolioCommit"}:
        raise TemporalDiscoveryContractError(
            "validation must bind validatorSchema and fuzzfolioCommit"
        )
    normalized_validation = {
        "validatorSchema": _safe(
            validation["validatorSchema"],
            name="validation.validatorSchema",
        ),
        "fuzzfolioCommit": str(validation["fuzzfolioCommit"] or "").strip(),
    }
    if normalized_validation["validatorSchema"] != TEMPORAL_SEARCH_VALIDATION_SCHEMA:
        raise TemporalDiscoveryContractError(
            "discovery requires the admitted temporal search validator schema"
        )
    if not re.fullmatch(r"[0-9a-f]{40}", normalized_validation["fuzzfolioCommit"]):
        raise TemporalDiscoveryContractError(
            "validation.fuzzfolioCommit must be an exact commit SHA"
        )

    worker = _mapping(payload["workerContract"], name="workerContract")
    if set(worker) != {"workerContractSha256", "workerContractSchema"}:
        raise TemporalDiscoveryContractError(
            "workerContract has a closed schema"
        )
    normalized_worker = {
        "workerContractSha256": _sha(
            worker["workerContractSha256"],
            name="workerContract.workerContractSha256",
        ),
        "workerContractSchema": _safe(
            worker["workerContractSchema"],
            name="workerContract.workerContractSchema",
        ),
    }

    instrument = str(payload["instrument"] or "").strip().upper()
    timeframe = str(payload["timeframe"] or "").strip().upper()
    if not instrument or not timeframe:
        raise TemporalDiscoveryContractError(
            "instrument and timeframe are required"
        )
    bar_limit = _integer(
        payload["barLimit"],
        name="barLimit",
        minimum=10,
        maximum=1_000_000,
    )

    seeds_raw = payload["seeds"]
    if not isinstance(seeds_raw, list) or not seeds_raw:
        raise TemporalDiscoveryContractError("seeds must be non-empty")
    seeds: list[dict[str, Any]] = []
    for index, raw_seed in enumerate(seeds_raw):
        seed = _mapping(raw_seed, name=f"seeds[{index}]")
        if set(seed) != {"seedId", "sourceProfile"}:
            raise TemporalDiscoveryContractError(
                "seed entries require seedId and sourceProfile"
            )
        profile = _ensure_explicit_management(
            _mapping(seed["sourceProfile"], name=f"seeds[{index}].sourceProfile")
        )
        instruments = profile.get("instruments")
        if instruments != [instrument]:
            raise TemporalDiscoveryContractError(
                f"seed {seed['seedId']!r} does not bind the declared instrument"
            )
        seeds.append(
            {
                "seedId": _safe(seed["seedId"], name=f"seeds[{index}].seedId"),
                "sourceProfile": profile,
                "sourceProfileSha256": canonical_sha256(profile),
            }
        )
    if len({seed["seedId"] for seed in seeds}) != len(seeds):
        raise TemporalDiscoveryContractError("seed IDs must be unique")

    windows_raw = payload["developmentWindows"]
    if not isinstance(windows_raw, list) or len(windows_raw) < 2:
        raise TemporalDiscoveryContractError(
            "at least two development windows are required"
        )
    windows = [_clone(item, name="development window") for item in windows_raw]
    window_ids = [str(item.get("windowId") or "") for item in windows]
    if any(not token for token in window_ids) or len(set(window_ids)) != len(window_ids):
        raise TemporalDiscoveryContractError(
            "development window IDs must be non-empty and unique"
        )

    templates_raw = payload["evidencePlanTemplates"]
    if not isinstance(templates_raw, list):
        raise TemporalDiscoveryContractError(
            "evidencePlanTemplates must be a list"
        )
    templates: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(templates_raw):
        current = _mapping(item, name=f"evidencePlanTemplates[{index}]")
        if set(current) != {"windowId", "evidencePlan"}:
            raise TemporalDiscoveryContractError(
                "evidence plan templates require windowId and evidencePlan"
            )
        window_id = str(current["windowId"] or "")
        if window_id in templates:
            raise TemporalDiscoveryContractError(
                "evidence plan template window IDs must be unique"
            )
        plan = _mapping(
            current["evidencePlan"],
            name=f"evidencePlanTemplates[{index}].evidencePlan",
        )
        if plan.get("schema_version") != "fuzzfolio.replay-evidence-plan.v2":
            raise TemporalDiscoveryContractError(
                "discovery requires replay evidence plan v2 templates"
            )
        templates[window_id] = plan
    if set(templates) != set(window_ids):
        raise TemporalDiscoveryContractError(
            "evidence plan templates must exactly cover development windows"
        )

    screening = _mapping(payload["screening"], name="screening")
    screening_keys = {
        "initialWindowIds",
        "confirmationWindowIds",
        "economicArchiveSize",
        "noveltyArchiveSize",
        "confirmationCandidateCap",
        "minimumTradesPerInitialWindowEconomic",
        "minimumTotalTradesNovelty",
        "finalEconomicArchiveSize",
        "finalNoveltyArchiveSize",
    }
    if set(screening) != screening_keys:
        raise TemporalDiscoveryContractError(
            "screening has a closed schema"
        )
    initial_ids = [str(value) for value in screening["initialWindowIds"]]
    confirmation_ids = [str(value) for value in screening["confirmationWindowIds"]]
    if (
        not initial_ids
        or not confirmation_ids
        or set(initial_ids) & set(confirmation_ids)
        or set(initial_ids) | set(confirmation_ids) != set(window_ids)
    ):
        raise TemporalDiscoveryContractError(
            "initial and confirmation windows must be a disjoint full partition"
        )
    normalized_screening = {
        "version": TEMPORAL_DISCOVERY_SELECTION_VERSION,
        "initialWindowIds": initial_ids,
        "confirmationWindowIds": confirmation_ids,
        "economicArchiveSize": _integer(
            screening["economicArchiveSize"],
            name="screening.economicArchiveSize",
            minimum=1,
            maximum=100_000,
        ),
        "noveltyArchiveSize": _integer(
            screening["noveltyArchiveSize"],
            name="screening.noveltyArchiveSize",
            minimum=1,
            maximum=100_000,
        ),
        "confirmationCandidateCap": _integer(
            screening["confirmationCandidateCap"],
            name="screening.confirmationCandidateCap",
            minimum=1,
            maximum=100_000,
        ),
        "minimumTradesPerInitialWindowEconomic": _integer(
            screening["minimumTradesPerInitialWindowEconomic"],
            name="screening.minimumTradesPerInitialWindowEconomic",
            minimum=0,
            maximum=1_000_000,
        ),
        "minimumTotalTradesNovelty": _integer(
            screening["minimumTotalTradesNovelty"],
            name="screening.minimumTotalTradesNovelty",
            minimum=0,
            maximum=1_000_000,
        ),
        "finalEconomicArchiveSize": _integer(
            screening["finalEconomicArchiveSize"],
            name="screening.finalEconomicArchiveSize",
            minimum=1,
            maximum=100_000,
        ),
        "finalNoveltyArchiveSize": _integer(
            screening["finalNoveltyArchiveSize"],
            name="screening.finalNoveltyArchiveSize",
            minimum=1,
            maximum=100_000,
        ),
    }

    bounds = _mapping(payload["bounds"], name="bounds")
    bound_keys = {
        "maxCandidates",
        "maxInitialTasks",
        "maxConfirmationCandidates",
        "maxConfirmationTasks",
        "maxTotalTasks",
        "maxAttempts",
        "deadlineSeconds",
    }
    if set(bounds) != bound_keys:
        raise TemporalDiscoveryContractError("bounds has a closed schema")
    normalized_bounds = {
        "maxCandidates": _integer(
            bounds["maxCandidates"],
            name="bounds.maxCandidates",
            minimum=1,
            maximum=100_000,
        ),
        "maxInitialTasks": _integer(
            bounds["maxInitialTasks"],
            name="bounds.maxInitialTasks",
            minimum=1,
            maximum=1_000_000,
        ),
        "maxConfirmationCandidates": _integer(
            bounds["maxConfirmationCandidates"],
            name="bounds.maxConfirmationCandidates",
            minimum=1,
            maximum=100_000,
        ),
        "maxConfirmationTasks": _integer(
            bounds["maxConfirmationTasks"],
            name="bounds.maxConfirmationTasks",
            minimum=1,
            maximum=1_000_000,
        ),
        "maxTotalTasks": _integer(
            bounds["maxTotalTasks"],
            name="bounds.maxTotalTasks",
            minimum=1,
            maximum=1_000_000,
        ),
        "maxAttempts": _integer(
            bounds["maxAttempts"],
            name="bounds.maxAttempts",
            minimum=1,
            maximum=100,
        ),
        "deadlineSeconds": _number(
            bounds["deadlineSeconds"],
            name="bounds.deadlineSeconds",
            minimum=1.0,
            maximum=86_400.0,
        ),
    }
    target = normalized_generator["targetUniquePrograms"]
    initial_tasks = target * len(initial_ids)
    confirmation_tasks = (
        normalized_screening["confirmationCandidateCap"]
        * len(confirmation_ids)
    )
    if target > normalized_bounds["maxCandidates"]:
        raise TemporalDiscoveryContractError(
            "target unique programs exceeds maxCandidates"
        )
    if initial_tasks > normalized_bounds["maxInitialTasks"]:
        raise TemporalDiscoveryContractError(
            "initial task matrix exceeds maxInitialTasks"
        )
    if (
        normalized_screening["confirmationCandidateCap"]
        > normalized_bounds["maxConfirmationCandidates"]
        or confirmation_tasks > normalized_bounds["maxConfirmationTasks"]
    ):
        raise TemporalDiscoveryContractError(
            "confirmation stage exceeds authority bounds"
        )
    if initial_tasks + confirmation_tasks > normalized_bounds["maxTotalTasks"]:
        raise TemporalDiscoveryContractError(
            "progressive task ceiling exceeds maxTotalTasks"
        )

    normalized = {
        "schemaVersion": TEMPORAL_DISCOVERY_PREPARATION_SCHEMA,
        "authorityLabel": _safe(
            payload["authorityLabel"],
            name="authorityLabel",
        ),
        "generator": normalized_generator,
        "validation": normalized_validation,
        "workerContract": normalized_worker,
        "instrument": instrument,
        "timeframe": timeframe,
        "barLimit": bar_limit,
        "seeds": seeds,
        "developmentWindows": windows,
        "evidencePlanTemplates": [
            {"windowId": window_id, "evidencePlan": templates[window_id]}
            for window_id in window_ids
        ],
        "prohibitedEvidence": _clone(
            payload["prohibitedEvidence"],
            name="prohibitedEvidence",
        ),
        "screening": normalized_screening,
        "bounds": normalized_bounds,
    }
    normalized["preparationSha256"] = canonical_sha256(normalized)
    return normalized


def _rotate_evidence_plan(
    template: Mapping[str, Any],
    *,
    raw_source_profile_sha256: str,
    source_profile: Mapping[str, Any],
    base_decision_timeframe: str,
    frozen_construction_catalog: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Rotate only profile-bound evidence fields under an immutable lake binding.

    A v2 plan's semantic digest is an attestation over the full lake request.
    Therefore a candidate may reuse a frozen binding only when the canonical
    dependency request derived from its actual profile is contained by that
    binding.  This is intentionally not a local binding rehash: a wider scope
    must be attested before a campaign is frozen.
    """

    plan = _mapping(template, name="evidence plan template")
    profile = _mapping(source_profile, name="source profile")
    if plan.get("schema_version") != "fuzzfolio.replay-evidence-plan.v2":
        raise TemporalDiscoveryContractError(
            "evidence-plan rotation requires replay evidence plan v2"
        )
    timeframe = str(base_decision_timeframe or "").strip().upper()
    if not timeframe:
        raise TemporalDiscoveryContractError(
            "evidence-plan rotation requires a base decision timeframe"
        )
    pairs = profile.get("instruments")
    if not isinstance(pairs, list) or not pairs:
        raise TemporalDiscoveryContractError(
            "candidate source profile requires instruments for lake-scope rotation"
        )
    try:
        frozen_binding = LakeWindowBinding.model_validate(
            plan.get("lake_window_binding")
        )
        required_request = resolve_replay_lake_window_request(
            pairs=[str(pair) for pair in pairs],
            base_timeframe=timeframe,
            profile_snapshot=profile,
            analysis_window_start=str(plan.get("analysis_window_start") or ""),
            analysis_window_end=str(plan.get("analysis_window_end") or ""),
            frozen_catalog=frozen_construction_catalog,
        )
    except (TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError(
            "candidate evidence-plan lake scope is malformed"
        ) from exc
    if not lake_window_request_contains(frozen_binding.request, required_request):
        raise TemporalDiscoveryContractError(
            "candidate-derived lake scope is outside the immutable pre-attested "
            "evidence binding; timeframe construction is not admissible"
        )

    # Emit the canonical existing binding exactly.  It remains authoritative;
    # profile/execution identity and the enclosing evidence-plan identity rotate
    # below, but the remote-attested lake semantic identity never does.
    plan["lake_window_binding"] = frozen_binding.model_dump(mode="json")
    execution_config = _mapping(
        profile.get("executionConfig"),
        name="source profile executionConfig",
    )
    management_library = execution_config.get("managementLibrary")
    if management_library is not None:
        _mapping(
            management_library,
            name="source profile managementLibrary",
        )
        execution_cell_sha256 = None
    else:
        exit_policy = _mapping(
            execution_config.get("exitPolicy"),
            name="source profile exitPolicy",
        )
        selected_cell = _mapping(
            exit_policy.get("selectedCell"),
            name="source profile selectedCell",
        )
        execution_cell_sha256 = canonical_sha256(selected_cell)

    plan["profile_snapshot_sha256"] = raw_source_profile_sha256
    plan["execution_cell_sha256"] = execution_cell_sha256
    plan.pop("plan_id", None)
    identity = dict(plan)
    identity.pop("lake_manifest_sha256", None)
    plan["plan_id"] = canonical_sha256(identity)
    return plan


def _finite_preparation(
    preparation: Mapping[str, Any],
    *,
    candidates: Sequence[Mapping[str, Any]],
    window_ids: Sequence[str],
    label_suffix: str,
    max_tasks: int,
) -> dict[str, Any]:
    normalized = _mapping(preparation, name="normalized discovery preparation")
    window_map = {
        item["windowId"]: item for item in normalized["developmentWindows"]
    }
    template_map = {
        item["windowId"]: item["evidencePlan"]
        for item in normalized["evidencePlanTemplates"]
    }
    windows = [window_map[window_id] for window_id in window_ids]
    finite_candidates: list[dict[str, Any]] = []
    for candidate in candidates:
        source_profile = _mapping(
            candidate["sourceProfile"],
            name="candidate sourceProfile",
        )
        raw_sha = _sha(
            candidate["sourceProfileSha256"],
            name="candidate sourceProfileSha256",
        )
        finite_candidates.append(
            {
                "candidateId": candidate["candidateId"],
                "sourceProfile": source_profile,
                "sourceProfileSha256": raw_sha,
                "instrument": normalized["instrument"],
                "timeframe": normalized["timeframe"],
                "barLimit": normalized["barLimit"],
                "windowInputs": [
                    {
                        "windowId": window_id,
                        "evidencePlan": _rotate_evidence_plan(
                            template_map[window_id],
                            raw_source_profile_sha256=raw_sha,
                            source_profile=source_profile,
                            base_decision_timeframe=normalized["timeframe"],
                        ),
                    }
                    for window_id in window_ids
                ],
            }
        )
    return {
        "schemaVersion": TEMPORAL_SEARCH_PREPARATION_SCHEMA,
        "authorityLabel": (
            normalized["authorityLabel"] + "-" + label_suffix
        ),
        "workerContract": normalized["workerContract"],
        "candidates": finite_candidates,
        "developmentWindows": windows,
        "prohibitedEvidence": normalized["prohibitedEvidence"],
        "bounds": {
            "maxCandidates": len(finite_candidates),
            "maxDevelopmentWindows": len(windows),
            "maxTasks": max_tasks,
            "maxAttempts": normalized["bounds"]["maxAttempts"],
            "deadlineSeconds": normalized["bounds"]["deadlineSeconds"],
        },
    }




__all__ = [
    "SubprocessCandidateValidator",
    "validator_provenance",
    "build_authored_validation_binding",
    "validate_authored_validation_binding",
    "build_legacy_reference_admission_binding",
    "validate_legacy_reference_admission_binding",
    "_normalize_preparation",
    "_rotate_evidence_plan",
    "_finite_preparation",
]
