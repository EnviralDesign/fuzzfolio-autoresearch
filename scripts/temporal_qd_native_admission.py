"""Admit Rust Temporal-QD generation against the production Python oracle.

This harness runs only the pre-economic pair-generation boundary.  It never
opens market data, freezes an evaluation campaign, contacts a lake/gateway, or
copies population artifacts.  Each engine receives the same verified frozen
authority and either the exact empty generation-zero archive or one explicitly
supplied, verified parent archive.  The engines own independent output roots
and identity ledgers.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time
from collections.abc import Callable, Iterable, Mapping
from typing import Any

import psutil

from autoresearch.temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)
from autoresearch.temporal_qd_evolution import (
    DEFAULT_QD_PARAMETERS,
    QD_POLICY,
    QD_POLICY_NAME,
    QD_POLICY_SHA256,
    QD_VERSION,
    _ledger_identity,
    _load_archive,
    _reproduction_cells,
    generate_qd_generation,
    qd_predeclared_evidence_context,
)
from autoresearch.temporal_qd_native import (
    PAIR_GENERATION_RUNTIME_PYTHON,
    PAIR_GENERATION_RUNTIME_RUST,
    build_pair_generation_runtime_config,
)
from autoresearch.temporal_qd_pair_factory import (
    PairAuthorityBundle,
    load_pair_run_config,
    pair_policy_from_config,
)
from autoresearch.temporal_qd_pair_generation import _rotating_parent_schedule
from scripts import temporal_qd_front_half_oracle as front_half_oracle


REPORT_SCHEMA = "temporal_qd_native_admission_report_v1"
AUTHORITY_SCHEMA = "temporal_qd_native_admission_authority_v1"
WORKER_SCHEMA = "temporal_qd_native_admission_worker_v1"
SHAPES = tuple(front_half_oracle.FIXED_SHAPE_METADATA)
ENGINES = (PAIR_GENERATION_RUNTIME_PYTHON, PAIR_GENERATION_RUNTIME_RUST)
_CHUNK_BYTES = 1024 * 1024
_PROCESS_ROLE_WORKER = "admissionPythonWorker"
_PROCESS_ROLE_BATCH = "nativeQdBatch"
_PROCESS_ROLE_DASHBOARD = "dashboardJsonlAuthority"
_PROCESS_ROLE_OTHER = "otherDescendants"
_PROCESS_ROLE_ORDER = (
    _PROCESS_ROLE_WORKER,
    _PROCESS_ROLE_BATCH,
    _PROCESS_ROLE_DASHBOARD,
    _PROCESS_ROLE_OTHER,
)
_MAX_EXECUTABLES_PER_NAMED_ROLE = 4
_ADMISSION_MODE_EMPTY_G0 = "empty_generation_zero_g0"
_ADMISSION_MODE_PARENT_ARCHIVE = "verified_parent_archive"


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _read_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(f"{label} is not readable JSON: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalDiscoveryContractError(f"{label} must be a JSON object: {path}")
    return value


def _write_object(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = _canonical_json(value) + "\n"
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(encoded, encoding="utf-8", newline="\n")
    os.replace(temporary, path)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(_CHUNK_BYTES):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _artifact_bytes(root: Path) -> int:
    return sum(
        path.stat().st_size
        for path in root.rglob("*")
        if path.is_file() and not path.is_symlink()
    )


def _parse_shapes(value: str) -> tuple[int, ...]:
    try:
        shapes = tuple(int(part.strip()) for part in value.split(",") if part.strip())
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--shapes must be comma-separated admission shapes") from exc
    if not shapes or len(set(shapes)) != len(shapes) or any(shape not in SHAPES for shape in shapes):
        raise argparse.ArgumentTypeError(
            "--shapes must contain unique values from 1,8,64,128,1024"
        )
    return shapes


def _publication_authority(config: Mapping[str, Any] | None = None) -> dict[str, Any]:
    if config is None:
        return {
            "qdVersion": QD_VERSION,
            "policyName": QD_POLICY_NAME,
            "policySha256": QD_POLICY_SHA256,
            "frozenPolicy": QD_POLICY,
        }
    return {
        "qdVersion": config["qdVersion"],
        "policyName": config["policyName"],
        "policySha256": config["policySha256"],
        "frozenPolicy": config["frozenPolicy"],
    }


def _assert_empty_archive(archive: Mapping[str, Any]) -> None:
    exact_zero_fields = (
        "candidateCountSeen",
        "occupiedCellCount",
        "memberCount",
        "qualityMemberCount",
        "observationalMemberCount",
        "negativeNoveltyMemberCount",
    )
    if (
        archive.get("generationIndex") != 0
        or archive.get("cells") != []
        or any(archive.get(field) != 0 for field in exact_zero_fields)
    ):
        raise TemporalDiscoveryContractError(
            "native admission requires an exact empty generation-zero archive"
        )


def _nonempty_parent_archive_binding(
    archive: Mapping[str, Any],
) -> tuple[int, dict[str, Any] | None, int]:
    """Validate a real breeder archive and derive its next generation.

    This calls the production reproduction filter rather than inferring
    eligibility from archive counters.  A parent archive can contain material
    that is intentionally not eligible to reproduce.
    """

    archive_generation = archive.get("generationIndex")
    if isinstance(archive_generation, bool) or not isinstance(archive_generation, int):
        raise TemporalDiscoveryContractError(
            "native admission parent archive generation index is invalid"
        )
    if archive_generation < 1:
        raise TemporalDiscoveryContractError(
            "native admission parent archive must be a completed nonempty generation"
        )
    cells = _reproduction_cells(archive, allow_empty_quality_bootstrap=False)
    eligible_parent_count = sum(
        len(cell.get("members") or []) for cell in cells if isinstance(cell, Mapping)
    )
    if eligible_parent_count < 1:
        raise TemporalDiscoveryContractError(
            "native admission parent archive has no eligible reproduction members"
        )
    # This production helper returns None for the historical four-to-one
    # schedule, and fails closed for malformed archive-bound schedules.  Do
    # not borrow a schedule from a supervisor's older initial archive.
    return archive_generation + 1, _rotating_parent_schedule(archive), eligible_parent_count


def _validate_authority(value: Mapping[str, Any]) -> dict[str, Any]:
    authority = json.loads(_canonical_json(value))
    supplied = authority.pop("authoritySha256", None)
    if authority.get("schemaVersion") != AUTHORITY_SCHEMA or supplied != canonical_sha256(authority):
        raise TemporalDiscoveryContractError("admission authority identity mismatch")
    authority["authoritySha256"] = supplied
    pair_config = load_pair_run_config(authority["pairRunConfig"])
    archive_path = Path(str(authority["parentArchivePath"]))
    archive, archive_sha = _load_archive(archive_path)
    if archive_sha != authority.get("parentArchiveSha256"):
        raise TemporalDiscoveryContractError("admission parent archive drifted")
    if archive.get("bidirectionalPairPolicy") != pair_policy_from_config(pair_config):
        raise TemporalDiscoveryContractError("admission archive pair policy drifted")
    mode = authority.get("parentArchiveMode")
    if mode == _ADMISSION_MODE_EMPTY_G0:
        _assert_empty_archive(archive)
        if authority.get("generationIndex") != 1:
            raise TemporalDiscoveryContractError(
                "empty generation-zero admission must generate generation 1"
            )
        if authority.get("g0BootstrapEnabled") is not True:
            raise TemporalDiscoveryContractError(
                "empty generation-zero admission must retain G0 bootstrap"
            )
        if authority.get("eligibleParentCount") != 0:
            raise TemporalDiscoveryContractError(
                "empty generation-zero admission cannot claim eligible parents"
            )
    elif mode == _ADMISSION_MODE_PARENT_ARCHIVE:
        generation_index, parent_schedule, eligible_parent_count = _nonempty_parent_archive_binding(
            archive
        )
        if authority.get("generationIndex") != generation_index:
            raise TemporalDiscoveryContractError(
                "admission parent generation index is not derived from its archive"
            )
        if authority.get("g0BootstrapEnabled") is not False:
            raise TemporalDiscoveryContractError(
                "parent archive admission cannot enable G0 bootstrap"
            )
        if authority.get("eligibleParentCount") != eligible_parent_count:
            raise TemporalDiscoveryContractError(
                "admission eligible parent count drifted"
            )
        if authority.get("parentSchedule") != parent_schedule:
            raise TemporalDiscoveryContractError(
                "admission parent schedule is not bound to its parent archive"
            )
        expected_schedule_mode = (
            "archive_bound_rotating" if parent_schedule is not None else "production_legacy"
        )
        if authority.get("parentScheduleMode") != expected_schedule_mode:
            raise TemporalDiscoveryContractError("admission parent schedule mode is invalid")
    else:
        raise TemporalDiscoveryContractError("admission parent archive mode is invalid")
    return authority


def load_admission_authority(
    *,
    supervisor_config_path: Path | None,
    pair_config_path: Path | None,
    initial_archive_path: Path | None,
    parent_archive_override_path: Path | None = None,
) -> dict[str, Any]:
    """Freeze and verify the one authority shared by both engines."""

    if supervisor_config_path is not None:
        if pair_config_path is not None or initial_archive_path is not None:
            raise TemporalDiscoveryContractError(
                "--supervisor-config cannot be combined with explicit pair/archive inputs"
            )
        config = _read_object(supervisor_config_path.resolve(), label="supervisor config")
        # This is the production verifier.  It reopens path-backed authority
        # inputs but does not run proposal evaluation or contact a service.
        from autoresearch.temporal_qd_supervisor import _validate_frozen_sources

        supplied_config_sha = config.get("configSha256")
        config_material = dict(config)
        config_material.pop("configSha256", None)
        if supplied_config_sha != canonical_sha256(config_material):
            raise TemporalDiscoveryContractError("supervisor frozen config identity mismatch")
        verification_method = "production_frozen_source_verifier"
        if "pairGenerationRuntime" in config:
            _validate_frozen_sources(config)
        else:
            # Frozen campaigns authored before runtime selection became an
            # explicit field remain useful pair/evidence authorities.  The
            # admission harness selects both runtimes itself, so re-run the
            # current production source verifier on a detached projection with
            # only that new coordinator field supplied.  The original config
            # hash above remains the reported authority identity.
            projected = json.loads(_canonical_json(config))
            projected["pairGenerationRuntime"] = build_pair_generation_runtime_config(
                engine=PAIR_GENERATION_RUNTIME_PYTHON,
                execution_timeout_seconds=3600,
            )
            projected.pop("configSha256", None)
            projected["configSha256"] = canonical_sha256(projected)
            _validate_frozen_sources(projected)
            verification_method = "production_frozen_source_verifier_with_runtime_field_projection"
        pair_value = config.get("bidirectionalPairGeneration")
        binding = config.get("initialArchive")
        evaluation = config.get("evaluation")
        if not isinstance(pair_value, Mapping) or not isinstance(binding, Mapping) or not isinstance(evaluation, Mapping):
            raise TemporalDiscoveryContractError(
                "supervisor config lacks pair generation, initial archive, or evidence authority"
            )
        pair_config = load_pair_run_config(pair_value)
        initial_archive_path_from_config = Path(str(binding.get("path") or "")).resolve()
        initial_archive, initial_archive_sha = _load_archive(initial_archive_path_from_config)
        if initial_archive_sha != binding.get("archiveSha256"):
            raise TemporalDiscoveryContractError("supervisor initial archive binding drifted")
        evidence_context = evaluation.get("predeclaredEvidenceContext")
        if not isinstance(evidence_context, Mapping):
            raise TemporalDiscoveryContractError("supervisor evidence identity context is unavailable")
        parameters = config.get("frozenSearchPolicy")
        if not isinstance(parameters, Mapping):
            raise TemporalDiscoveryContractError("supervisor search policy is unavailable")
        source: dict[str, Any] = {
            "kind": "verified_supervisor_config",
            "path": str(supervisor_config_path.resolve()),
            "configSha256": config["configSha256"],
            "verificationMethod": verification_method,
        }
        publication = _publication_authority(config)
        generation_funnel_enabled = bool((config.get("generationFunnel") or {}).get("enabled"))
        if parent_archive_override_path is None:
            archive_path = initial_archive_path_from_config
            archive = initial_archive
            archive_sha = initial_archive_sha
            parent_schedule = binding.get("parentSchedule")
            parent_mode = _ADMISSION_MODE_EMPTY_G0
            generation_index = 1
            g0_bootstrap_enabled = True
            eligible_parent_count = 0
            parent_schedule_mode = "supervisor_initial_binding"
        else:
            archive_path = parent_archive_override_path.resolve()
            archive, archive_sha = _load_archive(archive_path)
            if archive.get("bidirectionalPairPolicy") != pair_policy_from_config(pair_config):
                raise TemporalDiscoveryContractError(
                    "admission parent archive is bound to another pair policy"
                )
            generation_index, parent_schedule, eligible_parent_count = _nonempty_parent_archive_binding(
                archive
            )
            source["parentArchiveOverridePath"] = str(archive_path)
            source["parentArchiveOverrideSha256"] = archive_sha
            parent_mode = _ADMISSION_MODE_PARENT_ARCHIVE
            g0_bootstrap_enabled = False
            parent_schedule_mode = (
                "archive_bound_rotating" if parent_schedule is not None else "production_legacy"
            )
    else:
        if parent_archive_override_path is not None:
            raise TemporalDiscoveryContractError(
                "--parent-archive requires --supervisor-config"
            )
        if pair_config_path is None or initial_archive_path is None:
            raise TemporalDiscoveryContractError(
                "explicit admission requires both --pair-config and --initial-archive"
            )
        pair_config = load_pair_run_config(
            _read_object(pair_config_path.resolve(), label="pair config")
        )
        archive_path = initial_archive_path.resolve()
        archive, archive_sha = _load_archive(archive_path)
        evidence_context = qd_predeclared_evidence_context({})
        parameters = dict(DEFAULT_QD_PARAMETERS)
        source = {
            "kind": "explicit_pair_config_and_initial_archive",
            "pairConfigPath": str(pair_config_path.resolve()),
            "initialArchivePath": str(archive_path),
        }
        publication = _publication_authority()
        parent_schedule = None
        generation_funnel_enabled = False
        parent_mode = _ADMISSION_MODE_EMPTY_G0
        generation_index = 1
        g0_bootstrap_enabled = True
        eligible_parent_count = 0
        parent_schedule_mode = "explicit_initial_archive"
    if parent_mode == _ADMISSION_MODE_EMPTY_G0:
        _assert_empty_archive(archive)
    if archive.get("bidirectionalPairPolicy") != pair_policy_from_config(pair_config):
        raise TemporalDiscoveryContractError("admission archive is bound to another pair policy")
    authority: dict[str, Any] = {
        "schemaVersion": AUTHORITY_SCHEMA,
        "source": source,
        "pairRunConfig": pair_config,
        "parentArchivePath": str(archive_path),
        "parentArchiveSha256": archive_sha,
        "parentArchiveMode": parent_mode,
        "generationIndex": generation_index,
        "g0BootstrapEnabled": g0_bootstrap_enabled,
        "eligibleParentCount": eligible_parent_count,
        "parentSchedule": parent_schedule,
        "parentScheduleMode": parent_schedule_mode,
        "evidenceIdentityContext": evidence_context,
        "baseParameters": parameters,
        "qdPublicationAuthority": publication,
        "generationFunnelEnabled": generation_funnel_enabled,
        "marketEvidenceRead": False,
        "lakeContacted": False,
        "gatewayContacted": False,
        "economicWorkPerformed": False,
    }
    authority["authoritySha256"] = canonical_sha256(authority)
    return _validate_authority(authority)


def _shape_parameters(authority: Mapping[str, Any], shape: int) -> dict[str, Any]:
    parameters = json.loads(_canonical_json(authority["baseParameters"]))
    parameters["targetUniqueCandidates"] = shape
    parameters["maxProposalAttempts"] = max(
        int(parameters.get("maxProposalAttempts") or 0), shape * 8
    )
    return parameters


def _result_identity(result: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "completed",
        "configSha256",
        "generationIndex",
        "proposalCount",
        "candidateCount",
        "targetUniqueCandidates",
        "populationSha256",
        "populationFileSha256",
        "evaluationPopulationSha256",
        "journalSha256",
        "nextImmigrantContinuationOrdinal",
        "proposalSlots",
        "g0Bootstrap",
    )
    return {key: result[key] for key in keys if key in result}


def _run_generation_worker(request: Mapping[str, Any]) -> dict[str, Any]:
    authority = _validate_authority(request["authority"])
    engine = str(request["engine"])
    if engine not in ENGINES:
        raise TemporalDiscoveryContractError("admission worker engine is invalid")
    shape = int(request["shape"])
    output_root = Path(str(request["outputRoot"])).resolve()
    parameters = _shape_parameters(authority, shape)
    is_g0 = authority["parentArchiveMode"] == _ADMISSION_MODE_EMPTY_G0
    runtime = build_pair_generation_runtime_config(
        engine=engine,
        execution_timeout_seconds=max(60, int(math.ceil(float(request["timeoutSeconds"])))),
    )
    common = {
        "parent_archive_path": Path(str(authority["parentArchivePath"])),
        "parent_archive_sha256": authority["parentArchiveSha256"],
        "parent_schedule": authority.get("parentSchedule"),
        "output_root": output_root,
        "generation_index": int(authority["generationIndex"]),
        "allow_empty_quality_bootstrap": is_g0,
        "parameters": parameters,
        "evidence_identity_context": authority["evidenceIdentityContext"],
        "identity_ledger_path": output_root / "identity-ledger.json",
        "max_new_proposals": request.get("maxNewProposals"),
        "generation_funnel_enabled": bool(authority["generationFunnelEnabled"]),
        "pair_generation_runtime": runtime,
        "bidirectional_pair_run_config": authority["pairRunConfig"],
        "bidirectional_pair_policy": pair_policy_from_config(authority["pairRunConfig"]),
        "bidirectional_operator_implementation_identity": authority["pairRunConfig"][
            "operatorImplementation"
        ],
        "initial_construction_pool_size": shape if is_g0 else None,
        "evaluation_population_size": shape if is_g0 else None,
        "qd_publication_authority": authority["qdPublicationAuthority"],
    }
    if engine == PAIR_GENERATION_RUNTIME_PYTHON:
        with PairAuthorityBundle(authority["pairRunConfig"]) as pair_authority:
            result = generate_qd_generation(
                **common,
                bidirectional_pair_factory=pair_authority.factory,
                bidirectional_module_authority=pair_authority.operator,
                bidirectional_native_validator=pair_authority.validator,
                bidirectional_pair_compiler=pair_authority.compiler,
            )
    else:
        result = generate_qd_generation(**common)
    return {
        "schemaVersion": WORKER_SCHEMA,
        "engine": engine,
        "shape": shape,
        "maxNewProposals": request.get("maxNewProposals"),
        "resultIdentity": _result_identity(result),
    }


class _ProcessTreeTelemetry:
    """Bounded, best-effort RSS attribution for one isolated worker tree.

    The existing aggregate readings remain the admission benchmark contract.
    The additional role readings are diagnostic: each is the largest *single
    sample aggregate* for processes assigned to that role, so independently
    peaking roles are deliberately not added together.  We retain only four
    fixed role totals and one executable aggregate per role, never a growing
    PID/process history.
    """

    def __init__(self, pid: int, *, dashboard_validator_script: str | None = None) -> None:
        self.root = psutil.Process(pid)
        self.root_pid = pid
        self.dashboard_validator_script = self._normalise_path(dashboard_validator_script)
        self.peak_rss = 0
        self.cpu_seconds = 0.0
        self.read_bytes = 0
        self.write_bytes = 0
        self.samples = 0
        self.role_peak_rss = {role: 0 for role in _PROCESS_ROLE_ORDER}
        self.executable_role_peak_rss: dict[str, dict[str, int]] = {
            role: {} for role in _PROCESS_ROLE_ORDER if role != _PROCESS_ROLE_OTHER
        }

    @staticmethod
    def _normalise_path(value: str | None) -> str | None:
        if not value:
            return None
        try:
            return os.path.normcase(os.path.normpath(value))
        except (TypeError, ValueError, OSError):
            return None

    @staticmethod
    def _process_command_line(process: psutil.Process) -> tuple[str, ...]:
        try:
            command = process.cmdline()
        except (psutil.Error, OSError):
            return ()
        return tuple(str(part) for part in command)

    @staticmethod
    def _process_executable_name(process: psutil.Process) -> str:
        try:
            executable = process.exe()
        except (psutil.Error, OSError):
            executable = ""
        if not executable:
            try:
                executable = process.name()
            except (psutil.Error, OSError):
                executable = ""
        name = executable.replace("\\", "/").rsplit("/", 1)[-1].strip().lower()
        return name or "unknown-executable"

    def _role_for(self, process: psutil.Process, command: tuple[str, ...]) -> str:
        if process.pid == self.root_pid:
            return _PROCESS_ROLE_WORKER
        executable = self._process_executable_name(process)
        command_names = {
            item.replace("\\", "/").rsplit("/", 1)[-1].strip().lower()
            for item in command
            if item
        }
        if executable.startswith("temporal-qd-batch") or any(
            name.startswith("temporal-qd-batch") for name in command_names
        ):
            return _PROCESS_ROLE_BATCH
        if self.dashboard_validator_script is not None and any(
            self._normalise_path(item) == self.dashboard_validator_script for item in command
        ):
            return _PROCESS_ROLE_DASHBOARD
        return _PROCESS_ROLE_OTHER

    def _bounded_executable_bucket(self, *, role: str, executable: str) -> str | None:
        """Keep named-role attribution useful without retaining arbitrary trees."""

        if role == _PROCESS_ROLE_OTHER:
            return None
        buckets = self.executable_role_peak_rss[role]
        if executable in buckets or len(buckets) < _MAX_EXECUTABLES_PER_NAMED_ROLE:
            return executable
        return "other-observed-executables"

    def sample(self) -> None:
        try:
            processes = [self.root, *self.root.children(recursive=True)]
        except (psutil.Error, OSError):
            processes = []
        rss = 0
        cpu = 0.0
        reads = 0
        writes = 0
        rss_by_role = {role: 0 for role in _PROCESS_ROLE_ORDER}
        # There can be several arbitrary unknown descendants.  Fold them into
        # their fixed role instead of retaining a process/PID history.
        rss_by_executable_role: dict[str, dict[str, int]] = {
            role: {} for role in _PROCESS_ROLE_ORDER if role != _PROCESS_ROLE_OTHER
        }
        for process in {item.pid: item for item in processes}.values():
            try:
                process_rss = int(process.memory_info().rss)
                rss += process_rss
                times = process.cpu_times()
                cpu += float(times.user + times.system)
                io = process.io_counters()
                reads += int(io.read_bytes)
                writes += int(io.write_bytes)
            except (psutil.Error, OSError):
                continue
            command = self._process_command_line(process)
            role = self._role_for(process, command)
            rss_by_role[role] += process_rss
            bucket = self._bounded_executable_bucket(
                role=role, executable=self._process_executable_name(process)
            )
            if bucket is not None:
                by_executable = rss_by_executable_role[role]
                by_executable[bucket] = by_executable.get(bucket, 0) + process_rss
        self.samples += 1
        self.peak_rss = max(self.peak_rss, rss)
        self.cpu_seconds = max(self.cpu_seconds, cpu)
        self.read_bytes = max(self.read_bytes, reads)
        self.write_bytes = max(self.write_bytes, writes)
        for role in _PROCESS_ROLE_ORDER:
            self.role_peak_rss[role] = max(self.role_peak_rss[role], rss_by_role[role])
        for role, by_executable in rss_by_executable_role.items():
            peaks = self.executable_role_peak_rss[role]
            for executable, sampled_rss in by_executable.items():
                peaks[executable] = max(peaks.get(executable, 0), sampled_rss)

    def role_rss_report(self) -> dict[str, Any]:
        """Return additive, fixed-size role attribution suitable for JSON reports."""

        return {
            "processRolePeakRssBytes": dict(self.role_peak_rss),
            # Kept separately for report readers that want a per-executable
            # view.  Today each named role has one expected executable; the
            # untrusted/unknown descendant bucket intentionally remains folded.
            "executableRolePeakRssBytes": {
                **{
                    role: dict(sorted(peaks.items()))
                    for role, peaks in self.executable_role_peak_rss.items()
                },
                _PROCESS_ROLE_OTHER: {"allOtherDescendants": self.role_peak_rss[_PROCESS_ROLE_OTHER]},
            },
            "rssAttributionScope": "best_effort_sampled_recursive_process_tree",
        }


def _terminate_tree(process: subprocess.Popen[Any]) -> None:
    try:
        root = psutil.Process(process.pid)
        children = root.children(recursive=True)
    except (psutil.Error, OSError):
        children = []
    for child in reversed(children):
        try:
            child.kill()
        except (psutil.Error, OSError):
            pass
    try:
        process.kill()
    except OSError:
        pass


def _dashboard_validator_script_from_authority(authority: Mapping[str, Any]) -> str | None:
    """Return only the already-frozen child script used for role attribution."""

    pair_config = authority.get("pairRunConfig")
    if not isinstance(pair_config, Mapping):
        return None
    transport = pair_config.get("nativeJsonlAuthority")
    if not isinstance(transport, Mapping):
        return None
    script = transport.get("validatorScriptPath")
    return str(script) if isinstance(script, str) and script else None


def _run_isolated_invocation(
    *,
    authority: Mapping[str, Any],
    engine: str,
    shape: int,
    output_root: Path,
    max_new_proposals: int | None,
    timeout_seconds: float,
    control_root: Path,
    invocation_id: str,
) -> dict[str, Any]:
    request_path = control_root / "requests" / f"{invocation_id}.json"
    result_path = control_root / "results" / f"{invocation_id}.json"
    request = {
        "schemaVersion": WORKER_SCHEMA,
        "authority": authority,
        "engine": engine,
        "shape": shape,
        "outputRoot": str(output_root.resolve()),
        "maxNewProposals": max_new_proposals,
        "timeoutSeconds": timeout_seconds,
        "resultPath": str(result_path.resolve()),
    }
    _write_object(request_path, request)
    print(
        _canonical_json(
            {
                "event": "admission_invocation_started",
                "engine": engine,
                "shape": shape,
                "invocation": invocation_id,
                "maxNewProposals": max_new_proposals,
            }
        ),
        flush=True,
    )
    started = time.perf_counter()
    process = subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), "--_worker-request", str(request_path)],
        cwd=Path(__file__).resolve().parents[1],
    )
    telemetry = _ProcessTreeTelemetry(
        process.pid,
        dashboard_validator_script=_dashboard_validator_script_from_authority(authority),
    )
    timed_out = False
    while process.poll() is None:
        telemetry.sample()
        if time.perf_counter() - started > timeout_seconds:
            timed_out = True
            _terminate_tree(process)
            break
        time.sleep(0.05)
    telemetry.sample()
    process.wait()
    wall_seconds = time.perf_counter() - started
    if timed_out:
        raise TemporalDiscoveryContractError(
            f"{engine} shape {shape} exceeded the {timeout_seconds:g}s admission timeout"
        )
    if process.returncode != 0 or not result_path.is_file():
        raise TemporalDiscoveryContractError(
            f"{engine} shape {shape} admission worker failed with exit code {process.returncode}"
        )
    worker = _read_object(result_path, label="admission worker result")
    observation = {
        "wallSeconds": wall_seconds,
        "processCpuSeconds": telemetry.cpu_seconds,
        "peakRssBytes": telemetry.peak_rss,
        "readBytes": telemetry.read_bytes,
        "writeBytes": telemetry.write_bytes,
        "artifactBytes": _artifact_bytes(output_root),
        "sampleCount": telemetry.samples,
        "measurementScope": "isolated_worker_and_recursive_children",
        **telemetry.role_rss_report(),
    }
    print(
        _canonical_json(
            {
                "event": "admission_invocation_completed",
                "engine": engine,
                "shape": shape,
                "invocation": invocation_id,
                **observation,
            }
        ),
        flush=True,
    )
    return {"resultIdentity": worker["resultIdentity"], "telemetry": observation}


def _digest_rows(rows: Iterable[Mapping[str, Any]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        encoded = _canonical_json(row).encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return "sha256:" + digest.hexdigest()


def _identity_projection(root: Path) -> dict[str, Any]:
    proposal_digest = hashlib.sha256()
    candidate_digest = hashlib.sha256()
    proposal_count = 0
    candidate_count = 0
    for ordinal, path in enumerate(sorted((root / "proposal-journal").glob("*.json"))):
        entry = _read_object(path, label="proposal journal entry")
        if entry.get("proposalOrdinal") != ordinal:
            raise TemporalDiscoveryContractError("proposal journal ordinals are not contiguous")
        supplied_entry_sha = entry.get("entrySha256")
        material = dict(entry)
        material.pop("entrySha256", None)
        if supplied_entry_sha != canonical_sha256(material):
            raise TemporalDiscoveryContractError("proposal journal entry identity mismatch")
        proposal = entry.get("proposal")
        proposal_sha = proposal.get("proposalSha256") if isinstance(proposal, Mapping) else None
        row = {
            "proposalOrdinal": ordinal,
            "entrySha256": supplied_entry_sha,
            "proposalSha256": proposal_sha,
            "disposition": entry.get("disposition"),
        }
        encoded = _canonical_json(row).encode("utf-8")
        proposal_digest.update(len(encoded).to_bytes(8, "big"))
        proposal_digest.update(encoded)
        proposal_count += 1
        candidate = entry.get("candidate")
        if entry.get("disposition") == "accepted" and isinstance(candidate, Mapping):
            candidate_row = {
                key: candidate.get(key)
                for key in (
                    "candidateId",
                    "candidateIdentitySha256",
                    "programSha256",
                    "sourceProfileSha256",
                    "profileSnapshotSha256",
                    "pairProposalSha256",
                )
            }
            candidate_encoded = _canonical_json(candidate_row).encode("utf-8")
            candidate_digest.update(len(candidate_encoded).to_bytes(8, "big"))
            candidate_digest.update(candidate_encoded)
            candidate_count += 1
    ledger_path = root / "identity-ledger.json"
    ledger = _read_object(ledger_path, label="identity ledger")
    ledger_sha = _ledger_identity(ledger)
    ledger_records = ledger.get("records") or []
    if not isinstance(ledger_records, list):
        raise TemporalDiscoveryContractError("identity ledger records are invalid")
    g0_records = tuple(
        {
            "path": path.relative_to(root).as_posix(),
            "byteLength": path.stat().st_size,
            "fileSha256": _sha256_file(path),
        }
        for path in sorted((root / "g0-bootstrap").glob("*.json"))
    )
    return {
        "proposalCount": proposal_count,
        "proposalIdentitySha256": "sha256:" + proposal_digest.hexdigest(),
        "candidateCount": candidate_count,
        "candidateIdentitySha256": "sha256:" + candidate_digest.hexdigest(),
        "ledgerRecordCount": len(ledger_records),
        "ledgerSha256": ledger_sha,
        "ledgerFileSha256": _sha256_file(ledger_path),
        "ledgerRecordIdentitySha256": _digest_rows(ledger_records),
        "g0ArtifactCount": len(g0_records),
        "g0IdentitySha256": _digest_rows(g0_records),
    }


def _parent_origin_report(root: Path) -> dict[str, Any]:
    """Prove that a parent admission actually exercised reproduction.

    Byte parity alone could otherwise admit two engines that accidentally ran
    an immigrant-only path.  The proposal journal remains the source of truth
    because it records rejected as well as accepted structural proposals.
    """

    origin_counts: dict[str, int] = {}
    accepted_structural_count = 0
    accepted_mutation_count = 0
    accepted_crossover_count = 0
    for path in sorted((root / "proposal-journal").glob("*.json")):
        entry = _read_object(path, label="parent proposal journal entry")
        origin = entry.get("originKind")
        if not isinstance(origin, str):
            raise TemporalDiscoveryContractError("parent proposal entry lacks origin kind")
        origin_counts[origin] = origin_counts.get(origin, 0) + 1
        proposal = entry.get("proposal")
        if (
            isinstance(proposal, Mapping)
            and origin == "structural_offspring"
            and entry.get("disposition") == "accepted"
        ):
            accepted_structural_count += 1
            mutation_steps = proposal.get("mutationSteps")
            if isinstance(mutation_steps, list) and mutation_steps:
                accepted_mutation_count += 1
            if isinstance(proposal.get("crossoverAudit"), Mapping):
                accepted_crossover_count += 1
    structural_count = origin_counts.get("structural_offspring", 0)
    if structural_count < 1:
        raise TemporalDiscoveryContractError(
            "parent archive admission emitted no structural offspring proposals"
        )
    if accepted_mutation_count < 1:
        raise TemporalDiscoveryContractError(
            "parent archive admission materialized no accepted mutation offspring"
        )
    if accepted_crossover_count < 1:
        raise TemporalDiscoveryContractError(
            "parent archive admission materialized no accepted crossover offspring"
        )
    return {
        "originProposalCounts": dict(sorted(origin_counts.items())),
        "structuralOffspringProposalCount": structural_count,
        "acceptedStructuralOffspringCount": accepted_structural_count,
        "acceptedMutationOffspringCount": accepted_mutation_count,
        "acceptedCrossoverOffspringCount": accepted_crossover_count,
    }


InvocationRunner = Callable[..., dict[str, Any]]


def run_admission(
    *,
    authority: Mapping[str, Any],
    output_root: Path,
    shapes: tuple[int, ...] = (1,),
    split_proposal_count: int | None = None,
    timeout_seconds: float = 3600.0,
    invocation_runner: InvocationRunner = _run_isolated_invocation,
) -> dict[str, Any]:
    """Run and assert the complete cross-engine admission matrix."""

    checked_authority = _validate_authority(authority)
    if any(shape not in SHAPES for shape in shapes) or not shapes:
        raise TemporalDiscoveryContractError("admission shapes are invalid")
    if split_proposal_count is not None and split_proposal_count < 0:
        raise TemporalDiscoveryContractError("split proposal count cannot be negative")
    if timeout_seconds <= 0:
        raise TemporalDiscoveryContractError("admission timeout must be positive")
    root = output_root.resolve()
    if root.exists() and any(root.iterdir()):
        raise TemporalDiscoveryContractError("admission output root must be absent or empty")
    root.mkdir(parents=True, exist_ok=True)
    control_root = root / "control"
    _write_object(control_root / "authority.json", checked_authority)
    report: dict[str, Any] = {
        "schemaVersion": REPORT_SCHEMA,
        "mode": "no_market_no_lake_no_gateway_no_economic_work",
        "parentArchiveMode": checked_authority["parentArchiveMode"],
        "generationIndex": checked_authority["generationIndex"],
        "g0BootstrapEnabled": checked_authority["g0BootstrapEnabled"],
        "eligibleParentCount": checked_authority["eligibleParentCount"],
        "parentScheduleMode": checked_authority["parentScheduleMode"],
        "authoritySha256": checked_authority["authoritySha256"],
        "parentArchiveSha256": checked_authority["parentArchiveSha256"],
        "pairRunConfigSha256": checked_authority["pairRunConfig"]["pairRunConfigSha256"],
        "shapes": list(shapes),
        "engines": list(ENGINES),
        "maximumLocalConcurrency": 1,
        "populationHandling": "hash_compare_in_place_never_copy",
        "runs": [],
    }
    for shape in shapes:
        split_at = split_proposal_count if split_proposal_count is not None else (0 if shape == 1 else max(1, shape // 2))
        by_engine: dict[str, Any] = {}
        final_roots: dict[str, dict[str, Path]] = {}
        for engine in ENGINES:
            engine_label = "python" if engine == PAIR_GENERATION_RUNTIME_PYTHON else "rust"
            engine_root = root / f"shape-{shape:04d}" / engine_label
            full_root = engine_root / "full"
            split_root = engine_root / "split-restart"
            base = {
                "authority": checked_authority,
                "engine": engine,
                "shape": shape,
                "timeout_seconds": timeout_seconds,
                "control_root": control_root,
            }
            full = invocation_runner(
                **base,
                output_root=full_root,
                max_new_proposals=None,
                invocation_id=f"shape-{shape:04d}-{engine_label}-full",
            )
            partial = invocation_runner(
                **base,
                output_root=split_root,
                max_new_proposals=split_at,
                invocation_id=f"shape-{shape:04d}-{engine_label}-split",
            )
            if partial["resultIdentity"].get("completed") is True:
                raise TemporalDiscoveryContractError(
                    f"shape {shape} split count {split_at} completed before restart"
                )
            resumed = invocation_runner(
                **base,
                output_root=split_root,
                max_new_proposals=None,
                invocation_id=f"shape-{shape:04d}-{engine_label}-resume",
            )
            if full["resultIdentity"].get("completed") is not True or resumed["resultIdentity"].get("completed") is not True:
                raise TemporalDiscoveryContractError(f"{engine} shape {shape} did not complete")
            restart_comparison = front_half_oracle.compare_roots_bounded_exact(
                full_root, split_root, shape=shape
            )
            full_identity = _identity_projection(full_root)
            split_identity = _identity_projection(split_root)
            parent_origin = (
                _parent_origin_report(full_root)
                if checked_authority["parentArchiveMode"] == _ADMISSION_MODE_PARENT_ARCHIVE
                else None
            )
            if (
                restart_comparison["byteExact"] is not True
                or restart_comparison["semanticExact"] is not True
                or full["resultIdentity"] != resumed["resultIdentity"]
                or full_identity != split_identity
            ):
                raise TemporalDiscoveryContractError(
                    f"{engine} shape {shape} split/restart parity failed"
                )
            by_engine[engine_label] = {
                "full": full,
                "split": partial,
                "resume": resumed,
                "restartComparison": restart_comparison,
                "identityProjection": full_identity,
                **({"parentOriginReport": parent_origin} if parent_origin is not None else {}),
            }
            final_roots[engine_label] = {"full": full_root, "split": split_root}
        cross_full = front_half_oracle.compare_roots_bounded_exact(
            final_roots["python"]["full"], final_roots["rust"]["full"], shape=shape
        )
        cross_split = front_half_oracle.compare_roots_bounded_exact(
            final_roots["python"]["split"], final_roots["rust"]["split"], shape=shape
        )
        if (
            cross_full["byteExact"] is not True
            or cross_full["semanticExact"] is not True
            or cross_split["byteExact"] is not True
            or cross_split["semanticExact"] is not True
            or by_engine["python"]["full"]["resultIdentity"]
            != by_engine["rust"]["full"]["resultIdentity"]
            or by_engine["python"]["identityProjection"]
            != by_engine["rust"]["identityProjection"]
            or (
                checked_authority["parentArchiveMode"] == _ADMISSION_MODE_PARENT_ARCHIVE
                and by_engine["python"].get("parentOriginReport")
                != by_engine["rust"].get("parentOriginReport")
            )
        ):
            raise TemporalDiscoveryContractError(
                f"Python/Rust public admission parity failed for shape {shape}"
            )
        report["runs"].append(
            {
                "shape": shape,
                "splitProposalCount": split_at,
                "derivedParametersSha256": canonical_sha256(
                    _shape_parameters(checked_authority, shape)
                ),
                "python": by_engine["python"],
                "rust": by_engine["rust"],
                "crossEngineFullComparison": cross_full,
                "crossEngineSplitComparison": cross_split,
            }
        )
    report["allPublicSemanticTreesAndBytesExact"] = True
    report["allProposalCandidateLedgerAndG0IdentitiesExact"] = True
    report["allSplitRestartsExact"] = True
    report["reportSha256"] = canonical_sha256(report)
    _write_object(root / "admission-report.json", report)
    return report


def _worker_main(request_path: Path) -> int:
    request = _read_object(request_path.resolve(), label="admission worker request")
    result = _run_generation_worker(request)
    _write_object(Path(str(request["resultPath"])), result)
    return 0


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--supervisor-config", type=Path)
    source.add_argument("--pair-config", type=Path)
    parser.add_argument("--initial-archive", type=Path)
    parser.add_argument(
        "--parent-archive",
        type=Path,
        help=(
            "verified nonempty parent archive override; requires --supervisor-config "
            "and derives the next generation"
        ),
    )
    parser.add_argument("--shapes", type=_parse_shapes, default=(1,))
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--split-proposal-count", type=int)
    parser.add_argument("--timeout", type=float, default=3600.0)
    parser.add_argument("--_worker-request", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args._worker_request is not None:
        return _worker_main(args._worker_request)
    if args.output_root is None:
        parser.error("--output-root is required")
    if args.supervisor_config is None and args.pair_config is None:
        parser.error("provide --supervisor-config or --pair-config with --initial-archive")
    if args.pair_config is not None and args.initial_archive is None:
        parser.error("--initial-archive is required with --pair-config")
    if args.supervisor_config is not None and args.initial_archive is not None:
        parser.error("--initial-archive is derived from --supervisor-config")
    if args.parent_archive is not None and args.supervisor_config is None:
        parser.error("--parent-archive requires --supervisor-config")
    if args.split_proposal_count is not None and args.split_proposal_count < 0:
        parser.error("--split-proposal-count cannot be negative")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    authority = load_admission_authority(
        supervisor_config_path=args.supervisor_config,
        pair_config_path=args.pair_config,
        initial_archive_path=args.initial_archive,
        parent_archive_override_path=args.parent_archive,
    )
    report = run_admission(
        authority=authority,
        output_root=args.output_root,
        shapes=args.shapes,
        split_proposal_count=args.split_proposal_count,
        timeout_seconds=args.timeout,
    )
    print(
        _canonical_json(
            {
                "admitted": True,
                "report": str((args.output_root.resolve() / "admission-report.json")),
                "reportSha256": report["reportSha256"],
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
