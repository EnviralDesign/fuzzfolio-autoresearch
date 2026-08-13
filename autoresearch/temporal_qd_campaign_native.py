"""Closed Rust bridge for the bounded v5 candidate/window task matrix.

This module deliberately accepts a sealed authority only.  It has no factory,
Dashboard, profile compiler, or Python task enumeration path; callers that
need the historical freezer must name that Python function as an oracle.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from . import temporal_qd_native as native
from .temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256
from .temporal_qd_evaluation_population import raw_file_sha256

NATIVE_CAMPAIGN_MANIFEST_SCHEMA = "temporal_qd_native_campaign_task_matrix_manifest_v1"
NATIVE_CAMPAIGN_RESULT_SCHEMA = "temporal_qd_native_campaign_task_matrix_result_v1"
NATIVE_V5_FREEZE_MANIFEST_SCHEMA = "temporal_qd_v5_native_campaign_freeze_manifest_v2"
NATIVE_V5_CAMPAIGN_INPUT_RESULT_SCHEMA = "temporal_qd_v5_campaign_input_result_v1"
NATIVE_V5_CAMPAIGN_INPUT_CHECKPOINT_SCHEMA = "temporal_qd_v5_campaign_input_checkpoint_v1"
NATIVE_V5_LADDER_FREEZE_MANIFEST_SCHEMA = "temporal_qd_v5_native_evidence_ladder_freeze_manifest_v2"
NATIVE_V5_LADDER_FREEZE_RESULT_SCHEMA = "temporal_qd_v5_native_evidence_ladder_freeze_result_v1"
NATIVE_V5_LADDER_ARCHIVE_FREEZE_MANIFEST_SCHEMA = (
    "temporal_qd_v5_native_evidence_ladder_freeze_manifest_v3"
)
NATIVE_V5_LADDER_ARCHIVE_FREEZE_RESULT_SCHEMA = (
    "temporal_qd_v5_native_evidence_ladder_freeze_result_v3"
)
_BINARY_NAME = "temporal-qd-campaign-freeze"
_CURRENT_V5_COMPACT_DOCUMENT_LIMIT_BYTES = 1_048_576
_CURRENT_V5_COMPACT_STDOUT_LIMIT_BYTES = 1_048_576
_CURRENT_V5_COMPACT_STDERR_LIMIT_BYTES = 262_144


def _require_regular_file(path: Path, *, name: str) -> Path:
    """Return a real file only; v5 control-plane binaries may not be aliases."""

    try:
        status = path.lstat()
    except FileNotFoundError as exc:
        raise TemporalDiscoveryContractError(f"{name} is unavailable") from exc
    except OSError as exc:
        raise TemporalDiscoveryContractError(f"could not inspect {name}: {path}") from exc
    reparse_point = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
    attributes = getattr(status, "st_file_attributes", 0)
    if (
        stat.S_ISLNK(status.st_mode)
        or bool(attributes & reparse_point)
        or not stat.S_ISREG(status.st_mode)
    ):
        raise TemporalDiscoveryContractError(f"{name} is not a real regular file")
    return path


def _native_transport_path_matches(
    reported_path: object, expected_path: Path | str
) -> bool:
    """Admit Rust's one canonical Windows spelling for a validated path.

    ``std::fs::canonicalize`` prepends ``\\\\?\\`` to ordinary drive-rooted
    paths on Windows.  Preserve that reported spelling in every Rust receipt,
    but treat it as the same transport location as the already-resolved path
    passed to the native freezer.  No UNC, separator, case, or other alias is
    admitted.
    """

    if not isinstance(reported_path, str):
        return False
    try:
        expected = os.fspath(expected_path)
    except TypeError:
        return False
    if not isinstance(expected, str) or not expected:
        return False
    if reported_path == expected:
        return True
    if os.name != "nt":
        return False
    if (
        len(expected) < 3
        or not expected[0].isalpha()
        or expected[1] != ":"
        or expected[2] != "\\"
        or "/" in expected
    ):
        return False
    return reported_path == "\\\\?\\" + expected


def _read_current_v5_compact_bytes(
    path: Path | str,
    *,
    name: str,
    maximum_bytes: int = _CURRENT_V5_COMPACT_DOCUMENT_LIMIT_BYTES,
) -> bytes:
    """Open one current-v5 control document with a strict bounded read."""

    if (
        isinstance(maximum_bytes, bool)
        or not isinstance(maximum_bytes, int)
        or maximum_bytes < 1
    ):
        raise TemporalDiscoveryContractError(f"{name} compact-document limit is invalid")
    checked = _require_regular_file(Path(path), name=name)
    try:
        if checked.stat().st_size > maximum_bytes:
            raise TemporalDiscoveryContractError(
                f"{name} exceeds the control-document limit"
            )
        with checked.open("rb") as handle:
            raw = handle.read(maximum_bytes + 1)
    except TemporalDiscoveryContractError:
        raise
    except OSError as exc:
        raise TemporalDiscoveryContractError(f"could not read {name}") from exc
    if len(raw) > maximum_bytes:
        raise TemporalDiscoveryContractError(f"{name} exceeds the control-document limit")
    return raw


def _read_current_v5_compact_json(
    path: Path | str,
    *,
    name: str,
    maximum_bytes: int = _CURRENT_V5_COMPACT_DOCUMENT_LIMIT_BYTES,
) -> dict[str, Any]:
    try:
        value = json.loads(
            _read_current_v5_compact_bytes(
                path, name=name, maximum_bytes=maximum_bytes
            )
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(f"{name} is invalid") from exc
    if not isinstance(value, dict):
        raise TemporalDiscoveryContractError(f"{name} must be an object")
    return value


def _write_current_v5_compact_manifest(
    path: Path,
    *,
    encoded: str,
    name: str,
) -> None:
    payload = encoded.encode("utf-8")
    if len(payload) > _CURRENT_V5_COMPACT_DOCUMENT_LIMIT_BYTES:
        raise TemporalDiscoveryContractError(f"{name} exceeds the control-document limit")
    if path.exists():
        existing = _read_current_v5_compact_bytes(path, name=name)
        if existing != payload:
            raise TemporalDiscoveryContractError(f"{name} drifted")
        return
    try:
        with path.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        existing = _read_current_v5_compact_bytes(path, name=name)
        if existing != payload:
            raise TemporalDiscoveryContractError(f"{name} drifted")
    except OSError as exc:
        raise TemporalDiscoveryContractError(f"could not publish {name}") from exc


def _freeze_manifest_sha256(manifest: Mapping[str, Any]) -> str:
    """Identity of the portable freeze intent, not its local transport paths."""

    return canonical_sha256(
        {
            key: value
            for key, value in manifest.items()
            if key
            not in {
                "manifestSha256",
                "outputRoot",
                "finalArchiveSha256",
                "finalArchiveReductionResultSha256",
                "finalArchiveRawSha256",
                "finalArchiveSizeBytes",
                "ladderStage",
                "ladderCandidateLimit",
                "ladderAuthority",
            }
            and not key.endswith("Path")
        }
    )


def _v3_ladder_archive_freeze_manifest_sha256(manifest: Mapping[str, Any]) -> str:
    """Match Rust's v3 semantic transport projection exactly.

    The archive receipt path and local preparation/catalog/output paths are
    operational locations.  Their sealed identities remain in the manifest,
    while the portable manifest identity binds only their immutable content
    roots and ladder authority.
    """

    semantic = dict(manifest)
    semantic.pop("manifestSha256", None)
    semantic.pop("outputRoot", None)
    semantic.pop("templatePreparationPath", None)
    semantic.pop("constructionCatalogPath", None)
    authority = semantic.get("archiveAuthority")
    if isinstance(authority, Mapping):
        copied_authority = dict(authority)
        copied_authority.pop("receiptPath", None)
        semantic["archiveAuthority"] = copied_authority
    return canonical_sha256(semantic)


def _validate_v5_campaign_input_checkpoint(
    root: Path,
    manifest: Mapping[str, Any],
    result: Mapping[str, Any],
) -> dict[str, Any]:
    """Authenticate the compact campaign-input commit without rescanning payloads."""

    checkpoint_path = _require_regular_file(
        root / "campaign-input-checkpoint.json",
        name="native v5 campaign-input checkpoint",
    )
    checkpoint = _read_current_v5_compact_json(
        checkpoint_path,
        name="native v5 campaign-input checkpoint",
    )
    expected = {
        "schemaVersion",
        "contractVersion",
        "manifestSha256",
        "nativeRuntimeAuthoritySha256",
        "generationIndex",
        "campaignRole",
        "panelId",
        "authorityId",
        "campaignSha256",
        "evaluationIdentitySha256",
        "taskMatrixSha256",
        "candidateCount",
        "windowCount",
        "taskCount",
        "taskManifest",
        "cohortPopulation",
        "sourceInputs",
        "artifactMetrics",
        "checkpointSha256",
    }
    if set(checkpoint) != expected:
        raise TemporalDiscoveryContractError(
            "native v5 campaign-input checkpoint fields drifted"
        )
    if (
        checkpoint.get("schemaVersion")
        != NATIVE_V5_CAMPAIGN_INPUT_CHECKPOINT_SCHEMA
        or checkpoint.get("contractVersion") != "temporal_qd_native_foundation_v1"
        or checkpoint.get("checkpointSha256")
        != canonical_sha256(
            {
                key: value
                for key, value in checkpoint.items()
                if key != "checkpointSha256"
            }
        )
        or checkpoint.get("manifestSha256") != manifest.get("manifestSha256")
        or checkpoint.get("nativeRuntimeAuthoritySha256")
        != manifest.get("nativeRuntimeAuthoritySha256")
    ):
        raise TemporalDiscoveryContractError(
            "native v5 campaign-input checkpoint identity drifted"
        )
    candidate_count = checkpoint.get("candidateCount")
    window_count = checkpoint.get("windowCount")
    task_count = checkpoint.get("taskCount")
    if (
        isinstance(candidate_count, bool)
        or not isinstance(candidate_count, int)
        or isinstance(window_count, bool)
        or not isinstance(window_count, int)
        or isinstance(task_count, bool)
        or not isinstance(task_count, int)
        or candidate_count * window_count != task_count
    ):
        raise TemporalDiscoveryContractError(
            "native v5 campaign-input task cardinality drifted"
        )
    descriptors = (
        (
            checkpoint.get("taskManifest"),
            "screening-run/task-manifest.json",
            "taskMatrixSha256",
            checkpoint.get("taskMatrixSha256"),
        ),
        (
            checkpoint.get("cohortPopulation"),
            "cohort-population.json",
            "populationSha256",
            checkpoint.get("cohortPopulation", {}).get("populationSha256")
            if isinstance(checkpoint.get("cohortPopulation"), Mapping)
            else None,
        ),
    )
    payload_bytes = 0
    for raw, relative, semantic_field, semantic_sha in descriptors:
        if not isinstance(raw, Mapping) or set(raw) != {
            "relativePath",
            "rawSha256",
            "sizeBytes",
            semantic_field,
        }:
            raise TemporalDiscoveryContractError(
                "native v5 campaign-input payload descriptor drifted"
            )
        if raw.get("relativePath") != relative or raw.get(semantic_field) != semantic_sha:
            raise TemporalDiscoveryContractError(
                "native v5 campaign-input payload binding drifted"
            )
        for value in (raw.get("rawSha256"), semantic_sha):
            if (
                not isinstance(value, str)
                or len(value) != 71
                or not value.startswith("sha256:")
                or any(character not in "0123456789abcdef" for character in value[7:])
            ):
                raise TemporalDiscoveryContractError(
                    "native v5 campaign-input payload identity is invalid"
                )
        path = _require_regular_file(root / relative, name=f"campaign-input {relative}")
        size = raw.get("sizeBytes")
        if isinstance(size, bool) or not isinstance(size, int) or path.stat().st_size != size:
            raise TemporalDiscoveryContractError(
                "native v5 campaign-input payload size drifted"
            )
        payload_bytes += size
    source_inputs = checkpoint.get("sourceInputs")
    if not isinstance(source_inputs, Mapping) or set(source_inputs) != {
        "evaluationPopulationRawSha256",
        "templatePreparationSha256",
        "constructionCatalogSha256",
        "preparationSha256",
    }:
        raise TemporalDiscoveryContractError(
            "native v5 campaign-input source binding drifted"
        )
    if (
        source_inputs.get("evaluationPopulationRawSha256")
        != manifest.get("evaluationPopulationSha256")
        or source_inputs.get("templatePreparationSha256")
        != manifest.get("templatePreparationSha256")
        or source_inputs.get("constructionCatalogSha256")
        != manifest.get("constructionCatalogSha256")
    ):
        raise TemporalDiscoveryContractError(
            "native v5 campaign-input immutable source identity drifted"
        )
    metrics = checkpoint.get("artifactMetrics")
    if (
        not isinstance(metrics, Mapping)
        or metrics.get("payloadFileCount") != 2
        or metrics.get("payloadBytes") != payload_bytes
        or metrics.get("taskManifestBytes")
        != checkpoint["taskManifest"]["sizeBytes"]
        or metrics.get("cohortPopulationBytes")
        != checkpoint["cohortPopulation"]["sizeBytes"]
    ):
        raise TemporalDiscoveryContractError(
            "native v5 campaign-input artifact metrics drifted"
        )
    for key in (
        "campaignSha256",
        "authorityId",
        "evaluationIdentitySha256",
        "taskMatrixSha256",
        "candidateCount",
        "windowCount",
        "taskCount",
        "campaignRole",
        "panelId",
        "cohortPopulationSha256",
        "checkpointSha256",
    ):
        if key in result and result.get(key) != (
            checkpoint["cohortPopulation"]["populationSha256"]
            if key == "cohortPopulationSha256"
            else checkpoint.get(key)
        ):
            raise TemporalDiscoveryContractError(
                "native v5 campaign-input stdout/checkpoint binding drifted"
            )
    return dict(checkpoint)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _binary_path() -> Path:
    suffix = ".exe" if os.name == "nt" else ""
    return _repo_root() / "rust" / "temporal-qd" / "target" / "release" / f"{_BINARY_NAME}{suffix}"


def _ensure_binary() -> Path:
    binary = _binary_path()
    workspace = _repo_root() / "rust" / "temporal-qd"
    sources = [workspace / "Cargo.toml", workspace / "Cargo.lock"] + list(
        (workspace / "crates" / "qd-campaign-freeze").rglob("*.rs")
    ) + [workspace / "crates" / "qd-campaign-freeze" / "Cargo.toml"]
    if binary.is_file() and all(
        source.is_file() and source.stat().st_mtime <= binary.stat().st_mtime
        for source in sources
    ):
        return binary
    completed = subprocess.run(
        ["cargo", "build", "--release", "-p", _BINARY_NAME],
        cwd=workspace,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0 or not binary.is_file():
        raise TemporalDiscoveryContractError(
            "Rust-native campaign task-matrix binary failed to build: "
            + completed.stderr[-4000:]
        )
    return binary


def materialize_qd_campaign_task_matrix_native(
    *,
    authority_path: Path | str,
    output_root: Path | str,
    behavior_attribution_requirement: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Materialize a sealed authority through Rust; no Python fallback exists."""

    authority = Path(authority_path).resolve()
    root = Path(output_root).resolve()
    if not authority.is_file():
        raise TemporalDiscoveryContractError("native campaign authority is not a file")
    root.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schemaVersion": NATIVE_CAMPAIGN_MANIFEST_SCHEMA,
        "authorityPath": str(authority),
        "outputRoot": str(root),
        "behaviorAttributionRequirement": (
            dict(behavior_attribution_requirement)
            if behavior_attribution_requirement is not None
            else None
        ),
    }
    manifest_path = root / ".native-task-matrix-manifest.json"
    encoded = json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False) + "\n"
    if manifest_path.exists() and manifest_path.read_text(encoding="utf-8") != encoded:
        raise TemporalDiscoveryContractError("native campaign task-matrix manifest drifted")
    manifest_path.write_text(encoded, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        [_ensure_binary(), "--manifest", manifest_path],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        env={**os.environ, "TEMPORAL_QD_CAMPAIGN_FREEZE_REPORT_PEAK": "1"},
    )
    if completed.returncode != 0:
        raise TemporalDiscoveryContractError(
            "Rust-native campaign task-matrix failed: " + completed.stderr[-4000:]
        )
    try:
        result = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise TemporalDiscoveryContractError("Rust-native campaign task-matrix emitted invalid JSON") from exc
    if not isinstance(result, dict) or result.get("schemaVersion") != NATIVE_CAMPAIGN_RESULT_SCHEMA:
        raise TemporalDiscoveryContractError("Rust-native campaign task-matrix result schema drifted")
    if result.get("outputRoot") != str(root):
        raise TemporalDiscoveryContractError("Rust-native campaign task-matrix output binding drifted")
    # Runtime observations are deliberately outside every durable identity.
    # They are still returned to let the campaign operator compare the native
    # seam against the old Python O(C×W) materializer.
    for line in reversed(completed.stderr.splitlines()):
        try:
            sample = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(sample, dict) and isinstance(sample.get("peakWorkingSetBytes"), int):
            result.setdefault("telemetry", {})["processPeakWorkingSetBytes"] = sample[
                "peakWorkingSetBytes"
            ]
            break
    return result


def freeze_qd_v5_campaign_native(
    *,
    evaluation_population_path: Path | str,
    evaluation_population_raw_sha256: str | None = None,
    template_preparation_path: Path | str,
    template_preparation_sha256: str | None = None,
    construction_catalog_path: Path | str,
    construction_catalog_sha256: str | None = None,
    output_root: Path | str,
    execution_engine_commit: str,
    worker_contract_sha256: str,
    rotating_evidence: Mapping[str, Any],
    archive_policy_authority: Mapping[str, Any],
    behavior_attribution_requirement: Mapping[str, Any],
    campaign_role: str,
    panel_id: str,
    cohort_selection_path: Path | str | None = None,
    final_archive_reduction_result_path: Path | str | None = None,
    ladder_stage: str | None = None,
    ladder_candidate_limit: int | None = None,
    native_binary: Path | str | None = None,
    ladder_authority: Mapping[str, Any] | None = None,
    execution_timeout_seconds: int = 300,
) -> dict[str, Any]:
    """Run the all-native v5 campaign freezer.

    This is intentionally a transport-only bridge.  It binds immutable source
    bytes/configuration and invokes Rust; it neither projects population rows
    nor rotates evidence plans nor enumerates the task matrix.
    """

    evaluation = Path(evaluation_population_path).resolve()
    template = Path(template_preparation_path).resolve()
    catalog = Path(construction_catalog_path).resolve()
    root = Path(output_root).resolve()
    selection = (
        Path(cohort_selection_path).resolve()
        if cohort_selection_path is not None
        else None
    )
    for path, name in ((evaluation, "evaluation population"), (template, "template"), (catalog, "construction catalog")):
        if not path.is_file():
            raise TemporalDiscoveryContractError(f"native v5 campaign {name} is not a file")
    ladder = (
        final_archive_reduction_result_path is not None
        or ladder_stage is not None
        or ladder_candidate_limit is not None
    )
    if selection is not None and not selection.is_file():
        raise TemporalDiscoveryContractError("native v5 campaign cohort selection is not a file")
    if ladder and (final_archive_reduction_result_path is None or ladder_stage not in {"validation", "scrutiny"} or not isinstance(ladder_candidate_limit, int) or ladder_candidate_limit < 1 or ladder_authority is None):
        raise TemporalDiscoveryContractError("native v5 ladder freeze requires an archive-reduction receipt, sealed authority, validation/scrutiny stage, and positive limit")
    if ladder and selection is not None:
        raise TemporalDiscoveryContractError("native v5 ladder freeze derives its cohort selection; external selection is forbidden")
    root.mkdir(parents=True, exist_ok=True)
    manifest_path = root / ".native-v5-campaign-freeze-manifest.json"
    selection_receipt = root / "ladder-selection-receipt.json"
    resume_ladder = ladder and selection_receipt.is_file() and manifest_path.is_file()
    ladder_archive_binding: dict[str, Any] = {}
    if ladder:
        reduction_result = _require_regular_file(
            Path(final_archive_reduction_result_path).absolute(),
            name="native v5 ladder archive-reduction result",
        )
        reduced = _read_current_v5_compact_json(
            reduction_result,
            name="native v5 ladder archive-reduction result",
            maximum_bytes=131_072,
        )
        if (
            not isinstance(reduced, dict)
            or reduced.get("schemaVersion") != "temporal_qd_native_archive_reduction_result_v1"
            or reduced.get("status") != "completed"
            or reduced.get("archivePath") != "archive.json"
            or reduced.get("resultSha256") != canonical_sha256(
                {key: value for key, value in reduced.items() if key != "resultSha256"}
            )
            or not isinstance(reduced.get("archiveSha256"), str)
            or not isinstance(reduced.get("archiveRawSha256"), str)
            or isinstance(reduced.get("archiveSizeBytes"), bool)
            or not isinstance(reduced.get("archiveSizeBytes"), int)
        ):
            raise TemporalDiscoveryContractError("native v5 ladder archive-reduction result drifted")
        # This bridge authenticates only the bounded reducer receipt. Archive
        # bytes are deliberately never decoded or hashed here; Rust owns their
        # streaming re-open and byte/semantic validation.
        final_archive = reduction_result.parent / "archive.json"
        if not final_archive.is_file() and not resume_ladder:
            raise TemporalDiscoveryContractError("native v5 ladder final archive is not a file")
        if final_archive.is_file():
            _require_regular_file(final_archive, name="native v5 ladder final archive")
        ladder_archive_binding = {
            "finalArchiveReductionResultPath": str(reduction_result),
            "finalArchiveReductionResultSha256": reduced["resultSha256"],
            "finalArchivePath": str(final_archive),
            "finalArchiveSha256": reduced["archiveSha256"],
            "finalArchiveRawSha256": reduced["archiveRawSha256"],
            "finalArchiveSizeBytes": reduced["archiveSizeBytes"],
            "ladderStage": ladder_stage,
            "ladderCandidateLimit": ladder_candidate_limit,
        }
        if not final_archive.is_file() and resume_ladder:
            previous = _read_current_v5_compact_json(
                manifest_path,
                name="native v5 ladder restart manifest",
            )
            if (
                not isinstance(previous, dict)
                or previous.get("schemaVersion") != NATIVE_V5_LADDER_FREEZE_MANIFEST_SCHEMA
                or previous.get("manifestSha256") != _freeze_manifest_sha256(previous)
                or previous.get("finalArchivePath") != str(final_archive)
                or previous.get("finalArchiveReductionResultSha256") != reduced["resultSha256"]
                or previous.get("finalArchiveRawSha256") != reduced["archiveRawSha256"]
                or previous.get("finalArchiveSizeBytes") != reduced["archiveSizeBytes"]
                or previous.get("ladderStage") != ladder_stage
                or previous.get("ladderCandidateLimit") != ladder_candidate_limit
                or previous.get("ladderAuthority") != dict(ladder_authority)
            ):
                raise TemporalDiscoveryContractError("native v5 ladder restart manifest drifted")
            ladder_archive_binding = {
                key: previous[key]
                for key in (
                    "finalArchiveReductionResultPath",
                    "finalArchiveReductionResultSha256",
                    "finalArchivePath",
                    "finalArchiveSha256",
                    "finalArchiveRawSha256",
                    "finalArchiveSizeBytes",
                    "ladderStage",
                    "ladderCandidateLimit",
                )
            }
    if (
        isinstance(execution_timeout_seconds, bool)
        or not isinstance(execution_timeout_seconds, int)
        or execution_timeout_seconds < 1
    ):
        raise TemporalDiscoveryContractError(
            "Rust-native v5 campaign-freeze timeout must be a positive integer"
        )
    if native_binary is None:
        raise TemporalDiscoveryContractError(
            "Rust-native v5 campaign-freeze requires an explicitly pinned binary"
        )
    binary = _require_regular_file(
        Path(native_binary).absolute(), name="Rust-native v5 campaign-freeze binary"
    )
    binary_sha256 = raw_file_sha256(binary)
    runtime_authority = {
        "schemaVersion": "temporal_qd_native_campaign_freeze_runtime_authority_v1",
        "runtimeEpoch": "temporal_qd_native_campaign_freeze_epoch_v2",
        "binaryRole": _BINARY_NAME,
        "binarySha256": binary_sha256,
    }
    if evaluation_population_raw_sha256 is None:
        raise TemporalDiscoveryContractError(
            "native v5 campaign requires a receipt-bound evaluation population identity"
        )
    if (
        not isinstance(evaluation_population_raw_sha256, str)
        or len(evaluation_population_raw_sha256) != 71
        or not evaluation_population_raw_sha256.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in evaluation_population_raw_sha256[7:])
    ):
        raise TemporalDiscoveryContractError(
            "native v5 campaign evaluation population identity is invalid"
        )
    evaluation_population_sha256 = evaluation_population_raw_sha256

    def sealed_identity(value: object, *, name: str) -> str:
        if (
            not isinstance(value, str)
            or len(value) != 71
            or not value.startswith("sha256:")
            or any(character not in "0123456789abcdef" for character in value[7:])
        ):
            raise TemporalDiscoveryContractError(
                f"native v5 campaign {name} identity is invalid"
            )
        return value

    template_sha256 = sealed_identity(
        template_preparation_sha256, name="template preparation"
    )
    catalog_sha256 = sealed_identity(
        construction_catalog_sha256, name="construction catalog"
    )

    # No Python loop over candidate/window material is permitted here.
    manifest = {
        "schemaVersion": NATIVE_V5_LADDER_FREEZE_MANIFEST_SCHEMA if ladder else NATIVE_V5_FREEZE_MANIFEST_SCHEMA,
        "evaluationPopulationPath": str(evaluation),
        "evaluationPopulationSha256": evaluation_population_sha256,
        "cohortSelectionPath": str(selection) if selection is not None else None,
        "templatePreparationPath": str(template),
        # These are frozen semantic roots from the supervisor authority.  Do
        # not reopen/hash the template or catalog in Python: Rust is the only
        # evaluator of those sealed inputs on the current-v5 path.
        "templatePreparationSha256": template_sha256,
        "constructionCatalogPath": str(catalog),
        "constructionCatalogSha256": catalog_sha256,
        "outputRoot": str(root),
        "executionEngineCommit": execution_engine_commit,
        "workerContractSha256": worker_contract_sha256,
        "campaignRole": campaign_role,
        "panelId": panel_id,
        "rotatingEvidence": dict(rotating_evidence),
        "archivePolicyAuthority": dict(archive_policy_authority),
        "behaviorAttributionRequirement": dict(behavior_attribution_requirement),
        "nativeRuntimeAuthority": runtime_authority,
        "nativeRuntimeAuthoritySha256": canonical_sha256(runtime_authority),
        **(ladder_archive_binding if ladder else {}),
        **({"ladderAuthority": dict(ladder_authority)} if ladder else {}),
    }
    if ladder:
        manifest["ladderInputSha256"] = canonical_sha256(
            {
                "finalArchiveSha256": manifest["finalArchiveSha256"],
                "finalArchiveRawSha256": manifest["finalArchiveRawSha256"],
                "finalArchiveSizeBytes": manifest["finalArchiveSizeBytes"],
                "finalArchiveReductionResultSha256": manifest[
                    "finalArchiveReductionResultSha256"
                ],
                "ladderStage": manifest["ladderStage"],
                "ladderCandidateLimit": manifest["ladderCandidateLimit"],
                "ladderAuthority": manifest["ladderAuthority"],
            }
        )
    manifest["manifestSha256"] = _freeze_manifest_sha256(manifest)
    encoded = json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False) + "\n"
    _write_current_v5_compact_manifest(
        manifest_path,
        encoded=encoded,
        name="native v5 campaign-freeze manifest",
    )
    try:
        completed = native._run_checked(
            [str(binary), "--manifest", manifest_path],
            cwd=Path.cwd(),
            raise_on_nonzero=False,
            timeout=execution_timeout_seconds,
            env={**os.environ, "TEMPORAL_QD_CAMPAIGN_FREEZE_REPORT_PEAK": "1"},
            stdout_limit_bytes=_CURRENT_V5_COMPACT_STDOUT_LIMIT_BYTES,
            stderr_limit_bytes=_CURRENT_V5_COMPACT_STDERR_LIMIT_BYTES,
        )
    except (OSError, native.TemporalQDNativeError) as exc:
        raise TemporalDiscoveryContractError(
            f"Rust-native v5 campaign freeze subprocess failed: {exc}"
        ) from exc
    if completed.returncode != 0:
        raise TemporalDiscoveryContractError(
            "Rust-native v5 campaign freeze failed: "
            + completed.stderr.decode("utf-8", errors="replace").strip()[-4000:]
        )
    try:
        result = json.loads(completed.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError("Rust-native v5 campaign freeze emitted invalid JSON") from exc
    expected_schema = (
        NATIVE_V5_LADDER_FREEZE_RESULT_SCHEMA
        if ladder
        else NATIVE_V5_CAMPAIGN_INPUT_RESULT_SCHEMA
    )
    if not isinstance(result, dict) or result.get("schemaVersion") != expected_schema:
        raise TemporalDiscoveryContractError("Rust-native v5 campaign-freeze result schema drifted")
    if not _native_transport_path_matches(result.get("outputRoot"), root):
        raise TemporalDiscoveryContractError("Rust-native v5 campaign-freeze output binding drifted")
    _validate_v5_campaign_input_checkpoint(root, manifest, result)
    if raw_file_sha256(binary) != binary_sha256:
        raise TemporalDiscoveryContractError(
            "Rust-native v5 campaign-freeze binary changed during execution"
        )
    for line in reversed(
        completed.stderr.decode("utf-8", errors="replace").splitlines()
    ):
        try:
            sample = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(sample, dict) and isinstance(sample.get("peakWorkingSetBytes"), int):
            result.setdefault("telemetry", {})["processPeakWorkingSetBytes"] = sample["peakWorkingSetBytes"]
            break
    return result


def freeze_qd_v5_evidence_ladder_archive_native(
    *,
    archive_authority: Mapping[str, Any],
    template_preparation_path: Path | str,
    template_preparation_sha256: str,
    construction_catalog_path: Path | str,
    construction_catalog_sha256: str,
    output_root: Path | str,
    execution_engine_commit: str,
    worker_contract_sha256: str,
    rotating_evidence: Mapping[str, Any],
    archive_policy_authority: Mapping[str, Any],
    behavior_attribution_requirement: Mapping[str, Any],
    campaign_role: str,
    panel_id: str,
    ladder_stage: str,
    ladder_candidate_limit: int,
    ladder_authority: Mapping[str, Any],
    native_binary: Path | str | None = None,
    execution_timeout_seconds: int = 300,
) -> dict[str, Any]:
    """Execute the archive-native v3 ladder freezer without Python cohorts.

    This intentionally has no evaluation-population or cohort-selection
    argument.  Rust reopens the receipt-bound archive authority, derives rich
    candidates, and materializes its own compact cohort sidecars.  Python
    only carries bounded control documents into the pinned subprocess.
    """

    authority = dict(archive_authority)
    if set(authority) != {"kind", "receiptPath", "receiptSha256"}:
        raise TemporalDiscoveryContractError(
            "native v5 archive ladder authority schema is invalid"
        )
    if authority.get("kind") not in {
        "generation_finalizer_commit",
        "qd_archive_reducer_result",
    } or not isinstance(authority.get("receiptPath"), str) or not isinstance(
        authority.get("receiptSha256"), str
    ):
        raise TemporalDiscoveryContractError(
            "native v5 archive ladder authority is invalid"
        )
    if ladder_stage not in {"validation", "scrutiny"}:
        raise TemporalDiscoveryContractError("native v5 archive ladder stage is invalid")
    if (
        isinstance(ladder_candidate_limit, bool)
        or not isinstance(ladder_candidate_limit, int)
        or ladder_candidate_limit < 1
    ):
        raise TemporalDiscoveryContractError(
            "native v5 archive ladder candidate limit is invalid"
        )
    if (
        not isinstance(execution_timeout_seconds, int)
        or isinstance(execution_timeout_seconds, bool)
        or execution_timeout_seconds < 1
    ):
        raise TemporalDiscoveryContractError(
            "Rust-native v5 archive ladder timeout must be a positive integer"
        )
    if native_binary is None:
        raise TemporalDiscoveryContractError(
            "Rust-native v5 archive ladder requires an explicitly pinned binary"
        )
    binary = _require_regular_file(
        Path(native_binary).absolute(), name="Rust-native v5 campaign-freeze binary"
    )
    template = Path(template_preparation_path).resolve()
    catalog = Path(construction_catalog_path).resolve()
    root = Path(output_root).resolve()
    for path, name in (
        (template, "template"),
        (catalog, "construction catalog"),
    ):
        if not path.is_file():
            raise TemporalDiscoveryContractError(
                f"native v5 archive ladder {name} is not a file"
            )
    root.mkdir(parents=True, exist_ok=True)
    def sealed_identity(value: object, *, name: str) -> str:
        if (
            not isinstance(value, str)
            or len(value) != 71
            or not value.startswith("sha256:")
            or any(character not in "0123456789abcdef" for character in value[7:])
        ):
            raise TemporalDiscoveryContractError(
                f"native v5 archive ladder {name} identity is invalid"
            )
        return value

    # The frozen ladder authority already binds these opaque inputs.  Rust
    # authenticates their bytes; Python must not reopen/hash the preparation
    # or catalog on a current-v5 ladder route.
    template_sha256 = sealed_identity(
        template_preparation_sha256, name="template preparation"
    )
    catalog_sha256 = sealed_identity(
        construction_catalog_sha256, name="construction catalog"
    )
    binary_sha256 = raw_file_sha256(binary)
    runtime_authority = {
        "schemaVersion": "temporal_qd_native_campaign_freeze_runtime_authority_v1",
        "runtimeEpoch": "temporal_qd_native_campaign_freeze_epoch_v2",
        "binaryRole": _BINARY_NAME,
        "binarySha256": binary_sha256,
    }
    manifest = {
        "schemaVersion": NATIVE_V5_LADDER_ARCHIVE_FREEZE_MANIFEST_SCHEMA,
        "archiveAuthority": authority,
        "ladderStage": ladder_stage,
        "ladderCandidateLimit": ladder_candidate_limit,
        "ladderAuthority": dict(ladder_authority),
        "templatePreparationPath": str(template),
        "templatePreparationSha256": template_sha256,
        "constructionCatalogPath": str(catalog),
        "constructionCatalogSha256": catalog_sha256,
        "outputRoot": str(root),
        "executionEngineCommit": execution_engine_commit,
        "workerContractSha256": worker_contract_sha256,
        "campaignRole": campaign_role,
        "panelId": panel_id,
        "rotatingEvidence": dict(rotating_evidence),
        "archivePolicyAuthority": dict(archive_policy_authority),
        "behaviorAttributionRequirement": dict(behavior_attribution_requirement),
        "nativeRuntimeAuthority": runtime_authority,
        "nativeRuntimeAuthoritySha256": canonical_sha256(runtime_authority),
    }
    manifest["manifestSha256"] = _v3_ladder_archive_freeze_manifest_sha256(manifest)
    manifest_path = root / ".native-v5-evidence-ladder-freeze-manifest.json"
    encoded = (
        json.dumps(
            manifest, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False
        )
        + "\n"
    )
    _write_current_v5_compact_manifest(
        manifest_path,
        encoded=encoded,
        name="native v5 archive ladder manifest",
    )
    try:
        completed = native._run_checked(
            [str(binary), "--manifest", str(manifest_path)],
            cwd=Path.cwd(),
            raise_on_nonzero=False,
            timeout=execution_timeout_seconds,
            env={**os.environ, "TEMPORAL_QD_CAMPAIGN_FREEZE_REPORT_PEAK": "1"},
            stdout_limit_bytes=_CURRENT_V5_COMPACT_STDOUT_LIMIT_BYTES,
            stderr_limit_bytes=_CURRENT_V5_COMPACT_STDERR_LIMIT_BYTES,
        )
    except (OSError, native.TemporalQDNativeError) as exc:
        raise TemporalDiscoveryContractError(
            f"Rust-native v5 archive ladder freeze subprocess failed: {exc}"
        ) from exc
    if completed.returncode != 0:
        raise TemporalDiscoveryContractError(
            "Rust-native v5 archive ladder freeze failed: "
            + completed.stderr.decode("utf-8", errors="replace").strip()[-4000:]
        )
    try:
        result = json.loads(completed.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(
            "Rust-native v5 archive ladder freeze emitted invalid JSON"
        ) from exc
    expected_result = {
        "schemaVersion",
        "manifestSha256",
        "archiveAuthorityKind",
        "archiveAuthorityReceiptSha256",
        "archiveSha256",
        "ladderStage",
        "ladderCandidateLimit",
        "selectionSha256",
        "campaignSha256",
        "authorityId",
        "taskMatrixSha256",
    }
    if (
        not isinstance(result, dict)
        or set(result) != expected_result
        or result.get("schemaVersion") != NATIVE_V5_LADDER_ARCHIVE_FREEZE_RESULT_SCHEMA
        or result.get("manifestSha256") != manifest["manifestSha256"]
        or result.get("archiveAuthorityKind") != authority["kind"]
        or result.get("archiveAuthorityReceiptSha256") != authority["receiptSha256"]
        or result.get("ladderStage") != ladder_stage
        or result.get("ladderCandidateLimit") != ladder_candidate_limit
    ):
        raise TemporalDiscoveryContractError(
            "Rust-native v5 archive ladder freeze result schema drifted"
        )
    if raw_file_sha256(binary) != binary_sha256:
        raise TemporalDiscoveryContractError(
            "Rust-native v5 archive ladder freezer binary changed during execution"
        )
    return {
        "result": result,
        "outputRoot": str(root),
        "manifest": manifest,
        "manifestPath": str(manifest_path),
    }


def freeze_qd_v5_campaign_oracle(
    *,
    population_path: Path | str,
    template_preparation_path: Path | str,
    output_root: Path | str,
    execution_engine_commit: str,
    worker_contract_sha256: str,
    construction_catalog_path: Path | str,
    rotating_evidence: Mapping[str, Any],
    archive_policy_authority: Mapping[str, Any],
    behavior_attribution_requirement: Mapping[str, Any],
    campaign_role: str,
    panel_id: str,
) -> dict[str, Any]:
    """Explicit test-only Python oracle for v5 native-freeze parity.

    Production callers must use :func:`freeze_qd_v5_campaign_native`; this
    named helper intentionally retains the historical Python O(N×W) path only
    for byte/semantic parity assertions.
    """

    from .temporal_qd_campaign import freeze_qd_screening_campaign

    return freeze_qd_screening_campaign(
        population_path=population_path,
        template_preparation_path=template_preparation_path,
        output_root=output_root,
        execution_engine_commit=execution_engine_commit,
        worker_contract_sha256=worker_contract_sha256,
        construction_catalog_path=construction_catalog_path,
        rotating_evidence=rotating_evidence,
        archive_policy_authority=archive_policy_authority,
        behavior_attribution_requirement=behavior_attribution_requirement,
        campaign_role=campaign_role,
        panel_id=panel_id,
    )


__all__ = [
    "materialize_qd_campaign_task_matrix_native",
    "freeze_qd_v5_campaign_native",
    "freeze_qd_v5_evidence_ladder_archive_native",
    "freeze_qd_v5_campaign_oracle",
]
